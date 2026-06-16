use anyhow::Result;
use kafka_protocol::messages::fetch_request::FetchPartition;
use kafka_protocol::messages::fetch_response::{
    AbortedTransaction, FetchableTopicResponse, PartitionData,
};
use kafka_protocol::messages::{BrokerId, FetchRequest, FetchResponse, TopicName};
use kafka_protocol::protocol::StrBytes;
use tokio::sync::watch;
use tokio::task::JoinSet;
use tokio::time::{Duration, Instant};

use crate::broker::handlers::error_codes::{NOT_LEADER_OR_FOLLOWER, UNKNOWN_TOPIC_OR_PARTITION};
use crate::store::StoreError;

use super::super::super::KafkaBroker;
use super::records::encode_records;

pub async fn handle_fetch(broker: &KafkaBroker, request: FetchRequest) -> Result<FetchResponse> {
    let min_bytes = request.min_bytes.max(0) as usize;
    let max_wait_ms = request.max_wait_ms.max(0) as u64;
    // Without a byte threshold, wait budget, or response byte budget, this degenerates to a
    // single immediate read.
    if min_bytes == 0 || max_wait_ms == 0 || request.max_bytes <= 0 {
        return build_fetch_response(broker, &request).map(|read| read.response);
    }

    let deadline = Instant::now() + Duration::from_millis(max_wait_ms);
    loop {
        // Subscribe before reading so a concurrent append can wake this fetch instead of racing
        // past it between the read and the wait.
        let receivers = subscribe_requested_partitions(broker, &request);
        let read = build_fetch_response(broker, &request)?;
        // Return partition errors immediately, even for mixed requests with other empty
        // partitions, so clients do not wait behind protocol/metadata errors.
        if read.has_error
            || read.visible_bytes >= min_bytes
            || receivers.is_empty()
            || Instant::now() >= deadline
        {
            return Ok(read.response);
        }
        wait_for_partition_change(receivers, deadline).await;
    }
}

struct FetchRead {
    response: FetchResponse,
    visible_bytes: usize,
    has_error: bool,
}

fn build_fetch_response(broker: &KafkaBroker, request: &FetchRequest) -> Result<FetchRead> {
    let mut visible_bytes = 0;
    let mut has_error = false;
    // Track the remaining response budget across partitions in this fetch request.
    // This is still approximate: storage enforces byte limits using encoded on-disk batches,
    // while the broker may later re-encode only the visible records for the response.
    let mut remaining_bytes = request.max_bytes.max(0) as usize;
    let mut responses = Vec::new();
    // Build the Kafka response topic-by-topic while collecting long-poll termination signals.
    for topic in &request.topics {
        let mut partitions = Vec::new();
        let topic_name = topic.topic.to_string();
        for partition in &topic.partitions {
            let max_partition_bytes = partition_fetch_bytes(partition, remaining_bytes);
            let allow_first_batch_overflow = visible_bytes == 0;
            let (partition_data, partition_bytes) = fetch_partition_data(
                broker,
                &topic_name,
                partition,
                request.isolation_level,
                max_partition_bytes,
                allow_first_batch_overflow,
            )?;
            has_error |= partition_data.error_code != 0;
            visible_bytes += partition_bytes;
            remaining_bytes = remaining_bytes.saturating_sub(partition_bytes);
            partitions.push(partition_data);
        }
        responses.push(
            FetchableTopicResponse::default()
                .with_topic(TopicName(StrBytes::from(topic_name)))
                .with_partitions(partitions),
        );
    }
    Ok(FetchRead {
        visible_bytes,
        has_error,
        response: FetchResponse::default()
            .with_throttle_time_ms(0)
            .with_error_code(0)
            .with_session_id(0)
            .with_responses(responses),
    })
}

fn fetch_partition_data(
    broker: &KafkaBroker,
    topic_name: &str,
    partition: &FetchPartition,
    isolation_level: i8,
    max_bytes: usize,
    allow_first_batch_overflow: bool,
) -> Result<(PartitionData, usize)> {
    // Client fetches are served only by the leader replica for the requested partition.
    if !broker.is_local_partition_leader(topic_name, partition.partition) {
        return Ok((
            error_partition_data(partition.partition, NOT_LEADER_OR_FOLLOWER),
            0,
        ));
    }
    // Read a bounded slice from storage and translate storage misses into Kafka protocol errors.
    let fetched = match broker.store().fetch_records_for_client(
        topic_name,
        partition.partition,
        partition.fetch_offset,
        max_bytes,
    ) {
        Ok(fetched) => fetched,
        Err(StoreError::UnknownTopicOrPartition { .. }) => {
            return Ok((
                error_partition_data(partition.partition, UNKNOWN_TOPIC_OR_PARTITION),
                0,
            ));
        }
        Err(err) => return Err(err.into()),
    };
    // When replication progress exists, expose only data below the committed watermark.
    let high_watermark = broker
        .partition_high_watermark(topic_name, partition.partition)
        .unwrap_or(fetched.high_watermark);
    let log_start_offset = broker
        .store()
        .list_offsets(topic_name, partition.partition)
        .map(|(earliest, _)| earliest.offset)
        .unwrap_or(0);
    let transactional_view = visible_records(
        broker,
        topic_name,
        partition.partition,
        fetched,
        high_watermark,
        isolation_level,
    );
    let leader_epoch = broker
        .partition_leader_epoch(topic_name, partition.partition)
        .unwrap_or(0);
    let records = encode_records(&transactional_view.records, leader_epoch)?;
    let records_len = records.len();
    if !allow_first_batch_overflow && records_len > max_bytes {
        return Ok((
            success_partition_data(
                partition.partition,
                high_watermark,
                transactional_view.last_stable_offset,
                log_start_offset,
                transactional_view.aborted_transactions.clone(),
                bytes::Bytes::new(),
            ),
            0,
        ));
    }
    Ok((
        success_partition_data(
            partition.partition,
            high_watermark,
            transactional_view.last_stable_offset,
            log_start_offset,
            transactional_view.aborted_transactions,
            records,
        ),
        records_len,
    ))
}

fn partition_fetch_bytes(partition: &FetchPartition, remaining_bytes: usize) -> usize {
    // Respect both the partition-local request cap and the remaining request-wide byte budget.
    if partition.partition_max_bytes > 0 {
        (partition.partition_max_bytes as usize).min(remaining_bytes)
    } else {
        remaining_bytes
    }
}

fn visible_records(
    broker: &KafkaBroker,
    topic_name: &str,
    partition: i32,
    fetched: crate::store::FetchResult,
    high_watermark: i64,
    isolation_level: i8,
) -> TransactionalFetchView {
    let has_replica_progress = broker.partition_has_replica_progress(topic_name, partition);
    let use_local_tail =
        high_watermark == 0 && !fetched.records.is_empty() && !has_replica_progress;
    let visible = if use_local_tail {
        fetched.records
    } else {
        fetched
            .records
            .into_iter()
            .filter(|record| record.offset < high_watermark)
            .collect::<Vec<_>>()
    };
    let stable_boundary = if use_local_tail {
        visible
            .last()
            .map(|record| record.offset + 1)
            .unwrap_or(high_watermark)
    } else {
        high_watermark
    };

    if isolation_level != 1 {
        return TransactionalFetchView {
            records: visible
                .into_iter()
                .filter(|record| !record.control)
                .collect(),
            last_stable_offset: stable_boundary,
            aborted_transactions: None,
        };
    }

    read_committed_view(visible, stable_boundary)
}

fn error_partition_data(partition: i32, error_code: i16) -> PartitionData {
    PartitionData::default()
        .with_partition_index(partition)
        .with_error_code(error_code)
        .with_high_watermark(-1)
        .with_last_stable_offset(-1)
        .with_log_start_offset(-1)
        .with_aborted_transactions(None)
        .with_preferred_read_replica(BrokerId(-1))
        .with_records(None)
}

fn success_partition_data(
    partition: i32,
    high_watermark: i64,
    last_stable_offset: i64,
    log_start_offset: i64,
    aborted_transactions: Option<Vec<AbortedTransaction>>,
    records: bytes::Bytes,
) -> PartitionData {
    PartitionData::default()
        .with_partition_index(partition)
        .with_error_code(0)
        .with_high_watermark(high_watermark)
        .with_last_stable_offset(last_stable_offset)
        .with_log_start_offset(log_start_offset)
        .with_aborted_transactions(aborted_transactions)
        .with_preferred_read_replica(BrokerId(-1))
        .with_records(Some(records))
}

struct TransactionalFetchView {
    records: Vec<crate::store::BrokerRecord>,
    last_stable_offset: i64,
    aborted_transactions: Option<Vec<AbortedTransaction>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TransactionMarker {
    Commit,
    Abort,
    Unknown,
}

fn read_committed_view(
    records: Vec<crate::store::BrokerRecord>,
    high_watermark: i64,
) -> TransactionalFetchView {
    let mut visible = Vec::new();
    let mut pending: std::collections::BTreeMap<i64, Vec<crate::store::BrokerRecord>> =
        std::collections::BTreeMap::new();
    let mut first_offsets = std::collections::BTreeMap::new();
    let mut aborted_transactions = Vec::new();

    for record in records {
        if record.control {
            let marker = transaction_marker(record.key.as_deref());
            let resolved_producer = resolve_control_producer(record.producer_id, &pending);

            if let Some(producer_id) = resolved_producer {
                match marker {
                    TransactionMarker::Commit => {
                        if let Some(mut committed) = pending.remove(&producer_id) {
                            visible.append(&mut committed);
                        }
                        first_offsets.remove(&producer_id);
                    }
                    TransactionMarker::Abort => {
                        let first_offset = first_offsets.remove(&producer_id);
                        pending.remove(&producer_id);
                        if let Some(first_offset) = first_offset {
                            aborted_transactions.push(
                                AbortedTransaction::default()
                                    .with_producer_id(kafka_protocol::messages::ProducerId(
                                        producer_id,
                                    ))
                                    .with_first_offset(first_offset),
                            );
                        }
                    }
                    TransactionMarker::Unknown => {
                        if let Some(mut committed) = pending.remove(&producer_id) {
                            visible.append(&mut committed);
                        }
                        first_offsets.remove(&producer_id);
                    }
                }
            }
            continue;
        }
        if !record.transactional {
            visible.push(record);
            continue;
        }
        first_offsets
            .entry(record.producer_id)
            .or_insert(record.offset);
        pending.entry(record.producer_id).or_default().push(record);
    }

    let last_stable_offset = first_offsets
        .values()
        .min()
        .copied()
        .unwrap_or(high_watermark);

    TransactionalFetchView {
        records: visible,
        last_stable_offset,
        aborted_transactions: (!aborted_transactions.is_empty()).then_some(aborted_transactions),
    }
}

fn transaction_marker(key: Option<&[u8]>) -> TransactionMarker {
    let Some(key) = key else {
        return TransactionMarker::Unknown;
    };
    if key.len() < 4 {
        return TransactionMarker::Unknown;
    }
    let marker_type = i16::from_be_bytes([key[2], key[3]]);
    match marker_type {
        0 => TransactionMarker::Abort,
        1 => TransactionMarker::Commit,
        _ => TransactionMarker::Unknown,
    }
}

fn resolve_control_producer(
    producer_id: i64,
    pending: &std::collections::BTreeMap<i64, Vec<crate::store::BrokerRecord>>,
) -> Option<i64> {
    if pending.contains_key(&producer_id) {
        return Some(producer_id);
    }
    if pending.len() == 1 {
        return pending.keys().next().copied();
    }
    None
}

fn subscribe_requested_partitions(
    broker: &KafkaBroker,
    request: &FetchRequest,
) -> Vec<watch::Receiver<u64>> {
    let mut receivers = Vec::new();
    // Only leader partitions can wake the fetch, because follower fetches return errors.
    for topic in &request.topics {
        let topic_name = topic.topic.to_string();
        for partition in &topic.partitions {
            if broker.is_local_partition_leader(&topic_name, partition.partition)
                && broker
                    .store()
                    .list_offsets(&topic_name, partition.partition)
                    .is_ok()
            {
                receivers.push(broker.subscribe_fetch_signal(&topic_name, partition.partition));
            }
        }
    }
    receivers
}

async fn wait_for_partition_change(receivers: Vec<watch::Receiver<u64>>, deadline: Instant) {
    // This intentionally favors a small implementation over lower per-fetch overhead.
    // Large multi-partition fetches can replace this with select_all/FuturesUnordered later.
    let mut waiters = JoinSet::new();
    // Any subscribed partition change is enough to re-run the read path before the deadline.
    for mut receiver in receivers {
        waiters.spawn(async move {
            let _ = receiver.changed().await;
        });
    }
    tokio::select! {
        _ = tokio::time::sleep_until(deadline) => {}
        _ = waiters.join_next() => {}
    }
    waiters.abort_all();
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    fn broker_record(
        offset: i64,
        producer_id: i64,
        transactional: bool,
        control: bool,
        key: Option<Bytes>,
        value: Option<Bytes>,
    ) -> crate::store::BrokerRecord {
        crate::store::BrokerRecord {
            offset,
            timestamp_ms: 100 + offset,
            producer_id,
            producer_epoch: 0,
            sequence: offset as i32,
            partition_leader_epoch: 0,
            transactional,
            control,
            key,
            value,
            headers_json: b"[]".to_vec(),
        }
    }

    fn transaction_marker_key(marker_type: i16) -> Bytes {
        let mut key = Vec::with_capacity(4);
        key.extend_from_slice(&0_i16.to_be_bytes());
        key.extend_from_slice(&marker_type.to_be_bytes());
        Bytes::from(key)
    }

    #[test]
    fn read_committed_emits_records_after_commit_marker() {
        let view = read_committed_view(
            vec![
                broker_record(
                    0,
                    42,
                    true,
                    false,
                    None,
                    Some(Bytes::from_static(b"committed")),
                ),
                broker_record(1, 42, true, true, Some(transaction_marker_key(1)), None),
            ],
            2,
        );

        assert_eq!(view.records.len(), 1);
        assert_eq!(view.records[0].value.as_deref(), Some(&b"committed"[..]));
        assert_eq!(view.last_stable_offset, 2);
        assert!(view.aborted_transactions.is_none());
    }

    #[test]
    fn read_committed_reports_aborted_transactions() {
        let view = read_committed_view(
            vec![
                broker_record(
                    0,
                    99,
                    true,
                    false,
                    None,
                    Some(Bytes::from_static(b"aborted")),
                ),
                broker_record(1, 99, true, true, Some(transaction_marker_key(0)), None),
            ],
            2,
        );

        assert!(view.records.is_empty());
        assert_eq!(view.last_stable_offset, 2);
        let aborted = view.aborted_transactions.expect("aborted transactions");
        assert_eq!(aborted.len(), 1);
        assert_eq!(aborted[0].producer_id.0, 99);
        assert_eq!(aborted[0].first_offset, 0);
    }
}
