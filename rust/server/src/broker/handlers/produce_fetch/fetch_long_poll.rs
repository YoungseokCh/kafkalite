use anyhow::Result;
use kafka_protocol::messages::fetch_request::FetchPartition;
use kafka_protocol::messages::fetch_response::{FetchableTopicResponse, PartitionData};
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
    let visible_records = visible_records(
        broker,
        topic_name,
        partition.partition,
        fetched,
        high_watermark,
    );
    let records = encode_records(&visible_records)?;
    let records_len = records.len();
    if !allow_first_batch_overflow && records_len > max_bytes {
        return Ok((
            success_partition_data(partition.partition, high_watermark, bytes::Bytes::new()),
            0,
        ));
    }
    Ok((
        success_partition_data(partition.partition, high_watermark, records),
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
) -> Vec<crate::store::BrokerRecord> {
    // Single-node partitions without replica state can expose their local tail immediately.
    if high_watermark == 0
        && !fetched.records.is_empty()
        && !broker.partition_has_replica_progress(topic_name, partition)
    {
        return fetched.records;
    }
    fetched
        .records
        .into_iter()
        .filter(|record| record.offset < high_watermark)
        .collect()
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
    records: bytes::Bytes,
) -> PartitionData {
    PartitionData::default()
        .with_partition_index(partition)
        .with_error_code(0)
        .with_high_watermark(high_watermark)
        .with_last_stable_offset(high_watermark)
        .with_log_start_offset(0)
        .with_aborted_transactions(None)
        .with_preferred_read_replica(BrokerId(-1))
        .with_records(Some(records))
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
            if broker.is_local_partition_leader(&topic_name, partition.partition) {
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
