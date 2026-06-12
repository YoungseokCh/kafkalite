use anyhow::Result;
use kafka_protocol::messages::list_offsets_response::{
    ListOffsetsPartitionResponse, ListOffsetsTopicResponse,
};
use kafka_protocol::messages::produce_response::{PartitionProduceResponse, TopicProduceResponse};
use kafka_protocol::messages::{
    FetchRequest, FetchResponse, ListOffsetsRequest, ListOffsetsResponse, ProduceRequest,
    ProduceResponse, TopicName,
};
use kafka_protocol::protocol::StrBytes;
use kafka_protocol::records::RecordBatchDecoder;

use self::auto_create::maybe_auto_create_topic;
#[cfg(test)]
use self::records::encode_records;
use self::records::to_broker_record;
use super::error_codes::{
    INVALID_PRODUCER_EPOCH, NOT_LEADER_OR_FOLLOWER, OUT_OF_ORDER_SEQUENCE_NUMBER,
    UNKNOWN_PRODUCER_ID, UNKNOWN_TOPIC_OR_PARTITION,
};
use crate::store::StoreError;

use super::super::KafkaBroker;

mod auto_create;
mod fetch_long_poll;
mod records;

pub async fn handle_produce(
    broker: &KafkaBroker,
    request: ProduceRequest,
) -> Result<ProduceResponse> {
    let mut topics = Vec::new();
    for topic_data in request.topic_data {
        let topic_name = topic_data.name.to_string();
        let mut partitions = Vec::new();
        for partition_data in topic_data.partition_data {
            // Decode the Kafka wire batches into the broker's internal record shape.
            let raw_records = partition_data.records.unwrap_or_default();
            let mut record_bytes = raw_records.clone();
            let decoded = RecordBatchDecoder::decode_all(&mut record_bytes)?;
            let flattened = decoded
                .into_iter()
                .flat_map(|set| set.records)
                .map(to_broker_record)
                .collect::<Vec<_>>();
            let now = chrono::Utc::now().timestamp_millis();

            // Auto-create can materialize metadata before we validate leadership or storage state.
            maybe_auto_create_topic(broker, &topic_name, partition_data.index, now)?;

            // Produce requests are only accepted by the current leader for the partition.
            if !broker.is_local_partition_leader(&topic_name, partition_data.index) {
                partitions.push(
                    PartitionProduceResponse::default()
                        .with_index(partition_data.index)
                        .with_error_code(NOT_LEADER_OR_FOLLOWER)
                        .with_base_offset(-1)
                        .with_log_append_time_ms(-1)
                        .with_log_start_offset(0)
                        .with_record_errors(vec![])
                        .with_error_message(None),
                );
                continue;
            }

            // Reject requests for partitions that are not backed by the local store.
            let known_local = broker
                .store()
                .topic_metadata(Some(std::slice::from_ref(&topic_name)), now)
                .map(|topics| {
                    topics.iter().any(|topic| {
                        topic
                            .partitions
                            .iter()
                            .any(|p| p.partition == partition_data.index)
                    })
                })
                .unwrap_or(false);
            if !known_local {
                partitions.push(
                    PartitionProduceResponse::default()
                        .with_index(partition_data.index)
                        .with_error_code(UNKNOWN_TOPIC_OR_PARTITION)
                        .with_base_offset(-1)
                        .with_log_append_time_ms(-1)
                        .with_log_start_offset(0)
                        .with_record_errors(vec![])
                        .with_error_message(None),
                );
                continue;
            }

            // Capture the current tail so we can tell whether append made new data visible.
            let previous_log_end = broker
                .store()
                .list_offsets(&topic_name, partition_data.index)
                .map(|(_, latest)| latest.offset)
                .ok();

            // Append to the local log, then translate storage/leadership outcomes to Kafka errors.
            let produce_result =
                broker
                    .store()
                    .append_records(&topic_name, partition_data.index, &flattened, now);
            let (error_code, base_offset) = match produce_result {
                Ok((base_offset, last_offset)) => {
                    if !broker.is_local_partition_leader(&topic_name, partition_data.index) {
                        let _ = broker.store().truncate_partition(
                            &topic_name,
                            partition_data.index,
                            base_offset,
                        );
                        (NOT_LEADER_OR_FOLLOWER, -1)
                    } else {
                        let _ = broker.update_local_replica_progress(
                            &topic_name,
                            partition_data.index,
                            now,
                        );
                        // Wake long-poll fetch waiters only when this append advanced visible data.
                        if should_notify_fetch_waiters(&flattened, previous_log_end, last_offset) {
                            broker.notify_fetch_signal(&topic_name, partition_data.index);
                        }
                        (0, base_offset)
                    }
                }
                Err(StoreError::UnknownTopicOrPartition { .. }) => (UNKNOWN_TOPIC_OR_PARTITION, -1),
                Err(StoreError::InvalidProducerSequence { .. }) => {
                    (OUT_OF_ORDER_SEQUENCE_NUMBER, -1)
                }
                Err(StoreError::StaleProducerEpoch { .. }) => (INVALID_PRODUCER_EPOCH, -1),
                Err(StoreError::UnknownProducerId { .. }) => (UNKNOWN_PRODUCER_ID, -1),
                Err(err) => return Err(err.into()),
            };
            partitions.push(
                PartitionProduceResponse::default()
                    .with_index(partition_data.index)
                    .with_error_code(error_code)
                    .with_base_offset(base_offset)
                    .with_log_append_time_ms(-1)
                    .with_log_start_offset(0)
                    .with_record_errors(vec![])
                    .with_error_message(None),
            );
        }
        topics.push(
            TopicProduceResponse::default()
                .with_name(TopicName(StrBytes::from(topic_name.clone())))
                .with_partition_responses(partitions),
        );
    }

    Ok(ProduceResponse::default()
        .with_responses(topics)
        .with_throttle_time_ms(0))
}

fn should_notify_fetch_waiters(
    records: &[crate::store::BrokerRecord],
    previous_log_end: Option<i64>,
    last_offset: i64,
) -> bool {
    !records.is_empty() && previous_log_end.is_some_and(|log_end| last_offset >= log_end)
}

pub async fn handle_fetch(broker: &KafkaBroker, request: FetchRequest) -> Result<FetchResponse> {
    fetch_long_poll::handle_fetch(broker, request).await
}

pub async fn handle_list_offsets(
    broker: &KafkaBroker,
    request: ListOffsetsRequest,
    api_version: i16,
) -> Result<ListOffsetsResponse> {
    let mut topics = Vec::new();
    for topic in request.topics {
        let topic_name = topic.name.to_string();
        let mut partitions = Vec::new();
        for partition in topic.partitions {
            if !broker.is_local_partition_leader(&topic_name, partition.partition_index) {
                partitions.push(
                    ListOffsetsPartitionResponse::default()
                        .with_partition_index(partition.partition_index)
                        .with_error_code(NOT_LEADER_OR_FOLLOWER)
                        .with_timestamp(-1)
                        .with_offset(-1)
                        .with_leader_epoch(-1),
                );
                continue;
            }
            match broker
                .store()
                .list_offsets(&topic_name, partition.partition_index)
            {
                Ok((earliest, latest)) => {
                    let result = match partition.timestamp {
                        -2 => earliest,
                        -1 => latest,
                        _ => earliest,
                    };
                    partitions.push(
                        ListOffsetsPartitionResponse::default()
                            .with_partition_index(partition.partition_index)
                            .with_error_code(0)
                            .with_timestamp(result.timestamp_ms)
                            .with_offset(result.offset)
                            .with_leader_epoch(if api_version >= 4 { 0 } else { -1 }),
                    );
                }
                Err(StoreError::UnknownTopicOrPartition { .. }) => {
                    partitions.push(
                        ListOffsetsPartitionResponse::default()
                            .with_partition_index(partition.partition_index)
                            .with_error_code(UNKNOWN_TOPIC_OR_PARTITION)
                            .with_timestamp(-1)
                            .with_offset(-1)
                            .with_leader_epoch(-1),
                    );
                }
                Err(err) => return Err(err.into()),
            }
        }
        topics.push(
            ListOffsetsTopicResponse::default()
                .with_name(TopicName(StrBytes::from(topic_name.clone())))
                .with_partitions(partitions),
        );
    }
    Ok(ListOffsetsResponse::default()
        .with_throttle_time_ms(0)
        .with_topics(topics))
}

#[cfg(test)]
#[path = "produce_fetch/produce_fetch_tests.rs"]
mod produce_fetch_tests;
