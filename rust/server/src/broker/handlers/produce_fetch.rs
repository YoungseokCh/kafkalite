use anyhow::Result;
use kafka_protocol::messages::fetch_response::{FetchableTopicResponse, PartitionData};
use kafka_protocol::messages::list_offsets_response::{
    ListOffsetsPartitionResponse, ListOffsetsTopicResponse,
};
use kafka_protocol::messages::produce_response::{PartitionProduceResponse, TopicProduceResponse};
use kafka_protocol::messages::{
    BrokerId, FetchRequest, FetchResponse, ListOffsetsRequest, ListOffsetsResponse, ProduceRequest,
    ProduceResponse, TopicName,
};
use kafka_protocol::protocol::StrBytes;
use kafka_protocol::records::RecordBatchDecoder;

use self::auto_create::maybe_auto_create_topic;
use self::records::{encode_records, to_broker_record};
use crate::store::StoreError;

use super::super::KafkaBroker;

mod auto_create;
mod records;

const UNKNOWN_TOPIC_OR_PARTITION: i16 = 3;
const NOT_LEADER_OR_FOLLOWER: i16 = 6;
const OUT_OF_ORDER_SEQUENCE_NUMBER: i16 = 45;
const INVALID_PRODUCER_EPOCH: i16 = 47;
const UNKNOWN_PRODUCER_ID: i16 = 59;

pub async fn handle_produce(
    broker: &KafkaBroker,
    request: ProduceRequest,
) -> Result<ProduceResponse> {
    let mut topics = Vec::new();
    for topic_data in request.topic_data {
        let topic_name = topic_data.name.to_string();
        let mut partitions = Vec::new();
        for partition_data in topic_data.partition_data {
            let raw_records = partition_data.records.unwrap_or_default();
            let mut record_bytes = raw_records.clone();
            let decoded = RecordBatchDecoder::decode_all(&mut record_bytes)?;
            let flattened = decoded
                .into_iter()
                .flat_map(|set| set.records)
                .map(to_broker_record)
                .collect::<Vec<_>>();
            let now = chrono::Utc::now().timestamp_millis();
            maybe_auto_create_topic(broker, &topic_name, partition_data.index, now)?;
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
            let produce_result =
                broker
                    .store()
                    .append_records(&topic_name, partition_data.index, &flattened, now);
            let (error_code, base_offset) = match produce_result {
                Ok((base_offset, _)) => {
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

pub async fn handle_fetch(broker: &KafkaBroker, request: FetchRequest) -> Result<FetchResponse> {
    let mut responses = Vec::new();
    for topic in request.topics {
        let mut partitions = Vec::new();
        let topic_name = topic.topic.to_string();
        for partition in topic.partitions {
            if !broker.is_local_partition_leader(&topic_name, partition.partition) {
                partitions.push(
                    PartitionData::default()
                        .with_partition_index(partition.partition)
                        .with_error_code(NOT_LEADER_OR_FOLLOWER)
                        .with_high_watermark(-1)
                        .with_last_stable_offset(-1)
                        .with_log_start_offset(-1)
                        .with_aborted_transactions(None)
                        .with_preferred_read_replica(BrokerId(-1))
                        .with_records(None),
                );
                continue;
            }
            match broker.store().fetch_records_for_client(
                &topic_name,
                partition.partition,
                partition.fetch_offset,
                1_000,
            ) {
                Ok(fetched) => {
                    let high_watermark = broker
                        .partition_high_watermark(&topic_name, partition.partition)
                        .filter(|_| !fetched.records.is_empty())
                        .unwrap_or(fetched.high_watermark);
                    let visible_records = if high_watermark == 0
                        && !fetched.records.is_empty()
                        && !broker.partition_has_replica_progress(&topic_name, partition.partition)
                    {
                        fetched.records.clone()
                    } else {
                        fetched
                            .records
                            .into_iter()
                            .filter(|record| record.offset < high_watermark)
                            .collect::<Vec<_>>()
                    };
                    let records = encode_records(&visible_records)?;
                    partitions.push(
                        PartitionData::default()
                            .with_partition_index(partition.partition)
                            .with_error_code(0)
                            .with_high_watermark(high_watermark)
                            .with_last_stable_offset(high_watermark)
                            .with_log_start_offset(0)
                            .with_aborted_transactions(None)
                            .with_preferred_read_replica(BrokerId(-1))
                            .with_records(Some(records)),
                    );
                }
                Err(StoreError::UnknownTopicOrPartition { .. }) => {
                    partitions.push(
                        PartitionData::default()
                            .with_partition_index(partition.partition)
                            .with_error_code(UNKNOWN_TOPIC_OR_PARTITION)
                            .with_high_watermark(-1)
                            .with_last_stable_offset(-1)
                            .with_log_start_offset(-1)
                            .with_aborted_transactions(None)
                            .with_preferred_read_replica(BrokerId(-1))
                            .with_records(None),
                    );
                }
                Err(err) => return Err(err.into()),
            }
        }
        responses.push(
            FetchableTopicResponse::default()
                .with_topic(TopicName(StrBytes::from(topic_name.clone())))
                .with_partitions(partitions),
        );
    }
    Ok(FetchResponse::default()
        .with_throttle_time_ms(0)
        .with_error_code(0)
        .with_session_id(0)
        .with_responses(responses))
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
