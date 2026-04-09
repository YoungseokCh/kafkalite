use anyhow::Result;
use bytes::BytesMut;
use kafka_protocol::messages::fetch_response::{FetchableTopicResponse, PartitionData};
use kafka_protocol::messages::list_offsets_response::{
    ListOffsetsPartitionResponse, ListOffsetsTopicResponse,
};
use kafka_protocol::messages::produce_response::{PartitionProduceResponse, TopicProduceResponse};
use kafka_protocol::messages::{BrokerId, FetchRequest, FetchResponse, ListOffsetsRequest, ListOffsetsResponse, ProduceRequest, ProduceResponse, TopicName};
use kafka_protocol::protocol::StrBytes;
use kafka_protocol::records::{Compression, Record, RecordBatchDecoder, RecordBatchEncoder, RecordEncodeOptions, TimestampType};

use crate::store::BrokerRecord;

use super::super::KafkaBroker;

pub async fn handle_produce(broker: &KafkaBroker, request: ProduceRequest) -> Result<ProduceResponse> {
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
            let (base_offset, _) = broker.store().append_records(&topic_name, &flattened, now)?;
            partitions.push(
                PartitionProduceResponse::default()
                    .with_index(partition_data.index)
                    .with_error_code(0)
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
            let fetched = broker
                .store()
                .fetch_records(&topic_name, partition.fetch_offset, 1_000)?;
            let records = encode_records(&fetched.records)?;
            partitions.push(
                PartitionData::default()
                    .with_partition_index(partition.partition)
                    .with_error_code(0)
                    .with_high_watermark(fetched.high_watermark)
                    .with_last_stable_offset(fetched.high_watermark)
                    .with_log_start_offset(0)
                    .with_aborted_transactions(None)
                    .with_preferred_read_replica(BrokerId(-1))
                    .with_records(Some(records)),
            );
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
) -> Result<ListOffsetsResponse> {
    let mut topics = Vec::new();
    for topic in request.topics {
        let topic_name = topic.name.to_string();
        let (earliest, latest) = broker.store().list_offsets(&topic_name)?;
        let partitions = topic
            .partitions
            .into_iter()
            .map(|partition| {
                let result = match partition.timestamp {
                    -2 => earliest.clone(),
                    -1 => latest.clone(),
                    _ => earliest.clone(),
                };
                ListOffsetsPartitionResponse::default()
                    .with_partition_index(partition.partition_index)
                    .with_error_code(0)
                    .with_timestamp(result.timestamp_ms)
                    .with_offset(result.offset)
                    .with_leader_epoch(0)
            })
            .collect();
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

fn to_broker_record(record: Record) -> BrokerRecord {
    BrokerRecord {
        offset: record.offset,
        timestamp_ms: record.timestamp,
        producer_id: record.producer_id,
        producer_epoch: record.producer_epoch,
        sequence: record.sequence,
        key: record.key,
        value: record.value,
        headers_json: serde_json::to_vec(
            &record
                .headers
                .iter()
                .map(|(key, value)| (key.to_string(), value.clone().map(|bytes| bytes.to_vec())))
                .collect::<Vec<_>>(),
        )
        .unwrap_or_else(|_| b"[]".to_vec()),
    }
}

fn encode_records(records: &[BrokerRecord]) -> Result<bytes::Bytes> {
    let kafka_records = records
        .iter()
        .enumerate()
        .map(|(index, record)| Record {
            transactional: false,
            control: false,
            partition_leader_epoch: 0,
            producer_id: -1,
            producer_epoch: -1,
            timestamp_type: TimestampType::Creation,
            offset: record.offset.max(index as i64),
            sequence: record.sequence,
            timestamp: record.timestamp_ms,
            key: record.key.clone(),
            value: record.value.clone(),
            headers: Default::default(),
        })
        .collect::<Vec<_>>();
    let mut encoded = BytesMut::new();
    RecordBatchEncoder::encode(
        &mut encoded,
        &kafka_records,
        &RecordEncodeOptions {
            version: 2,
            compression: Compression::None,
        },
    )?;
    Ok(encoded.freeze())
}
