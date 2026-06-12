use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use kafka_protocol::indexmap::IndexMap;
use kafka_protocol::messages::produce_request::{PartitionProduceData, TopicProduceData};
use kafka_protocol::messages::{FetchRequest, ListOffsetsRequest, ProduceRequest, TopicName};
use kafka_protocol::records::{
    Compression, Record, RecordBatchEncoder, RecordEncodeOptions, TimestampType,
};
use tempfile::tempdir;

use crate::cluster::{ControllerQuorumVoter, ProcessRole};
use crate::config::Config;
use crate::store::{BrokerRecord, FileStore};
use kafka_protocol::protocol::StrBytes;

use super::*;

#[path = "produce_fetch_tests/produce_tests.rs"]
mod produce_tests;

#[path = "produce_fetch_tests/leadership_tests.rs"]
mod leadership_tests;

#[path = "produce_fetch_tests/record_codec_tests.rs"]
mod record_codec_tests;

#[path = "produce_fetch_tests/fetch_shape_tests.rs"]
mod fetch_shape_tests;

#[path = "produce_fetch_tests/fetch_watermark_tests.rs"]
mod fetch_watermark_tests;

#[path = "produce_fetch_tests/auto_create_tests.rs"]
mod auto_create_tests;

#[path = "produce_fetch_tests/fetch_mixed_tests.rs"]
mod fetch_mixed_tests;

#[path = "produce_fetch_tests/fetch_timestamp_tests.rs"]
mod fetch_timestamp_tests;

#[path = "produce_fetch_tests/fetch_offset_tests.rs"]
mod fetch_offset_tests;

#[path = "produce_fetch_tests/fetch_long_poll_tests.rs"]
mod fetch_long_poll_tests;

fn test_broker() -> KafkaBroker {
    let dir = tempdir().unwrap().keep();
    let config = Config::single_node(dir.join("data"), 9092, 1);
    let store = Arc::new(FileStore::open(&config.storage.data_dir).unwrap());
    KafkaBroker::new(config, store).unwrap()
}

fn produce_request(
    topic: &str,
    producer_id: i64,
    producer_epoch: i16,
    sequence: i32,
) -> ProduceRequest {
    produce_request_for_partition(topic, 0, producer_id, producer_epoch, sequence)
}

fn produce_request_for_partition(
    topic: &str,
    partition: i32,
    producer_id: i64,
    producer_epoch: i16,
    sequence: i32,
) -> ProduceRequest {
    let records = vec![Record {
        transactional: false,
        control: false,
        partition_leader_epoch: 0,
        producer_id,
        producer_epoch,
        timestamp_type: TimestampType::Creation,
        offset: 0,
        sequence,
        timestamp: 100,
        key: Some(Bytes::from_static(b"key")),
        value: Some(Bytes::from_static(b"value")),
        headers: Default::default(),
    }];
    let mut encoded = BytesMut::new();
    RecordBatchEncoder::encode(
        &mut encoded,
        &records,
        &RecordEncodeOptions {
            version: 2,
            compression: Compression::None,
        },
    )
    .unwrap();
    ProduceRequest::default()
        .with_acks(1)
        .with_timeout_ms(5_000)
        .with_topic_data(vec![
            TopicProduceData::default()
                .with_name(TopicName(StrBytes::from(topic.to_string())))
                .with_partition_data(vec![
                    PartitionProduceData::default()
                        .with_index(partition)
                        .with_records(Some(encoded.freeze())),
                ]),
        ])
}
