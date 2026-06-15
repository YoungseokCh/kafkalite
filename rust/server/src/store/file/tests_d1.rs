use bytes::Bytes;
use tempfile::tempdir;

use super::*;
use crate::store::{BrokerRecord, Storage};

#[test]
fn append_creates_only_kafka_user_partition_directory() {
    let dir = tempdir().unwrap();
    let store = FileStore::open(dir.path()).unwrap();
    let producer = store.init_producer().unwrap();
    let records = vec![BrokerRecord {
        offset: 0,
        timestamp_ms: 10,
        producer_id: producer.producer_id,
        producer_epoch: producer.producer_epoch,
        sequence: 0,
        key: Some(Bytes::from_static(b"key")),
        value: Some(Bytes::from_static(b"value")),
        headers_json: b"[]".to_vec(),
        partition_leader_epoch: 0,
        transactional: false,
        control: false,
    }];

    store.append_records("d1.events", 0, &records, 20).unwrap();

    assert_eq!(root_directories(dir.path()), vec!["d1.events-0"]);
}

#[test]
fn topic_offsets_are_recovered_from_log_after_reopen() {
    let dir = tempdir().unwrap();
    let store = FileStore::open(dir.path()).unwrap();
    let producer = store.init_producer().unwrap();
    let records = vec![
        BrokerRecord {
            offset: 0,
            timestamp_ms: 10,
            producer_id: producer.producer_id,
            producer_epoch: producer.producer_epoch,
            sequence: 0,
            key: Some(Bytes::from_static(b"key")),
            value: Some(Bytes::from_static(b"one")),
            headers_json: b"[]".to_vec(),
            partition_leader_epoch: 0,
            transactional: false,
            control: false,
        },
        BrokerRecord {
            offset: 0,
            timestamp_ms: 20,
            producer_id: producer.producer_id,
            producer_epoch: producer.producer_epoch,
            sequence: 1,
            key: Some(Bytes::from_static(b"key")),
            value: Some(Bytes::from_static(b"two")),
            headers_json: b"[]".to_vec(),
            partition_leader_epoch: 0,
            transactional: false,
            control: false,
        },
    ];
    store
        .append_records("recover.topic", 0, &records, 20)
        .unwrap();

    let reopened = FileStore::open(dir.path()).unwrap();
    let (_, latest) = reopened.list_offsets("recover.topic", 0).unwrap();
    let topic = reopened.describe_topic("recover.topic").unwrap();

    assert_eq!(latest.offset, 2);
    assert_eq!(topic.partition_count, 1);
    assert_eq!(topic.partitions[0].next_offset, 2);
}

#[test]
fn non_idempotent_producer_records_do_not_persist_sequence_state() {
    let dir = tempdir().unwrap();
    let store = FileStore::open(dir.path()).unwrap();
    let records = vec![BrokerRecord {
        offset: 0,
        timestamp_ms: 10,
        producer_id: -1,
        producer_epoch: -1,
        sequence: 0,
        key: Some(Bytes::from_static(b"key")),
        value: Some(Bytes::from_static(b"value")),
        headers_json: b"[]".to_vec(),
        partition_leader_epoch: 0,
        transactional: false,
        control: false,
    }];

    store
        .append_records("non-idempotent-state.events", 0, &records, 20)
        .unwrap();

    let reopened = FileStore::open(dir.path()).unwrap();
    assert!(
        reopened
            .describe_topic("non-idempotent-state.events")
            .is_some()
    );
    assert_eq!(
        root_directories(dir.path()),
        vec!["non-idempotent-state.events-0"]
    );
}
