use super::*;

#[test]
fn describe_storage_counts_root_level_topic_files() {
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
        .append_records("storage.bytes", 0, &records, 10)
        .unwrap();

    let summary = store.describe_storage().unwrap();

    assert!(summary.log_bytes > 0);
    assert!(summary.index_bytes > 0);
    assert!(summary.timeindex_bytes > 0);
    assert_eq!(summary.state_bytes, 0);
    assert_eq!(
        summary.total_bytes,
        summary.log_bytes + summary.index_bytes + summary.timeindex_bytes
    );
}

#[test]
fn describe_storage_does_not_create_non_standard_state() {
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
    store
        .append_records("storage.topic", 0, &records, 10)
        .unwrap();

    let summary = store.describe_storage().unwrap();

    assert_eq!(summary.state_bytes, 0);
    assert!(summary.log_bytes > 0);
    assert_eq!(
        summary.total_bytes,
        summary.log_bytes + summary.index_bytes + summary.timeindex_bytes
    );
    assert_eq!(root_directories(dir.path()), vec!["storage.topic-0"]);
}

#[test]
fn list_offset_for_timestamp_uses_timeindex_and_returns_first_matching_record() {
    let dir = tempdir().unwrap();
    let store = FileStore::open(dir.path()).unwrap();
    let records = vec![
        BrokerRecord {
            offset: 0,
            timestamp_ms: 10,
            producer_id: -1,
            producer_epoch: -1,
            sequence: 0,
            key: None,
            value: Some(Bytes::from_static(b"v0")),
            headers_json: b"[]".to_vec(),
            partition_leader_epoch: 0,
            transactional: false,
            control: false,
        },
        BrokerRecord {
            offset: 1,
            timestamp_ms: 20,
            producer_id: -1,
            producer_epoch: -1,
            sequence: 1,
            key: None,
            value: Some(Bytes::from_static(b"v1")),
            headers_json: b"[]".to_vec(),
            partition_leader_epoch: 0,
            transactional: false,
            control: false,
        },
        BrokerRecord {
            offset: 2,
            timestamp_ms: 30,
            producer_id: -1,
            producer_epoch: -1,
            sequence: 2,
            key: None,
            value: Some(Bytes::from_static(b"v2")),
            headers_json: b"[]".to_vec(),
            partition_leader_epoch: 0,
            transactional: false,
            control: false,
        },
    ];
    store
        .append_records("storage.lookup", 0, &records, 30)
        .unwrap();

    let match_at_20 = store
        .list_offset_for_timestamp("storage.lookup", 0, 20)
        .unwrap()
        .unwrap();
    let match_after_20 = store
        .list_offset_for_timestamp("storage.lookup", 0, 21)
        .unwrap()
        .unwrap();
    let missing = store
        .list_offset_for_timestamp("storage.lookup", 0, 31)
        .unwrap();

    assert_eq!(match_at_20.offset, 1);
    assert_eq!(match_at_20.timestamp_ms, 20);
    assert_eq!(match_after_20.offset, 2);
    assert_eq!(match_after_20.timestamp_ms, 30);
    assert_eq!(missing, None);
}
