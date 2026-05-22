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
    let producer = store.init_producer(10).unwrap();
    let records = vec![BrokerRecord {
        offset: 0,
        timestamp_ms: 10,
        producer_id: producer.producer_id,
        producer_epoch: producer.producer_epoch,
        sequence: 0,
        key: Some(Bytes::from_static(b"key")),
        value: Some(Bytes::from_static(b"value")),
        headers_json: b"[]".to_vec(),
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
