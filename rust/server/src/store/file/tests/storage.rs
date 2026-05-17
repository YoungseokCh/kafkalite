use super::*;

#[test]
fn describe_storage_counts_root_level_topic_files() {
    let dir = tempdir().unwrap();
    let store = FileStore::open(dir.path()).unwrap();
    let extra = dir.path().join("topics/stray.bin");
    std::fs::write(&extra, b"topic-root-bytes").unwrap();

    let summary = store.describe_storage().unwrap();

    assert_eq!(summary.log_bytes, 0);
    assert_eq!(summary.index_bytes, 0);
    assert_eq!(summary.timeindex_bytes, 0);
    assert_eq!(summary.state_bytes, 0);
    assert_eq!(summary.total_bytes, b"topic-root-bytes".len() as u64);
}

#[test]
fn describe_storage_tolerates_missing_state_directory() {
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

    std::fs::remove_dir_all(dir.path().join("state")).unwrap();

    let summary = store.describe_storage().unwrap();

    assert_eq!(summary.state_bytes, 0);
    assert!(summary.log_bytes > 0);
    assert!(summary.total_bytes >= summary.log_bytes);
}
