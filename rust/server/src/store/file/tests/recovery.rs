use super::*;

#[test]
fn truncated_tail_is_recovered_on_restart() {
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
        .append_records("recover.events", 0, &records, 10)
        .unwrap();
    std::fs::OpenOptions::new()
        .append(true)
        .open(dir.path().join("recover.events-0/00000000000000000000.log"))
        .unwrap()
        .write_all(b"partial-tail")
        .unwrap();

    let reopened = FileStore::open(dir.path()).unwrap();
    let fetched = reopened.fetch_records("recover.events", 0, 0, 10).unwrap();
    assert_eq!(fetched.records.len(), 1);
    assert_eq!(fetched.records[0].value.as_deref(), Some(&b"value"[..]));
}

#[test]
fn truncated_index_tail_is_rebuilt_on_restart() {
    let dir = tempdir().unwrap();
    let store = FileStore::open(dir.path()).unwrap();
    let producer = store.init_producer(10).unwrap();
    let records = (0..4)
        .map(|sequence| BrokerRecord {
            offset: 0,
            timestamp_ms: 10 + i64::from(sequence),
            producer_id: producer.producer_id,
            producer_epoch: producer.producer_epoch,
            sequence,
            key: Some(Bytes::from_static(b"key")),
            value: Some(Bytes::from_static(b"value")),
            headers_json: b"[]".to_vec(),
        })
        .collect::<Vec<_>>();
    store
        .append_records("recover.index", 0, &records, 10)
        .unwrap();

    std::fs::OpenOptions::new()
        .append(true)
        .open(
            dir.path()
                .join("recover.index-0/00000000000000000000.index"),
        )
        .unwrap()
        .write_all(&[1, 2, 3])
        .unwrap();

    let reopened = FileStore::open(dir.path()).unwrap();
    let fetched = reopened.fetch_records("recover.index", 0, 2, 10).unwrap();

    assert_eq!(fetched.records.len(), 2);
    assert_eq!(fetched.records[0].offset, 2);
    assert_eq!(fetched.records[1].offset, 3);
}

#[test]
fn truncate_partition_discards_tail_and_rebuilds_indexes() {
    let dir = tempdir().unwrap();
    let store = FileStore::open(dir.path()).unwrap();
    let producer = store.init_producer(10).unwrap();
    let records = (0..3)
        .map(|sequence| BrokerRecord {
            offset: 0,
            timestamp_ms: 10 + i64::from(sequence),
            producer_id: producer.producer_id,
            producer_epoch: producer.producer_epoch,
            sequence,
            key: Some(Bytes::from_static(b"key")),
            value: Some(Bytes::from_static(b"value")),
            headers_json: b"[]".to_vec(),
        })
        .collect::<Vec<_>>();
    store
        .append_records("truncate.topic", 0, &records, 10)
        .unwrap();

    store.truncate_partition("truncate.topic", 0, 2).unwrap();
    let fetched = store.fetch_records("truncate.topic", 0, 0, 10).unwrap();

    assert_eq!(fetched.records.len(), 2);
    assert_eq!(fetched.records[0].offset, 0);
    assert_eq!(fetched.records[1].offset, 1);
    assert_eq!(store.list_offsets("truncate.topic", 0).unwrap().1.offset, 2);
}

#[test]
fn opening_valid_kafka_layout_does_not_change_filesystem_bytes() {
    let dir = tempdir().unwrap();
    let store = FileStore::open(dir.path()).unwrap();
    let producer = store.init_producer(10).unwrap();
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
        },
    ];
    store
        .append_records("byte-exact.open", 0, &records, 20)
        .unwrap();
    drop(store);

    let before = filesystem_manifest(dir.path());
    let reopened = FileStore::open(dir.path()).unwrap();
    assert_eq!(
        reopened
            .fetch_records("byte-exact.open", 0, 0, 10)
            .unwrap()
            .records
            .len(),
        2
    );
    drop(reopened);

    assert_eq!(filesystem_manifest(dir.path()), before);
    assert_eq!(root_directories(dir.path()), vec!["byte-exact.open-0"]);
}
