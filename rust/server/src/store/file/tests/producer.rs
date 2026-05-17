use super::*;

#[path = "recovery.rs"]
mod recovery;

#[test]
fn appends_and_fetches_records() {
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
    let (base, last) = store
        .append_records("test.events", 0, &records, 10)
        .unwrap();
    assert_eq!((base, last), (0, 0));
    let fetched = store.fetch_records("test.events", 0, 0, 10).unwrap();
    assert_eq!(fetched.high_watermark, 1);
    assert_eq!(fetched.records.len(), 1);
}

#[test]
fn fetch_from_later_offset_uses_index_and_returns_tail_records() {
    let dir = tempdir().unwrap();
    let store = FileStore::open(dir.path()).unwrap();
    let producer = store.init_producer(10).unwrap();
    let records = (0..5)
        .map(|sequence| BrokerRecord {
            offset: 0,
            timestamp_ms: 10 + i64::from(sequence),
            producer_id: producer.producer_id,
            producer_epoch: producer.producer_epoch,
            sequence,
            key: Some(Bytes::from_static(b"key")),
            value: Some(Bytes::from(vec![b'a' + sequence as u8])),
            headers_json: b"[]".to_vec(),
        })
        .collect::<Vec<_>>();
    store
        .append_records("tail.events", 0, &records, 10)
        .unwrap();

    let fetched = store.fetch_records("tail.events", 0, 3, 10).unwrap();
    assert_eq!(fetched.records.len(), 2);
    assert_eq!(fetched.records[0].offset, 3);
    assert_eq!(fetched.records[1].offset, 4);
}

#[test]
fn duplicate_producer_retry_returns_original_offsets_without_double_append() {
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

    let first = store
        .append_records("retry.events", 0, &records, 10)
        .unwrap();
    let duplicate = store
        .append_records("retry.events", 0, &records, 20)
        .unwrap();
    let fetched = store.fetch_records("retry.events", 0, 0, 10).unwrap();

    assert_eq!(first, duplicate);
    assert_eq!(fetched.records.len(), 1);
}

#[test]
fn non_idempotent_retries_are_not_deduplicated() {
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

    let first = store
        .append_records("nonidempotent.events", 0, &records, 10)
        .unwrap();
    let second = store
        .append_records("nonidempotent.events", 0, &records, 20)
        .unwrap();
    let fetched = store
        .fetch_records("nonidempotent.events", 0, 0, 10)
        .unwrap();

    assert_eq!(first, (0, 0));
    assert_eq!(second, (1, 1));
    assert_eq!(fetched.records.len(), 2);
}

#[test]
fn non_idempotent_producer_records_append_after_restart() {
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

    let first = store
        .append_records("non-idempotent-restart.events", 0, &records, 10)
        .unwrap();

    let reopened = FileStore::open(dir.path()).unwrap();
    let second = reopened
        .append_records("non-idempotent-restart.events", 0, &records, 20)
        .unwrap();
    let fetched = reopened
        .fetch_records("non-idempotent-restart.events", 0, 0, 10)
        .unwrap();

    assert_eq!(first, (0, 0));
    assert_eq!(second, (1, 1));
    assert_eq!(fetched.records.len(), 2);
}

#[test]
fn stale_producer_epoch_is_rejected() {
    let dir = tempdir().unwrap();
    let store = FileStore::open(dir.path()).unwrap();
    let producer = store.init_producer(10).unwrap();
    let first = vec![BrokerRecord {
        offset: 0,
        timestamp_ms: 10,
        producer_id: producer.producer_id,
        producer_epoch: producer.producer_epoch + 1,
        sequence: 0,
        key: Some(Bytes::from_static(b"key")),
        value: Some(Bytes::from_static(b"value")),
        headers_json: b"[]".to_vec(),
    }];
    store.append_records("epoch.events", 0, &first, 10).unwrap();

    let stale = vec![BrokerRecord {
        offset: 0,
        timestamp_ms: 20,
        producer_id: producer.producer_id,
        producer_epoch: producer.producer_epoch,
        sequence: 1,
        key: Some(Bytes::from_static(b"key")),
        value: Some(Bytes::from_static(b"value2")),
        headers_json: b"[]".to_vec(),
    }];

    let result = store.append_records("epoch.events", 0, &stale, 20);
    assert!(matches!(result, Err(StoreError::StaleProducerEpoch { .. })));
}

#[test]
fn unknown_producer_id_is_rejected() {
    let dir = tempdir().unwrap();
    let store = FileStore::open(dir.path()).unwrap();
    let records = vec![BrokerRecord {
        offset: 0,
        timestamp_ms: 10,
        producer_id: 10,
        producer_epoch: 0,
        sequence: 0,
        key: Some(Bytes::from_static(b"key")),
        value: Some(Bytes::from_static(b"value")),
        headers_json: b"[]".to_vec(),
    }];

    let result = store.append_records("unknown-producer.topic", 0, &records, 10);
    assert!(matches!(result, Err(StoreError::UnknownProducerId { .. })));
}

#[test]
fn non_contiguous_idempotent_sequence_is_rejected() {
    let dir = tempdir().unwrap();
    let store = FileStore::open(dir.path()).unwrap();
    let producer = store.init_producer(10).unwrap();
    let first = vec![BrokerRecord {
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
        .append_records("seq.topic", 0, &first, 10)
        .expect("first append should succeed");

    let gapped = vec![BrokerRecord {
        offset: 0,
        timestamp_ms: 20,
        producer_id: producer.producer_id,
        producer_epoch: producer.producer_epoch,
        sequence: 2,
        key: Some(Bytes::from_static(b"key")),
        value: Some(Bytes::from_static(b"value-2")),
        headers_json: b"[]".to_vec(),
    }];
    let result = store.append_records("seq.topic", 0, &gapped, 20);

    assert!(matches!(
        result,
        Err(StoreError::InvalidProducerSequence { .. })
    ));
}
