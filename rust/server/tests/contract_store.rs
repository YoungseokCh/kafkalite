use std::io::Write;

use bytes::Bytes;
use kafkalite_server::{
    FileStore,
    store::{BrokerRecord, Storage, StoreError},
};
use tempfile::tempdir;

#[test]
fn store_contract_replays_duplicate_retry_without_double_append() {
    let dir = tempdir().unwrap();
    let store = FileStore::open(dir.path()).unwrap();
    let producer = store.init_producer().unwrap();
    let records = vec![record(&producer, 0, 10, b"value")];

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
fn store_contract_keeps_partition_offsets_independent() {
    let dir = tempdir().unwrap();
    let store = FileStore::open(dir.path()).unwrap();
    store.ensure_topic("multi.events", 3, 10).unwrap();
    let producer = store.init_producer().unwrap();

    store
        .append_records("multi.events", 1, &[record(&producer, 0, 10, b"p1")], 10)
        .unwrap();
    store
        .append_records("multi.events", 2, &[record(&producer, 1, 20, b"p2")], 20)
        .unwrap();

    let (_, latest_zero) = store.list_offsets("multi.events", 0).unwrap();
    let (_, latest_one) = store.list_offsets("multi.events", 1).unwrap();
    let (_, latest_two) = store.list_offsets("multi.events", 2).unwrap();
    let fetch_one = store.fetch_records("multi.events", 1, 0, 10).unwrap();
    let fetch_two = store.fetch_records("multi.events", 2, 0, 10).unwrap();

    assert_eq!(latest_zero.offset, 0);
    assert_eq!(latest_one.offset, 1);
    assert_eq!(latest_two.offset, 1);
    assert_eq!(fetch_one.records[0].value.as_deref(), Some(&b"p1"[..]));
    assert_eq!(fetch_two.records[0].value.as_deref(), Some(&b"p2"[..]));
}

#[test]
fn store_contract_rejects_stale_producer_epoch() {
    let dir = tempdir().unwrap();
    let store = FileStore::open(dir.path()).unwrap();
    let producer = store.init_producer().unwrap();

    store
        .append_records(
            "epoch.events",
            0,
            &[BrokerRecord {
                producer_epoch: producer.producer_epoch + 1,
                ..record(&producer, 0, 10, b"value")
            }],
            10,
        )
        .unwrap();

    let stale = store.append_records(
        "epoch.events",
        0,
        &[BrokerRecord {
            producer_epoch: producer.producer_epoch,
            sequence: 1,
            timestamp_ms: 20,
            value: Some(Bytes::from_static(b"stale")),
            ..record(&producer, 0, 10, b"value")
        }],
        20,
    );

    assert!(matches!(stale, Err(StoreError::StaleProducerEpoch { .. })));
}

#[test]
fn store_contract_recovers_torn_tail_on_reopen() {
    let dir = tempdir().unwrap();
    let store = FileStore::open(dir.path()).unwrap();
    let producer = store.init_producer().unwrap();
    store
        .append_records(
            "recover.events",
            0,
            &[record(&producer, 0, 10, b"value")],
            10,
        )
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

fn record(
    producer: &kafkalite_server::store::ProducerSession,
    sequence: i32,
    timestamp_ms: i64,
    value: &'static [u8],
) -> BrokerRecord {
    BrokerRecord {
        offset: 0,
        timestamp_ms,
        producer_id: producer.producer_id,
        producer_epoch: producer.producer_epoch,
        sequence,
        key: Some(Bytes::from_static(b"key")),
        value: Some(Bytes::from_static(value)),
        headers_json: b"[]".to_vec(),
    }
}
