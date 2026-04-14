use bytes::Bytes;
use tempfile::tempdir;

use super::*;

#[test]
fn append_only_adds_one_state_journal_entry_per_write() {
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

    let journal_path = dir.path().join("state/state.journal");
    assert_eq!(count_journal_entries(&journal_path), 1);

    store.append_records("d1.events", 0, &records, 20).unwrap();
    assert_eq!(count_journal_entries(&journal_path), 2);

    store.append_records("d1.events", 0, &records, 30).unwrap();
    assert_eq!(count_journal_entries(&journal_path), 2);

    let next = vec![BrokerRecord {
        sequence: 1,
        timestamp_ms: 30,
        ..records[0].clone()
    }];
    store.append_records("d1.events", 0, &next, 30).unwrap();
    assert_eq!(count_journal_entries(&journal_path), 3);
}

#[test]
fn topic_offsets_are_recovered_from_log_after_reopen() {
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
    }];

    store
        .append_records("non-idempotent-state.events", 0, &records, 20)
        .unwrap();

    let reopened = FileStore::open(dir.path()).unwrap();
    let journal = std::fs::read_to_string(dir.path().join("state/producers.snapshot"));

    assert!(journal.is_err());
    assert!(
        reopened
            .describe_topic("non-idempotent-state.events")
            .is_some()
    );
    let state_journal = std::fs::read(dir.path().join("state/state.journal")).unwrap();
    let text = String::from_utf8_lossy(&state_journal);
    assert!(!text.contains("non-idempotent-state.events:0:-1"));
}

fn count_journal_entries(path: &std::path::Path) -> usize {
    let bytes = std::fs::read(path).unwrap();
    let mut cursor = 0;
    let mut count = 0;
    while cursor + 8 <= bytes.len() {
        assert_eq!(&bytes[cursor..cursor + 4], b"KFSJ");
        let len = u32::from_le_bytes(bytes[cursor + 4..cursor + 8].try_into().unwrap()) as usize;
        cursor += 8 + len;
        count += 1;
    }
    assert_eq!(cursor, bytes.len());
    count
}
