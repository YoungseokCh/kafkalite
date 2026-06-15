use super::*;
use crate::store::file::log::StoredBatch;

#[test]
fn truncated_tail_is_recovered_on_restart() {
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
    let producer = store.init_producer().unwrap();
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
            partition_leader_epoch: 0,
            transactional: false,
            control: false,
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
    let producer = store.init_producer().unwrap();
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
            partition_leader_epoch: 0,
            transactional: false,
            control: false,
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

#[test]
fn appending_to_valid_kafka_layout_changes_only_expected_log_and_indexes() {
    let dir = tempdir().unwrap();
    let store = FileStore::open(dir.path()).unwrap();
    let initial_records = (0..16)
        .map(|offset| non_idempotent_record(offset, 10 + offset, b"initial"))
        .collect::<Vec<_>>();
    store
        .append_records("byte-exact.append", 0, &initial_records, 20)
        .unwrap();
    drop(store);

    let before = filesystem_manifest(dir.path());
    let reopened = FileStore::open(dir.path()).unwrap();
    let appended = non_idempotent_record(16, 100, b"tail");
    reopened
        .append_records("byte-exact.append", 0, std::slice::from_ref(&appended), 100)
        .unwrap();
    drop(reopened);

    let mut expected = before.clone();
    let log_path = "byte-exact.append-0/00000000000000000000.log";
    let index_path = "byte-exact.append-0/00000000000000000000.index";
    let time_index_path = "byte-exact.append-0/00000000000000000000.timeindex";
    let mut expected_log = before.get(log_path).unwrap().bytes.clone();
    let appended_payload = StoredBatch::from_records(&[appended])
        .encode_binary()
        .unwrap();
    let append_position = expected_log.len() as u64;
    expected_log.extend_from_slice(&appended_payload);
    replace_manifest_file_bytes(&mut expected, log_path, expected_log);

    let mut expected_index = before.get(index_path).unwrap().bytes.clone();
    append_expected_index_entry(
        &mut expected_index,
        16,
        append_position,
        appended_payload.len(),
        16,
    );
    replace_manifest_file_bytes(&mut expected, index_path, expected_index);

    let mut expected_time_index = before.get(time_index_path).unwrap().bytes.clone();
    append_expected_time_index_entry(&mut expected_time_index, 100, 16);
    replace_manifest_file_bytes(&mut expected, time_index_path, expected_time_index);

    assert_eq!(filesystem_manifest(dir.path()), expected);
    assert_eq!(root_directories(dir.path()), vec!["byte-exact.append-0"]);
}

#[test]
fn opening_foreign_kafka_indexes_keeps_bytes_unchanged() {
    let dir = tempdir().unwrap();
    seed_foreign_kafka_layout(dir.path(), "foreign.open");

    let before = filesystem_manifest(dir.path());
    let reopened = FileStore::open(dir.path()).unwrap();
    let fetched = reopened.fetch_records("foreign.open", 0, 0, 10).unwrap();
    drop(reopened);

    assert_eq!(fetched.records.len(), 2);
    assert_eq!(filesystem_manifest(dir.path()), before);
}

#[test]
fn appending_to_foreign_kafka_indexes_changes_only_log_bytes() {
    let dir = tempdir().unwrap();
    seed_foreign_kafka_layout(dir.path(), "foreign.append");

    let before = filesystem_manifest(dir.path());
    let reopened = FileStore::open(dir.path()).unwrap();
    let appended = non_idempotent_record(0, 30, b"two");
    reopened
        .append_records("foreign.append", 0, std::slice::from_ref(&appended), 30)
        .unwrap();
    let fetched = reopened.fetch_records("foreign.append", 0, 0, 10).unwrap();
    drop(reopened);

    let mut expected = before.clone();
    let log_path = "foreign.append-0/00000000000000000000.log";
    let mut expected_log = before.get(log_path).unwrap().bytes.clone();
    let expected_appended = BrokerRecord {
        offset: 2,
        ..appended.clone()
    };
    expected_log.extend_from_slice(
        &StoredBatch::from_records(&[expected_appended])
            .encode_binary()
            .unwrap(),
    );
    replace_manifest_file_bytes(&mut expected, log_path, expected_log);

    assert_eq!(
        fetched
            .records
            .iter()
            .map(|record| record.offset)
            .collect::<Vec<_>>(),
        vec![0, 1, 2]
    );
    assert_eq!(filesystem_manifest(dir.path()), expected);
}

#[test]
fn appending_to_kafka_indexes_writes_kafka_format_entries_after_sparse_interval() {
    let dir = tempdir().unwrap();
    let existing_log_len = seed_kafka_indexed_layout(dir.path(), "kafka.index.append");

    let before = filesystem_manifest(dir.path());
    let reopened = FileStore::open(dir.path()).unwrap();
    let appended = BrokerRecord {
        offset: 0,
        timestamp_ms: 30,
        producer_id: -1,
        producer_epoch: -1,
        sequence: 0,
        key: Some(Bytes::from_static(b"key")),
        value: Some(Bytes::from_static(b"tail")),
        headers_json: b"[]".to_vec(),
        partition_leader_epoch: 0,
        transactional: false,
        control: false,
    };
    reopened
        .append_records("kafka.index.append", 0, std::slice::from_ref(&appended), 30)
        .unwrap();
    drop(reopened);

    let log_path = "kafka.index.append-0/00000000000000000000.log";
    let index_path = "kafka.index.append-0/00000000000000000000.index";
    let time_index_path = "kafka.index.append-0/00000000000000000000.timeindex";

    let expected_appended = BrokerRecord {
        offset: 2,
        ..appended.clone()
    };
    let mut expected_log = before.get(log_path).unwrap().bytes.clone();
    expected_log.extend_from_slice(
        &StoredBatch::from_records(&[expected_appended])
            .encode_binary()
            .unwrap(),
    );
    let mut expected_index = before.get(index_path).unwrap().bytes.clone();
    expected_index.extend_from_slice(&2_i32.to_be_bytes());
    expected_index.extend_from_slice(&(existing_log_len as i32).to_be_bytes());

    let mut expected_time_index = before.get(time_index_path).unwrap().bytes.clone();
    expected_time_index.extend_from_slice(&30_i64.to_be_bytes());
    expected_time_index.extend_from_slice(&2_i32.to_be_bytes());

    let manifest = filesystem_manifest(dir.path());
    assert_eq!(manifest.get(log_path).unwrap().bytes, expected_log);
    assert_eq!(manifest.get(index_path).unwrap().bytes, expected_index);
    assert_eq!(
        manifest.get(time_index_path).unwrap().bytes,
        expected_time_index
    );
}

#[test]
fn appending_below_kafka_sparse_interval_preserves_index_bytes() {
    let dir = tempdir().unwrap();
    seed_small_kafka_indexed_layout(dir.path(), "kafka.index.small");

    let before = filesystem_manifest(dir.path());
    let reopened = FileStore::open(dir.path()).unwrap();
    let appended = BrokerRecord {
        offset: 0,
        timestamp_ms: 30,
        producer_id: -1,
        producer_epoch: -1,
        sequence: 0,
        key: Some(Bytes::from_static(b"key")),
        value: Some(Bytes::from_static(b"tail")),
        headers_json: b"[]".to_vec(),
        partition_leader_epoch: 0,
        transactional: false,
        control: false,
    };
    reopened
        .append_records("kafka.index.small", 0, std::slice::from_ref(&appended), 30)
        .unwrap();
    drop(reopened);

    let mut expected = before.clone();
    let log_path = "kafka.index.small-0/00000000000000000000.log";
    let time_index_path = "kafka.index.small-0/00000000000000000000.timeindex";
    let mut expected_log = before.get(log_path).unwrap().bytes.clone();
    let expected_appended = BrokerRecord {
        offset: 2,
        ..appended.clone()
    };
    expected_log.extend_from_slice(
        &StoredBatch::from_records(&[expected_appended])
            .encode_binary()
            .unwrap(),
    );
    replace_manifest_file_bytes(&mut expected, log_path, expected_log);

    let mut expected_time_index = before.get(time_index_path).unwrap().bytes.clone();
    expected_time_index.clear();
    expected_time_index.extend_from_slice(&30_i64.to_be_bytes());
    expected_time_index.extend_from_slice(&2_i32.to_be_bytes());
    replace_manifest_file_bytes(&mut expected, time_index_path, expected_time_index);

    assert_eq!(filesystem_manifest(dir.path()), expected);
}

#[test]
fn appending_with_non_increasing_timestamp_does_not_extend_kafka_timeindex() {
    let dir = tempdir().unwrap();
    let existing_log_len = seed_kafka_indexed_layout(dir.path(), "kafka.time.same-ts");

    let before = filesystem_manifest(dir.path());
    let reopened = FileStore::open(dir.path()).unwrap();
    let appended = BrokerRecord {
        offset: 0,
        timestamp_ms: 20,
        producer_id: -1,
        producer_epoch: -1,
        sequence: 0,
        key: Some(Bytes::from_static(b"key")),
        value: Some(Bytes::from_static(b"tail")),
        headers_json: b"[]".to_vec(),
        partition_leader_epoch: 0,
        transactional: false,
        control: false,
    };
    reopened
        .append_records("kafka.time.same-ts", 0, std::slice::from_ref(&appended), 20)
        .unwrap();
    drop(reopened);

    let mut expected = before.clone();
    let log_path = "kafka.time.same-ts-0/00000000000000000000.log";
    let index_path = "kafka.time.same-ts-0/00000000000000000000.index";
    let mut expected_log = before.get(log_path).unwrap().bytes.clone();
    let expected_appended = BrokerRecord {
        offset: 2,
        ..appended.clone()
    };
    expected_log.extend_from_slice(
        &StoredBatch::from_records(&[expected_appended])
            .encode_binary()
            .unwrap(),
    );
    replace_manifest_file_bytes(&mut expected, log_path, expected_log);

    let mut expected_index = before.get(index_path).unwrap().bytes.clone();
    expected_index.extend_from_slice(&2_i32.to_be_bytes());
    expected_index.extend_from_slice(&(existing_log_len as i32).to_be_bytes());
    replace_manifest_file_bytes(&mut expected, index_path, expected_index);

    assert_eq!(filesystem_manifest(dir.path()), expected);
}

#[test]
fn handoff_native_kafka_layout_appends_and_fetches_contiguous_offsets() {
    let dir = tempdir().unwrap();
    let partition_dir = dir.path().join("handoff.native-0");
    std::fs::create_dir_all(&partition_dir).unwrap();
    let existing_records = [
        non_idempotent_record(0, 10, b"zero"),
        non_idempotent_record(1, 20, b"one"),
    ];
    std::fs::File::create(partition_dir.join("00000000000000000000.log"))
        .unwrap()
        .write_all(
            &StoredBatch::from_records(&existing_records)
                .encode_binary()
                .unwrap(),
        )
        .unwrap();

    let store = FileStore::open(dir.path()).unwrap();
    let appended = non_idempotent_record(0, 30, b"two");
    let append_result = store
        .append_records("handoff.native", 0, &[appended], 30)
        .unwrap();
    let fetched = store.fetch_records("handoff.native", 0, 0, 10).unwrap();

    assert_eq!(append_result, (2, 2));
    assert_eq!(
        fetched
            .records
            .iter()
            .map(|record| record.offset)
            .collect::<Vec<_>>(),
        vec![0, 1, 2]
    );
}

fn seed_foreign_kafka_layout(root: &std::path::Path, topic: &str) {
    let partition_dir = root.join(format!("{topic}-0"));
    std::fs::create_dir_all(&partition_dir).unwrap();
    let existing_records = [
        non_idempotent_record(0, 10, b"zero"),
        non_idempotent_record(1, 20, b"one"),
    ];
    std::fs::File::create(partition_dir.join("00000000000000000000.log"))
        .unwrap()
        .write_all(
            &StoredBatch::from_records(&existing_records)
                .encode_binary()
                .unwrap(),
        )
        .unwrap();
    std::fs::File::create(partition_dir.join("00000000000000000000.index"))
        .unwrap()
        .write_all(b"foreign-kafka-index")
        .unwrap();
    std::fs::File::create(partition_dir.join("00000000000000000000.timeindex"))
        .unwrap()
        .write_all(b"foreign-kafka-timeindex")
        .unwrap();
}

fn seed_kafka_indexed_layout(root: &std::path::Path, topic: &str) -> u64 {
    let partition_dir = root.join(format!("{topic}-0"));
    std::fs::create_dir_all(&partition_dir).unwrap();
    let existing_records = [
        BrokerRecord {
            offset: 0,
            timestamp_ms: 10,
            producer_id: -1,
            producer_epoch: -1,
            sequence: 0,
            key: Some(Bytes::from_static(b"key")),
            value: Some(Bytes::from(vec![b'x'; 5000])),
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
            key: Some(Bytes::from_static(b"key")),
            value: Some(Bytes::from_static(b"one")),
            headers_json: b"[]".to_vec(),
            partition_leader_epoch: 0,
            transactional: false,
            control: false,
        },
    ];
    let log_bytes = StoredBatch::from_records(&existing_records)
        .encode_binary()
        .unwrap();
    let log_path = partition_dir.join("00000000000000000000.log");
    std::fs::File::create(&log_path)
        .unwrap()
        .write_all(&log_bytes)
        .unwrap();
    std::fs::File::create(partition_dir.join("00000000000000000000.index"))
        .unwrap()
        .write_all(&[0, 0, 0, 0, 0, 0, 0, 0])
        .unwrap();
    let mut timeindex = Vec::new();
    timeindex.extend_from_slice(&20_i64.to_be_bytes());
    timeindex.extend_from_slice(&1_i32.to_be_bytes());
    std::fs::File::create(partition_dir.join("00000000000000000000.timeindex"))
        .unwrap()
        .write_all(&timeindex)
        .unwrap();
    log_bytes.len() as u64
}

fn seed_small_kafka_indexed_layout(root: &std::path::Path, topic: &str) {
    let partition_dir = root.join(format!("{topic}-0"));
    std::fs::create_dir_all(&partition_dir).unwrap();
    let existing_records = [
        non_idempotent_record(0, 10, b"zero"),
        non_idempotent_record(1, 20, b"one"),
    ];
    let log_bytes = StoredBatch::from_records(&existing_records)
        .encode_binary()
        .unwrap();
    std::fs::File::create(partition_dir.join("00000000000000000000.log"))
        .unwrap()
        .write_all(&log_bytes)
        .unwrap();
    std::fs::File::create(partition_dir.join("00000000000000000000.index"))
        .unwrap()
        .write_all(&[0, 0, 0, 0, 0, 0, 0, 0])
        .unwrap();
    let mut timeindex = Vec::new();
    timeindex.extend_from_slice(&20_i64.to_be_bytes());
    timeindex.extend_from_slice(&1_i32.to_be_bytes());
    std::fs::File::create(partition_dir.join("00000000000000000000.timeindex"))
        .unwrap()
        .write_all(&timeindex)
        .unwrap();
}

#[test]
fn concurrent_appends_to_same_partition_remain_contiguous_after_reopen() {
    const THREADS: usize = 32;

    let dir = tempdir().unwrap();
    let store = std::sync::Arc::new(FileStore::open(dir.path()).unwrap());
    store.ensure_topic("race.append", 1, 0).unwrap();
    let start = std::sync::Arc::new(std::sync::Barrier::new(THREADS));
    let handles = (0..THREADS)
        .map(|thread_id| {
            let store = store.clone();
            let start = start.clone();
            std::thread::spawn(move || {
                let record = non_idempotent_record(0, thread_id as i64, b"race");
                start.wait();
                store
                    .append_records("race.append", 0, &[record], thread_id as i64)
                    .unwrap()
            })
        })
        .collect::<Vec<_>>();

    let mut offsets = handles
        .into_iter()
        .map(|handle| handle.join().unwrap().0)
        .collect::<Vec<_>>();
    offsets.sort_unstable();
    drop(store);

    let reopened = FileStore::open(dir.path()).unwrap();
    let fetched = reopened
        .fetch_records("race.append", 0, 0, THREADS)
        .unwrap();

    assert_eq!(offsets, (0..THREADS as i64).collect::<Vec<_>>());
    assert_eq!(fetched.records.len(), THREADS);
    assert_eq!(
        fetched
            .records
            .iter()
            .map(|record| record.offset)
            .collect::<Vec<_>>(),
        (0..THREADS as i64).collect::<Vec<_>>()
    );
}

fn non_idempotent_record(offset: i64, timestamp_ms: i64, value: &'static [u8]) -> BrokerRecord {
    BrokerRecord {
        offset,
        timestamp_ms,
        producer_id: -1,
        producer_epoch: -1,
        sequence: offset as i32,
        key: Some(Bytes::from_static(b"key")),
        value: Some(Bytes::from_static(value)),
        headers_json: b"[]".to_vec(),
        partition_leader_epoch: 0,
        transactional: false,
        control: false,
    }
}
