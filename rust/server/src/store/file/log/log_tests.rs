use std::fs::OpenOptions;
use std::io::Write;

use tempfile::tempdir;

use super::*;

fn sample_batch(offset: i64) -> StoredBatch {
    StoredBatch::from_records(&[BrokerRecord {
        offset,
        timestamp_ms: 100 + offset,
        producer_id: -1,
        producer_epoch: -1,
        sequence: offset as i32,
        key: None,
        value: Some(bytes::Bytes::from_static(b"value")),
        headers_json: b"[]".to_vec(),
    }])
}

#[test]
fn read_methods_return_empty_for_missing_segments() {
    let dir = tempdir().unwrap();
    let log = RecordLog::open(dir.path()).unwrap();

    let records = log.read_records("missing", 0, 0, 10).unwrap();
    let client_records = log.read_records_for_client("missing", 0, 0, 10).unwrap();

    assert!(records.is_empty());
    assert!(client_records.is_empty());
}

#[test]
fn missing_partition_paths_are_noop_for_recovery_and_truncate() {
    let dir = tempdir().unwrap();
    let log = RecordLog::open(dir.path()).unwrap();

    assert_eq!(
        log.partition_ids("missing-topic").unwrap(),
        Vec::<i32>::new()
    );
    log.recover_partition("missing-topic", 0).unwrap();
    log.truncate_to_offset("missing-topic", 0, 1).unwrap();
    log.rebuild_indexes_for_partition("missing-topic", 0)
        .unwrap();

    let state = log.recover_partition_state("missing-topic", 0).unwrap();
    assert_eq!(state.next_offset, 0);
}

#[test]
fn decode_binary_rejects_invalid_kafka_batch() {
    let err = StoredBatch::decode_binary(b"BAD!").unwrap_err().to_string();
    assert!(!err.is_empty());
}

#[test]
fn recover_partition_truncates_invalid_batch_payload() {
    let dir = tempdir().unwrap();
    let log = RecordLog::open(dir.path()).unwrap();
    log.ensure_partition("broken", 0).unwrap();

    let segment = log.segment_path("broken", 0);
    let mut file = OpenOptions::new()
        .write(true)
        .truncate(true)
        .open(&segment)
        .unwrap();
    file.write_all(b"BAD!").unwrap();
    drop(file);

    log.recover_partition("broken", 0).unwrap();

    let len = std::fs::metadata(segment).unwrap().len();
    assert_eq!(len, 0);
}

#[test]
fn append_batch_hits_sync_interval_branch() {
    let dir = tempdir().unwrap();
    let log = RecordLog::open(dir.path()).unwrap();

    for offset in 0..DEFAULT_POLICY.log_sync_interval {
        log.append_batch("sync", 0, &sample_batch(offset as i64))
            .unwrap();
    }

    let records = log.read_records("sync", 0, 0, 100).unwrap();
    assert_eq!(records.len(), DEFAULT_POLICY.log_sync_interval as usize);
}

#[test]
fn read_records_scans_log_when_index_file_missing() {
    let dir = tempdir().unwrap();
    let log = RecordLog::open(dir.path()).unwrap();
    log.append_batch("index-missing", 0, &sample_batch(0))
        .unwrap();
    std::fs::remove_file(log.index_path("index-missing", 0)).unwrap();

    let records = log.read_records("index-missing", 0, 0, 10).unwrap();
    assert_eq!(records.len(), 1);
}
