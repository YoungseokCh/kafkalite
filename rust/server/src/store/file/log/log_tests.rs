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
        partition_leader_epoch: 0,
        transactional: false,
        control: false,
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

    let segment = log.segment_paths("broken", 0).unwrap().pop().unwrap().log;
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
    let segment = log
        .segment_paths("index-missing", 0)
        .unwrap()
        .pop()
        .unwrap();
    std::fs::remove_file(segment.index).unwrap();

    let records = log.read_records("index-missing", 0, 0, 10).unwrap();
    assert_eq!(records.len(), 1);
}

#[test]
fn read_records_for_client_filters_records_before_start_offset_within_batch() {
    let dir = tempdir().unwrap();
    let log = RecordLog::open(dir.path()).unwrap();
    log.append_batch(
        "client-offset",
        0,
        &StoredBatch::from_records(&[
            BrokerRecord {
                offset: 0,
                timestamp_ms: 100,
                producer_id: -1,
                producer_epoch: -1,
                sequence: 0,
                key: None,
                value: Some(bytes::Bytes::from_static(b"value-0")),
                headers_json: b"[]".to_vec(),
                partition_leader_epoch: 0,
                transactional: false,
                control: false,
            },
            BrokerRecord {
                offset: 1,
                timestamp_ms: 101,
                producer_id: -1,
                producer_epoch: -1,
                sequence: 1,
                key: None,
                value: Some(bytes::Bytes::from_static(b"value-1")),
                headers_json: b"[]".to_vec(),
                partition_leader_epoch: 0,
                transactional: false,
                control: false,
            },
        ]),
    )
    .unwrap();

    let records = log
        .read_records_for_client("client-offset", 0, 1, usize::MAX)
        .unwrap();

    assert_eq!(
        records
            .iter()
            .map(|record| record.offset)
            .collect::<Vec<_>>(),
        vec![1]
    );
}

#[test]
fn rolls_to_new_segment_when_partition_exceeds_segment_bytes() {
    let dir = tempdir().unwrap();
    let log = RecordLog::open(dir.path()).unwrap();
    let value = vec![b'x'; DEFAULT_POLICY.segment_bytes as usize];
    log.append_batch(
        "rolled",
        0,
        &StoredBatch::from_records(&[BrokerRecord {
            offset: 0,
            timestamp_ms: 100,
            producer_id: -1,
            producer_epoch: -1,
            sequence: 0,
            key: None,
            value: Some(bytes::Bytes::from(value.clone())),
            headers_json: b"[]".to_vec(),
            partition_leader_epoch: 0,
            transactional: false,
            control: false,
        }]),
    )
    .unwrap();
    log.append_batch(
        "rolled",
        0,
        &StoredBatch::from_records(&[BrokerRecord {
            offset: 1,
            timestamp_ms: 101,
            producer_id: -1,
            producer_epoch: -1,
            sequence: 1,
            key: None,
            value: Some(bytes::Bytes::from(value)),
            headers_json: b"[]".to_vec(),
            partition_leader_epoch: 0,
            transactional: false,
            control: false,
        }]),
    )
    .unwrap();

    let segments = log.segment_paths("rolled", 0).unwrap();
    assert_eq!(segments.len(), 2);
    assert_eq!(segments[0].base_offset, 0);
    assert_eq!(segments[1].base_offset, 1);
}

#[test]
fn stored_batch_round_trips_transactional_metadata() {
    let batch = StoredBatch::from_records(&[BrokerRecord {
        offset: 5,
        timestamp_ms: 123,
        producer_id: 77,
        producer_epoch: 2,
        sequence: 9,
        key: Some(bytes::Bytes::from_static(b"k")),
        value: Some(bytes::Bytes::from_static(b"v")),
        headers_json: b"[]".to_vec(),
        partition_leader_epoch: 4,
        transactional: true,
        control: false,
    }]);

    let encoded = batch.encode_binary().unwrap();
    let decoded = StoredBatch::decode_binary(&encoded).unwrap();
    let record = &decoded.records[0];

    assert_eq!(record.partition_leader_epoch, 4);
    assert!(record.transactional);
    assert!(!record.control);
    assert_eq!(record.producer_id, 77);
    assert_eq!(record.producer_epoch, 2);
}
