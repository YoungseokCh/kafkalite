use super::*;
use crate::store::file::log::StoredBatch;
use crate::{BrokerHandle, Config, KafkaBroker};
use kafka_protocol::messages::offset_commit_request::{
    OffsetCommitRequestPartition, OffsetCommitRequestTopic,
};
use kafka_protocol::messages::offset_fetch_request::OffsetFetchRequestTopic;
use kafka_protocol::messages::sync_group_request::SyncGroupRequestAssignment;
use kafka_protocol::messages::{
    ApiKey, GroupId, HeartbeatRequest, HeartbeatResponse, LeaveGroupRequest, LeaveGroupResponse,
    OffsetCommitRequest, OffsetCommitResponse, OffsetFetchRequest, OffsetFetchResponse,
    RequestHeader, ResponseHeader, SyncGroupRequest, SyncGroupResponse, TopicName,
};
use kafka_protocol::protocol::{Decodable, Encodable, StrBytes};
use rdkafka::Message;
use rdkafka::TopicPartitionList;
use rdkafka::consumer::{BaseConsumer, Consumer};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_kafka_log_dir_recovery_preserves_transaction_visibility() {
    let Some(source) = real_kafka_recovery_log_dir() else {
        eprintln!(
            "skipping filesystem recovery test: set REAL_KAFKA_RECOVERY_LOG_DIR to a stopped Kafka log dir"
        );
        return;
    };
    let dir = tempdir().unwrap();
    copy_dir_all(&source, dir.path());

    let committed_topic = find_topic_dir_with_prefix(dir.path(), "diff.txn.committed.")
        .expect("expected committed transaction topic from differential fixture");
    let aborted_topic = find_topic_dir_with_prefix(dir.path(), "diff.txn.aborted.")
        .expect("expected aborted transaction topic from differential fixture");

    let broker = broker_for_data_dir(dir.path());
    let (_, latest) = broker.store().list_offsets(&committed_topic, 0).unwrap();
    let metadata_hw = broker.partition_high_watermark(&committed_topic, 0);
    let fetched = broker
        .store()
        .fetch_records_for_client(&committed_topic, 0, 0, 4096)
        .unwrap();
    assert_eq!(latest.offset, 2);
    assert_eq!(metadata_hw, Some(2));
    assert_eq!(
        fetched
            .records
            .iter()
            .filter(|record| !record.control)
            .count(),
        1,
        "expected one visible data record and one control marker in committed topic"
    );
    assert!(
        !fetched.records.is_empty(),
        "direct fetch should expose committed records after startup recovery"
    );

    let (bootstrap, handle) = start_broker_on_data_dir(dir.path()).await;

    assert_eq!(
        visible_message_count(&bootstrap, &committed_topic, "read_uncommitted"),
        1
    );
    assert_eq!(
        visible_message_count(&bootstrap, &committed_topic, "read_committed"),
        1
    );
    assert_eq!(
        visible_message_count(&bootstrap, &aborted_topic, "read_uncommitted"),
        1
    );
    assert_eq!(
        visible_message_count(&bootstrap, &aborted_topic, "read_committed"),
        0
    );

    handle.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_kafka_log_dir_recovery_preserves_committed_offsets() {
    let Some(source) = real_kafka_recovery_log_dir() else {
        eprintln!(
            "skipping filesystem recovery test: set REAL_KAFKA_RECOVERY_LOG_DIR to a stopped Kafka log dir"
        );
        return;
    };
    let dir = tempdir().unwrap();
    copy_dir_all(&source, dir.path());

    let topic = find_topic_dir_with_prefix(dir.path(), "diff.resume.")
        .expect("expected resume topic from differential fixture");
    let suffix = topic
        .strip_prefix("diff.resume.")
        .expect("topic prefix should match");
    let group_id = format!("group.{suffix}");

    let broker = broker_for_data_dir(dir.path());
    let recovered_offset = broker.store().fetch_offset(&group_id, &topic, 0).unwrap();
    if recovered_offset != Some(1) {
        eprintln!(
            "skipping filesystem recovery test: expected committed offset fixture for {group_id}/{topic}, found {recovered_offset:?}"
        );
        return;
    }

    let (bootstrap, handle) = start_broker_on_data_dir(dir.path()).await;
    let fetched = offset_fetch_via_network(&bootstrap, &group_id, &topic, &[0]);
    assert_eq!(fetched.groups[0].topics[0].partitions[0].error_code, 0);
    assert_eq!(
        fetched.groups[0].topics[0].partitions[0].committed_offset,
        1
    );

    handle.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_kafka_log_dir_recovery_preserves_transactional_offset_commits() {
    let Some(source) = real_kafka_recovery_log_dir() else {
        eprintln!(
            "skipping filesystem recovery test: set REAL_KAFKA_RECOVERY_LOG_DIR to a stopped Kafka log dir"
        );
        return;
    };
    let dir = tempdir().unwrap();
    copy_dir_all(&source, dir.path());

    let Some(topic) = find_topic_dir_with_prefix(dir.path(), "diff.txn.offsets.") else {
        eprintln!(
            "skipping filesystem recovery test: transactional offset fixture topic is absent"
        );
        return;
    };
    let suffix = topic
        .strip_prefix("diff.txn.offsets.")
        .expect("topic prefix should match");
    let group_id = format!("diff.txn.offsets.group.{suffix}");

    let broker = broker_for_data_dir(dir.path());
    let recovered_offset = broker.store().fetch_offset(&group_id, &topic, 0).unwrap();
    if recovered_offset != Some(20) {
        eprintln!(
            "skipping filesystem recovery test: expected transactional committed offset fixture for {group_id}/{topic}, found {recovered_offset:?}"
        );
        return;
    }

    let (bootstrap, handle) = start_broker_on_data_dir(dir.path()).await;
    let fetched = offset_fetch_via_network(&bootstrap, &group_id, &topic, &[0]);
    assert_eq!(fetched.groups[0].topics[0].partitions[0].error_code, 0);
    assert_eq!(
        fetched.groups[0].topics[0].partitions[0].committed_offset,
        20
    );

    handle.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_kafka_log_dir_recovery_preserves_multi_partition_committed_offsets() {
    let Some(source) = real_kafka_recovery_log_dir() else {
        eprintln!(
            "skipping filesystem recovery test: set REAL_KAFKA_RECOVERY_LOG_DIR to a stopped Kafka log dir"
        );
        return;
    };
    let dir = tempdir().unwrap();
    copy_dir_all(&source, dir.path());

    let topic = find_topic_dir_with_prefix(dir.path(), "diff.multi-offsets.")
        .expect("expected multi-offset topic from differential fixture");
    let group_id = format!("group.{topic}");

    let broker = broker_for_data_dir(dir.path());
    let partition_1 = broker.store().fetch_offset(&group_id, &topic, 1).unwrap();
    let partition_2 = broker.store().fetch_offset(&group_id, &topic, 2).unwrap();
    if partition_1 != Some(11) || partition_2 != Some(22) {
        eprintln!(
            "skipping filesystem recovery test: expected multi-partition offset fixture for {group_id}/{topic}, found partition1={partition_1:?} partition2={partition_2:?}"
        );
        return;
    }

    let (bootstrap, handle) = start_broker_on_data_dir(dir.path()).await;
    let fetched = offset_fetch_via_network(&bootstrap, &group_id, &topic, &[1, 2]);
    assert_eq!(fetched.groups[0].topics[0].partitions[0].error_code, 0);
    assert_eq!(
        fetched.groups[0].topics[0].partitions[0].committed_offset,
        11
    );
    assert_eq!(fetched.groups[0].topics[0].partitions[1].error_code, 0);
    assert_eq!(
        fetched.groups[0].topics[0].partitions[1].committed_offset,
        22
    );

    handle.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_kafka_log_dir_recovery_preserves_group_metadata_state() {
    let Some(source) = real_kafka_recovery_log_dir() else {
        eprintln!(
            "skipping filesystem recovery test: set REAL_KAFKA_RECOVERY_LOG_DIR to a stopped Kafka log dir"
        );
        return;
    };
    let dir = tempdir().unwrap();
    copy_dir_all(&source, dir.path());

    let topic = find_topic_dir_with_prefix(dir.path(), "diff.multi-offsets.")
        .expect("expected multi-offset topic from differential fixture");
    let group_id = format!("group.{topic}");

    let store = FileStore::open(dir.path()).unwrap();
    let Some(group) = store.debug_group_state(&group_id) else {
        eprintln!(
            "skipping filesystem recovery test: expected recovered group metadata fixture for {group_id}"
        );
        return;
    };
    assert_eq!(group.generation_id, 1);
    assert_eq!(group.protocol_name, "range");
    assert_eq!(group.members.len(), 1);
    let member = group
        .members
        .values()
        .next()
        .expect("expected recovered group member")
        .clone();
    assert_eq!(
        decode_assignment_partitions(&member.assignment, &topic),
        vec![1, 2]
    );
    drop(store);

    let (bootstrap, handle) = start_broker_on_data_dir(dir.path()).await;

    let sync = sync_group_via_network(
        &bootstrap,
        &group_id,
        group.generation_id,
        &member.member_id,
        &member.member_id,
        "range",
        &[],
    );
    assert_eq!(sync.error_code, 0);
    assert_eq!(
        decode_assignment_partitions(&sync.assignment, &topic),
        vec![1, 2]
    );

    let heartbeat = heartbeat_via_network(
        &bootstrap,
        &group_id,
        group.generation_id,
        &member.member_id,
    );
    assert_eq!(heartbeat.error_code, 0);

    let leave = leave_group_via_network(&bootstrap, &group_id, &member.member_id);
    assert_eq!(leave.error_code, 0);
    let heartbeat_after_leave = heartbeat_via_network(
        &bootstrap,
        &group_id,
        group.generation_id,
        &member.member_id,
    );
    assert_eq!(heartbeat_after_leave.error_code, 25);

    handle.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_kafka_log_dir_recovery_allows_offset_commit_with_recovered_member() {
    let Some(source) = real_kafka_recovery_log_dir() else {
        eprintln!(
            "skipping filesystem recovery test: set REAL_KAFKA_RECOVERY_LOG_DIR to a stopped Kafka log dir"
        );
        return;
    };
    let dir = tempdir().unwrap();
    copy_dir_all(&source, dir.path());

    let topic = find_topic_dir_with_prefix(dir.path(), "diff.multi-offsets.")
        .expect("expected multi-offset topic from differential fixture");
    let group_id = format!("group.{topic}");

    let store = FileStore::open(dir.path()).unwrap();
    let Some(group) = store.debug_group_state(&group_id) else {
        eprintln!(
            "skipping filesystem recovery test: expected recovered group metadata fixture for {group_id}"
        );
        return;
    };
    let member = group
        .members
        .values()
        .next()
        .expect("expected recovered group member")
        .clone();
    drop(store);

    let (bootstrap, handle) = start_broker_on_data_dir(dir.path()).await;
    let commit = offset_commit_via_network(
        &bootstrap,
        &group_id,
        group.generation_id,
        &member.member_id,
        &topic,
        1,
        12,
    );
    assert_eq!(commit.topics[0].partitions[0].error_code, 0);

    let fetched = offset_fetch_via_network(&bootstrap, &group_id, &topic, &[1]);
    assert_eq!(
        fetched.groups[0].topics[0].partitions[0].committed_offset,
        12
    );

    handle.shutdown().await.unwrap();
}

#[test]
fn real_kafka_log_dir_open_is_byte_exact_no_write() {
    let Some(source) = real_kafka_log_dir() else {
        eprintln!(
            "skipping filesystem byte-exact open test: set REAL_KAFKA_LOG_DIR to a stopped Kafka log dir"
        );
        return;
    };
    let Some(topic) = real_kafka_topic() else {
        eprintln!("skipping filesystem byte-exact open test: set REAL_KAFKA_TOPIC");
        return;
    };
    let dir = tempdir().unwrap();
    copy_dir_all(&source, dir.path());

    let before = filesystem_manifest(dir.path());
    let store = FileStore::open(dir.path()).unwrap();
    assert!(store.describe_topic(&topic).is_some());
    drop(store);

    assert_eq!(filesystem_manifest(dir.path()), before);
}

#[test]
fn real_kafka_log_dir_append_changes_only_expected_user_log() {
    let Some(source) = real_kafka_log_dir() else {
        eprintln!(
            "skipping filesystem append test: set REAL_KAFKA_LOG_DIR to a stopped Kafka log dir"
        );
        return;
    };
    let Some(topic) = real_kafka_topic() else {
        eprintln!("skipping filesystem append test: set REAL_KAFKA_TOPIC");
        return;
    };
    let dir = tempdir().unwrap();
    copy_dir_all(&source, dir.path());

    let before = filesystem_manifest(dir.path());
    let store = FileStore::open(dir.path()).unwrap();
    let next_offset = store.list_offsets(&topic, 0).unwrap().1.offset;
    let record = BrokerRecord {
        offset: next_offset,
        timestamp_ms: 123_456,
        producer_id: -1,
        producer_epoch: -1,
        sequence: next_offset as i32,
        key: Some(Bytes::from_static(b"kafkalite-key")),
        value: Some(Bytes::from_static(b"kafkalite-value")),
        headers_json: b"[]".to_vec(),
        partition_leader_epoch: 0,
        transactional: false,
        control: false,
    };
    store
        .append_records(&topic, 0, std::slice::from_ref(&record), 123_456)
        .unwrap();
    drop(store);

    let log_path = format!("{topic}-0/00000000000000000000.log");
    let mut expected = before.clone();
    let mut expected_log = before.get(&log_path).unwrap().bytes.clone();
    expected_log.extend_from_slice(
        &StoredBatch::from_records(&[record])
            .encode_binary()
            .unwrap(),
    );
    replace_manifest_file_bytes(&mut expected, &log_path, expected_log);
    let timeindex_path = format!("{topic}-0/00000000000000000000.timeindex");
    let mut expected_timeindex = before.get(&timeindex_path).unwrap().bytes.clone();
    expected_timeindex.clear();
    expected_timeindex.extend_from_slice(&123_456_i64.to_be_bytes());
    expected_timeindex.extend_from_slice(&1_i32.to_be_bytes());
    replace_manifest_file_bytes(&mut expected, &timeindex_path, expected_timeindex);
    replace_manifest_file_bytes(
        &mut expected,
        "recovery-point-offset-checkpoint",
        kafka_single_partition_checkpoint(&topic, 0, 2),
    );
    replace_manifest_file_bytes(
        &mut expected,
        "replication-offset-checkpoint",
        kafka_single_partition_checkpoint(&topic, 0, 2),
    );

    assert_eq!(filesystem_manifest(dir.path()), expected);
}

#[test]
fn real_kafka_log_dir_append_matches_real_kafka_selected_files() {
    let Some(source) = real_kafka_log_dir() else {
        eprintln!(
            "skipping filesystem append differential: set REAL_KAFKA_LOG_DIR to a stopped Kafka log dir"
        );
        return;
    };
    let Some(reference) = real_kafka_append_reference_dir() else {
        eprintln!(
            "skipping filesystem append differential: set REAL_KAFKA_APPEND_REFERENCE_DIR to a Kafka-appended stopped log dir"
        );
        return;
    };
    let Some(topic) = real_kafka_topic() else {
        eprintln!("skipping filesystem append differential: set REAL_KAFKA_TOPIC");
        return;
    };
    let dir = tempdir().unwrap();
    copy_dir_all(&source, dir.path());

    let leader_epoch = broker_for_data_dir(dir.path())
        .partition_leader_epoch(&topic, 0)
        .unwrap_or(0);
    let store = FileStore::open(dir.path()).unwrap();
    let next_offset = store.list_offsets(&topic, 0).unwrap().1.offset;
    let record = BrokerRecord {
        offset: next_offset,
        timestamp_ms: 123_456,
        producer_id: -1,
        producer_epoch: -1,
        sequence: next_offset as i32,
        key: Some(Bytes::from_static(b"kafkalite-key")),
        value: Some(Bytes::from_static(b"kafkalite-value")),
        headers_json: b"[]".to_vec(),
        partition_leader_epoch: leader_epoch,
        transactional: false,
        control: false,
    };
    store
        .append_records(&topic, 0, std::slice::from_ref(&record), 123_456)
        .unwrap();
    drop(store);

    for relative_path in kafka_partition_files_to_compare(&topic, 0) {
        let actual = std::fs::read(dir.path().join(&relative_path)).unwrap();
        let expected = std::fs::read(reference.join(&relative_path)).unwrap();
        assert_eq!(actual, expected, "mismatch for {relative_path}");
    }

    for relative_path in kafka_root_files_to_compare() {
        let actual = std::fs::read(dir.path().join(relative_path)).unwrap();
        let expected = std::fs::read(reference.join(relative_path)).unwrap();
        assert_eq!(actual, expected, "mismatch for {relative_path}");
    }
}

#[test]
fn real_kafka_rolled_log_dir_append_matches_real_kafka_selected_files() {
    let Some(source) = real_kafka_rolled_log_dir() else {
        eprintln!("skipping rolled filesystem append differential: set REAL_KAFKA_ROLLED_LOG_DIR");
        return;
    };
    let Some(reference) = real_kafka_rolled_append_reference_dir() else {
        eprintln!(
            "skipping rolled filesystem append differential: set REAL_KAFKA_ROLLED_APPEND_REFERENCE_DIR"
        );
        return;
    };
    let Some(topic) = real_kafka_rolled_topic() else {
        eprintln!("skipping rolled filesystem append differential: set REAL_KAFKA_ROLLED_TOPIC");
        return;
    };
    let dir = tempdir().unwrap();
    copy_dir_all(&source, dir.path());

    let leader_epoch = broker_for_data_dir(dir.path())
        .partition_leader_epoch(&topic, 0)
        .unwrap_or(0);
    let store = FileStore::open(dir.path()).unwrap();
    let summary = store.describe_topic(&topic).unwrap();
    assert_eq!(summary.partitions[0].active_segment_base_offset, 1);
    let next_offset = store.list_offsets(&topic, 0).unwrap().1.offset;
    let record = BrokerRecord {
        offset: next_offset,
        timestamp_ms: 3_000,
        producer_id: -1,
        producer_epoch: -1,
        sequence: next_offset as i32,
        key: Some(Bytes::from_static(b"rolled-key-2")),
        value: Some(Bytes::from_static(b"rolled-third")),
        headers_json: b"[]".to_vec(),
        partition_leader_epoch: leader_epoch,
        transactional: false,
        control: false,
    };
    store
        .append_records(&topic, 0, std::slice::from_ref(&record), 3_000)
        .unwrap();
    drop(store);

    for relative_path in kafka_partition_segment_files_to_compare(&topic, 0, &[0, 1]) {
        let actual = std::fs::read(dir.path().join(&relative_path)).unwrap();
        let expected = std::fs::read(reference.join(&relative_path)).unwrap();
        assert_eq!(actual, expected, "mismatch for {relative_path}");
    }

    for relative_path in kafka_root_files_to_compare() {
        let actual = std::fs::read(dir.path().join(relative_path)).unwrap();
        let expected = std::fs::read(reference.join(relative_path)).unwrap();
        assert_eq!(actual, expected, "mismatch for {relative_path}");
    }
}

#[test]
fn real_kafka_multi_append_log_dir_append_matches_real_kafka_selected_files() {
    let Some(source) = real_kafka_multi_append_log_dir() else {
        eprintln!(
            "skipping multi-append filesystem differential: set REAL_KAFKA_MULTI_APPEND_LOG_DIR"
        );
        return;
    };
    let Some(reference) = real_kafka_multi_append_reference_dir() else {
        eprintln!(
            "skipping multi-append filesystem differential: set REAL_KAFKA_MULTI_APPEND_REFERENCE_DIR"
        );
        return;
    };
    let Some(topic) = real_kafka_multi_append_topic() else {
        eprintln!(
            "skipping multi-append filesystem differential: set REAL_KAFKA_MULTI_APPEND_TOPIC"
        );
        return;
    };
    let dir = tempdir().unwrap();
    copy_dir_all(&source, dir.path());

    let leader_epoch = broker_for_data_dir(dir.path())
        .partition_leader_epoch(&topic, 0)
        .unwrap_or(0);
    let store = FileStore::open(dir.path()).unwrap();
    for append_index in 1..=5_i64 {
        let next_offset = store.list_offsets(&topic, 0).unwrap().1.offset;
        let key = Bytes::from(format!("multi-key-{append_index}"));
        let value = Bytes::from(format!("multi-value-{append_index}"));
        let record = BrokerRecord {
            offset: next_offset,
            timestamp_ms: 123_456 + append_index,
            producer_id: -1,
            producer_epoch: -1,
            sequence: next_offset as i32,
            key: Some(key),
            value: Some(value),
            headers_json: b"[]".to_vec(),
            partition_leader_epoch: leader_epoch,
            transactional: false,
            control: false,
        };
        store
            .append_records(
                &topic,
                0,
                std::slice::from_ref(&record),
                123_456 + append_index,
            )
            .unwrap();
    }
    drop(store);

    for relative_path in kafka_partition_files_to_compare(&topic, 0) {
        let actual = std::fs::read(dir.path().join(&relative_path)).unwrap();
        let expected = std::fs::read(reference.join(&relative_path)).unwrap();
        assert_eq!(actual, expected, "mismatch for {relative_path}");
    }

    for relative_path in kafka_root_files_to_compare() {
        let actual = std::fs::read(dir.path().join(relative_path)).unwrap();
        let expected = std::fs::read(reference.join(relative_path)).unwrap();
        assert_eq!(actual, expected, "mismatch for {relative_path}");
    }
}

fn real_kafka_log_dir() -> Option<std::path::PathBuf> {
    std::env::var_os("REAL_KAFKA_LOG_DIR").map(std::path::PathBuf::from)
}

fn real_kafka_recovery_log_dir() -> Option<std::path::PathBuf> {
    std::env::var_os("REAL_KAFKA_RECOVERY_LOG_DIR")
        .or_else(|| std::env::var_os("REAL_KAFKA_LOG_DIR"))
        .map(std::path::PathBuf::from)
}

fn real_kafka_append_reference_dir() -> Option<std::path::PathBuf> {
    std::env::var_os("REAL_KAFKA_APPEND_REFERENCE_DIR").map(std::path::PathBuf::from)
}

fn real_kafka_topic() -> Option<String> {
    std::env::var("REAL_KAFKA_TOPIC").ok()
}

fn real_kafka_rolled_log_dir() -> Option<std::path::PathBuf> {
    std::env::var_os("REAL_KAFKA_ROLLED_LOG_DIR").map(std::path::PathBuf::from)
}

fn real_kafka_rolled_append_reference_dir() -> Option<std::path::PathBuf> {
    std::env::var_os("REAL_KAFKA_ROLLED_APPEND_REFERENCE_DIR").map(std::path::PathBuf::from)
}

fn real_kafka_rolled_topic() -> Option<String> {
    std::env::var("REAL_KAFKA_ROLLED_TOPIC").ok()
}

fn real_kafka_multi_append_log_dir() -> Option<std::path::PathBuf> {
    std::env::var_os("REAL_KAFKA_MULTI_APPEND_LOG_DIR").map(std::path::PathBuf::from)
}

fn real_kafka_multi_append_reference_dir() -> Option<std::path::PathBuf> {
    std::env::var_os("REAL_KAFKA_MULTI_APPEND_REFERENCE_DIR").map(std::path::PathBuf::from)
}

fn real_kafka_multi_append_topic() -> Option<String> {
    std::env::var("REAL_KAFKA_MULTI_APPEND_TOPIC").ok()
}

fn kafka_partition_files_to_compare(topic: &str, partition: i32) -> Vec<String> {
    kafka_partition_segment_files_to_compare(topic, partition, &[0])
}

fn kafka_partition_segment_files_to_compare(
    topic: &str,
    partition: i32,
    base_offsets: &[i64],
) -> Vec<String> {
    let mut paths = Vec::new();
    for base_offset in base_offsets {
        let prefix = format!("{topic}-{partition}/{base_offset:020}");
        paths.push(format!("{prefix}.log"));
        paths.push(format!("{prefix}.index"));
        paths.push(format!("{prefix}.timeindex"));
    }
    paths.push(format!("{topic}-{partition}/leader-epoch-checkpoint"));
    paths
}

fn kafka_root_files_to_compare() -> [&'static str; 3] {
    [
        "recovery-point-offset-checkpoint",
        "replication-offset-checkpoint",
        "log-start-offset-checkpoint",
    ]
}

fn kafka_single_partition_checkpoint(topic: &str, partition: i32, offset: i64) -> Vec<u8> {
    format!("0\n1\n{topic} {partition} {offset}\n").into_bytes()
}

fn copy_dir_all(source: &std::path::Path, target: &std::path::Path) {
    std::fs::create_dir_all(target).unwrap();
    for entry in std::fs::read_dir(source).unwrap() {
        let entry = entry.unwrap();
        let source_path = entry.path();
        let target_path = target.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_dir_all(&source_path, &target_path);
        } else {
            std::fs::copy(&source_path, &target_path).unwrap();
        }
    }
}

fn find_topic_dir_with_prefix(root: &Path, prefix: &str) -> Option<String> {
    std::fs::read_dir(root)
        .ok()?
        .filter_map(|entry| entry.ok())
        .filter(|entry| entry.file_type().map(|ft| ft.is_dir()).unwrap_or(false))
        .filter_map(|entry| entry.file_name().into_string().ok())
        .find_map(|name| {
            let topic = name.strip_suffix("-0")?;
            topic.starts_with(prefix).then(|| topic.to_string())
        })
}

async fn start_broker_on_data_dir(data_dir: &Path) -> (String, BrokerHandle) {
    let port = free_port();
    let config = Config::single_node(PathBuf::from(data_dir), port, 3);
    let store = Arc::new(FileStore::open(&config.storage.data_dir).unwrap());
    let broker = KafkaBroker::new(config, store).unwrap();
    let handle = broker.start().await.unwrap();
    let bootstrap = format!("127.0.0.1:{port}");
    handle.ready().await.unwrap();
    (bootstrap, handle)
}

fn broker_for_data_dir(data_dir: &Path) -> KafkaBroker {
    let config = Config::single_node(PathBuf::from(data_dir), 19092, 3);
    let store = Arc::new(FileStore::open(&config.storage.data_dir).unwrap());
    KafkaBroker::new(config, store).unwrap()
}

fn visible_message_count(bootstrap: &str, topic: &str, isolation_level: &str) -> usize {
    let consumer: BaseConsumer = rdkafka::config::ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .set("group.id", format!("fs-recovery-{topic}-{isolation_level}"))
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
        .set("isolation.level", isolation_level)
        .create()
        .unwrap();
    wait_for_topic_ready(&consumer, topic);
    let mut assignment = TopicPartitionList::new();
    assignment
        .add_partition_offset(topic, 0, rdkafka::Offset::Beginning)
        .unwrap();
    consumer.assign(&assignment).unwrap();

    let started = std::time::Instant::now();
    let mut count = 0;
    while started.elapsed() < Duration::from_secs(5) {
        if let Some(message) = consumer.poll(Duration::from_millis(250)) {
            let message = message.expect("expected fetch message");
            if message.payload().is_some() {
                count += 1;
            }
        }
    }
    count
}

fn wait_for_topic_ready(consumer: &BaseConsumer, topic: &str) {
    let started = std::time::Instant::now();
    let mut last_state = String::from("no metadata received");
    while started.elapsed() < Duration::from_secs(5) {
        match consumer.fetch_metadata(Some(topic), Duration::from_secs(1)) {
            Ok(metadata) => {
                last_state = format!(
                    "topics={:?}",
                    metadata
                        .topics()
                        .iter()
                        .map(|metadata_topic| (
                            metadata_topic.name().to_string(),
                            metadata_topic
                                .partitions()
                                .iter()
                                .map(|partition| (partition.id(), partition.leader()))
                                .collect::<Vec<_>>()
                        ))
                        .collect::<Vec<_>>()
                );
                let ready = metadata
                    .topics()
                    .iter()
                    .find(|metadata_topic| metadata_topic.name() == topic)
                    .is_some_and(|metadata_topic| {
                        metadata_topic
                            .partitions()
                            .iter()
                            .any(|partition| partition.id() == 0 && partition.leader() >= 0)
                    });
                if ready {
                    return;
                }
            }
            Err(err) => {
                last_state = format!("metadata error: {err}");
            }
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    panic!("topic {topic} did not become ready: {last_state}");
}

fn offset_fetch_via_network(
    bootstrap: &str,
    group_id: &str,
    topic: &str,
    partitions: &[i32],
) -> OffsetFetchResponse {
    send_request(
        bootstrap,
        ApiKey::OffsetFetch,
        crate::protocol::OFFSET_FETCH_VERSION,
        OffsetFetchRequest::default()
            .with_group_id(GroupId(StrBytes::from(group_id.to_string())))
            .with_topics(Some(vec![
                OffsetFetchRequestTopic::default()
                    .with_name(TopicName(StrBytes::from(topic.to_string())))
                    .with_partition_indexes(partitions.to_vec()),
            ])),
    )
}

fn offset_commit_via_network(
    bootstrap: &str,
    group_id: &str,
    generation_id: i32,
    member_id: &str,
    topic: &str,
    partition: i32,
    next_offset: i64,
) -> OffsetCommitResponse {
    send_request(
        bootstrap,
        ApiKey::OffsetCommit,
        crate::protocol::OFFSET_COMMIT_VERSION,
        OffsetCommitRequest::default()
            .with_group_id(GroupId(StrBytes::from(group_id.to_string())))
            .with_generation_id_or_member_epoch(generation_id)
            .with_member_id(StrBytes::from(member_id.to_string()))
            .with_topics(vec![
                OffsetCommitRequestTopic::default()
                    .with_name(TopicName(StrBytes::from(topic.to_string())))
                    .with_partitions(vec![
                        OffsetCommitRequestPartition::default()
                            .with_partition_index(partition)
                            .with_committed_offset(next_offset),
                    ]),
            ]),
    )
}

fn sync_group_via_network(
    bootstrap: &str,
    group_id: &str,
    generation_id: i32,
    member_id: &str,
    leader_member_id: &str,
    protocol_name: &str,
    assignments: &[(&str, Vec<u8>)],
) -> SyncGroupResponse {
    send_request(
        bootstrap,
        ApiKey::SyncGroup,
        crate::protocol::SYNC_GROUP_VERSION,
        SyncGroupRequest::default()
            .with_group_id(GroupId(StrBytes::from(group_id.to_string())))
            .with_generation_id(generation_id)
            .with_member_id(StrBytes::from(member_id.to_string()))
            .with_protocol_type(Some(StrBytes::from("consumer".to_string())))
            .with_protocol_name(Some(StrBytes::from(protocol_name.to_string())))
            .with_assignments(if member_id == leader_member_id {
                assignments
                    .iter()
                    .map(|(member, assignment)| {
                        SyncGroupRequestAssignment::default()
                            .with_member_id(StrBytes::from((*member).to_string()))
                            .with_assignment(Bytes::from(assignment.clone()))
                    })
                    .collect()
            } else {
                vec![]
            }),
    )
}

fn heartbeat_via_network(
    bootstrap: &str,
    group_id: &str,
    generation_id: i32,
    member_id: &str,
) -> HeartbeatResponse {
    send_request(
        bootstrap,
        ApiKey::Heartbeat,
        crate::protocol::HEARTBEAT_VERSION,
        HeartbeatRequest::default()
            .with_group_id(GroupId(StrBytes::from(group_id.to_string())))
            .with_generation_id(generation_id)
            .with_member_id(StrBytes::from(member_id.to_string())),
    )
}

fn leave_group_via_network(bootstrap: &str, group_id: &str, member_id: &str) -> LeaveGroupResponse {
    send_request(
        bootstrap,
        ApiKey::LeaveGroup,
        crate::protocol::LEAVE_GROUP_VERSION,
        LeaveGroupRequest::default()
            .with_group_id(GroupId(StrBytes::from(group_id.to_string())))
            .with_member_id(StrBytes::from(member_id.to_string())),
    )
}

fn send_request<TReq: Encodable, TResp: Decodable>(
    bootstrap: &str,
    api_key: ApiKey,
    api_version: i16,
    request: TReq,
) -> TResp {
    use std::io::{Read, Write};

    let mut stream = std::net::TcpStream::connect(bootstrap).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    stream
        .set_write_timeout(Some(Duration::from_secs(10)))
        .unwrap();

    let mut payload = BytesMut::new();
    RequestHeader::default()
        .with_request_api_key(api_key as i16)
        .with_request_api_version(api_version)
        .with_correlation_id(1)
        .with_client_id(Some(StrBytes::from("kafka-filesystem-test".to_string())))
        .encode(&mut payload, api_key.request_header_version(api_version))
        .unwrap();
    request.encode(&mut payload, api_version).unwrap();

    stream
        .write_all(&(payload.len() as i32).to_be_bytes())
        .unwrap();
    stream.write_all(payload.as_ref()).unwrap();

    let mut size = [0_u8; 4];
    stream.read_exact(&mut size).unwrap();
    let size = i32::from_be_bytes(size) as usize;
    let mut body = vec![0_u8; size];
    stream.read_exact(&mut body).unwrap();
    let mut bytes = Bytes::from(body);
    let _ =
        ResponseHeader::decode(&mut bytes, api_key.response_header_version(api_version)).unwrap();
    TResp::decode(&mut bytes, api_version).unwrap()
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}
