use super::*;
use crate::store::file::log::StoredBatch;
use crate::{Config, KafkaBroker};
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

    handle.abort();
    let _ = handle.await;
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

async fn start_broker_on_data_dir(
    data_dir: &Path,
) -> (String, tokio::task::JoinHandle<anyhow::Result<()>>) {
    let port = free_port();
    let config = Config::single_node(PathBuf::from(data_dir), port, 3);
    let store = Arc::new(FileStore::open(&config.storage.data_dir).unwrap());
    let broker = KafkaBroker::new(config, store).unwrap();
    let handle = tokio::spawn(async move { broker.run().await });
    let bootstrap = format!("127.0.0.1:{port}");
    let started = std::time::Instant::now();
    while started.elapsed() < Duration::from_secs(5) {
        if handle.is_finished() {
            let outcome = handle.await.unwrap();
            panic!("broker exited before accepting connections: {outcome:?}");
        }
        if std::net::TcpStream::connect(&bootstrap).is_ok() {
            return (bootstrap, handle);
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("broker did not start listening on {bootstrap}");
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

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}
