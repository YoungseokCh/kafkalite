#[path = "differential/assignments.rs"]
mod assignments;
#[path = "differential/groups.rs"]
mod groups;
#[path = "differential/protocol.rs"]
mod protocol;
#[path = "differential/recovery.rs"]
mod recovery;
#[path = "differential/roundtrip.rs"]
mod roundtrip;
#[path = "differential/transactions.rs"]
mod transactions;

use std::net::TcpListener;
use std::sync::Arc;
use std::time::Duration;

use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::metadata::Metadata;
use rdkafka::producer::FutureProducer;

use kafkalite_server::{Config, FileStore, KafkaBroker};
use tempfile::tempdir;

const DIFFERENTIAL_DEFAULT_PARTITIONS: i32 = 3;
const INVALID_PARTITION_INDEX: i32 = 99;

#[derive(Debug, PartialEq, Eq)]
struct MetadataSnapshot {
    topic: String,
    partition_count: usize,
    partition_ids: Vec<i32>,
}

#[derive(Debug, PartialEq, Eq)]
struct ProduceConsumeSnapshot {
    partition: i32,
    offset: i64,
    payload: Vec<u8>,
    key: Vec<u8>,
}

#[derive(Debug, PartialEq, Eq)]
struct MultiPartitionRoundtripSnapshot {
    partitions: Vec<i32>,
    payloads: Vec<Vec<u8>>,
}

#[derive(Debug, PartialEq, Eq)]
struct MultiPartitionOffsetFetchSnapshot {
    partition_1_offset: i64,
    partition_2_offset: i64,
}

#[derive(Debug, PartialEq, Eq)]
struct FetchPartitionSnapshot {
    partition: i32,
    error_code: i16,
    high_watermark: i64,
    record_count: usize,
    payload_len: usize,
    values: Vec<Vec<u8>>,
}

#[derive(Debug, PartialEq, Eq)]
struct FetchSnapshot {
    partitions: Vec<FetchPartitionSnapshot>,
}

#[derive(Debug, PartialEq, Eq)]
struct PartitionScopedResumeSnapshot {
    resumed: Vec<(i32, Vec<u8>)>,
}

#[derive(Debug, PartialEq, Eq)]
struct RetentionSnapshot {
    beginning_offset: i64,
    end_offset: i64,
    log_start_offset: i64,
    values: Vec<Vec<u8>>,
}

#[derive(Debug, PartialEq, Eq)]
struct ResumeSnapshot {
    first_payload: Vec<u8>,
    resumed_payload: Vec<u8>,
}

#[derive(Debug, PartialEq, Eq)]
struct StartupTopicRecoverySnapshot {
    payload: Vec<u8>,
    key: Vec<u8>,
}

#[derive(Debug, PartialEq, Eq)]
struct InvalidPartitionSnapshot {
    error: String,
}

#[derive(Debug, PartialEq, Eq)]
struct StaleCommitSnapshot {
    stale_commit_error: i16,
    offset_after_stale_commit: i64,
    valid_commit_error: i16,
    offset_after_valid_commit: i64,
}

#[derive(Debug, PartialEq, Eq)]
struct CurrentMemberStaleCommitSnapshot {
    current_commit_error: i16,
    stale_commit_error: i16,
    offset_after_stale_commit: i64,
}

#[derive(Debug, PartialEq, Eq)]
struct StaleHeartbeatSnapshot {
    stale_heartbeat_error: i16,
    valid_commit_error: i16,
    offset_after_valid_commit: i64,
}

#[derive(Debug, PartialEq, Eq)]
struct StaleSyncSnapshot {
    stale_sync_error: i16,
    stale_sync_assignment_len: usize,
}

#[derive(Debug, PartialEq, Eq)]
struct EmptyAssignmentSnapshot {
    empty_member_error: i16,
    empty_member_assignment_len: usize,
    empty_member_assignment_decodable: bool,
    assigned_member_error: i16,
    assigned_member_assignment_len: usize,
    assigned_member_assignment_decodable: bool,
}

#[derive(Debug, PartialEq, Eq)]
struct LeaveGroupSnapshot {
    leave_error: i16,
    post_leave_heartbeat_error: i16,
}

#[derive(Debug, PartialEq, Eq)]
struct TransactionCoordinatorSnapshot {
    find_coordinator_error: i16,
    init_negative_timeout_error: i16,
    init_excessive_timeout_error: i16,
    init_success_error: i16,
    add_valid_top_level_error: i16,
    add_valid_partition_error: i16,
    add_missing_txn_top_level_error: i16,
    add_missing_txn_partition_error: i16,
    reinit_error: i16,
    reused_producer_id: bool,
    epoch_bumped: bool,
    end_stale_epoch_rejected: bool,
}

#[derive(Debug, PartialEq, Eq)]
struct TransactionVisibilitySnapshot {
    committed_read_uncommitted_count: usize,
    committed_read_committed_count: usize,
    aborted_read_uncommitted_count: usize,
    aborted_read_committed_count: usize,
}

#[derive(Debug, PartialEq, Eq)]
struct TransactionalOffsetCommitSnapshot {
    committed_txn_offset: i64,
    aborted_txn_offset: i64,
}

#[derive(Debug, PartialEq, Eq)]
struct MultiGroupTransactionalOffsetCommitSnapshot {
    group_a_committed_offset: i64,
    group_b_committed_offset: i64,
    group_a_aborted_offset: i64,
    group_b_aborted_offset: i64,
}

async fn start_local_broker() -> (
    String,
    tokio::task::JoinHandle<anyhow::Result<()>>,
    tempfile::TempDir,
) {
    start_local_broker_with_config(Config::single_node(
        std::path::PathBuf::from("./unused"),
        0,
        DIFFERENTIAL_DEFAULT_PARTITIONS,
    ))
    .await
}

async fn start_local_broker_with_config(
    mut config: Config,
) -> (
    String,
    tokio::task::JoinHandle<anyhow::Result<()>>,
    tempfile::TempDir,
) {
    let tempdir = tempdir().unwrap();
    config.storage.data_dir = tempdir.path().join("kafkalite-data");
    if config.broker.port == 0 {
        let port = free_port();
        config.broker.port = port;
        config.broker.advertised_port = port;
        if let Some(listener) = config.cluster.listeners.get_mut("PLAINTEXT") {
            listener.port = port;
        }
        if let Some(listener) = config.cluster.advertised_listeners.get_mut("PLAINTEXT") {
            listener.port = port;
        }
    }
    let store = Arc::new(
        FileStore::open_with_policy(&config.storage.data_dir, config.storage.policy()).unwrap(),
    );
    let bootstrap = format!("127.0.0.1:{}", config.broker.port);
    let broker = KafkaBroker::new(config, store).unwrap();
    let handle = tokio::spawn(async move { broker.run().await });
    tokio::time::sleep(Duration::from_millis(150)).await;
    (bootstrap, handle, tempdir)
}

fn producer(bootstrap: &str) -> FutureProducer {
    ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .set("message.timeout.ms", "5000")
        .set("enable.idempotence", "true")
        .create()
        .unwrap()
}

fn consumer(bootstrap: &str, group_id: &str) -> BaseConsumer {
    ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .set("group.id", group_id)
        .set("auto.offset.reset", "earliest")
        .create()
        .unwrap()
}

fn admin_client(bootstrap: &str) -> AdminClient<DefaultClientContext> {
    ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .create()
        .unwrap()
}

fn group_consumer(bootstrap: &str, group_id: &str) -> BaseConsumer {
    ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .set("group.id", group_id)
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
        .create()
        .unwrap()
}

fn poll_for_message(
    consumer: &BaseConsumer,
    timeout: Duration,
) -> rdkafka::message::BorrowedMessage<'_> {
    let started = std::time::Instant::now();
    while started.elapsed() < timeout {
        if let Some(result) = consumer.poll(Duration::from_millis(250)) {
            return result.expect("expected a message");
        }
    }
    panic!("expected a fetch result");
}

fn drive_consumer(consumer: &BaseConsumer, timeout: Duration) {
    let started = std::time::Instant::now();
    while started.elapsed() < timeout {
        let _ = consumer.poll(Duration::from_millis(250));
    }
}

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn find_topic<'a>(metadata: &'a Metadata, name: &str) -> &'a rdkafka::metadata::MetadataTopic {
    metadata
        .topics()
        .iter()
        .find(|topic| topic.name() == name)
        .expect("topic metadata should exist")
}

fn bootstrap_available(bootstrap: &str) -> bool {
    let consumer = consumer(bootstrap, "bootstrap-probe");
    consumer
        .fetch_metadata(None, Duration::from_secs(2))
        .is_ok()
}

fn wait_for_topic(bootstrap: &str, topic: &str, expected_partition_count: usize) {
    let consumer = consumer(bootstrap, &format!("wait-{topic}"));
    let started = std::time::Instant::now();
    while started.elapsed() < Duration::from_secs(10) {
        if let Ok(metadata) = consumer.fetch_metadata(Some(topic), Duration::from_secs(1)) {
            let topic_ready = metadata
                .topics()
                .iter()
                .find(|metadata_topic| metadata_topic.name() == topic)
                .is_some_and(|metadata_topic| {
                    metadata_topic.partitions().len() >= expected_partition_count
                        && metadata_topic
                            .partitions()
                            .iter()
                            .all(|partition| partition.leader() >= 0)
                });
            if topic_ready {
                return;
            }
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    panic!("topic {topic} did not become ready with {expected_partition_count} partitions");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn local_broker_supports_librdkafka_create_topics_admin_api() {
    let (local_bootstrap, handle, _tempdir) = start_local_broker().await;
    let topic = format!("diff.admin.{}", uuid::Uuid::new_v4().simple());
    let admin = admin_client(&local_bootstrap);
    let specs = [NewTopic::new(&topic, 1, TopicReplication::Fixed(1))];

    let result = admin
        .create_topics(&specs, &AdminOptions::new())
        .await
        .expect("librdkafka admin CreateTopics request should be supported");

    assert!(
        result.iter().all(Result::is_ok),
        "topic creation should succeed: {result:?}"
    );
    wait_for_topic(&local_bootstrap, &topic, 1);

    handle.abort();
    let _ = handle.await;
}
