use std::io::Write;
use std::net::TcpListener;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use kafkalite_server::{
    Config, FileStore, KafkaBroker,
    store::{BrokerRecord, Storage, StoreError},
};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::message::Message;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::topic_partition_list::{Offset, TopicPartitionList};
use tempfile::tempdir;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn broker_contract_covers_roundtrip_offsets_and_invalid_partition() {
    init_test_logging();
    let (bootstrap, handle, _tempdir) = start_broker().await;
    let producer = producer(&bootstrap);
    let topic = "contract.events";

    let (partition, offset) = producer
        .send(
            FutureRecord::to(topic).payload("hello").key("key"),
            Duration::from_secs(5),
        )
        .await
        .unwrap();
    assert_eq!((partition, offset), (0, 0));

    let consumer = base_consumer(&bootstrap, "contract-direct");
    let mut tpl = TopicPartitionList::new();
    tpl.add_partition_offset(topic, 0, Offset::Beginning)
        .unwrap();
    consumer.assign(&tpl).unwrap();
    let message = poll_for_message(&consumer, Duration::from_secs(5));
    assert_eq!(message.payload(), Some(&b"hello"[..]));
    assert_eq!(message.key(), Some(&b"key"[..]));

    let (low, high) = consumer
        .fetch_watermarks(topic, 0, Duration::from_secs(5))
        .unwrap();
    assert_eq!((low, high), (0, 1));

    let invalid = producer
        .send(
            FutureRecord::to(topic)
                .payload("bad")
                .key("bad")
                .partition(1),
            Duration::from_secs(5),
        )
        .await
        .expect_err("partition 1 should fail");
    assert!(format!("{:?}", invalid.0).contains("UnknownPartition"));

    stop_broker(handle).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn broker_contract_keeps_records_and_committed_offsets_across_restart() {
    init_test_logging();
    let tempdir = tempdir().unwrap();
    let topic = "contract.resume";

    let (bootstrap, handle) = start_broker_in_dir(&tempdir).await;
    let producer = producer(&bootstrap);
    for payload in ["first", "second"] {
        producer
            .send(
                FutureRecord::to(topic).payload(payload).key("resume-key"),
                Duration::from_secs(5),
            )
            .await
            .unwrap();
    }

    let consumer = group_consumer(&bootstrap, "contract-group");
    consumer.subscribe(&[topic]).unwrap();
    let first = poll_for_message(&consumer, Duration::from_secs(8));
    assert_eq!(first.payload(), Some(&b"first"[..]));
    consumer
        .commit_message(&first, rdkafka::consumer::CommitMode::Sync)
        .unwrap();
    drop(first);
    drop(consumer);
    stop_broker(handle).await;

    let (bootstrap, handle) = start_broker_in_dir(&tempdir).await;
    let direct = base_consumer(&bootstrap, "contract-restart-direct");
    let mut tpl = TopicPartitionList::new();
    tpl.add_partition_offset(topic, 0, Offset::Beginning)
        .unwrap();
    direct.assign(&tpl).unwrap();
    let persisted = poll_for_message(&direct, Duration::from_secs(5));
    assert_eq!(persisted.payload(), Some(&b"first"[..]));

    let resumed = group_consumer(&bootstrap, "contract-group");
    resumed.subscribe(&[topic]).unwrap();
    let next = poll_for_message(&resumed, Duration::from_secs(8));
    assert_eq!(next.payload(), Some(&b"second"[..]));

    stop_broker(handle).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn broker_contract_auto_creates_multi_partition_topic_for_valid_partition() {
    init_test_logging();
    let tempdir = tempdir().unwrap();
    let topic = "contract.multi";
    let (bootstrap, handle) = start_broker_in_dir_with_partitions(&tempdir, 3).await;
    let producer = producer(&bootstrap);

    let (partition, offset) = producer
        .send(
            FutureRecord::to(topic)
                .payload("p2")
                .key("key")
                .partition(2),
            Duration::from_secs(5),
        )
        .await
        .unwrap();
    assert_eq!((partition, offset), (2, 0));

    let metadata_consumer = base_consumer(&bootstrap, "contract-metadata");
    let metadata = metadata_consumer
        .fetch_metadata(Some(topic), Duration::from_secs(5))
        .unwrap();
    let topic_meta = metadata
        .topics()
        .iter()
        .find(|entry| entry.name() == topic)
        .unwrap();
    assert_eq!(topic_meta.partitions().len(), 3);
    assert_eq!(topic_meta.partitions()[0].id(), 0);
    assert_eq!(topic_meta.partitions()[1].id(), 1);
    assert_eq!(topic_meta.partitions()[2].id(), 2);

    let direct = base_consumer(&bootstrap, "contract-p2-direct");
    let mut tpl = TopicPartitionList::new();
    tpl.add_partition_offset(topic, 2, Offset::Beginning)
        .unwrap();
    direct.assign(&tpl).unwrap();
    let message = poll_for_message(&direct, Duration::from_secs(5));
    assert_eq!(message.payload(), Some(&b"p2"[..]));

    stop_broker(handle).await;
}

#[test]
fn store_contract_replays_duplicate_retry_without_double_append() {
    let dir = tempdir().unwrap();
    let store = FileStore::open(dir.path()).unwrap();
    let producer = store.init_producer(10).unwrap();
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
    let producer = store.init_producer(10).unwrap();

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
    let producer = store.init_producer(10).unwrap();

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
    let producer = store.init_producer(10).unwrap();
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
        .open(
            dir.path()
                .join("topics/recover.events/partitions/0/00000000000000000000.log"),
        )
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

fn init_test_logging() {
    let _ = env_logger::builder().is_test(true).try_init();
}

async fn start_broker() -> (
    String,
    tokio::task::JoinHandle<anyhow::Result<()>>,
    tempfile::TempDir,
) {
    let tempdir = tempdir().unwrap();
    let (bootstrap, handle) = start_broker_in_dir(&tempdir).await;
    (bootstrap, handle, tempdir)
}

async fn start_broker_in_dir(
    tempdir: &tempfile::TempDir,
) -> (String, tokio::task::JoinHandle<anyhow::Result<()>>) {
    start_broker_in_dir_with_partitions(tempdir, 1).await
}

async fn start_broker_in_dir_with_partitions(
    tempdir: &tempfile::TempDir,
    default_partitions: i32,
) -> (String, tokio::task::JoinHandle<anyhow::Result<()>>) {
    let port = free_port();
    let config = Config::single_node(
        tempdir.path().join("kafkalite-data"),
        port,
        default_partitions,
    );
    let store = Arc::new(FileStore::open(&config.storage.data_dir).unwrap());
    let broker = KafkaBroker::new(config, store).unwrap();
    let handle = tokio::spawn(async move { broker.run().await });
    tokio::time::sleep(Duration::from_millis(150)).await;
    (format!("127.0.0.1:{port}"), handle)
}

async fn stop_broker(handle: tokio::task::JoinHandle<anyhow::Result<()>>) {
    handle.abort();
    let _ = handle.await;
}

fn producer(bootstrap: &str) -> FutureProducer {
    ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .set("message.timeout.ms", "3000")
        .set("enable.idempotence", "true")
        .create()
        .unwrap()
}

fn base_consumer(bootstrap: &str, group_id: &str) -> BaseConsumer {
    ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .set("group.id", group_id)
        .set("auto.offset.reset", "earliest")
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

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}
