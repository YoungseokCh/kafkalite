use std::net::TcpListener;
use std::sync::Arc;
use std::time::Duration;

use kafkalite_server::{
    Config, KafkaBroker, SqliteStore,
    config::{BrokerConfig, StorageConfig},
};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::message::Message;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::topic_partition_list::{Offset, TopicPartitionList};
use tempfile::tempdir;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rdkafka_producer_and_consumer_smoke() {
    init_test_logging();
    let (bootstrap, handle, _tempdir) = start_broker().await;
    let producer = producer(&bootstrap);

    let (partition, offset) = producer
        .send(
            FutureRecord::to("compat.events").payload("hello").key("key"),
            Duration::from_secs(3),
        )
        .await
        .unwrap();
    assert_eq!(partition, 0);
    assert_eq!(offset, 0);

    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", &bootstrap)
        .set("group.id", "compat-direct")
        .set("auto.offset.reset", "earliest")
        .set("enable.partition.eof", "false")
        .create()
        .unwrap();
    let mut tpl = TopicPartitionList::new();
    tpl.add_partition_offset("compat.events", 0, Offset::Beginning)
        .unwrap();
    consumer.assign(&tpl).unwrap();

    let message = consumer
        .poll(Duration::from_secs(5))
        .expect("expected a fetch result")
        .expect("expected a message");
    assert_eq!(message.payload(), Some(&b"hello"[..]));
    assert_eq!(message.key(), Some(&b"key"[..]));

    handle.abort();
    let _ = handle.await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rdkafka_group_consumer_commit_smoke() {
    init_test_logging();
    let (bootstrap, handle, _tempdir) = start_broker().await;
    let producer = producer(&bootstrap);

    producer
        .send(
            FutureRecord::to("group.events")
                .payload("payload")
                .key("commit-key"),
            Duration::from_secs(3),
        )
        .await
        .unwrap();

    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", &bootstrap)
        .set("group.id", "compat-group")
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
        .create()
        .unwrap();
    consumer.subscribe(&["group.events"]).unwrap();

    let message = poll_for_message(&consumer, Duration::from_secs(8));
    assert_eq!(message.payload(), Some(&b"payload"[..]));
    consumer.commit_message(&message, rdkafka::consumer::CommitMode::Sync).unwrap();

    handle.abort();
    let _ = handle.await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn records_survive_broker_restart() {
    init_test_logging();
    let tempdir = tempdir().unwrap();
    let (bootstrap, handle) = start_broker_in_dir(&tempdir).await;
    let producer = producer(&bootstrap);

    producer
        .send(
            FutureRecord::to("restart.events")
                .payload("persisted")
                .key("restart-key"),
            Duration::from_secs(3),
        )
        .await
        .unwrap();
    handle.abort();
    let _ = handle.await;

    let (bootstrap, handle) = start_broker_in_dir(&tempdir).await;
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", &bootstrap)
        .set("group.id", "restart-direct")
        .set("auto.offset.reset", "earliest")
        .create()
        .unwrap();
    let mut tpl = TopicPartitionList::new();
    tpl.add_partition_offset("restart.events", 0, Offset::Beginning)
        .unwrap();
    consumer.assign(&tpl).unwrap();

    let message = poll_for_message(&consumer, Duration::from_secs(5));
    assert_eq!(message.payload(), Some(&b"persisted"[..]));

    handle.abort();
    let _ = handle.await;
}

fn init_test_logging() {
    let _ = env_logger::builder().is_test(true).try_init();
}

async fn start_broker() -> (String, tokio::task::JoinHandle<anyhow::Result<()>>, tempfile::TempDir) {
    let tempdir = tempdir().unwrap();
    let (bootstrap, handle) = start_broker_in_dir(&tempdir).await;
    (bootstrap, handle, tempdir)
}

async fn start_broker_in_dir(
    tempdir: &tempfile::TempDir,
) -> (String, tokio::task::JoinHandle<anyhow::Result<()>>) {
    let port = free_port();
    let config = Config {
        broker: BrokerConfig {
            port,
            advertised_port: port,
            ..BrokerConfig::default()
        },
        storage: StorageConfig {
            db_path: tempdir.path().join("kafkalite.db"),
        },
    };
    let store = Arc::new(SqliteStore::open(&config.storage.db_path).unwrap());
    let broker = KafkaBroker::new(config, store);
    let handle = tokio::spawn(async move { broker.run().await });
    tokio::time::sleep(Duration::from_millis(150)).await;
    (format!("127.0.0.1:{port}"), handle)
}

fn producer(bootstrap: &str) -> FutureProducer {
    ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .set("message.timeout.ms", "3000")
        .set("enable.idempotence", "true")
        .create()
        .unwrap()
}

fn poll_for_message(consumer: &BaseConsumer, timeout: Duration) -> rdkafka::message::BorrowedMessage<'_> {
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
