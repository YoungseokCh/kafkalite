use std::net::TcpListener;
use std::sync::Arc;
use std::time::Duration;

use kafkalite_server::{Config, FileStore, KafkaBroker};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::BaseConsumer;
use rdkafka::producer::FutureProducer;
use tempfile::tempdir;

pub fn init_test_logging() {
    let _ = env_logger::builder().is_test(true).try_init();
}

pub async fn start_broker() -> (
    String,
    tokio::task::JoinHandle<anyhow::Result<()>>,
    tempfile::TempDir,
) {
    let tempdir = tempdir().unwrap();
    let (bootstrap, handle) = start_broker_in_dir(&tempdir).await;
    (bootstrap, handle, tempdir)
}

pub async fn start_broker_in_dir(
    tempdir: &tempfile::TempDir,
) -> (String, tokio::task::JoinHandle<anyhow::Result<()>>) {
    start_broker_in_dir_with_partitions(tempdir, 1).await
}

pub async fn start_broker_in_dir_with_partitions(
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

pub fn producer(bootstrap: &str) -> FutureProducer {
    ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .set("message.timeout.ms", "3000")
        .set("enable.idempotence", "true")
        .create()
        .unwrap()
}

pub fn base_consumer(bootstrap: &str, group_id: &str) -> BaseConsumer {
    ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .set("group.id", group_id)
        .set("auto.offset.reset", "earliest")
        .create()
        .unwrap()
}

pub fn poll_for_message(
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

pub fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}
