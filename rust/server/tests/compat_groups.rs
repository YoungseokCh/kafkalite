use std::net::TcpListener;
use std::sync::Arc;
use std::time::Duration;

use kafkalite_server::{Config, FileStore, KafkaBroker};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::message::Message;
use rdkafka::producer::{FutureProducer, FutureRecord};
use tempfile::tempdir;

fn init_test_logging() {
    let _ = env_logger::builder().is_test(true).try_init();
}

async fn start_broker() -> (
    String,
    tokio::task::JoinHandle<anyhow::Result<()>>,
    tempfile::TempDir,
) {
    let tempdir = tempdir().unwrap();
    let port = free_port();
    let config = Config::single_node(tempdir.path().join("kafkalite-data"), port, 1);
    let store = Arc::new(FileStore::open(&config.storage.data_dir).unwrap());
    let broker = KafkaBroker::new(config, store).unwrap();
    let handle = tokio::spawn(async move { broker.run().await });
    tokio::time::sleep(Duration::from_millis(150)).await;
    (format!("127.0.0.1:{port}"), handle, tempdir)
}

fn producer(bootstrap: &str) -> FutureProducer {
    ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .set("message.timeout.ms", "3000")
        .set("enable.idempotence", "true")
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

fn group_consumer(bootstrap: &str, group_id: &str) -> BaseConsumer {
    group_consumer_with_session_timeout(bootstrap, group_id, 45_000)
}

fn group_consumer_with_session_timeout(
    bootstrap: &str,
    group_id: &str,
    session_timeout_ms: i32,
) -> BaseConsumer {
    ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .set("group.id", group_id)
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
        .set("session.timeout.ms", session_timeout_ms.to_string())
        .set("debug", "protocol,broker,cgrp,fetch")
        .create()
        .unwrap()
}

fn drive_group_consumer(consumer: &BaseConsumer, timeout: Duration) {
    let started = std::time::Instant::now();
    while started.elapsed() < timeout {
        let _ = consumer.poll(Duration::from_millis(250));
    }
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
    consumer
        .commit_message(&message, rdkafka::consumer::CommitMode::Sync)
        .unwrap();

    handle.abort();
    let _ = handle.await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn group_consumer_subscribed_before_produce_receives_after_topic_materializes() {
    init_test_logging();
    let (bootstrap, handle, _tempdir) = start_broker().await;
    let topic = "event_ready.project.processor";
    let consumer = group_consumer(&bootstrap, "subscribe-before-produce");
    consumer.subscribe(&[topic]).unwrap();
    drive_group_consumer(&consumer, Duration::from_secs(2));

    let producer = producer(&bootstrap);
    producer
        .send(
            FutureRecord::to(topic).payload("released").key("project"),
            Duration::from_secs(3),
        )
        .await
        .unwrap();

    let message = poll_for_message(&consumer, Duration::from_secs(10));
    assert_eq!(message.payload(), Some(&b"released"[..]));

    handle.abort();
    let _ = handle.await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn partition_assignment_moves_to_remaining_group_member() {
    init_test_logging();
    let (bootstrap, handle, _tempdir) = start_broker().await;
    let producer = producer(&bootstrap);

    producer
        .send(
            FutureRecord::to("handoff.events")
                .payload("first-owner")
                .key("handoff-key"),
            Duration::from_secs(3),
        )
        .await
        .unwrap();

    let consumer_one = group_consumer_with_session_timeout(&bootstrap, "handoff-group", 6_000);
    consumer_one.subscribe(&["handoff.events"]).unwrap();
    let first = poll_for_message(&consumer_one, Duration::from_secs(8));
    assert_eq!(first.payload(), Some(&b"first-owner"[..]));
    consumer_one
        .commit_message(&first, rdkafka::consumer::CommitMode::Sync)
        .unwrap();
    drop(first);

    let consumer_two = group_consumer_with_session_timeout(&bootstrap, "handoff-group", 6_000);
    consumer_two.subscribe(&["handoff.events"]).unwrap();
    drive_group_consumer(&consumer_two, Duration::from_secs(1));

    consumer_one.unsubscribe();
    drop(consumer_one);
    drive_group_consumer(&consumer_two, Duration::from_secs(7));

    producer
        .send(
            FutureRecord::to("handoff.events")
                .payload("second-owner")
                .key("handoff-key"),
            Duration::from_secs(3),
        )
        .await
        .unwrap();

    let second = poll_for_message(&consumer_two, Duration::from_secs(10));
    assert_eq!(second.payload(), Some(&b"second-owner"[..]));

    drop(second);
    drop(consumer_two);
    handle.abort();
    let _ = handle.await;
}
