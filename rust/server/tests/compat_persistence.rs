use std::net::TcpListener;
use std::sync::Arc;
use std::time::Duration;

use kafkalite_server::{Config, FileStore, KafkaBroker};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::message::Message;
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use rdkafka::topic_partition_list::{Offset, TopicPartitionList};
use rdkafka::util::Timeout;
use tempfile::tempdir;

fn init_test_logging() {
    let _ = env_logger::builder().is_test(true).try_init();
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

fn producer(bootstrap: &str) -> FutureProducer {
    ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .set("message.timeout.ms", "3000")
        .set("enable.idempotence", "true")
        .create()
        .unwrap()
}

fn transactional_producer(bootstrap: &str, transactional_id: &str) -> FutureProducer {
    ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .set("message.timeout.ms", "3000")
        .set("enable.idempotence", "true")
        .set("transactional.id", transactional_id)
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
    ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .set("group.id", group_id)
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
        .set("session.timeout.ms", "45000")
        .set("debug", "protocol,broker,cgrp,fetch")
        .create()
        .unwrap()
}

fn direct_consumer_at_beginning(
    bootstrap: &str,
    group_id: &str,
    topic: &str,
    partition: i32,
) -> BaseConsumer {
    let consumer = base_consumer(bootstrap, group_id);
    let mut assignment = TopicPartitionList::new();
    assignment
        .add_partition_offset(topic, partition, Offset::Beginning)
        .unwrap();
    consumer.assign(&assignment).unwrap();
    consumer
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
    let consumer = direct_consumer_at_beginning(&bootstrap, "restart-direct", "restart.events", 0);

    let message = poll_for_message(&consumer, Duration::from_secs(5));
    assert_eq!(message.payload(), Some(&b"persisted"[..]));

    handle.abort();
    let _ = handle.await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn committed_offsets_survive_broker_restart() {
    init_test_logging();
    let tempdir = tempdir().unwrap();
    let (bootstrap, handle) = start_broker_in_dir(&tempdir).await;
    let producer = producer(&bootstrap);

    for payload in ["first", "second"] {
        producer
            .send(
                FutureRecord::to("resume.events")
                    .payload(payload)
                    .key("resume-key"),
                Duration::from_secs(3),
            )
            .await
            .unwrap();
    }

    let consumer = group_consumer(&bootstrap, "resume-group");
    consumer.subscribe(&["resume.events"]).unwrap();
    let message = poll_for_message(&consumer, Duration::from_secs(8));
    assert_eq!(message.payload(), Some(&b"first"[..]));
    consumer
        .commit_message(&message, rdkafka::consumer::CommitMode::Sync)
        .unwrap();
    drop(message);
    drop(consumer);

    handle.abort();
    let _ = handle.await;

    let (bootstrap, handle) = start_broker_in_dir(&tempdir).await;
    let consumer = group_consumer(&bootstrap, "resume-group");
    consumer.subscribe(&["resume.events"]).unwrap();
    let message = poll_for_message(&consumer, Duration::from_secs(8));
    assert_eq!(message.payload(), Some(&b"second"[..]));

    handle.abort();
    let _ = handle.await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn committed_offsets_are_partition_scoped() {
    init_test_logging();
    let tempdir = tempdir().unwrap();
    let (bootstrap, handle) = start_broker_in_dir_with_partitions(&tempdir, 3).await;
    let producer = producer(&bootstrap);

    producer
        .send(
            FutureRecord::to("compat.resume.multi")
                .payload("p1-first")
                .key("k")
                .partition(1),
            Duration::from_secs(3),
        )
        .await
        .unwrap();
    let consumer = group_consumer(&bootstrap, "compat-multi-group");
    consumer.subscribe(&["compat.resume.multi"]).unwrap();

    let first = poll_for_message(&consumer, Duration::from_secs(8));
    assert_eq!(first.partition(), 1);
    assert_eq!(first.payload(), Some(&b"p1-first"[..]));
    consumer
        .commit_message(&first, rdkafka::consumer::CommitMode::Sync)
        .unwrap();
    drop(first);
    drop(consumer);

    producer
        .send(
            FutureRecord::to("compat.resume.multi")
                .payload("p1-second")
                .key("k")
                .partition(1),
            Duration::from_secs(3),
        )
        .await
        .unwrap();
    producer
        .send(
            FutureRecord::to("compat.resume.multi")
                .payload("p2-only")
                .key("k")
                .partition(2),
            Duration::from_secs(3),
        )
        .await
        .unwrap();

    handle.abort();
    let _ = handle.await;

    let (bootstrap, handle) = start_broker_in_dir_with_partitions(&tempdir, 3).await;
    let resumed = group_consumer(&bootstrap, "compat-multi-group");
    resumed.subscribe(&["compat.resume.multi"]).unwrap();

    let first = poll_for_message(&resumed, Duration::from_secs(8));
    let second = poll_for_message(&resumed, Duration::from_secs(8));
    let mut seen = [
        (first.partition(), first.payload().unwrap().to_vec()),
        (second.partition(), second.payload().unwrap().to_vec()),
    ];
    seen.sort_by_key(|row| row.0);
    assert_eq!(seen[0].0, 1);
    assert_eq!(seen[0].1, b"p1-second".to_vec());
    assert_eq!(seen[1].0, 2);
    assert_eq!(seen[1].1, b"p2-only".to_vec());

    handle.abort();
    let _ = handle.await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn transactional_offsets_survive_broker_restart() {
    init_test_logging();
    let tempdir = tempdir().unwrap();
    let (bootstrap, handle) = start_broker_in_dir(&tempdir).await;
    let producer = producer(&bootstrap);

    for payload in ["first", "second", "third"] {
        producer
            .send(
                FutureRecord::to("txn.resume.events")
                    .payload(payload)
                    .key("resume-key"),
                Duration::from_secs(3),
            )
            .await
            .unwrap();
    }

    let consumer = group_consumer(&bootstrap, "txn-resume-group");
    consumer.subscribe(&["txn.resume.events"]).unwrap();
    let first = poll_for_message(&consumer, Duration::from_secs(8));
    assert_eq!(first.payload(), Some(&b"first"[..]));

    let mut baseline = TopicPartitionList::new();
    baseline
        .add_partition_offset("txn.resume.events", 0, Offset::Offset(1))
        .unwrap();
    consumer
        .commit(&baseline, rdkafka::consumer::CommitMode::Sync)
        .unwrap();

    let transactional = transactional_producer(&bootstrap, "txn-resume-id");
    transactional
        .init_transactions(Timeout::After(Duration::from_secs(10)))
        .unwrap();
    let group_metadata = consumer.group_metadata().unwrap();
    transactional.begin_transaction().unwrap();
    let mut offsets = TopicPartitionList::new();
    offsets
        .add_partition_offset("txn.resume.events", 0, Offset::Offset(2))
        .unwrap();
    transactional
        .send_offsets_to_transaction(
            &offsets,
            &group_metadata,
            Timeout::After(Duration::from_secs(10)),
        )
        .unwrap();
    transactional
        .commit_transaction(Timeout::After(Duration::from_secs(10)))
        .unwrap();
    drop(first);
    drop(consumer);

    handle.abort();
    let _ = handle.await;

    let (bootstrap, handle) = start_broker_in_dir(&tempdir).await;
    let resumed = group_consumer(&bootstrap, "txn-resume-group");
    resumed.subscribe(&["txn.resume.events"]).unwrap();
    let next = poll_for_message(&resumed, Duration::from_secs(8));
    assert_eq!(next.payload(), Some(&b"third"[..]));

    handle.abort();
    let _ = handle.await;
}
