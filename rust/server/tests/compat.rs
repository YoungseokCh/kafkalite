use std::time::Duration;

use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::message::Message;
use rdkafka::producer::FutureRecord;
use rdkafka::topic_partition_list::{Offset, TopicPartitionList};

mod support;

use support::{base_consumer, init_test_logging, poll_for_message, producer, start_broker};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rdkafka_producer_and_consumer_smoke() {
    init_test_logging();
    let (bootstrap, handle, _tempdir) = start_broker().await;
    let producer = producer(&bootstrap);

    let (partition, offset) = producer
        .send(
            FutureRecord::to("compat.events")
                .payload("hello")
                .key("key"),
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
async fn multiple_topics_keep_independent_offsets() {
    init_test_logging();
    let (bootstrap, handle, _tempdir) = start_broker().await;
    let producer = producer(&bootstrap);

    for (topic, payload) in [("events.alpha", "alpha-1"), ("events.beta", "beta-1")] {
        let (_partition, offset) = producer
            .send(
                FutureRecord::to(topic).payload(payload).key(topic),
                Duration::from_secs(3),
            )
            .await
            .unwrap();
        assert_eq!(offset, 0);
    }

    for (topic, expected) in [
        ("events.alpha", b"alpha-1".as_slice()),
        ("events.beta", b"beta-1".as_slice()),
    ] {
        let consumer = direct_consumer_at_beginning(&bootstrap, topic, topic, 0);
        let message = poll_for_message(&consumer, Duration::from_secs(5));
        assert_eq!(message.payload(), Some(expected));
    }

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
