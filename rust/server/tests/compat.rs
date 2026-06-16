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

    drop(message);
    drop(consumer);
    drop(producer);
    handle.shutdown().await.unwrap();
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
        drop(message);
        drop(consumer);
    }

    drop(producer);
    handle.shutdown().await.unwrap();
}
