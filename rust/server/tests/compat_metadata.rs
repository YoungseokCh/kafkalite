use std::time::Duration;

use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::message::Message;
use rdkafka::metadata::Metadata;
use rdkafka::producer::FutureRecord;
use rdkafka::topic_partition_list::{Offset, TopicPartitionList};
use tempfile::tempdir;

mod support;

use support::{
    base_consumer, init_test_logging, poll_for_message, producer, start_broker,
    start_broker_in_dir_with_partitions,
};

fn find_topic<'a>(metadata: &'a Metadata, name: &str) -> &'a rdkafka::metadata::MetadataTopic {
    metadata
        .topics()
        .iter()
        .find(|topic| topic.name() == name)
        .expect("topic metadata should exist")
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
async fn metadata_reports_unknown_topic_until_first_produce() {
    init_test_logging();
    let (bootstrap, handle, _tempdir) = start_broker().await;
    let consumer = base_consumer(&bootstrap, "metadata-check");
    let producer = producer(&bootstrap);

    let metadata = consumer
        .fetch_metadata(
            Some("dynamic.events.project.processor"),
            Duration::from_secs(5),
        )
        .unwrap();
    let topic = find_topic(&metadata, "dynamic.events.project.processor");
    assert_eq!(topic.partitions().len(), 0);

    producer
        .send(
            FutureRecord::to("dynamic.events.project.processor")
                .payload("created")
                .key("dynamic"),
            Duration::from_secs(3),
        )
        .await
        .unwrap();

    let metadata = consumer
        .fetch_metadata(
            Some("dynamic.events.project.processor"),
            Duration::from_secs(5),
        )
        .unwrap();
    let topic = find_topic(&metadata, "dynamic.events.project.processor");
    assert_eq!(topic.partitions().len(), 1);
    assert_eq!(topic.partitions()[0].id(), 0);

    handle.abort();
    let _ = handle.await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn admin_client_create_topics_materializes_topic() {
    init_test_logging();
    let (bootstrap, handle, _tempdir) = start_broker().await;
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", &bootstrap)
        .create()
        .unwrap();

    let results = admin
        .create_topics(
            &[NewTopic::new(
                "admin.compat.topic",
                1,
                TopicReplication::Fixed(1),
            )],
            &AdminOptions::new().operation_timeout(Some(Duration::from_secs(3))),
        )
        .await
        .unwrap();
    assert!(results[0].is_ok());

    let consumer = base_consumer(&bootstrap, "admin-compat-meta");
    let metadata = consumer
        .fetch_metadata(Some("admin.compat.topic"), Duration::from_secs(5))
        .unwrap();
    let topic = find_topic(&metadata, "admin.compat.topic");
    assert_eq!(topic.partitions().len(), 1);

    handle.abort();
    let _ = handle.await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn multi_partition_metadata_and_direct_fetch_work() {
    init_test_logging();
    let tempdir = tempdir().unwrap();
    let (bootstrap, handle) = start_broker_in_dir_with_partitions(&tempdir, 3).await;
    let producer = producer(&bootstrap);

    let (partition, offset) = producer
        .send(
            FutureRecord::to("compat.multi")
                .payload("p2")
                .key("p2-key")
                .partition(2),
            Duration::from_secs(3),
        )
        .await
        .unwrap();
    assert_eq!(partition, 2);
    assert_eq!(offset, 0);

    let consumer = base_consumer(&bootstrap, "compat-multi-meta");
    let metadata = consumer
        .fetch_metadata(Some("compat.multi"), Duration::from_secs(5))
        .unwrap();
    let topic = find_topic(&metadata, "compat.multi");
    assert_eq!(topic.partitions().len(), 3);
    assert_eq!(topic.partitions()[0].id(), 0);
    assert_eq!(topic.partitions()[1].id(), 1);
    assert_eq!(topic.partitions()[2].id(), 2);

    let direct = direct_consumer_at_beginning(&bootstrap, "compat-multi-direct", "compat.multi", 2);
    let message = poll_for_message(&direct, Duration::from_secs(5));
    assert_eq!(message.payload(), Some(&b"p2"[..]));

    handle.abort();
    let _ = handle.await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn metadata_remains_available_after_broker_restart() {
    init_test_logging();
    let tempdir = tempdir().unwrap();
    let topic = "compat.meta.persist";

    let (bootstrap, handle) = start_broker_in_dir_with_partitions(&tempdir, 3).await;
    let producer = producer(&bootstrap);
    producer
        .send(
            FutureRecord::to(topic).payload("p1").key("k").partition(1),
            Duration::from_secs(3),
        )
        .await
        .unwrap();

    let consumer = base_consumer(&bootstrap, "compat-meta-persist-1");
    let metadata = consumer
        .fetch_metadata(Some(topic), Duration::from_secs(5))
        .unwrap();
    let topic_metadata = find_topic(&metadata, topic);
    assert_eq!(topic_metadata.partitions().len(), 3);

    handle.abort();
    let _ = handle.await;

    let (bootstrap, handle) = start_broker_in_dir_with_partitions(&tempdir, 3).await;
    let consumer = base_consumer(&bootstrap, "compat-meta-persist-2");
    let metadata = consumer
        .fetch_metadata(Some(topic), Duration::from_secs(5))
        .unwrap();
    let topic_metadata = find_topic(&metadata, topic);
    assert_eq!(topic_metadata.partitions().len(), 3);
    assert_eq!(topic_metadata.partitions()[0].id(), 0);
    assert_eq!(topic_metadata.partitions()[1].id(), 1);
    assert_eq!(topic_metadata.partitions()[2].id(), 2);

    handle.abort();
    let _ = handle.await;
}
