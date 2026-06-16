use std::time::Duration;

use kafkalite_server::BrokerHandle;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::message::Message;
use rdkafka::producer::FutureRecord;
use rdkafka::topic_partition_list::{Offset, TopicPartitionList};
use tempfile::tempdir;

mod support;

use support::{
    base_consumer, init_test_logging, poll_for_message, producer, start_broker,
    start_broker_in_dir, start_broker_in_dir_with_partitions,
};

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

    drop(message);
    drop(consumer);
    drop(producer);
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
    drop(producer);
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

    drop(persisted);
    drop(direct);
    drop(next);
    drop(resumed);
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

    drop(message);
    drop(direct);
    drop(metadata_consumer);
    drop(producer);
    stop_broker(handle).await;
}

async fn stop_broker(handle: BrokerHandle) {
    handle.shutdown().await.unwrap();
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
