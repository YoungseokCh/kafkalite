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
use rdkafka::metadata::Metadata;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::topic_partition_list::{Offset, TopicPartitionList};
use tempfile::tempdir;
use uuid::Uuid;

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
struct ResumeSnapshot {
    first_payload: Vec<u8>,
    resumed_payload: Vec<u8>,
}

#[derive(Debug, PartialEq, Eq)]
struct InvalidPartitionSnapshot {
    error: String,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_kafka_and_local_broker_match_supported_roundtrips() {
    let Some(real_bootstrap) = std::env::var_os("REAL_KAFKA_BOOTSTRAP") else {
        eprintln!("skipping differential test: set REAL_KAFKA_BOOTSTRAP to a reachable Kafka bootstrap server");
        return;
    };

    let (local_bootstrap, handle, _tempdir) = start_local_broker().await;
    let real_bootstrap = real_bootstrap.into_string().expect("bootstrap must be utf-8");
    if !bootstrap_available(&real_bootstrap) {
        eprintln!("skipping differential test: bootstrap {real_bootstrap} is unreachable");
        handle.abort();
        let _ = handle.await;
        return;
    }
    let suffix = Uuid::new_v4().simple().to_string();

    let metadata_topic = format!("diff.metadata.{suffix}");
    let roundtrip_topic = format!("diff.roundtrip.{suffix}");
    let resume_topic = format!("diff.resume.{suffix}");

    let real_metadata = metadata_snapshot(&real_bootstrap, &metadata_topic).await;
    let local_metadata = metadata_snapshot(&local_bootstrap, &metadata_topic).await;
    assert_eq!(local_metadata, real_metadata);

    let real_roundtrip = produce_consume_snapshot(&real_bootstrap, &roundtrip_topic).await;
    let local_roundtrip = produce_consume_snapshot(&local_bootstrap, &roundtrip_topic).await;
    assert_eq!(local_roundtrip, real_roundtrip);

    let real_resume = commit_resume_snapshot(&real_bootstrap, &resume_topic, &format!("group.{suffix}")).await;
    let local_resume = commit_resume_snapshot(&local_bootstrap, &resume_topic, &format!("group.{suffix}")).await;
    assert_eq!(local_resume, real_resume);

    let invalid_partition_topic = format!("diff.invalid-partition.{suffix}");
    let real_invalid = invalid_partition_snapshot(&real_bootstrap, &invalid_partition_topic).await;
    let local_invalid = invalid_partition_snapshot(&local_bootstrap, &invalid_partition_topic).await;
    assert_eq!(local_invalid, real_invalid);

    handle.abort();
    let _ = handle.await;
}

async fn invalid_partition_snapshot(bootstrap: &str, topic: &str) -> InvalidPartitionSnapshot {
    let producer = producer(bootstrap);
    producer
        .send(
            FutureRecord::to(topic)
                .payload("seed")
                .key("seed-key"),
            Duration::from_secs(10),
        )
        .await
        .unwrap();

    let error = producer
        .send(
            FutureRecord::to(topic)
                .payload("bad")
                .key("bad-key")
                .partition(1),
            Duration::from_secs(10),
        )
        .await
        .expect_err("partition 1 should fail");

    InvalidPartitionSnapshot {
        error: format!("{:?}", error.0),
    }
}

async fn metadata_snapshot(bootstrap: &str, topic: &str) -> MetadataSnapshot {
    let consumer = consumer(bootstrap, &format!("meta-{topic}"));
    let metadata = consumer.fetch_metadata(Some(topic), Duration::from_secs(10)).unwrap();
    let topic = find_topic(&metadata, topic);
    MetadataSnapshot {
        topic: topic.name().to_string(),
        partition_count: topic.partitions().len(),
        partition_ids: topic.partitions().iter().map(|partition| partition.id()).collect(),
    }
}

async fn produce_consume_snapshot(bootstrap: &str, topic: &str) -> ProduceConsumeSnapshot {
    let producer = producer(bootstrap);
    let payload = format!("payload-{topic}");
    let key = format!("key-{topic}");
    let (partition, offset) = producer
        .send(
            FutureRecord::to(topic).payload(&payload).key(&key),
            Duration::from_secs(10),
        )
        .await
        .unwrap();

    let consumer = consumer(bootstrap, &format!("direct-{topic}"));
    let mut tpl = TopicPartitionList::new();
    tpl.add_partition_offset(topic, 0, Offset::Beginning).unwrap();
    consumer.assign(&tpl).unwrap();
    let message = poll_for_message(&consumer, Duration::from_secs(10));

    ProduceConsumeSnapshot {
        partition,
        offset,
        payload: message.payload().unwrap().to_vec(),
        key: message.key().unwrap().to_vec(),
    }
}

async fn commit_resume_snapshot(bootstrap: &str, topic: &str, group_id: &str) -> ResumeSnapshot {
    let producer = producer(bootstrap);
    let first_payload = format!("first-{topic}");
    let second_payload = format!("second-{topic}");
    for payload in [&first_payload, &second_payload] {
        producer
            .send(
                FutureRecord::to(topic).payload(payload).key("resume-key"),
                Duration::from_secs(10),
            )
            .await
            .unwrap();
    }

    let consumer = group_consumer(bootstrap, group_id);
    consumer.subscribe(&[topic]).unwrap();
    let first = poll_for_message(&consumer, Duration::from_secs(10));
    consumer.commit_message(&first, rdkafka::consumer::CommitMode::Sync).unwrap();
    let first_bytes = first.payload().unwrap().to_vec();
    drop(first);
    drop(consumer);

    let consumer = group_consumer(bootstrap, group_id);
    consumer.subscribe(&[topic]).unwrap();
    let resumed = poll_for_message(&consumer, Duration::from_secs(10));
    let resumed_bytes = resumed.payload().unwrap().to_vec();

    ResumeSnapshot {
        first_payload: first_bytes,
        resumed_payload: resumed_bytes,
    }
}

async fn start_local_broker() -> (String, tokio::task::JoinHandle<anyhow::Result<()>>, tempfile::TempDir) {
    let tempdir = tempdir().unwrap();
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
    (format!("127.0.0.1:{port}"), handle, tempdir)
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

fn group_consumer(bootstrap: &str, group_id: &str) -> BaseConsumer {
    ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .set("group.id", group_id)
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
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

fn find_topic<'a>(metadata: &'a Metadata, name: &str) -> &'a rdkafka::metadata::MetadataTopic {
    metadata
        .topics()
        .iter()
        .find(|topic| topic.name() == name)
        .expect("topic metadata should exist")
}

fn bootstrap_available(bootstrap: &str) -> bool {
    let consumer = consumer(bootstrap, "bootstrap-probe");
    consumer.fetch_metadata(None, Duration::from_secs(2)).is_ok()
}
