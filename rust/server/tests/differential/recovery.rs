use std::time::Duration;

use rdkafka::Message;
use rdkafka::consumer::Consumer;
use rdkafka::producer::FutureRecord;
use uuid::Uuid;

use super::{bootstrap_available, drive_consumer, group_consumer, poll_for_message, producer};

use super::StartupTopicRecoverySnapshot;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_kafka_and_local_broker_match_subscribe_before_produce_recovery() {
    let Some(real_bootstrap) = std::env::var_os("REAL_KAFKA_BOOTSTRAP") else {
        eprintln!(
            "skipping differential test: set REAL_KAFKA_BOOTSTRAP to a reachable Kafka bootstrap server"
        );
        return;
    };

    let (local_bootstrap, handle, _tempdir) = super::start_local_broker().await;
    let real_bootstrap = real_bootstrap
        .into_string()
        .expect("bootstrap must be utf-8");
    if !bootstrap_available(&real_bootstrap) {
        eprintln!("skipping differential test: bootstrap {real_bootstrap} is unreachable");
        handle.shutdown().await.unwrap();
        return;
    }

    let suffix = Uuid::new_v4().simple().to_string();
    let topic = format!("diff.event-ready.{suffix}.asset-processor");
    let group_id = format!("group.startup-recovery.{suffix}");

    let real_snapshot = subscribe_before_produce_snapshot(&real_bootstrap, &topic, &group_id).await;
    let local_snapshot =
        subscribe_before_produce_snapshot(&local_bootstrap, &topic, &group_id).await;

    handle.shutdown().await.unwrap();

    assert_eq!(local_snapshot, real_snapshot);
}

pub(super) async fn subscribe_before_produce_snapshot(
    bootstrap: &str,
    topic: &str,
    group_id: &str,
) -> StartupTopicRecoverySnapshot {
    let consumer = group_consumer(bootstrap, group_id);
    consumer.subscribe(&[topic]).unwrap();
    drive_consumer(&consumer, Duration::from_secs(2));

    let producer = producer(bootstrap);
    producer
        .send(
            FutureRecord::to(topic)
                .payload("event-ready")
                .key("asset-processor"),
            Duration::from_secs(10),
        )
        .await
        .unwrap();

    let message = poll_for_message(&consumer, Duration::from_secs(10));
    StartupTopicRecoverySnapshot {
        payload: message.payload().unwrap().to_vec(),
        key: message.key().unwrap().to_vec(),
    }
}
