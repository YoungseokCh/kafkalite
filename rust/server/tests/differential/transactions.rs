use std::time::Duration;

use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use rdkafka::util::Timeout;

use super::{TransactionCoordinatorSnapshot, TransactionVisibilitySnapshot, producer, protocol};
use kafka_protocol::messages::ApiKey;

fn advertised_version_range(bootstrap: &str, api_key: ApiKey) -> Option<(i16, i16)> {
    protocol::api_versions(bootstrap)
        .api_keys
        .into_iter()
        .find(|api| api.api_key == api_key as i16)
        .map(|api| (api.min_version, api.max_version))
}

fn coordinator_bootstrap(bootstrap: &str, transactional_id: &str) -> (String, i16) {
    let started = std::time::Instant::now();
    loop {
        let find = protocol::find_transaction_coordinator(bootstrap, transactional_id);
        let error_code = find
            .coordinators
            .first()
            .map(|coordinator| coordinator.error_code)
            .unwrap_or(find.error_code);
        if error_code == 0 {
            let coordinator = find
                .coordinators
                .first()
                .expect("transaction coordinator should be present");
            let host = coordinator.host.to_string();
            let endpoint = if host.contains(':') {
                host
            } else {
                format!("{host}:{}", coordinator.port)
            };
            return (endpoint, error_code);
        }
        if started.elapsed() >= Duration::from_secs(10) {
            return (bootstrap.to_string(), error_code);
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}

#[ignore = "diagnostic: Apache Kafka 3.9 KRaft test container does not reliably advertise transaction APIs via ApiVersions"]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_kafka_transaction_api_versions_cover_local_advertised_versions() {
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
    if !super::bootstrap_available(&real_bootstrap) {
        eprintln!("skipping differential test: bootstrap {real_bootstrap} is unreachable");
        handle.abort();
        let _ = handle.await;
        return;
    }

    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let topic = format!("diff.txn.versions.{suffix}");
    let transactional_id = format!("diff.txn.versions.id.{suffix}");
    ensure_topic(&real_bootstrap, &topic).await;
    ensure_topic(&local_bootstrap, &topic).await;
    let real_init = protocol::init_producer_id(&real_bootstrap, Some(&transactional_id), 5_000);
    let local_init = protocol::init_producer_id(&local_bootstrap, Some(&transactional_id), 5_000);
    assert_eq!(
        real_init.error_code, 0,
        "real Kafka should initialize transactional producer"
    );
    assert_eq!(
        local_init.error_code, 0,
        "local broker should initialize transactional producer"
    );

    for api_key in [
        ApiKey::InitProducerId,
        ApiKey::AddPartitionsToTxn,
        ApiKey::EndTxn,
    ] {
        let local = advertised_version_range(&local_bootstrap, api_key)
            .unwrap_or_else(|| panic!("local broker should advertise transaction API {api_key:?}"));
        let real = advertised_version_range(&real_bootstrap, api_key)
            .unwrap_or_else(|| panic!("real broker should advertise transaction API {api_key:?}"));
        assert!(
            local.0 >= real.0 && local.1 <= real.1,
            "local broker advertises {api_key:?} range {local:?}, but real Kafka advertises {real:?}"
        );
    }

    handle.abort();
    let _ = handle.await;
}

pub(super) async fn transaction_coordinator_snapshot(
    bootstrap: &str,
    topic: &str,
    transactional_id: &str,
) -> TransactionCoordinatorSnapshot {
    let producer = producer(bootstrap);
    producer
        .send(
            FutureRecord::to(topic).payload("seed").key("seed-key"),
            Duration::from_secs(10),
        )
        .await
        .unwrap();

    let (coordinator_bootstrap, find_coordinator_error) =
        coordinator_bootstrap(bootstrap, transactional_id);

    let negative_timeout = protocol::init_producer_id(
        &coordinator_bootstrap,
        Some(&format!("{transactional_id}-negative")),
        -1,
    );
    let excessive_timeout = protocol::init_producer_id(
        &coordinator_bootstrap,
        Some(&format!("{transactional_id}-excessive")),
        900_001,
    );
    let first_init =
        protocol::init_producer_id(&coordinator_bootstrap, Some(transactional_id), 5_000);

    let add_valid = protocol::add_partitions_to_txn(
        &coordinator_bootstrap,
        transactional_id,
        first_init.producer_id.0,
        first_init.producer_epoch,
        topic,
        0,
    );
    let add_missing = protocol::add_partitions_to_txn(
        &coordinator_bootstrap,
        &format!("{transactional_id}-missing"),
        9_999,
        0,
        topic,
        0,
    );
    let second_init =
        protocol::init_producer_id(&coordinator_bootstrap, Some(transactional_id), 5_000);
    let stale_end = protocol::end_txn(
        &coordinator_bootstrap,
        transactional_id,
        first_init.producer_id.0,
        first_init.producer_epoch,
        true,
    );

    TransactionCoordinatorSnapshot {
        find_coordinator_error,
        init_negative_timeout_error: negative_timeout.error_code,
        init_excessive_timeout_error: excessive_timeout.error_code,
        init_success_error: first_init.error_code,
        add_valid_top_level_error: add_valid.error_code,
        add_valid_partition_error: add_valid.results_by_transaction[0].topic_results[0]
            .results_by_partition[0]
            .partition_error_code,
        add_missing_txn_top_level_error: add_missing.error_code,
        add_missing_txn_partition_error: add_missing.results_by_transaction[0].topic_results[0]
            .results_by_partition[0]
            .partition_error_code,
        reinit_error: second_init.error_code,
        reused_producer_id: second_init.producer_id == first_init.producer_id,
        epoch_bumped: second_init.producer_epoch > first_init.producer_epoch,
        end_stale_epoch_error: stale_end.error_code,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_kafka_and_local_broker_match_transaction_coordinator_basics() {
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
    if !super::bootstrap_available(&real_bootstrap) {
        eprintln!("skipping differential test: bootstrap {real_bootstrap} is unreachable");
        handle.abort();
        let _ = handle.await;
        return;
    }

    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let topic = format!("diff.txn.topic.{suffix}");
    let transactional_id = format!("diff.txn.id.{suffix}");

    let real_snapshot =
        transaction_coordinator_snapshot(&real_bootstrap, &topic, &transactional_id).await;
    let local_snapshot =
        transaction_coordinator_snapshot(&local_bootstrap, &topic, &transactional_id).await;
    assert_eq!(local_snapshot, real_snapshot);

    handle.abort();
    let _ = handle.await;
}

fn transactional_producer(bootstrap: &str, transactional_id: &str) -> FutureProducer {
    ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .set("message.timeout.ms", "5000")
        .set("enable.idempotence", "true")
        .set("transactional.id", transactional_id)
        .create()
        .unwrap()
}

fn isolation_consumer(bootstrap: &str, group_id: &str, isolation_level: &str) -> BaseConsumer {
    ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .set("group.id", group_id)
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
        .set("isolation.level", isolation_level)
        .create()
        .unwrap()
}

fn admin_client(bootstrap: &str) -> AdminClient<DefaultClientContext> {
    ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .create()
        .unwrap()
}

async fn ensure_topic(bootstrap: &str, topic: &str) {
    let admin = admin_client(bootstrap);
    let _ = admin
        .create_topics(
            &[NewTopic::new(topic, 1, TopicReplication::Fixed(1))],
            &AdminOptions::new(),
        )
        .await
        .unwrap();
    super::wait_for_topic(bootstrap, topic, 1);
}

fn count_visible_messages(
    bootstrap: &str,
    topic: &str,
    group_id: &str,
    isolation_level: &str,
) -> usize {
    let consumer = isolation_consumer(bootstrap, group_id, isolation_level);
    consumer.subscribe(&[topic]).unwrap();
    let started = std::time::Instant::now();
    let mut count = 0usize;
    while started.elapsed() < Duration::from_secs(5) {
        if let Some(result) = consumer.poll(Duration::from_millis(250)) {
            let _ = result.expect("expected Kafka fetch result");
            count += 1;
        }
    }
    count
}

async fn transaction_visibility_snapshot(
    bootstrap: &str,
    committed_topic: &str,
    aborted_topic: &str,
    transactional_id: &str,
) -> TransactionVisibilitySnapshot {
    ensure_topic(bootstrap, committed_topic).await;
    ensure_topic(bootstrap, aborted_topic).await;
    let producer = transactional_producer(bootstrap, transactional_id);
    producer
        .init_transactions(Timeout::After(Duration::from_secs(10)))
        .unwrap();

    producer.begin_transaction().unwrap();
    producer
        .send(
            FutureRecord::to(committed_topic)
                .payload("committed")
                .key("key-committed"),
            Duration::from_secs(10),
        )
        .await
        .unwrap();
    producer
        .commit_transaction(Timeout::After(Duration::from_secs(10)))
        .unwrap();

    producer.begin_transaction().unwrap();
    producer
        .send(
            FutureRecord::to(aborted_topic)
                .payload("aborted")
                .key("key-aborted"),
            Duration::from_secs(10),
        )
        .await
        .unwrap();
    producer
        .abort_transaction(Timeout::After(Duration::from_secs(10)))
        .unwrap();

    TransactionVisibilitySnapshot {
        committed_read_uncommitted_count: count_visible_messages(
            bootstrap,
            committed_topic,
            &format!("group.committed.ru.{transactional_id}"),
            "read_uncommitted",
        ),
        committed_read_committed_count: count_visible_messages(
            bootstrap,
            committed_topic,
            &format!("group.committed.rc.{transactional_id}"),
            "read_committed",
        ),
        aborted_read_uncommitted_count: count_visible_messages(
            bootstrap,
            aborted_topic,
            &format!("group.aborted.ru.{transactional_id}"),
            "read_uncommitted",
        ),
        aborted_read_committed_count: count_visible_messages(
            bootstrap,
            aborted_topic,
            &format!("group.aborted.rc.{transactional_id}"),
            "read_committed",
        ),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_kafka_and_local_broker_match_transaction_visibility() {
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
    if !super::bootstrap_available(&real_bootstrap) {
        eprintln!("skipping differential test: bootstrap {real_bootstrap} is unreachable");
        handle.abort();
        let _ = handle.await;
        return;
    }

    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let committed_topic = format!("diff.txn.committed.{suffix}");
    let aborted_topic = format!("diff.txn.aborted.{suffix}");
    let transactional_id = format!("diff.txn.visibility.{suffix}");

    let real_snapshot = transaction_visibility_snapshot(
        &real_bootstrap,
        &committed_topic,
        &aborted_topic,
        &transactional_id,
    )
    .await;
    let local_snapshot = transaction_visibility_snapshot(
        &local_bootstrap,
        &committed_topic,
        &aborted_topic,
        &transactional_id,
    )
    .await;
    assert_eq!(local_snapshot, real_snapshot);

    handle.abort();
    let _ = handle.await;
}
