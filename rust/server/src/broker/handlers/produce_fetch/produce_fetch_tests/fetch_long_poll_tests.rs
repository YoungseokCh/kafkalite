use super::*;
use kafka_protocol::records::RecordBatchDecoder;
use tokio::time::{Duration, Instant, sleep};

fn tail_fetch_request(topic: &str, partition: i32, offset: i64, max_wait_ms: i32) -> FetchRequest {
    FetchRequest::default()
        .with_min_bytes(1)
        .with_max_wait_ms(max_wait_ms)
        .with_topics(vec![
            kafka_protocol::messages::fetch_request::FetchTopic::default()
                .with_topic(TopicName(StrBytes::from(topic.to_string())))
                .with_partitions(vec![
                    kafka_protocol::messages::fetch_request::FetchPartition::default()
                        .with_partition(partition)
                        .with_fetch_offset(offset)
                        .with_partition_max_bytes(1024),
                ]),
        ])
}

fn limited_fetch_request(
    topic: &str,
    partitions: Vec<(i32, i64, i32)>,
    max_bytes: i32,
    min_bytes: i32,
    max_wait_ms: i32,
) -> FetchRequest {
    FetchRequest::default()
        .with_max_bytes(max_bytes)
        .with_min_bytes(min_bytes)
        .with_max_wait_ms(max_wait_ms)
        .with_topics(vec![
            kafka_protocol::messages::fetch_request::FetchTopic::default()
                .with_topic(TopicName(StrBytes::from(topic.to_string())))
                .with_partitions(
                    partitions
                        .into_iter()
                        .map(|(partition, offset, partition_max_bytes)| {
                            kafka_protocol::messages::fetch_request::FetchPartition::default()
                                .with_partition(partition)
                                .with_fetch_offset(offset)
                                .with_partition_max_bytes(partition_max_bytes)
                        })
                        .collect(),
                ),
        ])
}

fn decoded_record_count(payload: &bytes::Bytes) -> usize {
    let mut bytes = payload.clone();
    RecordBatchDecoder::decode_all(&mut bytes)
        .unwrap()
        .into_iter()
        .flat_map(|batch| batch.records)
        .count()
}

#[tokio::test]
async fn tail_fetch_waits_until_max_wait_ms() {
    let broker = test_broker();
    // Seed one record so the next fetch starts at the current tail and has to wait.
    handle_produce(&broker, produce_request("long.poll.timeout", -1, -1, -1))
        .await
        .unwrap();

    // Fetch from the tail and verify the handler waits close to the requested timeout.
    let started = Instant::now();
    let fetch = handle_fetch(&broker, tail_fetch_request("long.poll.timeout", 0, 1, 80))
        .await
        .unwrap();

    assert!(started.elapsed() >= Duration::from_millis(60));
    assert!(
        fetch.responses[0].partitions[0]
            .records
            .as_ref()
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn tail_fetch_wakes_when_same_partition_receives_records() {
    let broker = test_broker();
    // Seed the partition so the long-poll request starts from the current end offset.
    handle_produce(&broker, produce_request("long.poll.wake", -1, -1, -1))
        .await
        .unwrap();

    // Start a long-poll fetch, then produce to the same partition to trigger wake-up.
    let fetch_broker = broker.clone();
    let fetch_task = tokio::spawn(async move {
        handle_fetch(
            &fetch_broker,
            tail_fetch_request("long.poll.wake", 0, 1, 1_000),
        )
        .await
    });
    sleep(Duration::from_millis(50)).await;

    handle_produce(&broker, produce_request("long.poll.wake", -1, -1, -1))
        .await
        .unwrap();

    let fetch = tokio::time::timeout(Duration::from_millis(500), fetch_task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(
        !fetch.responses[0].partitions[0]
            .records
            .as_ref()
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn tail_fetch_ignores_records_on_other_partitions() {
    let broker = test_broker();
    // Create two partitions and tail only partition 0.
    broker
        .store()
        .ensure_topic("long.poll.partitioned", 2, 0)
        .unwrap();
    handle_produce(
        &broker,
        produce_request_for_partition("long.poll.partitioned", 0, -1, -1, -1),
    )
    .await
    .unwrap();

    let fetch_broker = broker.clone();
    let started = Instant::now();
    let fetch_task = tokio::spawn(async move {
        handle_fetch(
            &fetch_broker,
            tail_fetch_request("long.poll.partitioned", 0, 1, 120),
        )
        .await
    });
    sleep(Duration::from_millis(30)).await;

    // Writing to partition 1 must not wake the fetch waiting on partition 0.
    handle_produce(
        &broker,
        produce_request_for_partition("long.poll.partitioned", 1, -1, -1, -1),
    )
    .await
    .unwrap();

    let fetch = fetch_task.await.unwrap().unwrap();
    assert!(started.elapsed() >= Duration::from_millis(90));
    assert!(
        fetch.responses[0].partitions[0]
            .records
            .as_ref()
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn unknown_topic_fetch_returns_immediately() {
    let broker = test_broker();

    // Unknown partitions should return an error immediately instead of long-polling.
    let started = Instant::now();
    let fetch = handle_fetch(&broker, tail_fetch_request("long.poll.missing", 0, 0, 500))
        .await
        .unwrap();

    assert!(started.elapsed() < Duration::from_millis(100));
    assert_eq!(
        fetch.responses[0].partitions[0].error_code,
        UNKNOWN_TOPIC_OR_PARTITION
    );
}

#[tokio::test]
async fn unknown_topic_fetch_does_not_create_fetch_signal_entries() {
    let broker = test_broker();

    assert_eq!(broker.fetch_signal_count(), 0);

    let fetch = handle_fetch(&broker, tail_fetch_request("long.poll.missing", 0, 0, 500))
        .await
        .unwrap();

    assert_eq!(
        fetch.responses[0].partitions[0].error_code,
        UNKNOWN_TOPIC_OR_PARTITION
    );
    assert_eq!(broker.fetch_signal_count(), 0);
}

#[tokio::test]
async fn zero_fetch_max_bytes_returns_immediately_without_long_polling() {
    let broker = test_broker();
    handle_produce(
        &broker,
        produce_request("long.poll.zero.max.bytes", -1, -1, -1),
    )
    .await
    .unwrap();

    let started = Instant::now();
    let fetch = handle_fetch(
        &broker,
        limited_fetch_request("long.poll.zero.max.bytes", vec![(0, 0, 1024)], 0, 1, 500),
    )
    .await
    .unwrap();

    assert!(started.elapsed() < Duration::from_millis(100));
    assert!(
        fetch.responses[0].partitions[0]
            .records
            .as_ref()
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn duplicate_retry_does_not_notify_fetch_waiters() {
    let broker = test_broker();
    let session = broker.store().init_producer().unwrap();
    let request = produce_request(
        "long.poll.duplicate",
        session.producer_id,
        session.producer_epoch,
        0,
    );
    handle_produce(&broker, request.clone()).await.unwrap();
    let mut receiver = broker.subscribe_fetch_signal("long.poll.duplicate", 0);

    // Retrying the same idempotent batch should not advance visibility or wake waiters.
    handle_produce(&broker, request).await.unwrap();

    assert!(
        tokio::time::timeout(Duration::from_millis(50), receiver.changed())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn partition_max_bytes_returns_only_first_oversized_batch() {
    let broker = test_broker();
    handle_produce(
        &broker,
        produce_request("fetch.bytes.partition", -1, -1, -1),
    )
    .await
    .unwrap();
    handle_produce(
        &broker,
        produce_request("fetch.bytes.partition", -1, -1, -1),
    )
    .await
    .unwrap();

    let fetch = handle_fetch(
        &broker,
        limited_fetch_request("fetch.bytes.partition", vec![(0, 0, 1)], i32::MAX, 0, 0),
    )
    .await
    .unwrap();

    let records = fetch.responses[0].partitions[0].records.as_ref().unwrap();
    assert_eq!(decoded_record_count(records), 1);
}

#[tokio::test]
async fn first_non_empty_partition_may_exceed_request_max_bytes() {
    let broker = test_broker();
    handle_produce(
        &broker,
        produce_request("fetch.bytes.first.partition", -1, -1, -1),
    )
    .await
    .unwrap();

    let fetch = handle_fetch(
        &broker,
        limited_fetch_request("fetch.bytes.first.partition", vec![(0, 0, 1024)], 1, 0, 0),
    )
    .await
    .unwrap();

    // Kafka still returns the first non-empty batch so the client can make progress even when
    // that batch is larger than the total request budget.
    let records = fetch.responses[0].partitions[0].records.as_ref().unwrap();
    assert_eq!(decoded_record_count(records), 1);
}

#[tokio::test]
async fn fetch_max_bytes_limits_later_partitions() {
    let broker = test_broker();
    broker
        .store()
        .ensure_topic("fetch.bytes.total", 2, 0)
        .unwrap();
    handle_produce(
        &broker,
        produce_request_for_partition("fetch.bytes.total", 0, -1, -1, -1),
    )
    .await
    .unwrap();
    handle_produce(
        &broker,
        produce_request_for_partition("fetch.bytes.total", 1, -1, -1, -1),
    )
    .await
    .unwrap();

    let fetch = handle_fetch(
        &broker,
        limited_fetch_request(
            "fetch.bytes.total",
            vec![(0, 0, 1024), (1, 0, 1024)],
            1,
            0,
            0,
        ),
    )
    .await
    .unwrap();

    assert!(
        !fetch.responses[0].partitions[0]
            .records
            .as_ref()
            .unwrap()
            .is_empty()
    );
    assert!(
        fetch.responses[0].partitions[1]
            .records
            .as_ref()
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn later_partitions_do_not_overflow_after_first_batch_consumes_budget() {
    let broker = test_broker();
    broker
        .store()
        .ensure_topic("fetch.bytes.remaining", 2, 0)
        .unwrap();
    handle_produce(
        &broker,
        produce_request_for_partition("fetch.bytes.remaining", 0, -1, -1, -1),
    )
    .await
    .unwrap();
    handle_produce(
        &broker,
        produce_request_for_partition("fetch.bytes.remaining", 1, -1, -1, -1),
    )
    .await
    .unwrap();

    let fetch = handle_fetch(
        &broker,
        limited_fetch_request(
            "fetch.bytes.remaining",
            vec![(0, 0, 1024), (1, 0, 1024)],
            1,
            0,
            0,
        ),
    )
    .await
    .unwrap();

    // Only the first non-empty partition may overflow the total budget. Once that happens, later
    // partitions must respect the exhausted request-wide byte budget and stay empty.
    let first_records = fetch.responses[0].partitions[0].records.as_ref().unwrap();
    let second_records = fetch.responses[0].partitions[1].records.as_ref().unwrap();
    assert_eq!(decoded_record_count(first_records), 1);
    assert!(second_records.is_empty());
}

#[tokio::test]
async fn fetch_waits_when_available_bytes_are_below_min_bytes() {
    let broker = test_broker();
    handle_produce(&broker, produce_request("fetch.bytes.min", -1, -1, -1))
        .await
        .unwrap();

    let started = Instant::now();
    let fetch = handle_fetch(
        &broker,
        limited_fetch_request("fetch.bytes.min", vec![(0, 0, 1024)], 1024, 1024, 80),
    )
    .await
    .unwrap();

    assert!(started.elapsed() >= Duration::from_millis(60));
    assert!(
        !fetch.responses[0].partitions[0]
            .records
            .as_ref()
            .unwrap()
            .is_empty()
    );
}
