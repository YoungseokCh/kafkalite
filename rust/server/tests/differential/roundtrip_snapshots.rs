use bytes::Bytes;
use kafka_protocol::messages::fetch_request::{FetchPartition, FetchTopic};
use kafka_protocol::messages::{FetchRequest, TopicName};
use kafka_protocol::protocol::StrBytes;
use kafka_protocol::records::RecordBatchDecoder;
use rdkafka::admin::{AdminOptions, NewTopic, TopicReplication};
use rdkafka::consumer::Consumer;
use rdkafka::message::Message;
use rdkafka::producer::FutureRecord;
use rdkafka::topic_partition_list::{Offset, TopicPartitionList};

use super::super::protocol;
use super::super::{
    DIFFERENTIAL_DEFAULT_PARTITIONS, FetchPartitionSnapshot, FetchSnapshot, admin_client, consumer,
    find_topic, group_consumer, poll_for_message, producer, wait_for_topic,
};
use super::super::{
    MetadataSnapshot, MultiPartitionOffsetFetchSnapshot, MultiPartitionRoundtripSnapshot,
    PartitionScopedResumeSnapshot, ProduceConsumeSnapshot, ResumeSnapshot,
};

pub(super) async fn metadata_snapshot(bootstrap: &str, topic: &str) -> MetadataSnapshot {
    let consumer = consumer(bootstrap, &format!("meta-{topic}"));
    let metadata = consumer
        .fetch_metadata(Some(topic), std::time::Duration::from_secs(10))
        .unwrap();
    let topic = find_topic(&metadata, topic);
    MetadataSnapshot {
        topic: topic.name().to_string(),
        partition_count: topic.partitions().len(),
        partition_ids: topic
            .partitions()
            .iter()
            .map(|partition| partition.id())
            .collect(),
    }
}

pub(super) async fn fetch_first_batch_exceeds_partition_budget_snapshot(
    bootstrap: &str,
    topic: &str,
) -> FetchSnapshot {
    create_topic(bootstrap, topic, 1).await;

    let producer = producer(bootstrap);
    let payload = vec![b'x'; 4096];
    producer
        .send(
            FutureRecord::to(topic)
                .payload(&payload)
                .key("oversized")
                .partition(0),
            std::time::Duration::from_secs(10),
        )
        .await
        .unwrap();

    let fetch = protocol::fetch(
        bootstrap,
        FetchRequest::default()
            .with_max_bytes(512)
            .with_topics(vec![
                FetchTopic::default()
                    .with_topic(TopicName(StrBytes::from(topic.to_string())))
                    .with_partitions(vec![
                        FetchPartition::default()
                            .with_partition(0)
                            .with_fetch_offset(0)
                            .with_partition_max_bytes(512),
                    ]),
            ]),
    );
    snapshot_fetch_response(fetch)
}

pub(super) async fn fetch_request_max_bytes_across_partitions_snapshot(
    bootstrap: &str,
    topic: &str,
) -> FetchSnapshot {
    create_topic(bootstrap, topic, 2).await;

    let producer = producer(bootstrap);
    let payload = vec![b'y'; 1024];
    producer
        .send(
            FutureRecord::to(topic)
                .payload(&payload)
                .key("p0")
                .partition(0),
            std::time::Duration::from_secs(10),
        )
        .await
        .unwrap();
    producer
        .send(
            FutureRecord::to(topic)
                .payload(&payload)
                .key("p1")
                .partition(1),
            std::time::Duration::from_secs(10),
        )
        .await
        .unwrap();

    let fetch = protocol::fetch(
        bootstrap,
        FetchRequest::default()
            .with_max_bytes(1600)
            .with_topics(vec![
                FetchTopic::default()
                    .with_topic(TopicName(StrBytes::from(topic.to_string())))
                    .with_partitions(vec![
                        FetchPartition::default()
                            .with_partition(0)
                            .with_fetch_offset(0)
                            .with_partition_max_bytes(1600),
                        FetchPartition::default()
                            .with_partition(1)
                            .with_fetch_offset(0)
                            .with_partition_max_bytes(1600),
                    ]),
            ]),
    );
    snapshot_fetch_response(fetch)
}

pub(super) async fn produce_consume_snapshot(
    bootstrap: &str,
    topic: &str,
) -> ProduceConsumeSnapshot {
    let producer = producer(bootstrap);
    let payload = format!("payload-{topic}");
    let key = format!("key-{topic}");
    let (partition, offset) = producer
        .send(
            FutureRecord::to(topic)
                .payload(&payload)
                .key(&key)
                .partition(0),
            std::time::Duration::from_secs(10),
        )
        .await
        .unwrap();

    let consumer = consumer(bootstrap, &format!("direct-{topic}"));
    wait_for_topic(bootstrap, topic, 1);
    let mut tpl = TopicPartitionList::new();
    tpl.add_partition_offset(topic, partition, Offset::Beginning)
        .unwrap();
    consumer.assign(&tpl).unwrap();
    let message = poll_for_message(&consumer, std::time::Duration::from_secs(10));

    ProduceConsumeSnapshot {
        partition,
        offset,
        payload: message.payload().unwrap().to_vec(),
        key: message.key().unwrap().to_vec(),
    }
}

async fn create_topic(bootstrap: &str, topic: &str, partitions: i32) {
    let admin = admin_client(bootstrap);
    let result = admin
        .create_topics(
            &[NewTopic::new(topic, partitions, TopicReplication::Fixed(1))],
            &AdminOptions::new(),
        )
        .await
        .expect("CreateTopics should return a result");
    assert!(
        result.iter().all(Result::is_ok),
        "topic creation should succeed: {result:?}"
    );
    wait_for_topic(bootstrap, topic, partitions as usize);
}

fn snapshot_fetch_response(fetch: kafka_protocol::messages::FetchResponse) -> FetchSnapshot {
    let mut partitions = fetch
        .responses
        .into_iter()
        .flat_map(|topic| topic.partitions)
        .map(|partition| {
            let payload_len = partition.records.as_ref().map_or(0, bytes::Bytes::len);
            let values = partition
                .records
                .as_ref()
                .map_or_else(Vec::new, decode_fetch_values);
            FetchPartitionSnapshot {
                partition: partition.partition_index,
                error_code: partition.error_code,
                high_watermark: partition.high_watermark,
                record_count: values.len(),
                payload_len,
                values,
            }
        })
        .collect::<Vec<_>>();
    partitions.sort_by_key(|partition| partition.partition);
    FetchSnapshot { partitions }
}

fn decode_fetch_values(payload: &Bytes) -> Vec<Vec<u8>> {
    let mut bytes = payload.clone();
    RecordBatchDecoder::decode_all(&mut bytes)
        .unwrap()
        .into_iter()
        .flat_map(|batch| batch.records)
        .map(|record| record.value.unwrap_or_default().to_vec())
        .collect()
}

pub(super) async fn commit_resume_snapshot(
    bootstrap: &str,
    topic: &str,
    group_id: &str,
) -> ResumeSnapshot {
    let producer = producer(bootstrap);
    let first_payload = format!("first-{topic}");
    let second_payload = format!("second-{topic}");
    for payload in [&first_payload, &second_payload] {
        producer
            .send(
                FutureRecord::to(topic).payload(payload).key("resume-key"),
                std::time::Duration::from_secs(10),
            )
            .await
            .unwrap();
    }

    let consumer = group_consumer(bootstrap, group_id);
    consumer.subscribe(&[topic]).unwrap();
    let first = poll_for_message(&consumer, std::time::Duration::from_secs(10));
    consumer
        .commit_message(&first, rdkafka::consumer::CommitMode::Sync)
        .unwrap();
    let first_bytes = first.payload().unwrap().to_vec();
    drop(first);
    drop(consumer);

    let consumer = group_consumer(bootstrap, group_id);
    consumer.subscribe(&[topic]).unwrap();
    let resumed = poll_for_message(&consumer, std::time::Duration::from_secs(10));
    let resumed_bytes = resumed.payload().unwrap().to_vec();

    ResumeSnapshot {
        first_payload: first_bytes,
        resumed_payload: resumed_bytes,
    }
}

pub(super) async fn multi_partition_roundtrip_snapshot(
    bootstrap: &str,
    topic: &str,
) -> MultiPartitionRoundtripSnapshot {
    let producer = producer(bootstrap);
    let payload_one = format!("one-{topic}");
    let payload_two = format!("two-{topic}");
    producer
        .send(
            FutureRecord::to(topic)
                .payload(&payload_one)
                .key("key")
                .partition(1),
            std::time::Duration::from_secs(10),
        )
        .await
        .unwrap();
    producer
        .send(
            FutureRecord::to(topic)
                .payload(&payload_two)
                .key("key")
                .partition(2),
            std::time::Duration::from_secs(10),
        )
        .await
        .unwrap();

    let consumer = consumer(bootstrap, &format!("multi-{topic}"));
    wait_for_topic(bootstrap, topic, DIFFERENTIAL_DEFAULT_PARTITIONS as usize);
    let mut tpl = TopicPartitionList::new();
    tpl.add_partition_offset(topic, 1, Offset::Beginning)
        .unwrap();
    tpl.add_partition_offset(topic, 2, Offset::Beginning)
        .unwrap();
    consumer.assign(&tpl).unwrap();
    let first = poll_for_message(&consumer, std::time::Duration::from_secs(10));
    let second = poll_for_message(&consumer, std::time::Duration::from_secs(10));

    let mut rows = vec![
        (first.partition(), first.payload().unwrap().to_vec()),
        (second.partition(), second.payload().unwrap().to_vec()),
    ];
    rows.sort_by_key(|row| row.0);
    MultiPartitionRoundtripSnapshot {
        partitions: rows.iter().map(|row| row.0).collect(),
        payloads: rows.into_iter().map(|row| row.1).collect(),
    }
}

pub(super) async fn multi_partition_offset_fetch_snapshot(
    bootstrap: &str,
    topic: &str,
) -> MultiPartitionOffsetFetchSnapshot {
    let producer = producer(bootstrap);
    producer
        .send(
            FutureRecord::to(topic)
                .payload("p1")
                .key("key")
                .partition(1),
            std::time::Duration::from_secs(10),
        )
        .await
        .unwrap();
    producer
        .send(
            FutureRecord::to(topic)
                .payload("p2")
                .key("key")
                .partition(2),
            std::time::Duration::from_secs(10),
        )
        .await
        .unwrap();

    let join = protocol::join_group(bootstrap, &format!("group.{topic}"), None, topic, b"v1");
    let assignment = protocol::encode_assignment_partitions(topic, &[1, 2]);
    let _sync = protocol::sync_group(
        bootstrap,
        &format!("group.{topic}"),
        join.generation_id,
        &join.member_id,
        &join.member_id,
        &[(&join.member_id, assignment)],
    );

    let commit_one = protocol::offset_commit(
        bootstrap,
        &format!("group.{topic}"),
        join.generation_id,
        &join.member_id,
        topic,
        1,
        11,
    );
    let commit_two = protocol::offset_commit(
        bootstrap,
        &format!("group.{topic}"),
        join.generation_id,
        &join.member_id,
        topic,
        2,
        22,
    );
    assert_eq!(commit_one.topics[0].partitions[0].error_code, 0);
    assert_eq!(commit_two.topics[0].partitions[0].error_code, 0);

    let fetched = protocol::offset_fetch(bootstrap, &format!("group.{topic}"), topic, &[1, 2]);
    MultiPartitionOffsetFetchSnapshot {
        partition_1_offset: fetched.topics[0].partitions[0].committed_offset,
        partition_2_offset: fetched.topics[0].partitions[1].committed_offset,
    }
}

pub(super) async fn partition_scoped_resume_snapshot(
    bootstrap: &str,
    topic: &str,
) -> PartitionScopedResumeSnapshot {
    let producer = producer(bootstrap);
    producer
        .send(
            FutureRecord::to(topic)
                .payload("p1-first")
                .key("k")
                .partition(1),
            std::time::Duration::from_secs(10),
        )
        .await
        .unwrap();

    let group_id = format!("group.partition-scoped.{topic}");
    let consumer = group_consumer(bootstrap, &group_id);
    consumer.subscribe(&[topic]).unwrap();
    let first = poll_for_message(&consumer, std::time::Duration::from_secs(10));
    assert_eq!(first.partition(), 1);
    consumer
        .commit_message(&first, rdkafka::consumer::CommitMode::Sync)
        .unwrap();
    drop(first);
    drop(consumer);

    producer
        .send(
            FutureRecord::to(topic)
                .payload("p1-second")
                .key("k")
                .partition(1),
            std::time::Duration::from_secs(10),
        )
        .await
        .unwrap();
    producer
        .send(
            FutureRecord::to(topic)
                .payload("p2-only")
                .key("k")
                .partition(2),
            std::time::Duration::from_secs(10),
        )
        .await
        .unwrap();

    let resumed = group_consumer(bootstrap, &group_id);
    resumed.subscribe(&[topic]).unwrap();
    let first = poll_for_message(&resumed, std::time::Duration::from_secs(10));
    let second = poll_for_message(&resumed, std::time::Duration::from_secs(10));
    let mut rows = vec![
        (first.partition(), first.payload().unwrap().to_vec()),
        (second.partition(), second.payload().unwrap().to_vec()),
    ];
    rows.sort_by_key(|row| row.0);

    PartitionScopedResumeSnapshot { resumed: rows }
}
