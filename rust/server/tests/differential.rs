use std::net::TcpListener;
use std::sync::Arc;
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use kafkalite_server::{
    Config, FileStore, KafkaBroker,
    config::{BrokerConfig, StorageConfig},
    protocol,
};
use kafka_protocol::messages::consumer_protocol_assignment::TopicPartition as AssignmentTopicPartition;
use kafka_protocol::messages::join_group_request::JoinGroupRequestProtocol;
use kafka_protocol::messages::offset_commit_request::{
    OffsetCommitRequestPartition, OffsetCommitRequestTopic,
};
use kafka_protocol::messages::offset_fetch_request::OffsetFetchRequestTopic;
use kafka_protocol::messages::sync_group_request::SyncGroupRequestAssignment;
use kafka_protocol::messages::{
    ApiKey, ConsumerProtocolAssignment, ConsumerProtocolSubscription, GroupId,
    HeartbeatRequest, HeartbeatResponse, JoinGroupRequest, JoinGroupResponse,
    OffsetCommitRequest, OffsetCommitResponse, OffsetFetchRequest, OffsetFetchResponse,
    RequestHeader, ResponseHeader, SyncGroupRequest, SyncGroupResponse, TopicName,
};
use kafka_protocol::protocol::{Decodable, Encodable, StrBytes};
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

#[derive(Debug, PartialEq, Eq)]
struct StaleCommitSnapshot {
    stale_commit_error: i16,
    offset_after_stale_commit: i64,
    valid_commit_error: i16,
    offset_after_valid_commit: i64,
}

#[derive(Debug, PartialEq, Eq)]
struct StaleHeartbeatSnapshot {
    stale_heartbeat_error: i16,
    valid_commit_error: i16,
    offset_after_valid_commit: i64,
}

#[derive(Debug, PartialEq, Eq)]
struct StaleSyncSnapshot {
    stale_sync_error: i16,
    stale_sync_assignment_len: usize,
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

    let stale_commit_topic = format!("diff.stale-commit.{suffix}");
    let real_stale_commit = stale_commit_after_handoff_snapshot(&real_bootstrap, &stale_commit_topic, &format!("group.stale.{suffix}")).await;
    let local_stale_commit = stale_commit_after_handoff_snapshot(&local_bootstrap, &stale_commit_topic, &format!("group.stale.{suffix}")).await;
    assert_eq!(local_stale_commit, real_stale_commit);

    let heartbeat_topic = format!("diff.stale-heartbeat.{suffix}");
    let real_stale_heartbeat = stale_heartbeat_after_timeout_snapshot(&real_bootstrap, &heartbeat_topic, &format!("group.heartbeat.{suffix}")).await;
    let local_stale_heartbeat = stale_heartbeat_after_timeout_snapshot(&local_bootstrap, &heartbeat_topic, &format!("group.heartbeat.{suffix}")).await;
    assert_eq!(local_stale_heartbeat, real_stale_heartbeat);

    let stale_sync_topic = format!("diff.stale-sync.{suffix}");
    let real_stale_sync = stale_sync_after_handoff_snapshot(&real_bootstrap, &stale_sync_topic, &format!("group.sync.{suffix}")).await;
    let local_stale_sync = stale_sync_after_handoff_snapshot(&local_bootstrap, &stale_sync_topic, &format!("group.sync.{suffix}")).await;
    assert_eq!(local_stale_sync, real_stale_sync);

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

async fn stale_commit_after_handoff_snapshot(bootstrap: &str, topic: &str, group_id: &str) -> StaleCommitSnapshot {
    let producer = producer(bootstrap);
    producer
        .send(
            FutureRecord::to(topic).payload("seed").key("seed-key"),
            Duration::from_secs(10),
        )
        .await
        .unwrap();

    let join_v1 = join_group_with_timeout(bootstrap, group_id, None, topic, b"v1", 100);
    let assignment = encode_assignment(topic);
    let _sync_v1 = sync_group(bootstrap, group_id, join_v1.generation_id, &join_v1.member_id, &join_v1.member_id, &[(&join_v1.member_id, assignment.clone())]);
    let initial_commit = offset_commit(bootstrap, group_id, join_v1.generation_id, &join_v1.member_id, topic, 1);
    assert_eq!(initial_commit.topics[0].partitions[0].error_code, 0);

    let bootstrap_b = bootstrap.to_string();
    let group_b = group_id.to_string();
    let topic_b = topic.to_string();
    let join_b_handle = std::thread::spawn(move || join_group(&bootstrap_b, &group_b, None, &topic_b, b"v2"));
    std::thread::sleep(Duration::from_millis(50));
    let join_a_v2 = join_group(bootstrap, group_id, Some(join_v1.member_id.as_ref()), topic, b"v1");
    let join_b_v2 = join_b_handle.join().unwrap();

    let generation = join_a_v2.generation_id;
    let leader = if join_a_v2.leader == join_a_v2.member_id {
        join_a_v2.member_id.clone()
    } else {
        join_b_v2.leader.clone()
    };
    let leader_assignments = vec![
        (join_a_v2.member_id.as_ref(), encode_empty_assignment()),
        (join_b_v2.member_id.as_ref(), encode_assignment(topic)),
    ];
    let _leader_sync = sync_group(bootstrap, group_id, generation, &leader, &leader, &leader_assignments);

    let stale_commit = offset_commit(bootstrap, group_id, join_v1.generation_id, &join_v1.member_id, topic, 9);
    let offset_after_stale = offset_fetch(bootstrap, group_id, topic);
    let valid_commit = offset_commit(bootstrap, group_id, generation, &join_b_v2.member_id, topic, 2);
    let offset_after_valid = offset_fetch(bootstrap, group_id, topic);

    StaleCommitSnapshot {
        stale_commit_error: stale_commit.topics[0].partitions[0].error_code,
        offset_after_stale_commit: offset_after_stale.topics[0].partitions[0].committed_offset,
        valid_commit_error: valid_commit.topics[0].partitions[0].error_code,
        offset_after_valid_commit: offset_after_valid.topics[0].partitions[0].committed_offset,
    }
}

async fn stale_heartbeat_after_timeout_snapshot(
    bootstrap: &str,
    topic: &str,
    group_id: &str,
) -> StaleHeartbeatSnapshot {
    let producer = producer(bootstrap);
    producer
        .send(
            FutureRecord::to(topic).payload("seed").key("seed-key"),
            Duration::from_secs(10),
        )
        .await
        .unwrap();

    let join_v1 = join_group_with_timeout(bootstrap, group_id, None, topic, b"v1", 100);
    let assignment = encode_assignment(topic);
    let _sync_v1 = sync_group(
        bootstrap,
        group_id,
        join_v1.generation_id,
        &join_v1.member_id,
        &join_v1.member_id,
        &[(&join_v1.member_id, assignment.clone())],
    );

    std::thread::sleep(Duration::from_millis(1_000));
    let join_v2 = join_group_with_timeout(bootstrap, group_id, None, topic, b"v2", 100);
    let _sync_v2 = sync_group(
        bootstrap,
        group_id,
        join_v2.generation_id,
        &join_v2.member_id,
        &join_v2.member_id,
        &[(&join_v2.member_id, assignment)],
    );

    let stale_heartbeat = heartbeat(bootstrap, group_id, join_v1.generation_id, &join_v1.member_id);
    let valid_commit = offset_commit(bootstrap, group_id, join_v2.generation_id, &join_v2.member_id, topic, 2);
    let offset_after_valid = offset_fetch(bootstrap, group_id, topic);

    StaleHeartbeatSnapshot {
        stale_heartbeat_error: stale_heartbeat.error_code,
        valid_commit_error: valid_commit.topics[0].partitions[0].error_code,
        offset_after_valid_commit: offset_after_valid.topics[0].partitions[0].committed_offset,
    }
}

async fn stale_sync_after_handoff_snapshot(
    bootstrap: &str,
    topic: &str,
    group_id: &str,
) -> StaleSyncSnapshot {
    let producer = producer(bootstrap);
    producer
        .send(
            FutureRecord::to(topic).payload("seed").key("seed-key"),
            Duration::from_secs(10),
        )
        .await
        .unwrap();

    let join_v1 = join_group(bootstrap, group_id, None, topic, b"v1");
    let initial_assignment = encode_assignment(topic);
    let _sync_v1 = sync_group(
        bootstrap,
        group_id,
        join_v1.generation_id,
        &join_v1.member_id,
        &join_v1.member_id,
        &[(&join_v1.member_id, initial_assignment)],
    );

    std::thread::sleep(Duration::from_millis(1_000));
    let join_v2 = join_group_with_timeout(bootstrap, group_id, None, topic, b"v2", 100);

    let generation = join_v2.generation_id;
    let leader = join_v2.leader.clone();
    let assignments = vec![(join_v2.member_id.as_ref(), encode_assignment(topic))];
    let _leader_sync = sync_group(bootstrap, group_id, generation, &leader, &leader, &assignments);

    let stale_sync = sync_group(
        bootstrap,
        group_id,
        join_v1.generation_id,
        &join_v1.member_id,
        &leader,
        &[],
    );
    StaleSyncSnapshot {
        stale_sync_error: stale_sync.error_code,
        stale_sync_assignment_len: stale_sync.assignment.len(),
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
            data_dir: tempdir.path().join("kafkalite-data"),
        },
    };
    let store = Arc::new(FileStore::open(&config.storage.data_dir).unwrap());
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

fn join_group(
    bootstrap: &str,
    group_id: &str,
    member_id: Option<&str>,
    topic: &str,
    user_data: &[u8],
) -> JoinGroupResponse {
    join_group_with_timeout(bootstrap, group_id, member_id, topic, user_data, 5_000)
}

fn join_group_with_timeout(
    bootstrap: &str,
    group_id: &str,
    member_id: Option<&str>,
    topic: &str,
    user_data: &[u8],
    timeout_ms: i32,
) -> JoinGroupResponse {
    send_request::<JoinGroupRequest, JoinGroupResponse>(
        bootstrap,
        ApiKey::JoinGroup,
        protocol::JOIN_GROUP_VERSION,
        JoinGroupRequest::default()
            .with_group_id(GroupId(StrBytes::from(group_id.to_string())))
            .with_session_timeout_ms(timeout_ms)
            .with_rebalance_timeout_ms(timeout_ms)
            .with_member_id(StrBytes::from(member_id.unwrap_or("").to_string()))
            .with_protocol_type(StrBytes::from("consumer".to_string()))
            .with_protocols(vec![
                JoinGroupRequestProtocol::default()
                    .with_name(StrBytes::from("range".to_string()))
                    .with_metadata(Bytes::from(encode_subscription(topic, user_data))),
            ]),
    )
}

fn heartbeat(
    bootstrap: &str,
    group_id: &str,
    generation_id: i32,
    member_id: &str,
) -> HeartbeatResponse {
    send_request::<HeartbeatRequest, HeartbeatResponse>(
        bootstrap,
        ApiKey::Heartbeat,
        protocol::HEARTBEAT_VERSION,
        HeartbeatRequest::default()
            .with_group_id(GroupId(StrBytes::from(group_id.to_string())))
            .with_generation_id(generation_id)
            .with_member_id(StrBytes::from(member_id.to_string())),
    )
}

fn sync_group(
    bootstrap: &str,
    group_id: &str,
    generation_id: i32,
    member_id: &str,
    leader_member_id: &str,
    assignments: &[(&str, Vec<u8>)],
) -> SyncGroupResponse {
    send_request::<SyncGroupRequest, SyncGroupResponse>(
        bootstrap,
        ApiKey::SyncGroup,
        protocol::SYNC_GROUP_VERSION,
        SyncGroupRequest::default()
            .with_group_id(GroupId(StrBytes::from(group_id.to_string())))
            .with_generation_id(generation_id)
            .with_member_id(StrBytes::from(member_id.to_string()))
            .with_protocol_type(Some(StrBytes::from("consumer".to_string())))
            .with_protocol_name(Some(StrBytes::from("range".to_string())))
            .with_assignments(
                if member_id == leader_member_id {
                    assignments
                        .iter()
                        .map(|(member, assignment)| {
                            SyncGroupRequestAssignment::default()
                                .with_member_id(StrBytes::from((*member).to_string()))
                                .with_assignment(Bytes::from(assignment.clone()))
                        })
                        .collect()
                } else {
                    vec![]
                },
            ),
    )
}

fn offset_commit(
    bootstrap: &str,
    group_id: &str,
    generation_id: i32,
    member_id: &str,
    topic: &str,
    next_offset: i64,
) -> OffsetCommitResponse {
    send_request::<OffsetCommitRequest, OffsetCommitResponse>(
        bootstrap,
        ApiKey::OffsetCommit,
        protocol::OFFSET_COMMIT_VERSION,
        OffsetCommitRequest::default()
            .with_group_id(GroupId(StrBytes::from(group_id.to_string())))
            .with_generation_id_or_member_epoch(generation_id)
            .with_member_id(StrBytes::from(member_id.to_string()))
            .with_group_instance_id(None)
            .with_topics(vec![
                OffsetCommitRequestTopic::default()
                    .with_name(TopicName(StrBytes::from(topic.to_string())))
                    .with_partitions(vec![
                        OffsetCommitRequestPartition::default()
                            .with_partition_index(0)
                            .with_committed_offset(next_offset),
                    ]),
            ]),
    )
}

fn offset_fetch(bootstrap: &str, group_id: &str, topic: &str) -> OffsetFetchResponse {
    send_request::<OffsetFetchRequest, OffsetFetchResponse>(
        bootstrap,
        ApiKey::OffsetFetch,
        protocol::OFFSET_FETCH_VERSION,
        OffsetFetchRequest::default()
            .with_group_id(GroupId(StrBytes::from(group_id.to_string())))
            .with_topics(Some(vec![
                OffsetFetchRequestTopic::default()
                    .with_name(TopicName(StrBytes::from(topic.to_string())))
                    .with_partition_indexes(vec![0]),
            ])),
    )
}

fn send_request<TReq: Encodable, TResp: Decodable>(
    bootstrap: &str,
    api_key: ApiKey,
    api_version: i16,
    request: TReq,
) -> TResp {
    use std::io::{Read, Write};

    let mut stream = std::net::TcpStream::connect(bootstrap).unwrap();
    stream.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
    stream.set_write_timeout(Some(Duration::from_secs(10))).unwrap();

    let mut payload = BytesMut::new();
    RequestHeader::default()
        .with_request_api_key(api_key as i16)
        .with_request_api_version(api_version)
        .with_correlation_id(1)
        .with_client_id(Some(StrBytes::from("differential".to_string())))
        .encode(&mut payload, api_key.request_header_version(api_version))
        .unwrap();
    request.encode(&mut payload, api_version).unwrap();

    stream.write_all(&(payload.len() as i32).to_be_bytes()).unwrap();
    stream.write_all(payload.as_ref()).unwrap();

    let mut size = [0_u8; 4];
    stream.read_exact(&mut size).unwrap();
    let size = i32::from_be_bytes(size) as usize;
    let mut body = vec![0_u8; size];
    stream.read_exact(&mut body).unwrap();
    let mut bytes = Bytes::from(body);
    let _ = ResponseHeader::decode(&mut bytes, api_key.response_header_version(api_version)).unwrap();
    TResp::decode(&mut bytes, api_version).unwrap()
}

fn encode_subscription(topic: &str, user_data: &[u8]) -> Vec<u8> {
    let subscription = ConsumerProtocolSubscription::default()
        .with_topics(vec![StrBytes::from(topic.to_string())])
        .with_user_data(Some(Bytes::copy_from_slice(user_data)));
    let mut bytes = BytesMut::new();
    subscription.encode(&mut bytes, 3).unwrap();
    bytes.to_vec()
}

fn encode_assignment(topic: &str) -> Vec<u8> {
    let assignment = ConsumerProtocolAssignment::default().with_assigned_partitions(vec![
        AssignmentTopicPartition::default()
            .with_topic(TopicName(StrBytes::from(topic.to_string())))
            .with_partitions(vec![0]),
    ]);
    let mut bytes = BytesMut::new();
    assignment.encode(&mut bytes, 3).unwrap();
    bytes.to_vec()
}

fn encode_empty_assignment() -> Vec<u8> {
    let assignment = ConsumerProtocolAssignment::default().with_assigned_partitions(vec![]);
    let mut bytes = BytesMut::new();
    assignment.encode(&mut bytes, 3).unwrap();
    bytes.to_vec()
}
