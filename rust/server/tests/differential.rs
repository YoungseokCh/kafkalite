use std::net::TcpListener;
use std::sync::Arc;
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use kafka_protocol::messages::consumer_protocol_assignment::TopicPartition as AssignmentTopicPartition;
use kafka_protocol::messages::join_group_request::JoinGroupRequestProtocol;
use kafka_protocol::messages::leave_group_request::MemberIdentity;
use kafka_protocol::messages::offset_commit_request::{
    OffsetCommitRequestPartition, OffsetCommitRequestTopic,
};
use kafka_protocol::messages::offset_fetch_request::OffsetFetchRequestTopic;
use kafka_protocol::messages::sync_group_request::SyncGroupRequestAssignment;
use kafka_protocol::messages::{
    ApiKey, ConsumerProtocolAssignment, ConsumerProtocolSubscription, GroupId, HeartbeatRequest,
    HeartbeatResponse, JoinGroupRequest, JoinGroupResponse, LeaveGroupRequest, LeaveGroupResponse,
    OffsetCommitRequest, OffsetCommitResponse, OffsetFetchRequest, OffsetFetchResponse,
    RequestHeader, ResponseHeader, SyncGroupRequest, SyncGroupResponse, TopicName,
};
use kafka_protocol::protocol::{Decodable, Encodable, StrBytes};
use kafkalite_server::cluster::{
    ControllerQuorumVoter, ProcessRole, UpdatePartitionReplicationRequest,
};
use kafkalite_server::{Config, FileStore, KafkaBroker, protocol};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::message::Message;
use rdkafka::metadata::Metadata;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::topic_partition_list::{Offset, TopicPartitionList};
use tempfile::tempdir;
use uuid::Uuid;

const DIFFERENTIAL_DEFAULT_PARTITIONS: i32 = 3;
const INVALID_PARTITION_INDEX: i32 = 99;

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
struct MultiPartitionRoundtripSnapshot {
    partitions: Vec<i32>,
    payloads: Vec<Vec<u8>>,
}

#[derive(Debug, PartialEq, Eq)]
struct MultiPartitionOffsetFetchSnapshot {
    partition_1_offset: i64,
    partition_2_offset: i64,
}

#[derive(Debug, PartialEq, Eq)]
struct PartitionScopedResumeSnapshot {
    resumed: Vec<(i32, Vec<u8>)>,
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
struct CurrentMemberStaleCommitSnapshot {
    current_commit_error: i16,
    stale_commit_error: i16,
    offset_after_stale_commit: i64,
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

#[derive(Debug, PartialEq, Eq)]
struct EmptyAssignmentSnapshot {
    empty_member_error: i16,
    empty_member_assignment_len: usize,
    empty_member_assignment_decodable: bool,
    assigned_member_error: i16,
    assigned_member_assignment_len: usize,
    assigned_member_assignment_decodable: bool,
}

#[derive(Debug, PartialEq, Eq)]
struct LeaveGroupSnapshot {
    leave_error: i16,
    post_leave_heartbeat_error: i16,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_kafka_and_local_broker_match_supported_roundtrips() {
    let Some(real_bootstrap) = std::env::var_os("REAL_KAFKA_BOOTSTRAP") else {
        eprintln!(
            "skipping differential test: set REAL_KAFKA_BOOTSTRAP to a reachable Kafka bootstrap server"
        );
        return;
    };

    let (local_bootstrap, handle, _tempdir) = start_local_broker().await;
    let real_bootstrap = real_bootstrap
        .into_string()
        .expect("bootstrap must be utf-8");
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

    let multi_roundtrip_topic = format!("diff.multi-roundtrip.{suffix}");
    let real_multi_roundtrip =
        multi_partition_roundtrip_snapshot(&real_bootstrap, &multi_roundtrip_topic).await;
    let local_multi_roundtrip =
        multi_partition_roundtrip_snapshot(&local_bootstrap, &multi_roundtrip_topic).await;
    assert_eq!(local_multi_roundtrip, real_multi_roundtrip);

    let real_resume =
        commit_resume_snapshot(&real_bootstrap, &resume_topic, &format!("group.{suffix}")).await;
    let local_resume =
        commit_resume_snapshot(&local_bootstrap, &resume_topic, &format!("group.{suffix}")).await;
    assert_eq!(local_resume, real_resume);

    let invalid_partition_topic = format!("diff.invalid-partition.{suffix}");
    let real_invalid = invalid_partition_snapshot(&real_bootstrap, &invalid_partition_topic).await;
    let local_invalid =
        invalid_partition_snapshot(&local_bootstrap, &invalid_partition_topic).await;
    assert_eq!(local_invalid, real_invalid);

    let stale_commit_topic = format!("diff.stale-commit.{suffix}");
    let real_stale_commit = stale_commit_after_handoff_snapshot(
        &real_bootstrap,
        &stale_commit_topic,
        &format!("group.stale.{suffix}"),
    )
    .await;
    let local_stale_commit = stale_commit_after_handoff_snapshot(
        &local_bootstrap,
        &stale_commit_topic,
        &format!("group.stale.{suffix}"),
    )
    .await;
    assert_eq!(local_stale_commit, real_stale_commit);

    let current_member_stale_commit_topic = format!("diff.current-member-stale-commit.{suffix}");
    let real_current_member_stale_commit = current_member_stale_commit_snapshot(
        &real_bootstrap,
        &current_member_stale_commit_topic,
        &format!("group.current-stale.{suffix}"),
    )
    .await;
    let local_current_member_stale_commit = current_member_stale_commit_snapshot(
        &local_bootstrap,
        &current_member_stale_commit_topic,
        &format!("group.current-stale.{suffix}"),
    )
    .await;
    assert_eq!(
        local_current_member_stale_commit,
        real_current_member_stale_commit
    );

    let offset_fetch_topic = format!("diff.multi-offsets.{suffix}");
    let real_offset_fetch =
        multi_partition_offset_fetch_snapshot(&real_bootstrap, &offset_fetch_topic).await;
    let local_offset_fetch =
        multi_partition_offset_fetch_snapshot(&local_bootstrap, &offset_fetch_topic).await;
    assert_eq!(local_offset_fetch, real_offset_fetch);

    let partition_scoped_topic = format!("diff.partition-scoped-resume.{suffix}");
    let real_partition_scoped_resume =
        partition_scoped_resume_snapshot(&real_bootstrap, &partition_scoped_topic).await;
    let local_partition_scoped_resume =
        partition_scoped_resume_snapshot(&local_bootstrap, &partition_scoped_topic).await;
    assert_eq!(local_partition_scoped_resume, real_partition_scoped_resume);

    let heartbeat_topic = format!("diff.stale-heartbeat.{suffix}");
    let real_stale_heartbeat = stale_heartbeat_after_timeout_snapshot(
        &real_bootstrap,
        &heartbeat_topic,
        &format!("group.heartbeat.{suffix}"),
    )
    .await;
    let local_stale_heartbeat = stale_heartbeat_after_timeout_snapshot(
        &local_bootstrap,
        &heartbeat_topic,
        &format!("group.heartbeat.{suffix}"),
    )
    .await;
    assert_eq!(local_stale_heartbeat, real_stale_heartbeat);

    let stale_sync_topic = format!("diff.stale-sync.{suffix}");
    let real_stale_sync = stale_sync_after_handoff_snapshot(
        &real_bootstrap,
        &stale_sync_topic,
        &format!("group.sync.{suffix}"),
    )
    .await;
    let local_stale_sync = stale_sync_after_handoff_snapshot(
        &local_bootstrap,
        &stale_sync_topic,
        &format!("group.sync.{suffix}"),
    )
    .await;
    assert_eq!(local_stale_sync, real_stale_sync);

    let empty_assignment_topic = format!("diff.empty-assignment.{suffix}");
    let real_empty_assignment = empty_assignment_sync_snapshot(
        &real_bootstrap,
        &empty_assignment_topic,
        &format!("group.empty-assignment.{suffix}"),
    )
    .await;
    let local_empty_assignment = empty_assignment_sync_snapshot(
        &local_bootstrap,
        &empty_assignment_topic,
        &format!("group.empty-assignment.{suffix}"),
    )
    .await;
    assert_eq!(local_empty_assignment, real_empty_assignment);

    let leave_group_topic = format!("diff.leave-group.{suffix}");
    let real_leave_group = leave_group_snapshot(
        &real_bootstrap,
        &leave_group_topic,
        &format!("group.leave.{suffix}"),
    )
    .await;
    let local_leave_group = leave_group_snapshot(
        &local_bootstrap,
        &leave_group_topic,
        &format!("group.leave.{suffix}"),
    )
    .await;
    assert_eq!(local_leave_group, real_leave_group);

    handle.abort();
    let _ = handle.await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn metadata_reads_are_side_effect_free_for_existing_topics() {
    let tempdir = tempdir().unwrap();
    let port = free_port();
    let mut config = Config::single_node(tempdir.path().join("kafkalite-data"), port, 1);
    config.cluster.node_id = 1;
    config.cluster.process_roles = vec![ProcessRole::Broker, ProcessRole::Controller];
    config.cluster.controller_quorum_voters = vec![
        ControllerQuorumVoter {
            node_id: 1,
            host: "node1".to_string(),
            port: 9093,
        },
        ControllerQuorumVoter {
            node_id: 2,
            host: "node2".to_string(),
            port: 9094,
        },
    ];
    let store = Arc::new(FileStore::open(&config.storage.data_dir).unwrap());
    let broker = KafkaBroker::new(config, store).unwrap();
    let broker_handle = broker.clone();
    let handle = tokio::spawn(async move { broker_handle.run().await });
    tokio::time::sleep(Duration::from_millis(150)).await;
    let bootstrap = format!("127.0.0.1:{port}");

    broker
        .store()
        .ensure_topic("diff.side-effect", 1, 0)
        .unwrap();
    let metadata = broker
        .store()
        .topic_metadata(Some(&["diff.side-effect".to_string()]), 0)
        .unwrap();
    broker.sync_topic_metadata(&metadata).unwrap();
    broker
        .cluster()
        .handle_update_partition_replication(UpdatePartitionReplicationRequest {
            topic_name: "diff.side-effect".to_string(),
            partition_index: 0,
            replicas: vec![1, 2],
            isr: vec![1],
            leader_epoch: 3,
        })
        .unwrap();
    let before = broker.cluster().metadata_image();

    let consumer = consumer(&bootstrap, "metadata-side-effect");
    let _ = consumer
        .fetch_metadata(Some("diff.side-effect"), Duration::from_secs(5))
        .unwrap();

    let after = broker.cluster().metadata_image();
    assert_eq!(
        before.topics[0].partitions[0].replicas,
        after.topics[0].partitions[0].replicas
    );
    assert_eq!(
        before.topics[0].partitions[0].isr,
        after.topics[0].partitions[0].isr
    );
    assert_eq!(
        before.topics[0].partitions[0].leader_epoch,
        after.topics[0].partitions[0].leader_epoch
    );

    handle.abort();
    let _ = handle.await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_kafka_and_local_broker_match_empty_assignment_sync() {
    let Some(real_bootstrap) = std::env::var_os("REAL_KAFKA_BOOTSTRAP") else {
        eprintln!(
            "skipping differential test: set REAL_KAFKA_BOOTSTRAP to a reachable Kafka bootstrap server"
        );
        return;
    };

    let (local_bootstrap, handle, _tempdir) = start_local_broker().await;
    let real_bootstrap = real_bootstrap
        .into_string()
        .expect("bootstrap must be utf-8");
    if !bootstrap_available(&real_bootstrap) {
        eprintln!("skipping differential test: bootstrap {real_bootstrap} is unreachable");
        handle.abort();
        let _ = handle.await;
        return;
    }

    let suffix = Uuid::new_v4().simple().to_string();
    let topic = format!("diff.empty-assignment-only.{suffix}");
    let group_id = format!("group.empty-assignment-only.{suffix}");

    let real_snapshot = empty_assignment_sync_snapshot(&real_bootstrap, &topic, &group_id).await;
    let local_snapshot = empty_assignment_sync_snapshot(&local_bootstrap, &topic, &group_id).await;

    handle.abort();
    let _ = handle.await;

    assert_eq!(local_snapshot, real_snapshot);
}

async fn invalid_partition_snapshot(bootstrap: &str, topic: &str) -> InvalidPartitionSnapshot {
    let producer = producer(bootstrap);
    producer
        .send(
            FutureRecord::to(topic).payload("seed").key("seed-key"),
            Duration::from_secs(10),
        )
        .await
        .unwrap();

    let error = producer
        .send(
            FutureRecord::to(topic)
                .payload("bad")
                .key("bad-key")
                .partition(INVALID_PARTITION_INDEX),
            Duration::from_secs(10),
        )
        .await
        .expect_err("invalid partition should fail");

    InvalidPartitionSnapshot {
        error: format!("{:?}", error.0),
    }
}

async fn stale_commit_after_handoff_snapshot(
    bootstrap: &str,
    topic: &str,
    group_id: &str,
) -> StaleCommitSnapshot {
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
    let initial_commit = offset_commit(
        bootstrap,
        group_id,
        join_v1.generation_id,
        &join_v1.member_id,
        topic,
        0,
        1,
    );
    assert_eq!(initial_commit.topics[0].partitions[0].error_code, 0);

    let bootstrap_b = bootstrap.to_string();
    let group_b = group_id.to_string();
    let topic_b = topic.to_string();
    let join_b_handle =
        std::thread::spawn(move || join_group(&bootstrap_b, &group_b, None, &topic_b, b"v2"));
    std::thread::sleep(Duration::from_millis(50));
    let join_a_v2 = join_group(
        bootstrap,
        group_id,
        Some(join_v1.member_id.as_ref()),
        topic,
        b"v1",
    );
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
    let _leader_sync = sync_group(
        bootstrap,
        group_id,
        generation,
        &leader,
        &leader,
        &leader_assignments,
    );

    let stale_commit = offset_commit(
        bootstrap,
        group_id,
        join_v1.generation_id,
        &join_v1.member_id,
        topic,
        0,
        9,
    );
    let offset_after_stale = offset_fetch(bootstrap, group_id, topic, &[0]);
    let valid_commit = offset_commit(
        bootstrap,
        group_id,
        generation,
        &join_b_v2.member_id,
        topic,
        0,
        2,
    );
    let offset_after_valid = offset_fetch(bootstrap, group_id, topic, &[0]);

    StaleCommitSnapshot {
        stale_commit_error: stale_commit.topics[0].partitions[0].error_code,
        offset_after_stale_commit: offset_after_stale.topics[0].partitions[0].committed_offset,
        valid_commit_error: valid_commit.topics[0].partitions[0].error_code,
        offset_after_valid_commit: offset_after_valid.topics[0].partitions[0].committed_offset,
    }
}

async fn current_member_stale_commit_snapshot(
    bootstrap: &str,
    topic: &str,
    group_id: &str,
) -> CurrentMemberStaleCommitSnapshot {
    let producer = producer(bootstrap);
    producer
        .send(
            FutureRecord::to(topic).payload("seed").key("seed-key"),
            Duration::from_secs(10),
        )
        .await
        .unwrap();

    let join = join_group(bootstrap, group_id, None, topic, b"v1");
    let assignment = encode_assignment(topic);
    let _sync = sync_group(
        bootstrap,
        group_id,
        join.generation_id,
        &join.member_id,
        &join.member_id,
        &[(&join.member_id, assignment)],
    );

    let current_commit = offset_commit(
        bootstrap,
        group_id,
        join.generation_id,
        &join.member_id,
        topic,
        0,
        1,
    );
    let stale_commit = offset_commit(
        bootstrap,
        group_id,
        join.generation_id - 1,
        &join.member_id,
        topic,
        0,
        2,
    );
    let offset_after_stale = offset_fetch(bootstrap, group_id, topic, &[0]);

    CurrentMemberStaleCommitSnapshot {
        current_commit_error: current_commit.topics[0].partitions[0].error_code,
        stale_commit_error: stale_commit.topics[0].partitions[0].error_code,
        offset_after_stale_commit: offset_after_stale.topics[0].partitions[0].committed_offset,
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

    let stale_heartbeat = heartbeat(
        bootstrap,
        group_id,
        join_v1.generation_id,
        &join_v1.member_id,
    );
    let valid_commit = offset_commit(
        bootstrap,
        group_id,
        join_v2.generation_id,
        &join_v2.member_id,
        topic,
        0,
        2,
    );
    let offset_after_valid = offset_fetch(bootstrap, group_id, topic, &[0]);

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
    let _leader_sync = sync_group(
        bootstrap,
        group_id,
        generation,
        &leader,
        &leader,
        &assignments,
    );

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

async fn empty_assignment_sync_snapshot(
    bootstrap: &str,
    topic: &str,
    group_id: &str,
) -> EmptyAssignmentSnapshot {
    let producer = producer(bootstrap);
    producer
        .send(
            FutureRecord::to(topic).payload("seed").key("seed-key"),
            Duration::from_secs(10),
        )
        .await
        .unwrap();

    let join_a_v1 = join_group(bootstrap, group_id, None, topic, b"v1");
    let bootstrap_b = bootstrap.to_string();
    let group_b = group_id.to_string();
    let topic_b = topic.to_string();
    let member_b_handle =
        std::thread::spawn(move || join_group(&bootstrap_b, &group_b, None, &topic_b, b"v2"));
    std::thread::sleep(Duration::from_millis(50));
    let join_a_v2 = join_group(
        bootstrap,
        group_id,
        Some(join_a_v1.member_id.as_ref()),
        topic,
        b"v1",
    );
    let join_b_v2 = member_b_handle.join().unwrap();

    let generation = join_a_v2.generation_id;
    let member_a = join_a_v2.member_id.clone();
    let member_b = join_b_v2.member_id.clone();
    let leader = if join_a_v2.leader == member_a {
        member_a.clone()
    } else {
        join_b_v2.leader.clone()
    };
    let assigned_member = if leader == member_a {
        member_b.clone()
    } else {
        member_a.clone()
    };
    let empty_member = if assigned_member == member_a {
        member_b.clone()
    } else {
        member_a.clone()
    };
    let assignments = vec![
        (empty_member.as_ref(), Vec::new()),
        (assigned_member.as_ref(), encode_assignment(topic)),
    ];

    let leader_sync = sync_group(
        bootstrap,
        group_id,
        generation,
        &leader,
        &leader,
        &assignments,
    );
    let non_leader = if leader == member_a {
        &member_b
    } else {
        &member_a
    };
    let follower_sync = sync_group(bootstrap, group_id, generation, non_leader, &leader, &[]);

    let (empty_sync, assigned_sync) = if empty_member == leader {
        (leader_sync, follower_sync)
    } else {
        (follower_sync, leader_sync)
    };

    EmptyAssignmentSnapshot {
        empty_member_error: empty_sync.error_code,
        empty_member_assignment_len: empty_sync.assignment.len(),
        empty_member_assignment_decodable: decode_assignment(&empty_sync.assignment),
        assigned_member_error: assigned_sync.error_code,
        assigned_member_assignment_len: assigned_sync.assignment.len(),
        assigned_member_assignment_decodable: decode_assignment(&assigned_sync.assignment),
    }
}

async fn leave_group_snapshot(bootstrap: &str, topic: &str, group_id: &str) -> LeaveGroupSnapshot {
    let producer = producer(bootstrap);
    producer
        .send(
            FutureRecord::to(topic).payload("seed").key("seed-key"),
            Duration::from_secs(10),
        )
        .await
        .unwrap();

    let join = complete_join_group(bootstrap, group_id, topic, b"v1");
    let assignment = encode_assignment(topic);
    let _sync = sync_group(
        bootstrap,
        group_id,
        join.generation_id,
        &join.member_id,
        &join.member_id,
        &[(&join.member_id, assignment)],
    );

    let leave = leave_group(bootstrap, group_id, &join.member_id);
    let post_leave_heartbeat = heartbeat(bootstrap, group_id, join.generation_id, &join.member_id);

    LeaveGroupSnapshot {
        leave_error: leave.error_code,
        post_leave_heartbeat_error: post_leave_heartbeat.error_code,
    }
}

async fn metadata_snapshot(bootstrap: &str, topic: &str) -> MetadataSnapshot {
    let consumer = consumer(bootstrap, &format!("meta-{topic}"));
    let metadata = consumer
        .fetch_metadata(Some(topic), Duration::from_secs(10))
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

async fn produce_consume_snapshot(bootstrap: &str, topic: &str) -> ProduceConsumeSnapshot {
    let producer = producer(bootstrap);
    let payload = format!("payload-{topic}");
    let key = format!("key-{topic}");
    let (partition, offset) = producer
        .send(
            FutureRecord::to(topic)
                .payload(&payload)
                .key(&key)
                .partition(0),
            Duration::from_secs(10),
        )
        .await
        .unwrap();

    let consumer = consumer(bootstrap, &format!("direct-{topic}"));
    wait_for_topic(bootstrap, topic, 1);
    let mut tpl = TopicPartitionList::new();
    tpl.add_partition_offset(topic, partition, Offset::Beginning)
        .unwrap();
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
    consumer
        .commit_message(&first, rdkafka::consumer::CommitMode::Sync)
        .unwrap();
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

async fn multi_partition_roundtrip_snapshot(
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
            Duration::from_secs(10),
        )
        .await
        .unwrap();
    producer
        .send(
            FutureRecord::to(topic)
                .payload(&payload_two)
                .key("key")
                .partition(2),
            Duration::from_secs(10),
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
    let first = poll_for_message(&consumer, Duration::from_secs(10));
    let second = poll_for_message(&consumer, Duration::from_secs(10));

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

async fn multi_partition_offset_fetch_snapshot(
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
            Duration::from_secs(10),
        )
        .await
        .unwrap();
    producer
        .send(
            FutureRecord::to(topic)
                .payload("p2")
                .key("key")
                .partition(2),
            Duration::from_secs(10),
        )
        .await
        .unwrap();

    let join = join_group(bootstrap, &format!("group.{topic}"), None, topic, b"v1");
    let assignment = encode_assignment_partitions(topic, &[1, 2]);
    let _sync = sync_group(
        bootstrap,
        &format!("group.{topic}"),
        join.generation_id,
        &join.member_id,
        &join.member_id,
        &[(&join.member_id, assignment)],
    );

    let commit_one = offset_commit(
        bootstrap,
        &format!("group.{topic}"),
        join.generation_id,
        &join.member_id,
        topic,
        1,
        11,
    );
    let commit_two = offset_commit(
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

    let fetched = offset_fetch(bootstrap, &format!("group.{topic}"), topic, &[1, 2]);
    MultiPartitionOffsetFetchSnapshot {
        partition_1_offset: fetched.topics[0].partitions[0].committed_offset,
        partition_2_offset: fetched.topics[0].partitions[1].committed_offset,
    }
}

async fn partition_scoped_resume_snapshot(
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
            Duration::from_secs(10),
        )
        .await
        .unwrap();

    let group_id = format!("group.partition-scoped.{topic}");
    let consumer = group_consumer(bootstrap, &group_id);
    consumer.subscribe(&[topic]).unwrap();
    let first = poll_for_message(&consumer, Duration::from_secs(10));
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
            Duration::from_secs(10),
        )
        .await
        .unwrap();
    producer
        .send(
            FutureRecord::to(topic)
                .payload("p2-only")
                .key("k")
                .partition(2),
            Duration::from_secs(10),
        )
        .await
        .unwrap();

    let resumed = group_consumer(bootstrap, &group_id);
    resumed.subscribe(&[topic]).unwrap();
    let first = poll_for_message(&resumed, Duration::from_secs(10));
    let second = poll_for_message(&resumed, Duration::from_secs(10));
    let mut rows = vec![
        (first.partition(), first.payload().unwrap().to_vec()),
        (second.partition(), second.payload().unwrap().to_vec()),
    ];
    rows.sort_by_key(|row| row.0);

    PartitionScopedResumeSnapshot { resumed: rows }
}

async fn start_local_broker() -> (
    String,
    tokio::task::JoinHandle<anyhow::Result<()>>,
    tempfile::TempDir,
) {
    let tempdir = tempdir().unwrap();
    let port = free_port();
    let config = Config::single_node(
        tempdir.path().join("kafkalite-data"),
        port,
        DIFFERENTIAL_DEFAULT_PARTITIONS,
    );
    let store = Arc::new(FileStore::open(&config.storage.data_dir).unwrap());
    let broker = KafkaBroker::new(config, store).unwrap();
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

fn poll_for_message(
    consumer: &BaseConsumer,
    timeout: Duration,
) -> rdkafka::message::BorrowedMessage<'_> {
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
    consumer
        .fetch_metadata(None, Duration::from_secs(2))
        .is_ok()
}

fn wait_for_topic(bootstrap: &str, topic: &str, expected_partition_count: usize) {
    let consumer = consumer(bootstrap, &format!("wait-{topic}"));
    let started = std::time::Instant::now();
    while started.elapsed() < Duration::from_secs(10) {
        if let Ok(metadata) = consumer.fetch_metadata(Some(topic), Duration::from_secs(1))
            && metadata
                .topics()
                .iter()
                .find(|metadata_topic| metadata_topic.name() == topic)
                .is_some_and(|metadata_topic| {
                    metadata_topic.partitions().len() >= expected_partition_count
                })
        {
            return;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    panic!("topic {topic} did not become visible with {expected_partition_count} partitions");
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

fn complete_join_group(
    bootstrap: &str,
    group_id: &str,
    topic: &str,
    user_data: &[u8],
) -> JoinGroupResponse {
    const MEMBER_ID_REQUIRED: i16 = 79;

    let joined = join_group(bootstrap, group_id, None, topic, user_data);
    if joined.error_code == MEMBER_ID_REQUIRED {
        return join_group(
            bootstrap,
            group_id,
            Some(joined.member_id.as_ref()),
            topic,
            user_data,
        );
    }
    joined
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

fn leave_group(bootstrap: &str, group_id: &str, member_id: &str) -> LeaveGroupResponse {
    send_request::<LeaveGroupRequest, LeaveGroupResponse>(
        bootstrap,
        ApiKey::LeaveGroup,
        protocol::LEAVE_GROUP_VERSION,
        LeaveGroupRequest::default()
            .with_group_id(GroupId(StrBytes::from(group_id.to_string())))
            .with_members(vec![
                MemberIdentity::default()
                    .with_member_id(StrBytes::from(member_id.to_string()))
                    .with_group_instance_id(None),
            ]),
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
            .with_assignments(if member_id == leader_member_id {
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
            }),
    )
}

fn offset_commit(
    bootstrap: &str,
    group_id: &str,
    generation_id: i32,
    member_id: &str,
    topic: &str,
    partition: i32,
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
                            .with_partition_index(partition)
                            .with_committed_offset(next_offset),
                    ]),
            ]),
    )
}

fn offset_fetch(
    bootstrap: &str,
    group_id: &str,
    topic: &str,
    partitions: &[i32],
) -> OffsetFetchResponse {
    send_request::<OffsetFetchRequest, OffsetFetchResponse>(
        bootstrap,
        ApiKey::OffsetFetch,
        protocol::OFFSET_FETCH_VERSION,
        OffsetFetchRequest::default()
            .with_group_id(GroupId(StrBytes::from(group_id.to_string())))
            .with_topics(Some(vec![
                OffsetFetchRequestTopic::default()
                    .with_name(TopicName(StrBytes::from(topic.to_string())))
                    .with_partition_indexes(partitions.to_vec()),
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
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    stream
        .set_write_timeout(Some(Duration::from_secs(10)))
        .unwrap();

    let mut payload = BytesMut::new();
    RequestHeader::default()
        .with_request_api_key(api_key as i16)
        .with_request_api_version(api_version)
        .with_correlation_id(1)
        .with_client_id(Some(StrBytes::from("differential".to_string())))
        .encode(&mut payload, api_key.request_header_version(api_version))
        .unwrap();
    request.encode(&mut payload, api_version).unwrap();

    stream
        .write_all(&(payload.len() as i32).to_be_bytes())
        .unwrap();
    stream.write_all(payload.as_ref()).unwrap();

    let mut size = [0_u8; 4];
    stream.read_exact(&mut size).unwrap();
    let size = i32::from_be_bytes(size) as usize;
    let mut body = vec![0_u8; size];
    stream.read_exact(&mut body).unwrap();
    let mut bytes = Bytes::from(body);
    let _ =
        ResponseHeader::decode(&mut bytes, api_key.response_header_version(api_version)).unwrap();
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
    encode_assignment_partitions(topic, &[0])
}

fn encode_assignment_partitions(topic: &str, partitions: &[i32]) -> Vec<u8> {
    let assignment = ConsumerProtocolAssignment::default().with_assigned_partitions(vec![
        AssignmentTopicPartition::default()
            .with_topic(TopicName(StrBytes::from(topic.to_string())))
            .with_partitions(partitions.to_vec()),
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

fn decode_assignment(bytes: &[u8]) -> bool {
    let mut payload = Bytes::copy_from_slice(bytes);
    ConsumerProtocolAssignment::decode(&mut payload, 3).is_ok()
}
