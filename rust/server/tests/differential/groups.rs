use std::time::Duration;

use rdkafka::producer::FutureRecord;

use super::INVALID_PARTITION_INDEX;
use super::{
    CurrentMemberStaleCommitSnapshot, InvalidPartitionSnapshot, StaleCommitSnapshot,
    StaleHeartbeatSnapshot,
};
use super::{producer, protocol};

pub(super) async fn invalid_partition_snapshot(
    bootstrap: &str,
    topic: &str,
) -> InvalidPartitionSnapshot {
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

pub(super) async fn stale_commit_after_handoff_snapshot(
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

    let join_v1 = protocol::join_group_with_timeout(bootstrap, group_id, None, topic, b"v1", 100);
    let assignment = protocol::encode_assignment(topic);
    let _sync_v1 = protocol::sync_group(
        bootstrap,
        group_id,
        join_v1.generation_id,
        &join_v1.member_id,
        &join_v1.member_id,
        &[(&join_v1.member_id, assignment.clone())],
    );
    let initial_commit = protocol::offset_commit(
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
    let join_b_handle = std::thread::spawn(move || {
        protocol::join_group(&bootstrap_b, &group_b, None, &topic_b, b"v2")
    });
    std::thread::sleep(Duration::from_millis(50));
    let join_a_v2 = protocol::join_group(
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
        (
            join_a_v2.member_id.as_ref(),
            protocol::encode_empty_assignment(),
        ),
        (
            join_b_v2.member_id.as_ref(),
            protocol::encode_assignment(topic),
        ),
    ];
    let _leader_sync = protocol::sync_group(
        bootstrap,
        group_id,
        generation,
        &leader,
        &leader,
        &leader_assignments,
    );

    let stale_commit = protocol::offset_commit(
        bootstrap,
        group_id,
        join_v1.generation_id,
        &join_v1.member_id,
        topic,
        0,
        9,
    );
    let offset_after_stale = protocol::offset_fetch(bootstrap, group_id, topic, &[0]);
    let valid_commit = protocol::offset_commit(
        bootstrap,
        group_id,
        generation,
        &join_b_v2.member_id,
        topic,
        0,
        2,
    );
    let offset_after_valid = protocol::offset_fetch(bootstrap, group_id, topic, &[0]);

    StaleCommitSnapshot {
        stale_commit_error: stale_commit.topics[0].partitions[0].error_code,
        offset_after_stale_commit: offset_after_stale.topics[0].partitions[0].committed_offset,
        valid_commit_error: valid_commit.topics[0].partitions[0].error_code,
        offset_after_valid_commit: offset_after_valid.topics[0].partitions[0].committed_offset,
    }
}

pub(super) async fn current_member_stale_commit_snapshot(
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

    let join = protocol::join_group(bootstrap, group_id, None, topic, b"v1");
    let assignment = protocol::encode_assignment(topic);
    let _sync = protocol::sync_group(
        bootstrap,
        group_id,
        join.generation_id,
        &join.member_id,
        &join.member_id,
        &[(&join.member_id, assignment)],
    );

    let current_commit = protocol::offset_commit(
        bootstrap,
        group_id,
        join.generation_id,
        &join.member_id,
        topic,
        0,
        1,
    );
    let stale_commit = protocol::offset_commit(
        bootstrap,
        group_id,
        join.generation_id - 1,
        &join.member_id,
        topic,
        0,
        2,
    );
    let offset_after_stale = protocol::offset_fetch(bootstrap, group_id, topic, &[0]);

    CurrentMemberStaleCommitSnapshot {
        current_commit_error: current_commit.topics[0].partitions[0].error_code,
        stale_commit_error: stale_commit.topics[0].partitions[0].error_code,
        offset_after_stale_commit: offset_after_stale.topics[0].partitions[0].committed_offset,
    }
}

pub(super) async fn stale_heartbeat_after_timeout_snapshot(
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

    let join_v1 = protocol::join_group_with_timeout(bootstrap, group_id, None, topic, b"v1", 100);
    let assignment = protocol::encode_assignment(topic);
    let _sync_v1 = protocol::sync_group(
        bootstrap,
        group_id,
        join_v1.generation_id,
        &join_v1.member_id,
        &join_v1.member_id,
        &[(&join_v1.member_id, assignment.clone())],
    );

    std::thread::sleep(Duration::from_millis(1_000));
    let join_v2 = protocol::join_group_with_timeout(bootstrap, group_id, None, topic, b"v2", 100);
    let _sync_v2 = protocol::sync_group(
        bootstrap,
        group_id,
        join_v2.generation_id,
        &join_v2.member_id,
        &join_v2.member_id,
        &[(&join_v2.member_id, assignment)],
    );

    let stale_heartbeat = protocol::heartbeat(
        bootstrap,
        group_id,
        join_v1.generation_id,
        &join_v1.member_id,
    );
    let valid_commit = protocol::offset_commit(
        bootstrap,
        group_id,
        join_v2.generation_id,
        &join_v2.member_id,
        topic,
        0,
        2,
    );
    let offset_after_valid = protocol::offset_fetch(bootstrap, group_id, topic, &[0]);

    StaleHeartbeatSnapshot {
        stale_heartbeat_error: stale_heartbeat.error_code,
        valid_commit_error: valid_commit.topics[0].partitions[0].error_code,
        offset_after_valid_commit: offset_after_valid.topics[0].partitions[0].committed_offset,
    }
}
