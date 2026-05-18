use std::time::Duration;

use rdkafka::producer::FutureRecord;
use uuid::Uuid;

use super::{EmptyAssignmentSnapshot, LeaveGroupSnapshot, StaleSyncSnapshot};

use super::protocol;
use super::{bootstrap_available, producer};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_kafka_and_local_broker_match_empty_assignment_sync() {
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

pub(super) async fn empty_assignment_sync_snapshot(
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

    let join_a_v1 = super::protocol::join_group(bootstrap, group_id, None, topic, b"v1");
    let bootstrap_b = bootstrap.to_string();
    let group_b = group_id.to_string();
    let topic_b = topic.to_string();
    let member_b_handle = std::thread::spawn(move || {
        super::protocol::join_group(&bootstrap_b, &group_b, None, &topic_b, b"v2")
    });
    std::thread::sleep(Duration::from_millis(50));
    let join_a_v2 = super::protocol::join_group(
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
        (assigned_member.as_ref(), protocol::encode_assignment(topic)),
    ];

    let leader_sync = protocol::sync_group(
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
    let follower_sync =
        protocol::sync_group(bootstrap, group_id, generation, non_leader, &leader, &[]);

    let (empty_sync, assigned_sync) = if empty_member == leader {
        (leader_sync, follower_sync)
    } else {
        (follower_sync, leader_sync)
    };

    EmptyAssignmentSnapshot {
        empty_member_error: empty_sync.error_code,
        empty_member_assignment_len: empty_sync.assignment.len(),
        empty_member_assignment_decodable: protocol::decode_assignment(&empty_sync.assignment),
        assigned_member_error: assigned_sync.error_code,
        assigned_member_assignment_len: assigned_sync.assignment.len(),
        assigned_member_assignment_decodable: protocol::decode_assignment(
            &assigned_sync.assignment,
        ),
    }
}

pub(super) async fn stale_sync_after_handoff_snapshot(
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

    let join_v1 = super::protocol::join_group(bootstrap, group_id, None, topic, b"v1");
    let initial_assignment = super::protocol::encode_assignment(topic);
    let _sync_v1 = super::protocol::sync_group(
        bootstrap,
        group_id,
        join_v1.generation_id,
        &join_v1.member_id,
        &join_v1.member_id,
        &[(&join_v1.member_id, initial_assignment)],
    );

    std::thread::sleep(Duration::from_millis(1_000));
    let join_v2 =
        super::protocol::join_group_with_timeout(bootstrap, group_id, None, topic, b"v2", 100);

    let generation = join_v2.generation_id;
    let leader = join_v2.leader.clone();
    let assignments = vec![(
        join_v2.member_id.as_ref(),
        super::protocol::encode_assignment(topic),
    )];
    let _leader_sync = super::protocol::sync_group(
        bootstrap,
        group_id,
        generation,
        &leader,
        &leader,
        &assignments,
    );

    let stale_sync = super::protocol::sync_group(
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

pub(super) async fn leave_group_snapshot(
    bootstrap: &str,
    topic: &str,
    group_id: &str,
) -> LeaveGroupSnapshot {
    let producer = producer(bootstrap);
    producer
        .send(
            FutureRecord::to(topic).payload("seed").key("seed-key"),
            Duration::from_secs(10),
        )
        .await
        .unwrap();

    let join = super::protocol::complete_join_group(bootstrap, group_id, topic, b"v1");
    let assignment = super::protocol::encode_assignment(topic);
    let _sync = super::protocol::sync_group(
        bootstrap,
        group_id,
        join.generation_id,
        &join.member_id,
        &join.member_id,
        &[(&join.member_id, assignment)],
    );

    let leave = super::protocol::leave_group(bootstrap, group_id, &join.member_id);
    let post_leave_heartbeat =
        super::protocol::heartbeat(bootstrap, group_id, join.generation_id, &join.member_id);

    LeaveGroupSnapshot {
        leave_error: leave.error_code,
        post_leave_heartbeat_error: post_leave_heartbeat.error_code,
    }
}
