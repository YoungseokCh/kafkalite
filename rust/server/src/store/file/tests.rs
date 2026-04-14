use bytes::{Bytes, BytesMut};
use kafka_protocol::messages::{ConsumerProtocolAssignment, ConsumerProtocolSubscription};
use kafka_protocol::protocol::{Decodable, Encodable, StrBytes};
use std::io::Write;
use tempfile::tempdir;

use super::*;
use crate::store::{GroupJoinRequest, OffsetCommitRequest, StoreError};

#[test]
fn appends_and_fetches_records() {
    let dir = tempdir().unwrap();
    let store = FileStore::open(dir.path()).unwrap();
    let producer = store.init_producer(10).unwrap();
    let records = vec![BrokerRecord {
        offset: 0,
        timestamp_ms: 10,
        producer_id: producer.producer_id,
        producer_epoch: producer.producer_epoch,
        sequence: 0,
        key: Some(Bytes::from_static(b"key")),
        value: Some(Bytes::from_static(b"value")),
        headers_json: b"[]".to_vec(),
    }];
    let (base, last) = store
        .append_records("test.events", 0, &records, 10)
        .unwrap();
    assert_eq!((base, last), (0, 0));
    let fetched = store.fetch_records("test.events", 0, 0, 10).unwrap();
    assert_eq!(fetched.high_watermark, 1);
    assert_eq!(fetched.records.len(), 1);
}

#[test]
fn fetch_from_later_offset_uses_index_and_returns_tail_records() {
    let dir = tempdir().unwrap();
    let store = FileStore::open(dir.path()).unwrap();
    let producer = store.init_producer(10).unwrap();
    let records = (0..5)
        .map(|sequence| BrokerRecord {
            offset: 0,
            timestamp_ms: 10 + i64::from(sequence),
            producer_id: producer.producer_id,
            producer_epoch: producer.producer_epoch,
            sequence,
            key: Some(Bytes::from_static(b"key")),
            value: Some(Bytes::from(vec![b'a' + sequence as u8])),
            headers_json: b"[]".to_vec(),
        })
        .collect::<Vec<_>>();
    store
        .append_records("tail.events", 0, &records, 10)
        .unwrap();

    let fetched = store.fetch_records("tail.events", 0, 3, 10).unwrap();
    assert_eq!(fetched.records.len(), 2);
    assert_eq!(fetched.records[0].offset, 3);
    assert_eq!(fetched.records[1].offset, 4);
}

#[test]
fn assignment_respects_member_subscriptions() {
    let dir = tempdir().unwrap();
    let store = FileStore::open(dir.path()).unwrap();
    store.ensure_topic("topic-a", 1, 10).unwrap();
    store.ensure_topic("topic-b", 1, 10).unwrap();
    store.ensure_topic("topic-c", 1, 10).unwrap();
    let subscription_a = encode_subscription(&["topic-a"]);
    let subscription_b = encode_subscription(&["topic-b", "topic-c"]);
    let _ = store
        .join_group(GroupJoinRequest {
            group_id: "group-a",
            member_id: Some("member-a"),
            protocol_type: "consumer",
            protocol_name: "range",
            metadata: &subscription_a,
            session_timeout_ms: 5_000,
            rebalance_timeout_ms: 5_000,
            now_ms: 100,
        })
        .unwrap();
    let member_b = store
        .join_group(GroupJoinRequest {
            group_id: "group-a",
            member_id: Some("member-b"),
            protocol_type: "consumer",
            protocol_name: "range",
            metadata: &subscription_b,
            session_timeout_ms: 5_000,
            rebalance_timeout_ms: 5_000,
            now_ms: 200,
        })
        .unwrap();
    let sync_a = store
        .sync_group(
            "group-a",
            "member-a",
            member_b.generation_id,
            "range",
            &[],
            300,
        )
        .unwrap();
    let sync_b = store
        .sync_group(
            "group-a",
            "member-b",
            member_b.generation_id,
            "range",
            &[],
            300,
        )
        .unwrap();
    assert_eq!(
        decode_assignment_topics(&sync_a.assignment),
        vec!["topic-a"]
    );
    assert_eq!(
        decode_assignment_topics(&sync_b.assignment),
        vec!["topic-b", "topic-c"]
    );
}

#[test]
fn offset_commit_requires_current_member_but_allows_stale_generation_for_same_member() {
    let dir = tempdir().unwrap();
    let store = FileStore::open(dir.path()).unwrap();
    store.ensure_topic("topic-a", 1, 10).unwrap();
    let subscription = encode_subscription(&["topic-a"]);
    let joined = store
        .join_group(GroupJoinRequest {
            group_id: "group-b",
            member_id: Some("member-a"),
            protocol_type: "consumer",
            protocol_name: "range",
            metadata: &subscription,
            session_timeout_ms: 5_000,
            rebalance_timeout_ms: 5_000,
            now_ms: 100,
        })
        .unwrap();
    store
        .commit_offset(commit_request(
            "group-b",
            "member-a",
            joined.generation_id,
            "topic-a",
            0,
            1,
            200,
        ))
        .unwrap();
    let stale = store.commit_offset(commit_request(
        "group-b",
        "member-a",
        joined.generation_id - 1,
        "topic-a",
        0,
        2,
        300,
    ));
    assert!(stale.is_ok());
    let future = store.commit_offset(commit_request(
        "group-b",
        "member-a",
        joined.generation_id + 1,
        "topic-a",
        0,
        3,
        300,
    ));
    assert!(matches!(future, Err(StoreError::StaleGeneration { .. })));
    let unknown = store.commit_offset(commit_request(
        "group-b",
        "member-b",
        joined.generation_id,
        "topic-a",
        0,
        2,
        300,
    ));
    assert!(matches!(unknown, Err(StoreError::UnknownMember { .. })));
}

#[test]
fn group_membership_is_soft_across_restart_but_offsets_remain_durable() {
    let dir = tempdir().unwrap();
    let store = FileStore::open(dir.path()).unwrap();
    store.ensure_topic("topic-a", 1, 10).unwrap();
    let subscription = encode_subscription(&["topic-a"]);
    let joined = store
        .join_group(GroupJoinRequest {
            group_id: "group-soft",
            member_id: Some("member-a"),
            protocol_type: "consumer",
            protocol_name: "range",
            metadata: &subscription,
            session_timeout_ms: 5_000,
            rebalance_timeout_ms: 5_000,
            now_ms: 100,
        })
        .unwrap();
    store
        .commit_offset(commit_request(
            "group-soft",
            "member-a",
            joined.generation_id,
            "topic-a",
            0,
            1,
            200,
        ))
        .unwrap();

    let reopened = FileStore::open(dir.path()).unwrap();
    assert_eq!(
        reopened.fetch_offset("group-soft", "topic-a", 0).unwrap(),
        Some(1)
    );

    let stale_runtime_member = reopened.commit_offset(commit_request(
        "group-soft",
        "member-a",
        joined.generation_id,
        "topic-a",
        0,
        2,
        300,
    ));
    assert!(matches!(
        stale_runtime_member,
        Err(StoreError::UnknownMember { .. })
    ));
}

#[test]
fn heartbeat_does_not_grow_state_journal_but_offset_commit_does() {
    let dir = tempdir().unwrap();
    let store = FileStore::open(dir.path()).unwrap();
    store.ensure_topic("topic-a", 1, 10).unwrap();
    let subscription = encode_subscription(&["topic-a"]);
    let joined = store
        .join_group(GroupJoinRequest {
            group_id: "group-journal",
            member_id: Some("member-a"),
            protocol_type: "consumer",
            protocol_name: "range",
            metadata: &subscription,
            session_timeout_ms: 5_000,
            rebalance_timeout_ms: 5_000,
            now_ms: 100,
        })
        .unwrap();
    let journal_path = dir.path().join("state/state.journal");

    let after_join = std::fs::metadata(&journal_path).unwrap().len();
    store
        .heartbeat("group-journal", "member-a", joined.generation_id, 200)
        .unwrap();
    let after_heartbeat = std::fs::metadata(&journal_path).unwrap().len();
    assert_eq!(after_join, 0);
    assert_eq!(after_heartbeat, after_join);

    store
        .commit_offset(commit_request(
            "group-journal",
            "member-a",
            joined.generation_id,
            "topic-a",
            0,
            1,
            300,
        ))
        .unwrap();
    let after_commit = std::fs::metadata(&journal_path).unwrap().len();
    assert!(after_commit > after_heartbeat);
}

#[test]
fn no_op_rejoin_keeps_generation() {
    let dir = tempdir().unwrap();
    let store = FileStore::open(dir.path()).unwrap();
    let subscription = encode_subscription(&["topic-a"]);
    let first = store
        .join_group(GroupJoinRequest {
            group_id: "group-c",
            member_id: Some("member-a"),
            protocol_type: "consumer",
            protocol_name: "range",
            metadata: &subscription,
            session_timeout_ms: 5_000,
            rebalance_timeout_ms: 5_000,
            now_ms: 100,
        })
        .unwrap();
    let second = store
        .join_group(GroupJoinRequest {
            group_id: "group-c",
            member_id: Some("member-a"),
            protocol_type: "consumer",
            protocol_name: "range",
            metadata: &subscription,
            session_timeout_ms: 5_000,
            rebalance_timeout_ms: 5_000,
            now_ms: 200,
        })
        .unwrap();
    assert_eq!(first.generation_id, second.generation_id);
}

#[test]
fn expired_member_is_pruned_on_next_join() {
    let dir = tempdir().unwrap();
    let store = FileStore::open(dir.path()).unwrap();
    store.ensure_topic("topic-a", 1, 10).unwrap();
    let subscription = encode_subscription(&["topic-a"]);
    let _ = store
        .join_group(GroupJoinRequest {
            group_id: "group-d",
            member_id: Some("member-a"),
            protocol_type: "consumer",
            protocol_name: "range",
            metadata: &subscription,
            session_timeout_ms: 10,
            rebalance_timeout_ms: 10,
            now_ms: 100,
        })
        .unwrap();
    let second = store
        .join_group(GroupJoinRequest {
            group_id: "group-d",
            member_id: Some("member-b"),
            protocol_type: "consumer",
            protocol_name: "range",
            metadata: &subscription,
            session_timeout_ms: 10,
            rebalance_timeout_ms: 10,
            now_ms: 200,
        })
        .unwrap();
    let sync = store
        .sync_group(
            "group-d",
            "member-b",
            second.generation_id,
            "range",
            &[],
            210,
        )
        .unwrap();
    assert_eq!(decode_assignment_topics(&sync.assignment), vec!["topic-a"]);
    assert!(
        store
            .heartbeat("group-d", "member-a", second.generation_id, 220)
            .is_err()
    );
}

#[test]
fn offsets_are_committed_and_fetched_per_partition() {
    let dir = tempdir().unwrap();
    let store = FileStore::open(dir.path()).unwrap();
    store.ensure_topic("topic-a", 3, 10).unwrap();
    let subscription = encode_subscription(&["topic-a"]);
    let joined = store
        .join_group(GroupJoinRequest {
            group_id: "group-partitions",
            member_id: Some("member-a"),
            protocol_type: "consumer",
            protocol_name: "range",
            metadata: &subscription,
            session_timeout_ms: 5_000,
            rebalance_timeout_ms: 5_000,
            now_ms: 100,
        })
        .unwrap();

    store
        .commit_offset(commit_request(
            "group-partitions",
            "member-a",
            joined.generation_id,
            "topic-a",
            1,
            11,
            200,
        ))
        .unwrap();
    store
        .commit_offset(commit_request(
            "group-partitions",
            "member-a",
            joined.generation_id,
            "topic-a",
            2,
            22,
            210,
        ))
        .unwrap();

    assert_eq!(
        store
            .fetch_offset("group-partitions", "topic-a", 0)
            .unwrap(),
        None
    );
    assert_eq!(
        store
            .fetch_offset("group-partitions", "topic-a", 1)
            .unwrap(),
        Some(11)
    );
    assert_eq!(
        store
            .fetch_offset("group-partitions", "topic-a", 2)
            .unwrap(),
        Some(22)
    );
}

#[test]
fn assignments_split_topic_partitions_across_members() {
    let dir = tempdir().unwrap();
    let store = FileStore::open(dir.path()).unwrap();
    store.ensure_topic("topic-a", 4, 10).unwrap();
    let subscription = encode_subscription(&["topic-a"]);
    let _first = store
        .join_group(GroupJoinRequest {
            group_id: "group-range",
            member_id: Some("member-a"),
            protocol_type: "consumer",
            protocol_name: "range",
            metadata: &subscription,
            session_timeout_ms: 5_000,
            rebalance_timeout_ms: 5_000,
            now_ms: 100,
        })
        .unwrap();
    let second = store
        .join_group(GroupJoinRequest {
            group_id: "group-range",
            member_id: Some("member-b"),
            protocol_type: "consumer",
            protocol_name: "range",
            metadata: &subscription,
            session_timeout_ms: 5_000,
            rebalance_timeout_ms: 5_000,
            now_ms: 200,
        })
        .unwrap();

    let sync_a = store
        .sync_group(
            "group-range",
            "member-a",
            second.generation_id,
            "range",
            &[],
            300,
        )
        .unwrap();
    let sync_b = store
        .sync_group(
            "group-range",
            "member-b",
            second.generation_id,
            "range",
            &[],
            300,
        )
        .unwrap();

    assert_eq!(
        decode_assignment_partitions(&sync_a.assignment, "topic-a"),
        vec![0, 1]
    );
    assert_eq!(
        decode_assignment_partitions(&sync_b.assignment, "topic-a"),
        vec![2, 3]
    );
}

#[test]
fn leader_sync_with_empty_assignment_bytes_is_rejected() {
    let dir = tempdir().unwrap();
    let store = FileStore::open(dir.path()).unwrap();
    store.ensure_topic("topic-a", 2, 10).unwrap();
    let subscription = encode_subscription(&["topic-a"]);
    let _leader = store
        .join_group(GroupJoinRequest {
            group_id: "group-missing-assignment",
            member_id: Some("member-a"),
            protocol_type: "consumer",
            protocol_name: "range",
            metadata: &subscription,
            session_timeout_ms: 5_000,
            rebalance_timeout_ms: 5_000,
            now_ms: 100,
        })
        .unwrap();
    let follower = store
        .join_group(GroupJoinRequest {
            group_id: "group-missing-assignment",
            member_id: Some("member-b"),
            protocol_type: "consumer",
            protocol_name: "range",
            metadata: &subscription,
            session_timeout_ms: 5_000,
            rebalance_timeout_ms: 5_000,
            now_ms: 200,
        })
        .unwrap();

    let leader_sync = store.sync_group(
        "group-missing-assignment",
        "member-a",
        follower.generation_id,
        "range",
        &[("member-a".to_string(), Vec::new())],
        300,
    );
    let follower_sync = store.sync_group(
        "group-missing-assignment",
        "member-b",
        follower.generation_id,
        "range",
        &[],
        300,
    );

    assert!(matches!(leader_sync, Err(StoreError::UnknownMember { .. })));
    assert!(matches!(
        follower_sync,
        Err(StoreError::UnknownMember { .. })
    ));
}

#[test]
fn truncated_tail_is_recovered_on_restart() {
    let dir = tempdir().unwrap();
    let store = FileStore::open(dir.path()).unwrap();
    let producer = store.init_producer(10).unwrap();
    let records = vec![BrokerRecord {
        offset: 0,
        timestamp_ms: 10,
        producer_id: producer.producer_id,
        producer_epoch: producer.producer_epoch,
        sequence: 0,
        key: Some(Bytes::from_static(b"key")),
        value: Some(Bytes::from_static(b"value")),
        headers_json: b"[]".to_vec(),
    }];
    store
        .append_records("recover.events", 0, &records, 10)
        .unwrap();
    std::fs::OpenOptions::new()
        .append(true)
        .open(
            dir.path()
                .join("topics/recover.events/partitions/0/00000000000000000000.log"),
        )
        .unwrap()
        .write_all(b"partial-tail")
        .unwrap();

    let reopened = FileStore::open(dir.path()).unwrap();
    let fetched = reopened.fetch_records("recover.events", 0, 0, 10).unwrap();
    assert_eq!(fetched.records.len(), 1);
    assert_eq!(fetched.records[0].value.as_deref(), Some(&b"value"[..]));
}

#[test]
fn duplicate_producer_retry_returns_original_offsets_without_double_append() {
    let dir = tempdir().unwrap();
    let store = FileStore::open(dir.path()).unwrap();
    let producer = store.init_producer(10).unwrap();
    let records = vec![BrokerRecord {
        offset: 0,
        timestamp_ms: 10,
        producer_id: producer.producer_id,
        producer_epoch: producer.producer_epoch,
        sequence: 0,
        key: Some(Bytes::from_static(b"key")),
        value: Some(Bytes::from_static(b"value")),
        headers_json: b"[]".to_vec(),
    }];

    let first = store
        .append_records("retry.events", 0, &records, 10)
        .unwrap();
    let duplicate = store
        .append_records("retry.events", 0, &records, 20)
        .unwrap();
    let fetched = store.fetch_records("retry.events", 0, 0, 10).unwrap();

    assert_eq!(first, duplicate);
    assert_eq!(fetched.records.len(), 1);
}

#[test]
fn non_idempotent_producer_records_are_not_deduplicated() {
    let dir = tempdir().unwrap();
    let store = FileStore::open(dir.path()).unwrap();
    let records = vec![BrokerRecord {
        offset: 0,
        timestamp_ms: 10,
        producer_id: -1,
        producer_epoch: -1,
        sequence: 0,
        key: Some(Bytes::from_static(b"key")),
        value: Some(Bytes::from_static(b"value")),
        headers_json: b"[]".to_vec(),
    }];

    let first = store
        .append_records("non-idempotent.events", 0, &records, 10)
        .unwrap();
    let second = store
        .append_records("non-idempotent.events", 0, &records, 20)
        .unwrap();
    let fetched = store
        .fetch_records("non-idempotent.events", 0, 0, 10)
        .unwrap();

    assert_eq!(first, (0, 0));
    assert_eq!(second, (1, 1));
    assert_eq!(fetched.records.len(), 2);
}

#[test]
fn non_idempotent_producer_records_append_after_restart() {
    let dir = tempdir().unwrap();
    let store = FileStore::open(dir.path()).unwrap();
    let records = vec![BrokerRecord {
        offset: 0,
        timestamp_ms: 10,
        producer_id: -1,
        producer_epoch: -1,
        sequence: 0,
        key: Some(Bytes::from_static(b"key")),
        value: Some(Bytes::from_static(b"value")),
        headers_json: b"[]".to_vec(),
    }];

    let first = store
        .append_records("non-idempotent-restart.events", 0, &records, 10)
        .unwrap();

    let reopened = FileStore::open(dir.path()).unwrap();
    let second = reopened
        .append_records("non-idempotent-restart.events", 0, &records, 20)
        .unwrap();
    let fetched = reopened
        .fetch_records("non-idempotent-restart.events", 0, 0, 10)
        .unwrap();

    assert_eq!(first, (0, 0));
    assert_eq!(second, (1, 1));
    assert_eq!(fetched.records.len(), 2);
}

#[test]
fn stale_producer_epoch_is_rejected() {
    let dir = tempdir().unwrap();
    let store = FileStore::open(dir.path()).unwrap();
    let producer = store.init_producer(10).unwrap();
    let first = vec![BrokerRecord {
        offset: 0,
        timestamp_ms: 10,
        producer_id: producer.producer_id,
        producer_epoch: producer.producer_epoch + 1,
        sequence: 0,
        key: Some(Bytes::from_static(b"key")),
        value: Some(Bytes::from_static(b"value")),
        headers_json: b"[]".to_vec(),
    }];
    store.append_records("epoch.events", 0, &first, 10).unwrap();

    let stale = vec![BrokerRecord {
        offset: 0,
        timestamp_ms: 20,
        producer_id: producer.producer_id,
        producer_epoch: producer.producer_epoch,
        sequence: 1,
        key: Some(Bytes::from_static(b"key")),
        value: Some(Bytes::from_static(b"value2")),
        headers_json: b"[]".to_vec(),
    }];

    let result = store.append_records("epoch.events", 0, &stale, 20);
    assert!(matches!(result, Err(StoreError::StaleProducerEpoch { .. })));
}

#[test]
fn replica_fetch_and_apply_preserve_offsets_and_clamp_high_watermark() {
    let leader_dir = tempdir().unwrap();
    let follower_dir = tempdir().unwrap();
    let leader = FileStore::open(leader_dir.path()).unwrap();
    let follower = FileStore::open(follower_dir.path()).unwrap();
    let producer = leader.init_producer(10).unwrap();
    let records = vec![
        BrokerRecord {
            offset: 0,
            timestamp_ms: 10,
            producer_id: producer.producer_id,
            producer_epoch: producer.producer_epoch,
            sequence: 0,
            key: Some(Bytes::from_static(b"key")),
            value: Some(Bytes::from_static(b"one")),
            headers_json: b"[]".to_vec(),
        },
        BrokerRecord {
            offset: 0,
            timestamp_ms: 20,
            producer_id: producer.producer_id,
            producer_epoch: producer.producer_epoch,
            sequence: 1,
            key: Some(Bytes::from_static(b"key")),
            value: Some(Bytes::from_static(b"two")),
            headers_json: b"[]".to_vec(),
        },
    ];
    leader
        .append_records("replica.events", 0, &records, 20)
        .unwrap();
    follower.ensure_topic("replica.events", 1, 10).unwrap();

    let fetched = leader
        .replica_fetch_records("replica.events", 0, 0, 10)
        .unwrap();
    let applied = follower
        .apply_replica_records(
            "replica.events",
            0,
            &fetched.records[..1],
            fetched.high_watermark,
            30,
        )
        .unwrap();
    let follower_fetch = follower.fetch_records("replica.events", 0, 0, 10).unwrap();

    assert_eq!(fetched.log_end_offset, 2);
    assert_eq!(applied.log_end_offset, 1);
    assert_eq!(applied.high_watermark, 1);
    assert_eq!(follower_fetch.high_watermark, 1);
    assert_eq!(follower_fetch.records.len(), 1);
    assert_eq!(follower_fetch.records[0].offset, 0);
    assert_eq!(
        follower_fetch.records[0].value.as_deref(),
        Some(&b"one"[..])
    );
}

#[test]
fn replica_apply_rejects_offset_mismatches() {
    let dir = tempdir().unwrap();
    let store = FileStore::open(dir.path()).unwrap();
    store.ensure_topic("replica.events", 1, 10).unwrap();

    let result = store.apply_replica_records(
        "replica.events",
        0,
        &[BrokerRecord {
            offset: 1,
            timestamp_ms: 10,
            producer_id: -1,
            producer_epoch: -1,
            sequence: -1,
            key: None,
            value: Some(Bytes::from_static(b"value")),
            headers_json: b"[]".to_vec(),
        }],
        1,
        20,
    );

    assert!(matches!(
        result,
        Err(StoreError::ReplicaOffsetMismatch {
            expected: 0,
            actual: 1,
        })
    ));
}

fn encode_subscription(topics: &[&str]) -> Vec<u8> {
    let subscription = ConsumerProtocolSubscription::default().with_topics(
        topics
            .iter()
            .map(|topic| StrBytes::from((*topic).to_string()))
            .collect(),
    );
    let mut bytes = BytesMut::new();
    subscription.encode(&mut bytes, 3).unwrap();
    bytes.to_vec()
}

fn decode_assignment_topics(bytes: &[u8]) -> Vec<String> {
    let mut payload = Bytes::copy_from_slice(bytes);
    let assignment = ConsumerProtocolAssignment::decode(&mut payload, 3).unwrap();
    assignment
        .assigned_partitions
        .into_iter()
        .map(|partition| partition.topic.to_string())
        .collect()
}

fn decode_assignment_partitions(bytes: &[u8], topic: &str) -> Vec<i32> {
    let mut payload = Bytes::copy_from_slice(bytes);
    let assignment = ConsumerProtocolAssignment::decode(&mut payload, 3).unwrap();
    assignment
        .assigned_partitions
        .into_iter()
        .find(|partition| partition.topic.to_string() == topic)
        .map(|partition| partition.partitions)
        .unwrap_or_default()
}

fn commit_request<'a>(
    group_id: &'a str,
    member_id: &'a str,
    generation_id: i32,
    topic: &'a str,
    partition: i32,
    next_offset: i64,
    now_ms: i64,
) -> OffsetCommitRequest<'a> {
    OffsetCommitRequest {
        group_id,
        member_id,
        generation_id,
        topic,
        partition,
        next_offset,
        now_ms,
    }
}
