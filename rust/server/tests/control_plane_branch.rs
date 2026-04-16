use bytes::{Bytes, BytesMut};
use kafka_protocol::messages::{ConsumerProtocolAssignment, ConsumerProtocolSubscription};
use kafka_protocol::protocol::{Decodable, Encodable, StrBytes};
use tempfile::tempdir;

use kafkalite_server::FileStore;
use kafkalite_server::store::{GroupJoinRequest, OffsetCommitRequest, Storage, StoreError};

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

#[test]
fn sync_group_rejects_unknown_group_and_unknown_member() {
    let dir = tempdir().unwrap();
    let store = FileStore::open(dir.path()).unwrap();

    let missing_group = store.sync_group("missing", "member-a", 1, "range", &[], 10);
    assert!(matches!(
        missing_group,
        Err(StoreError::StaleGeneration { .. })
    ));

    store.ensure_topic("topic-a", 1, 10).unwrap();
    let subscription = encode_subscription(&["topic-a"]);
    let joined = store
        .join_group(GroupJoinRequest {
            group_id: "group-a",
            member_id: Some("member-a"),
            protocol_type: "consumer",
            protocol_name: "range",
            metadata: &subscription,
            session_timeout_ms: 5_000,
            rebalance_timeout_ms: 5_000,
            now_ms: 100,
        })
        .unwrap();

    let unknown_member = store.sync_group(
        "group-a",
        "member-b",
        joined.generation_id,
        "range",
        &[],
        200,
    );
    assert!(matches!(
        unknown_member,
        Err(StoreError::UnknownMember { .. })
    ));
}

#[test]
fn sync_group_rejects_stale_generation_for_current_member() {
    let dir = tempdir().unwrap();
    let store = FileStore::open(dir.path()).unwrap();
    store.ensure_topic("topic-a", 1, 10).unwrap();
    let subscription = encode_subscription(&["topic-a"]);
    let joined = store
        .join_group(GroupJoinRequest {
            group_id: "group-stale",
            member_id: Some("member-a"),
            protocol_type: "consumer",
            protocol_name: "range",
            metadata: &subscription,
            session_timeout_ms: 5_000,
            rebalance_timeout_ms: 5_000,
            now_ms: 100,
        })
        .unwrap();

    let stale = store.sync_group(
        "group-stale",
        "member-a",
        joined.generation_id - 1,
        "range",
        &[],
        200,
    );
    assert!(matches!(stale, Err(StoreError::UnknownMember { .. })));
}

#[test]
fn sync_group_with_explicit_assignments_updates_known_member_only() {
    let dir = tempdir().unwrap();
    let store = FileStore::open(dir.path()).unwrap();
    store.ensure_topic("topic-a", 1, 10).unwrap();
    let subscription = encode_subscription(&["topic-a"]);
    let _ = store
        .join_group(GroupJoinRequest {
            group_id: "group-assign",
            member_id: Some("member-a"),
            protocol_type: "consumer",
            protocol_name: "range",
            metadata: &subscription,
            session_timeout_ms: 5_000,
            rebalance_timeout_ms: 5_000,
            now_ms: 100,
        })
        .unwrap();
    let joined = store
        .join_group(GroupJoinRequest {
            group_id: "group-assign",
            member_id: Some("member-b"),
            protocol_type: "consumer",
            protocol_name: "range",
            metadata: &subscription,
            session_timeout_ms: 5_000,
            rebalance_timeout_ms: 5_000,
            now_ms: 200,
        })
        .unwrap();

    let assignment = vec![7, 8, 9];
    let response = store
        .sync_group(
            "group-assign",
            "member-a",
            joined.generation_id,
            "range",
            &[
                ("member-a".to_string(), assignment.clone()),
                ("ghost-member".to_string(), vec![1, 2, 3]),
            ],
            300,
        )
        .unwrap();
    assert_eq!(response.assignment, assignment);
}

#[test]
fn invalid_subscription_metadata_produces_empty_assignment() {
    let dir = tempdir().unwrap();
    let store = FileStore::open(dir.path()).unwrap();
    store.ensure_topic("topic-a", 1, 10).unwrap();
    let invalid_subscription = vec![1, 2, 3, 4];
    let joined = store
        .join_group(GroupJoinRequest {
            group_id: "group-invalid-subscription",
            member_id: Some("member-a"),
            protocol_type: "consumer",
            protocol_name: "range",
            metadata: &invalid_subscription,
            session_timeout_ms: 5_000,
            rebalance_timeout_ms: 5_000,
            now_ms: 100,
        })
        .unwrap();

    let response = store
        .sync_group(
            "group-invalid-subscription",
            "member-a",
            joined.generation_id,
            "range",
            &[],
            200,
        )
        .unwrap();

    assert!(decode_assignment_topics(&response.assignment).is_empty());
}

#[test]
fn heartbeat_and_commit_offset_validate_membership_and_generation() {
    let dir = tempdir().unwrap();
    let store = FileStore::open(dir.path()).unwrap();
    store.ensure_topic("topic-a", 1, 10).unwrap();

    let missing_group_heartbeat = store.heartbeat("missing", "member-a", 1, 10);
    assert!(matches!(
        missing_group_heartbeat,
        Err(StoreError::UnknownMember { .. })
    ));

    let subscription = encode_subscription(&["topic-a"]);
    let joined = store
        .join_group(GroupJoinRequest {
            group_id: "group-heartbeat",
            member_id: Some("member-a"),
            protocol_type: "consumer",
            protocol_name: "range",
            metadata: &subscription,
            session_timeout_ms: 5_000,
            rebalance_timeout_ms: 5_000,
            now_ms: 100,
        })
        .unwrap();

    let unknown_member_heartbeat =
        store.heartbeat("group-heartbeat", "member-b", joined.generation_id, 200);
    assert!(matches!(
        unknown_member_heartbeat,
        Err(StoreError::UnknownMember { .. })
    ));

    let stale_generation_heartbeat =
        store.heartbeat("group-heartbeat", "member-a", joined.generation_id + 1, 200);
    assert!(matches!(
        stale_generation_heartbeat,
        Err(StoreError::StaleGeneration { .. })
    ));

    let future_commit = store.commit_offset(OffsetCommitRequest {
        group_id: "group-heartbeat",
        member_id: "member-a",
        generation_id: joined.generation_id + 1,
        topic: "topic-a",
        partition: 0,
        next_offset: 1,
        now_ms: 300,
    });
    assert!(matches!(
        future_commit,
        Err(StoreError::StaleGeneration { .. })
    ));
}

#[test]
fn empty_member_id_is_auto_generated_and_assignments_cover_topic() {
    let dir = tempdir().unwrap();
    let store = FileStore::open(dir.path()).unwrap();
    store.ensure_topic("topic-a", 2, 10).unwrap();

    let subscription = encode_subscription(&["topic-a"]);
    let joined = store
        .join_group(GroupJoinRequest {
            group_id: "group-auto-member",
            member_id: Some(""),
            protocol_type: "consumer",
            protocol_name: "range",
            metadata: &subscription,
            session_timeout_ms: 5_000,
            rebalance_timeout_ms: 5_000,
            now_ms: 123,
        })
        .unwrap();
    assert_eq!(joined.member_id, "group-auto-member-member-123");

    let synced = store
        .sync_group(
            "group-auto-member",
            &joined.member_id,
            joined.generation_id,
            "range",
            &[],
            124,
        )
        .unwrap();
    assert_eq!(
        decode_assignment_topics(&synced.assignment),
        vec!["topic-a"]
    );

    store
        .leave_group("missing-group", "missing-member", 130)
        .unwrap();
}
