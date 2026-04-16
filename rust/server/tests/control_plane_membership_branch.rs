use bytes::BytesMut;
use kafka_protocol::messages::ConsumerProtocolSubscription;
use kafka_protocol::protocol::{Encodable, StrBytes};
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

#[test]
fn rejoin_field_changes_bump_generation_for_same_member() {
    let dir = tempdir().unwrap();
    let store = FileStore::open(dir.path()).unwrap();

    let base = store
        .join_group(GroupJoinRequest {
            group_id: "group-rejoin-fields",
            member_id: Some("member-a"),
            protocol_type: "consumer",
            protocol_name: "range",
            metadata: &encode_subscription(&["topic-a"]),
            session_timeout_ms: 5_000,
            rebalance_timeout_ms: 5_000,
            now_ms: 100,
        })
        .unwrap();

    let changed_protocol_name = store
        .join_group(GroupJoinRequest {
            group_id: "group-rejoin-fields",
            member_id: Some("member-a"),
            protocol_type: "consumer",
            protocol_name: "roundrobin",
            metadata: &encode_subscription(&["topic-a"]),
            session_timeout_ms: 5_000,
            rebalance_timeout_ms: 5_000,
            now_ms: 200,
        })
        .unwrap();
    assert!(changed_protocol_name.generation_id > base.generation_id);

    let changed_metadata = store
        .join_group(GroupJoinRequest {
            group_id: "group-rejoin-fields",
            member_id: Some("member-a"),
            protocol_type: "consumer",
            protocol_name: "roundrobin",
            metadata: &encode_subscription(&["topic-b"]),
            session_timeout_ms: 5_000,
            rebalance_timeout_ms: 5_000,
            now_ms: 300,
        })
        .unwrap();
    assert!(changed_metadata.generation_id > changed_protocol_name.generation_id);

    let changed_session_timeout = store
        .join_group(GroupJoinRequest {
            group_id: "group-rejoin-fields",
            member_id: Some("member-a"),
            protocol_type: "consumer",
            protocol_name: "roundrobin",
            metadata: &encode_subscription(&["topic-b"]),
            session_timeout_ms: 4_000,
            rebalance_timeout_ms: 5_000,
            now_ms: 400,
        })
        .unwrap();
    assert!(changed_session_timeout.generation_id > changed_metadata.generation_id);
}

#[test]
fn commit_offset_rejects_unknown_member_for_existing_group() {
    let dir = tempdir().unwrap();
    let store = FileStore::open(dir.path()).unwrap();
    store.ensure_topic("topic-a", 1, 10).unwrap();

    let joined = store
        .join_group(GroupJoinRequest {
            group_id: "group-commit-unknown-member",
            member_id: Some("member-a"),
            protocol_type: "consumer",
            protocol_name: "range",
            metadata: &encode_subscription(&["topic-a"]),
            session_timeout_ms: 5_000,
            rebalance_timeout_ms: 5_000,
            now_ms: 100,
        })
        .unwrap();

    let result = store.commit_offset(OffsetCommitRequest {
        group_id: "group-commit-unknown-member",
        member_id: "member-b",
        generation_id: joined.generation_id,
        topic: "topic-a",
        partition: 0,
        next_offset: 1,
        now_ms: 200,
    });
    assert!(matches!(result, Err(StoreError::UnknownMember { .. })));
}

#[test]
fn leave_group_unknown_member_keeps_generation() {
    let dir = tempdir().unwrap();
    let store = FileStore::open(dir.path()).unwrap();

    let first = store
        .join_group(GroupJoinRequest {
            group_id: "group-leave-unknown-member",
            member_id: Some("member-a"),
            protocol_type: "consumer",
            protocol_name: "range",
            metadata: &encode_subscription(&["topic-a"]),
            session_timeout_ms: 5_000,
            rebalance_timeout_ms: 5_000,
            now_ms: 100,
        })
        .unwrap();

    store
        .leave_group("group-leave-unknown-member", "member-b", 200)
        .unwrap();

    let second = store
        .join_group(GroupJoinRequest {
            group_id: "group-leave-unknown-member",
            member_id: Some("member-a"),
            protocol_type: "consumer",
            protocol_name: "range",
            metadata: &encode_subscription(&["topic-a"]),
            session_timeout_ms: 5_000,
            rebalance_timeout_ms: 5_000,
            now_ms: 300,
        })
        .unwrap();
    assert_eq!(second.generation_id, first.generation_id);
}
