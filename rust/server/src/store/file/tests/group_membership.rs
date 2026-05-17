use super::*;

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
