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
fn leave_group_remains_durable_across_restart() {
    let dir = tempdir().unwrap();
    let store = FileStore::open(dir.path()).unwrap();
    store.ensure_topic("topic-a", 1, 10).unwrap();
    let subscription = encode_subscription(&["topic-a"]);
    let joined = store
        .join_group(GroupJoinRequest {
            group_id: "group-left",
            member_id: Some("member-a"),
            protocol_type: "consumer",
            protocol_name: "range",
            metadata: &subscription,
            session_timeout_ms: 5_000,
            rebalance_timeout_ms: 5_000,
            now_ms: 100,
        })
        .unwrap();
    store.leave_group("group-left", "member-a", 200).unwrap();

    let reopened = FileStore::open(dir.path()).unwrap();
    let heartbeat = reopened.heartbeat("group-left", "member-a", joined.generation_id, 300);

    assert!(matches!(heartbeat, Err(StoreError::UnknownMember { .. })));
}

#[test]
fn heartbeat_and_offset_commit_do_not_create_non_topic_directories() {
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
    store
        .heartbeat("group-journal", "member-a", joined.generation_id, 200)
        .unwrap();

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

    let directories = root_directories(dir.path());
    let offset_directories = directories
        .iter()
        .filter(|name| name.starts_with("__consumer_offsets-"))
        .collect::<Vec<_>>();
    assert_eq!(directories.len(), 2);
    assert_eq!(offset_directories.len(), 1);
    assert!(directories.contains(&"topic-a-0".to_string()));
}
