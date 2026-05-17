use super::*;

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
fn commit_offset_rejects_unknown_partition_before_membership_checks() {
    let dir = tempdir().unwrap();
    let store = FileStore::open(dir.path()).unwrap();
    store.ensure_topic("topic-a", 1, 10).unwrap();

    let result = store.commit_offset(commit_request(
        "group-missing-partition",
        "member-a",
        1,
        "topic-a",
        1,
        1,
        20,
    ));

    assert!(matches!(
        result,
        Err(StoreError::UnknownTopicOrPartition {
            topic,
            partition: 1,
        }) if topic == "topic-a"
    ));
}

#[test]
fn fetch_offset_rejects_unknown_partition_before_group_lookup() {
    let dir = tempdir().unwrap();
    let store = FileStore::open(dir.path()).unwrap();
    store.ensure_topic("topic-a", 1, 10).unwrap();

    let result = store.fetch_offset("group-missing-partition", "topic-a", 1);

    assert!(matches!(
        result,
        Err(StoreError::UnknownTopicOrPartition {
            topic,
            partition: 1,
        }) if topic == "topic-a"
    ));
}
