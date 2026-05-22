use super::*;

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
fn synced_group_assignment_survives_restart() {
    let dir = tempdir().unwrap();
    let store = FileStore::open(dir.path()).unwrap();
    store.ensure_topic("topic-a", 2, 10).unwrap();
    let subscription = encode_subscription(&["topic-a"]);
    let joined = store
        .join_group(GroupJoinRequest {
            group_id: "group-durable-assignment",
            member_id: Some("member-a"),
            protocol_type: "consumer",
            protocol_name: "range",
            metadata: &subscription,
            session_timeout_ms: 5_000,
            rebalance_timeout_ms: 5_000,
            now_ms: 100,
        })
        .unwrap();
    let first_sync = store
        .sync_group(
            "group-durable-assignment",
            "member-a",
            joined.generation_id,
            "range",
            &[],
            200,
        )
        .unwrap();
    assert_eq!(
        decode_assignment_partitions(&first_sync.assignment, "topic-a"),
        vec![0, 1]
    );

    let reopened = FileStore::open(dir.path()).unwrap();
    let restored_sync = reopened
        .sync_group(
            "group-durable-assignment",
            "member-a",
            joined.generation_id,
            "range",
            &[],
            300,
        )
        .unwrap();

    assert_eq!(restored_sync.assignment, first_sync.assignment);
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
fn group_membership_and_offsets_remain_durable_across_restart() {
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

    reopened
        .commit_offset(commit_request(
            "group-soft",
            "member-a",
            joined.generation_id,
            "topic-a",
            0,
            2,
            300,
        ))
        .unwrap();
    assert_eq!(
        reopened.fetch_offset("group-soft", "topic-a", 0).unwrap(),
        Some(2)
    );

    let unknown_runtime_member = reopened.commit_offset(commit_request(
        "group-soft",
        "member-b",
        joined.generation_id,
        "topic-a",
        0,
        3,
        300,
    ));
    assert!(matches!(
        unknown_runtime_member,
        Err(StoreError::UnknownMember { .. })
    ));
}

#[test]
fn committed_offset_resume_survives_restart_after_tombstone() {
    let dir = tempdir().unwrap();
    let store = FileStore::open(dir.path()).unwrap();
    store.ensure_topic("topic-mixed", 1, 10).unwrap();
    let subscription = encode_subscription(&["topic-mixed"]);
    let joined = store
        .join_group(GroupJoinRequest {
            group_id: "group-mixed",
            member_id: Some("member-a"),
            protocol_type: "consumer",
            protocol_name: "range",
            metadata: &subscription,
            session_timeout_ms: 5_000,
            rebalance_timeout_ms: 5_000,
            now_ms: 100,
        })
        .unwrap();
    let records = vec![
        BrokerRecord {
            offset: 0,
            timestamp_ms: 10,
            producer_id: -1,
            producer_epoch: -1,
            sequence: 0,
            key: Some(Bytes::from_static(b"key-one")),
            value: Some(Bytes::from_static(b"payload-one")),
            headers_json: b"[]".to_vec(),
        },
        BrokerRecord {
            offset: 0,
            timestamp_ms: 11,
            producer_id: -1,
            producer_epoch: -1,
            sequence: 1,
            key: Some(Bytes::from_static(b"key-two")),
            value: None,
            headers_json: b"[]".to_vec(),
        },
        BrokerRecord {
            offset: 0,
            timestamp_ms: 12,
            producer_id: -1,
            producer_epoch: -1,
            sequence: 2,
            key: None,
            value: Some(Bytes::from_static(b"payload-three")),
            headers_json: b"[]".to_vec(),
        },
    ];
    store
        .append_records("topic-mixed", 0, &records, 100)
        .unwrap();
    store
        .commit_offset(commit_request(
            "group-mixed",
            "member-a",
            joined.generation_id,
            "topic-mixed",
            0,
            2,
            200,
        ))
        .unwrap();

    let reopened = FileStore::open(dir.path()).unwrap();
    assert_eq!(
        reopened
            .fetch_offset("group-mixed", "topic-mixed", 0)
            .unwrap(),
        Some(2)
    );

    let fetched = reopened.fetch_records("topic-mixed", 0, 2, 10).unwrap();
    assert_eq!(fetched.records.len(), 1);
    assert_eq!(fetched.records[0].offset, 2);
    assert_eq!(fetched.records[0].key, None);
    assert_eq!(
        fetched.records[0].value,
        Some(Bytes::from_static(b"payload-three"))
    );
}
