use bytes::{Bytes, BytesMut};
use kafka_protocol::messages::{ConsumerProtocolAssignment, ConsumerProtocolSubscription};
use kafka_protocol::protocol::{Decodable, Encodable, StrBytes};
use std::io::Write;
use tempfile::tempdir;

use super::*;
use crate::store::{GroupJoinRequest, StoreError};

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
    let (base, last) = store.append_records("test.events", &records, 10).unwrap();
    assert_eq!((base, last), (0, 0));
    let fetched = store.fetch_records("test.events", 0, 10).unwrap();
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
    store.append_records("tail.events", &records, 10).unwrap();

    let fetched = store.fetch_records("tail.events", 3, 10).unwrap();
    assert_eq!(fetched.records.len(), 2);
    assert_eq!(fetched.records[0].offset, 3);
    assert_eq!(fetched.records[1].offset, 4);
}

#[test]
fn assignment_respects_member_subscriptions() {
    let dir = tempdir().unwrap();
    let store = FileStore::open(dir.path()).unwrap();
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
        .commit_offset(
            "group-b",
            "member-a",
            joined.generation_id,
            "topic-a",
            1,
            200,
        )
        .unwrap();
    let stale = store.commit_offset(
        "group-b",
        "member-a",
        joined.generation_id - 1,
        "topic-a",
        2,
        300,
    );
    assert!(stale.is_ok());
    let future = store.commit_offset(
        "group-b",
        "member-a",
        joined.generation_id + 1,
        "topic-a",
        3,
        300,
    );
    assert!(matches!(future, Err(StoreError::StaleGeneration { .. })));
    let unknown = store.commit_offset(
        "group-b",
        "member-b",
        joined.generation_id,
        "topic-a",
        2,
        300,
    );
    assert!(matches!(unknown, Err(StoreError::UnknownMember { .. })));
}

#[test]
fn group_membership_is_soft_across_restart_but_offsets_remain_durable() {
    let dir = tempdir().unwrap();
    let store = FileStore::open(dir.path()).unwrap();
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
        .commit_offset(
            "group-soft",
            "member-a",
            joined.generation_id,
            "topic-a",
            1,
            200,
        )
        .unwrap();

    let reopened = FileStore::open(dir.path()).unwrap();
    assert_eq!(
        reopened.fetch_offset("group-soft", "topic-a").unwrap(),
        Some(1)
    );

    let stale_runtime_member = reopened.commit_offset(
        "group-soft",
        "member-a",
        joined.generation_id,
        "topic-a",
        2,
        300,
    );
    assert!(matches!(
        stale_runtime_member,
        Err(StoreError::UnknownMember { .. })
    ));
}

#[test]
fn heartbeat_does_not_grow_state_journal_but_offset_commit_does() {
    let dir = tempdir().unwrap();
    let store = FileStore::open(dir.path()).unwrap();
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
        .commit_offset(
            "group-journal",
            "member-a",
            joined.generation_id,
            "topic-a",
            1,
            300,
        )
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
        .append_records("recover.events", &records, 10)
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
    let fetched = reopened.fetch_records("recover.events", 0, 10).unwrap();
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

    let first = store.append_records("retry.events", &records, 10).unwrap();
    let duplicate = store.append_records("retry.events", &records, 20).unwrap();
    let fetched = store.fetch_records("retry.events", 0, 10).unwrap();

    assert_eq!(first, duplicate);
    assert_eq!(fetched.records.len(), 1);
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
    store.append_records("epoch.events", &first, 10).unwrap();

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

    let result = store.append_records("epoch.events", &stale, 20);
    assert!(matches!(result, Err(StoreError::StaleProducerEpoch { .. })));
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
