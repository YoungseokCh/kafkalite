use tempfile::tempdir;

use kafkalite_server::FileStore;
use kafkalite_server::store::{GroupJoinRequest, Storage, StoreError};

#[test]
fn leave_group_removes_existing_member() {
    let dir = tempdir().unwrap();
    let store = FileStore::open(dir.path()).unwrap();
    store.ensure_topic("topic-a", 1, 10).unwrap();

    let joined = store
        .join_group(GroupJoinRequest {
            group_id: "group-leave",
            member_id: Some("member-a"),
            protocol_type: "consumer",
            protocol_name: "range",
            metadata: &[],
            session_timeout_ms: 5_000,
            rebalance_timeout_ms: 5_000,
            now_ms: 100,
        })
        .unwrap();

    store.leave_group("group-leave", "member-a", 200).unwrap();

    let heartbeat = store.heartbeat("group-leave", "member-a", joined.generation_id, 300);
    assert!(matches!(heartbeat, Err(StoreError::UnknownMember { .. })));
}
