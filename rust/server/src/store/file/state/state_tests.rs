use super::*;

#[test]
fn snapshot_load_uses_default_producer_state_when_missing() {
    let snapshots = SnapshotSet::load();

    assert_eq!(snapshots.producers.next_producer_id, 1);
    assert!(snapshots.topics.is_empty());
    assert!(snapshots.offsets.is_empty());
}
