use super::*;
use tempfile::tempdir;

#[test]
fn replay_applies_producer_and_offset_entries() {
    let dir = tempdir().unwrap();
    let journal = StateJournal::open(dir.path()).unwrap();

    journal
        .append_producer_state(
            &ProducerState {
                next_producer_id: 42,
                sequences: BTreeMap::new(),
            },
            0,
        )
        .unwrap();
    journal
        .append_offsets(&BTreeMap::from([("group-a/topic-a/0".to_string(), 9)]))
        .unwrap();

    let mut snapshots = SnapshotSet::load(dir.path()).unwrap();
    journal.replay(&mut snapshots).unwrap();

    assert_eq!(snapshots.producers.next_producer_id, 42);
    assert_eq!(snapshots.offsets.get("group-a/topic-a/0"), Some(&9));
}

#[test]
fn replay_rejects_invalid_journal_magic() {
    let dir = tempdir().unwrap();
    let journal_path = dir.path().join("state/state.journal");
    std::fs::create_dir_all(journal_path.parent().unwrap()).unwrap();
    std::fs::write(&journal_path, b"BAD!\x00\x00\x00\x00").unwrap();

    let journal = StateJournal::open(dir.path()).unwrap();
    let mut snapshots = SnapshotSet::load(dir.path()).unwrap();
    let err = journal.replay(&mut snapshots).unwrap_err().to_string();

    assert!(err.contains("invalid journal magic"));
}

#[test]
fn snapshot_load_uses_default_producer_state_when_missing() {
    let dir = tempdir().unwrap();

    let snapshots = SnapshotSet::load(dir.path()).unwrap();

    assert_eq!(snapshots.producers.next_producer_id, 1);
    assert!(snapshots.topics.is_empty());
    assert!(snapshots.offsets.is_empty());
}

#[test]
fn snapshot_load_reads_existing_producer_snapshot() {
    let dir = tempdir().unwrap();
    let state_dir = dir.path().join("state");
    std::fs::create_dir_all(&state_dir).unwrap();
    std::fs::write(
        state_dir.join("producers.snapshot"),
        r#"{"next_producer_id":9,"sequences":{}}"#,
    )
    .unwrap();

    let snapshots = SnapshotSet::load(dir.path()).unwrap();

    assert_eq!(snapshots.producers.next_producer_id, 9);
}

#[test]
fn snapshot_load_ignores_groups_snapshot_by_policy() {
    let dir = tempdir().unwrap();
    let state_dir = dir.path().join("state");
    std::fs::create_dir_all(&state_dir).unwrap();
    std::fs::write(
        state_dir.join("groups.snapshot"),
        r#"{"group-a":{"generation_id":1,"protocol_type":"consumer","protocol_name":"range","leader_member_id":null,"members":{},"updated_at_unix_ms":1}}"#,
    )
    .unwrap();

    let snapshots = SnapshotSet::load(dir.path()).unwrap();

    assert!(snapshots.groups.is_empty());
}

#[test]
fn replay_on_empty_journal_is_noop() {
    let dir = tempdir().unwrap();
    let journal = StateJournal::open(dir.path()).unwrap();
    let mut snapshots = SnapshotSet::load(dir.path()).unwrap();

    journal.replay(&mut snapshots).unwrap();

    assert_eq!(snapshots.producers.next_producer_id, 1);
    assert!(snapshots.offsets.is_empty());
}
