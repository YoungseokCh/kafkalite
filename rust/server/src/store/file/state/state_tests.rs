use super::super::root_directories;
use super::*;
use tempfile::tempdir;

#[test]
fn append_does_not_create_non_standard_state_files() {
    let dir = tempdir().unwrap();
    let journal = StateJournal::new();

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

    let mut snapshots = SnapshotSet::load();
    journal.replay(&mut snapshots).unwrap();

    assert_eq!(snapshots.producers.next_producer_id, 1);
    assert!(snapshots.offsets.is_empty());
    assert!(root_directories(dir.path()).is_empty());
}

#[test]
fn snapshot_load_uses_default_producer_state_when_missing() {
    let snapshots = SnapshotSet::load();

    assert_eq!(snapshots.producers.next_producer_id, 1);
    assert!(snapshots.topics.is_empty());
    assert!(snapshots.offsets.is_empty());
}

#[test]
fn replay_on_empty_journal_is_noop() {
    let journal = StateJournal::new();
    let mut snapshots = SnapshotSet::load();

    journal.replay(&mut snapshots).unwrap();

    assert_eq!(snapshots.producers.next_producer_id, 1);
    assert!(snapshots.offsets.is_empty());
}
