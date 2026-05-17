use std::io::Write;

use tempfile::tempdir;

use super::*;
use crate::config::Config;
use crate::store::{PartitionMetadata, TopicMetadata};

#[test]
fn persists_broker_and_topic_metadata() {
    let dir = tempdir().unwrap();
    let mut config = Config::single_node(dir.path().join("data"), 29092, 1);
    config.broker.broker_id = 7;
    config.broker.advertised_host = "broker.local".to_string();
    let mut store = MetadataStore::open(dir.path(), &config).unwrap();
    store
        .sync_broker(BrokerMetadata {
            node_id: 7,
            host: "broker.local".to_string(),
            port: 29092,
        })
        .unwrap();
    store.sync_controller(7).unwrap();
    store
        .sync_topics(
            &[TopicMetadata {
                name: "topic-a".to_string(),
                partitions: vec![PartitionMetadata { partition: 0 }],
            }],
            7,
        )
        .unwrap();

    let reopened = MetadataStore::open(dir.path(), &config).unwrap();
    assert_eq!(reopened.image().controller_id, 7);
    assert_eq!(reopened.metadata_offset(), 1);
    assert_eq!(reopened.image().brokers.len(), 1);
    assert_eq!(reopened.image().topics.len(), 1);
    assert_eq!(reopened.image().topics[0].partitions[0].leader_id, 7);
}

#[test]
fn replayed_log_updates_existing_topic_image() {
    let dir = tempdir().unwrap();
    let config = Config::single_node(dir.path().join("data"), 29092, 1);
    let mut store = MetadataStore::open(dir.path(), &config).unwrap();
    store
        .sync_topics(
            &[TopicMetadata {
                name: "topic-a".to_string(),
                partitions: vec![PartitionMetadata { partition: 0 }],
            }],
            1,
        )
        .unwrap();
    store
        .sync_topics(
            &[TopicMetadata {
                name: "topic-a".to_string(),
                partitions: vec![
                    PartitionMetadata { partition: 0 },
                    PartitionMetadata { partition: 1 },
                ],
            }],
            1,
        )
        .unwrap();

    let reopened = MetadataStore::open(dir.path(), &config).unwrap();
    assert_eq!(reopened.image().topics.len(), 1);
    assert_eq!(reopened.image().topics[0].partitions.len(), 2);
    assert_eq!(reopened.metadata_offset(), 1);
}

#[test]
fn append_remote_records_checks_previous_offset() {
    let dir = tempdir().unwrap();
    let config = Config::single_node(dir.path().join("data"), 29092, 1);
    let mut store = MetadataStore::open(dir.path(), &config).unwrap();

    let accepted = store
        .append_remote_records(-1, &[MetadataRecord::SetController { controller_id: 3 }])
        .unwrap();
    let rejected = store
        .append_remote_records(-1, &[MetadataRecord::SetController { controller_id: 4 }])
        .unwrap();

    assert!(accepted);
    assert!(!rejected);
    assert_eq!(store.image().controller_id, 3);
    assert_eq!(store.metadata_offset(), 0);
}

#[test]
fn replay_skips_log_entries_already_in_snapshot() {
    let dir = tempdir().unwrap();
    let config = Config::single_node(dir.path().join("data"), 29092, 1);
    let mut store = MetadataStore::open(dir.path(), &config).unwrap();
    store.sync_controller(3).unwrap();

    let mut file = File::create(dir.path().join(LOG_FILE)).unwrap();
    serde_json::to_writer(
        &mut file,
        &MetadataLogEntry {
            metadata_offset: Some(0),
            record: MetadataRecord::SetController { controller_id: 3 },
        },
    )
    .unwrap();
    file.write_all(b"\n").unwrap();
    serde_json::to_writer(
        &mut file,
        &MetadataLogEntry {
            metadata_offset: Some(1),
            record: MetadataRecord::RegisterBroker(BrokerMetadata {
                node_id: 9,
                host: "broker-9.local".to_string(),
                port: 39092,
            }),
        },
    )
    .unwrap();
    file.write_all(b"\n").unwrap();

    let reopened = MetadataStore::open(dir.path(), &config).unwrap();
    assert_eq!(reopened.image().controller_id, 3);
    assert_eq!(reopened.image().brokers.len(), 1);
    assert_eq!(reopened.metadata_offset(), 1);

    let reopened_again = MetadataStore::open(dir.path(), &config).unwrap();
    assert_eq!(reopened_again.metadata_offset(), 1);
    assert_eq!(reopened_again.image().brokers.len(), 1);
}

#[test]
fn replay_truncates_partial_metadata_log_tail_after_valid_records() {
    let dir = tempdir().unwrap();
    let config = Config::single_node(dir.path().join("data"), 29092, 1);
    let mut store = MetadataStore::open(dir.path(), &config).unwrap();
    store.sync_controller(3).unwrap();

    let mut file = File::create(dir.path().join(LOG_FILE)).unwrap();
    serde_json::to_writer(
        &mut file,
        &MetadataLogEntry {
            metadata_offset: Some(1),
            record: MetadataRecord::RegisterBroker(BrokerMetadata {
                node_id: 9,
                host: "broker-9.local".to_string(),
                port: 39092,
            }),
        },
    )
    .unwrap();
    file.write_all(b"\n").unwrap();
    file.write_all(b"{\"metadata_offset\":2").unwrap();
    file.sync_all().unwrap();

    let reopened = MetadataStore::open(dir.path(), &config).unwrap();
    assert_eq!(reopened.metadata_offset(), 1);
    assert_eq!(reopened.image().brokers.len(), 1);

    let log_contents = std::fs::read_to_string(dir.path().join(LOG_FILE)).unwrap();
    assert_eq!(log_contents.lines().count(), 1);
    assert!(log_contents.ends_with('\n'));
}

#[test]
fn sync_helpers_return_false_for_noop_updates() {
    let dir = tempdir().unwrap();
    let config = Config::single_node(dir.path().join("data"), 29092, 1);
    let mut store = MetadataStore::open(dir.path(), &config).unwrap();

    assert!(!store.sync_controller(config.broker.broker_id).unwrap());

    let broker = BrokerMetadata {
        node_id: config.broker.broker_id,
        host: config.broker.advertised_host.clone(),
        port: config.broker.advertised_port,
    };
    assert!(store.sync_broker(broker.clone()).unwrap());
    assert!(!store.sync_broker(broker).unwrap());

    let topic = TopicMetadata {
        name: "topic-a".to_string(),
        partitions: vec![PartitionMetadata { partition: 0 }],
    };
    assert!(store.sync_topics(std::slice::from_ref(&topic), 1).unwrap());
    assert!(!store.sync_topics(std::slice::from_ref(&topic), 1).unwrap());
}

#[test]
fn append_remote_records_accepts_empty_batch_without_mutation() {
    let dir = tempdir().unwrap();
    let config = Config::single_node(dir.path().join("data"), 29092, 1);
    let mut store = MetadataStore::open(dir.path(), &config).unwrap();

    assert!(store.append_remote_records(-1, &[]).unwrap());
    assert_eq!(store.metadata_offset(), -1);
}

#[test]
fn parse_log_entry_supports_legacy_record_lines_and_trim_handles_crlf() {
    let legacy =
        serde_json::to_string(&MetadataRecord::SetController { controller_id: 9 }).unwrap();
    let parsed = parse_log_entry(&legacy).unwrap();
    assert!(parsed.metadata_offset.is_none());
    assert_eq!(
        parsed.record,
        MetadataRecord::SetController { controller_id: 9 }
    );

    assert_eq!(trim_line_endings(b"line\r\n"), b"line");
}
