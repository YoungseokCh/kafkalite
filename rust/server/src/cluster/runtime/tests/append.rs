use tempfile::tempdir;

use crate::cluster::{
    AppendMetadataRequest, BrokerHeartbeatRequest, ClusterRuntime, MetadataRecord, ProcessRole,
    RegisterBrokerRequest,
};
use crate::config::Config;

#[test]
fn register_broker_updates_metadata_and_heartbeat_accepts_current_epoch() {
    let dir = tempdir().unwrap();
    let mut config = Config::single_node(dir.path().join("data"), 19092, 1);
    config.cluster.node_id = 4;
    config.cluster.process_roles = vec![ProcessRole::Broker, ProcessRole::Controller];

    let runtime = ClusterRuntime::from_config(&config).unwrap();
    let response = runtime
        .handle_register_broker(
            RegisterBrokerRequest {
                node_id: 9,
                advertised_host: "broker-9.local".to_string(),
                advertised_port: 39092,
            },
            500,
        )
        .unwrap();
    assert_eq!(response.leader_id, Some(4));

    let heartbeat = runtime
        .handle_broker_heartbeat(BrokerHeartbeatRequest {
            node_id: 9,
            broker_epoch: response.broker_epoch,
            timestamp_ms: 600,
        })
        .unwrap();
    assert!(heartbeat.accepted);

    let metadata = runtime.metadata_image();
    assert_eq!(metadata.controller_id, 4);
    assert!(metadata.brokers.iter().any(|broker| broker.node_id == 9));
}

#[test]
fn append_metadata_updates_offset_and_term() {
    let dir = tempdir().unwrap();
    let mut config = Config::single_node(dir.path().join("data"), 19092, 1);
    config.cluster.node_id = 4;
    config.cluster.process_roles = vec![ProcessRole::Controller];

    let runtime = ClusterRuntime::from_config(&config).unwrap();
    let initial_offset = runtime.metadata_image().metadata_offset;
    let response = runtime
        .handle_append_metadata(AppendMetadataRequest {
            term: 2,
            leader_id: 9,
            prev_metadata_offset: initial_offset,
            records: vec![MetadataRecord::SetController { controller_id: 9 }],
        })
        .unwrap();

    assert!(response.accepted);
    assert_eq!(response.term, 2);
    assert_eq!(runtime.metadata_image().controller_id, 9);
    assert_eq!(runtime.metadata_image().metadata_offset, initial_offset + 1);
}

#[test]
fn stale_term_append_is_rejected() {
    let dir = tempdir().unwrap();
    let mut config = Config::single_node(dir.path().join("data"), 19092, 1);
    config.cluster.node_id = 4;
    config.cluster.process_roles = vec![ProcessRole::Controller];

    let runtime = ClusterRuntime::from_config(&config).unwrap();
    let _ = runtime
        .handle_append_metadata(AppendMetadataRequest {
            term: 2,
            leader_id: 9,
            prev_metadata_offset: runtime.metadata_image().metadata_offset,
            records: vec![MetadataRecord::SetController { controller_id: 9 }],
        })
        .unwrap();
    let rejected = runtime
        .handle_append_metadata(AppendMetadataRequest {
            term: 1,
            leader_id: 8,
            prev_metadata_offset: runtime.metadata_image().metadata_offset,
            records: vec![MetadataRecord::SetController { controller_id: 8 }],
        })
        .unwrap();

    assert!(!rejected.accepted);
    assert_eq!(runtime.metadata_image().controller_id, 9);
}

#[test]
fn same_term_append_with_different_leader_is_rejected() {
    let dir = tempdir().unwrap();
    let mut config = Config::single_node(dir.path().join("data"), 19092, 1);
    config.cluster.node_id = 4;
    config.cluster.process_roles = vec![ProcessRole::Controller];

    let runtime = ClusterRuntime::from_config(&config).unwrap();
    let accepted = runtime
        .handle_append_metadata(AppendMetadataRequest {
            term: 2,
            leader_id: 9,
            prev_metadata_offset: runtime.metadata_image().metadata_offset,
            records: vec![MetadataRecord::SetController { controller_id: 9 }],
        })
        .unwrap();
    let rejected = runtime
        .handle_append_metadata(AppendMetadataRequest {
            term: 2,
            leader_id: 8,
            prev_metadata_offset: runtime.metadata_image().metadata_offset,
            records: vec![MetadataRecord::SetController { controller_id: 8 }],
        })
        .unwrap();

    assert!(accepted.accepted);
    assert!(!rejected.accepted);
    assert_eq!(runtime.quorum_snapshot().leader_id, Some(9));
}

#[test]
fn append_rejects_non_voter_leader() {
    let dir = tempdir().unwrap();
    let mut config = Config::single_node(dir.path().join("data"), 19092, 1);
    config.cluster.node_id = 1;
    config.cluster.process_roles = vec![ProcessRole::Controller];
    let runtime = ClusterRuntime::from_config(&config).unwrap();
    let rejected = runtime
        .handle_append_metadata(AppendMetadataRequest {
            term: 1,
            leader_id: 999,
            prev_metadata_offset: runtime.metadata_image().metadata_offset,
            records: vec![MetadataRecord::SetController { controller_id: 1 }],
        })
        .unwrap();

    assert!(!rejected.accepted);
}
