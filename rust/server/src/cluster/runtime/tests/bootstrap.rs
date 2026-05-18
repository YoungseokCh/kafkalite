use tempfile::tempdir;

use crate::cluster::{ClusterRuntime, ProcessRole, test_support::ThreeNodeClusterHarness};
use crate::config::Config;

#[test]
fn controller_role_bootstraps_local_leader() {
    let dir = tempdir().unwrap();
    let mut config = Config::single_node(dir.path().join("data"), 19092, 1);
    config.cluster.node_id = 4;
    config.cluster.process_roles = vec![ProcessRole::Broker, ProcessRole::Controller];

    let runtime = ClusterRuntime::from_config(&config).unwrap();

    let quorum = runtime.quorum_snapshot();
    let controller = runtime.controller_snapshot();
    let metadata = runtime.metadata_image();
    assert_eq!(quorum.leader_id, Some(4));
    assert_eq!(quorum.controller_epoch, 1);
    assert_eq!(controller.leader_id, Some(4));
    assert_eq!(controller.registered_brokers.len(), 1);
    assert_eq!(controller.registered_brokers[0].node_id, 1);
    assert_eq!(metadata.controller_id, 4);
    assert_eq!(metadata.brokers.len(), 1);
}

#[test]
fn broker_only_bootstrap_registers_broker_without_controller_leader() {
    let dir = tempdir().unwrap();
    let mut config = Config::single_node(dir.path().join("data"), 19092, 1);
    config.cluster.node_id = 7;
    config.cluster.process_roles = vec![ProcessRole::Broker];

    let runtime = ClusterRuntime::from_config(&config).unwrap();

    assert_eq!(runtime.quorum_snapshot().leader_id, None);
    let controller = runtime.controller_snapshot();
    assert_eq!(controller.leader_id, None);
    assert_eq!(controller.registered_brokers.len(), 1);
    assert_eq!(runtime.metadata_image().brokers.len(), 1);
}

#[test]
fn from_config_propagates_metadata_store_open_errors() {
    let dir = tempdir().unwrap();
    let file_path = dir.path().join("not-a-directory");
    std::fs::write(&file_path, b"blocked").unwrap();
    let mut config = Config::single_node(file_path, 19092, 1);
    config.cluster.process_roles = vec![ProcessRole::Controller];

    let err = ClusterRuntime::from_config(&config)
        .unwrap_err()
        .to_string();

    assert!(err.contains("Not a directory") || err.contains("not a directory"));
}

#[test]
fn multi_controller_bootstrap_starts_without_self_election() {
    let dir = tempdir().unwrap();
    let mut config = Config::single_node(dir.path().join("data"), 19092, 1);
    config.cluster.node_id = 4;
    config.cluster.process_roles = vec![ProcessRole::Controller];
    config.cluster.controller_quorum_voters = vec![
        crate::cluster::ControllerQuorumVoter {
            node_id: 4,
            host: "node4".to_string(),
            port: 9093,
        },
        crate::cluster::ControllerQuorumVoter {
            node_id: 5,
            host: "node5".to_string(),
            port: 9093,
        },
    ];

    let runtime = ClusterRuntime::from_config(&config).unwrap();

    assert_eq!(runtime.quorum_snapshot().leader_id, None);
}

#[test]
fn three_node_election_requires_majority_votes() {
    let harness = ThreeNodeClusterHarness::new_controller_triplet();
    let transport = harness.transport_from_node(1);
    let targets = vec![
        transport.resolve_target(2).unwrap(),
        transport.resolve_target(3).unwrap(),
    ];

    let elected = harness
        .node1
        .runtime
        .run_election(&transport, &targets)
        .unwrap();

    assert!(elected);
    assert_eq!(harness.node1.runtime.quorum_snapshot().leader_id, Some(1));
    assert_eq!(harness.node1.runtime.metadata_image().controller_id, 1);
}

#[test]
fn three_node_election_fails_without_majority() {
    let harness = ThreeNodeClusterHarness::new_controller_triplet();
    let transport = harness.transport_from_node(1);
    let targets = vec![];

    let elected = harness
        .node1
        .runtime
        .run_election(&transport, &targets)
        .unwrap();

    assert!(!elected);
    assert_eq!(harness.node1.runtime.quorum_snapshot().leader_id, None);
}

#[test]
fn single_voter_controller_can_auto_create_without_waiting_for_election() {
    let dir = tempdir().unwrap();
    let mut config = Config::single_node(dir.path().join("data"), 19092, 1);
    config.cluster.node_id = 1;
    config.cluster.process_roles = vec![ProcessRole::Controller];
    config.cluster.controller_quorum_voters = vec![crate::cluster::ControllerQuorumVoter {
        node_id: 1,
        host: "node1".to_string(),
        port: 9093,
    }];

    let runtime = ClusterRuntime::from_config(&config).unwrap();

    assert!(runtime.can_auto_create_topics_locally());
}
