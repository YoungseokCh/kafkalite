use kafkalite_server::cluster::{
    AppendMetadataRequest, BeginPartitionReassignmentRequest, ClusterRuntime,
    ControllerQuorumVoter, InMemoryClusterNetwork, InMemoryRemoteClusterRpcTransport,
    MetadataRecord, ProcessRole, UpdatePartitionReplicationRequest,
};
use kafkalite_server::config::Config;
use tempfile::tempdir;

fn controller_runtime(node_id: i32, data_dir: std::path::PathBuf) -> ClusterRuntime {
    let mut config = Config::single_node(data_dir, 19092, 1);
    config.cluster.node_id = node_id;
    config.cluster.process_roles = vec![ProcessRole::Controller];
    config.cluster.controller_quorum_voters = vec![
        ControllerQuorumVoter {
            node_id: 1,
            host: "node1".to_string(),
            port: 9093,
        },
        ControllerQuorumVoter {
            node_id: 2,
            host: "node2".to_string(),
            port: 9093,
        },
    ];
    ClusterRuntime::from_config(&config).unwrap()
}

fn set_controller_leader(runtime: &ClusterRuntime, leader_id: i32) {
    let _ = runtime
        .handle_append_metadata(AppendMetadataRequest {
            term: 1,
            leader_id,
            prev_metadata_offset: runtime.metadata_image().metadata_offset,
            records: vec![MetadataRecord::SetController {
                controller_id: leader_id,
            }],
        })
        .unwrap();
}

#[test]
fn route_update_partition_replication_routes_to_known_remote_controller() {
    let node1_dir = tempdir().unwrap();
    let node2_dir = tempdir().unwrap();
    let node1 = controller_runtime(1, node1_dir.path().join("data"));
    let node2 = controller_runtime(2, node2_dir.path().join("data"));
    set_controller_leader(&node1, 2);
    set_controller_leader(&node2, 2);
    node2
        .sync_local_topics(
            &[kafkalite_server::store::TopicMetadata {
                name: "route-remote.topic".to_string(),
                partitions: vec![kafkalite_server::store::PartitionMetadata { partition: 0 }],
            }],
            2,
        )
        .unwrap();

    let network = InMemoryClusterNetwork::default();
    network.register(1, node1.clone());
    network.register(2, node2.clone());
    let transport = InMemoryRemoteClusterRpcTransport::new(node1.config(), network);

    let response = node1
        .route_update_partition_replication(
            &transport,
            UpdatePartitionReplicationRequest {
                topic_name: "route-remote.topic".to_string(),
                partition_index: 0,
                replicas: vec![2, 3],
                isr: vec![2],
                leader_epoch: 1,
            },
        )
        .unwrap();

    assert!(response.accepted);
    let partition = &node2.metadata_image().topics[0].partitions[0];
    assert_eq!(partition.replicas, vec![2, 3]);
    assert_eq!(partition.isr, vec![2]);
}

#[test]
fn route_begin_reassignment_routes_to_known_remote_controller() {
    let node1_dir = tempdir().unwrap();
    let node2_dir = tempdir().unwrap();
    let node1 = controller_runtime(1, node1_dir.path().join("data"));
    let node2 = controller_runtime(2, node2_dir.path().join("data"));
    set_controller_leader(&node1, 2);
    set_controller_leader(&node2, 2);
    node2
        .sync_local_topics(
            &[kafkalite_server::store::TopicMetadata {
                name: "route-reassign.topic".to_string(),
                partitions: vec![kafkalite_server::store::PartitionMetadata { partition: 0 }],
            }],
            2,
        )
        .unwrap();

    let network = InMemoryClusterNetwork::default();
    network.register(1, node1.clone());
    network.register(2, node2.clone());
    let transport = InMemoryRemoteClusterRpcTransport::new(node1.config(), network);

    let response = node1
        .route_begin_partition_reassignment(
            &transport,
            BeginPartitionReassignmentRequest {
                topic_name: "route-reassign.topic".to_string(),
                partition_index: 0,
                target_replicas: vec![2, 3],
            },
        )
        .unwrap();

    assert!(response.accepted);
    let reassignment = node2
        .metadata_image()
        .partition_reassignment("route-reassign.topic", 0)
        .unwrap();
    assert_eq!(reassignment.target_replicas, vec![2, 3]);
}
