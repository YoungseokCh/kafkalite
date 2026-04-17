use kafkalite_server::cluster::{
    AppendMetadataRequest, BeginPartitionReassignmentRequest, ClusterRuntime,
    ControllerQuorumVoter, MetadataRecord, ProcessRole, TcpClusterRpcTransport,
    UpdatePartitionLeaderRequest, UpdatePartitionReplicationRequest,
};
use kafkalite_server::config::Config;
use tempfile::tempdir;
use tokio::net::TcpListener;

fn voters(node_ids: &[i32]) -> Vec<ControllerQuorumVoter> {
    node_ids
        .iter()
        .map(|node_id| ControllerQuorumVoter {
            node_id: *node_id,
            host: format!("node{node_id}"),
            port: 9093,
        })
        .collect()
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

#[tokio::test]
async fn tcp_route_methods_fail_closed_without_known_controller_target() {
    let dir = tempdir().unwrap();
    let mut config = Config::single_node(dir.path().join("data"), 19092, 1);
    config.cluster.process_roles = vec![ProcessRole::Broker];
    config.cluster.controller_quorum_voters = voters(&[1, 2]);
    let runtime = ClusterRuntime::from_config(&config).unwrap();
    let transport = TcpClusterRpcTransport;
    let metadata_offset = runtime.metadata_image().metadata_offset;

    let leader = runtime
        .route_update_partition_leader_via_tcp(
            &transport,
            UpdatePartitionLeaderRequest {
                topic_name: "tcp-route.topic".to_string(),
                partition_index: 0,
                leader_id: 2,
                leader_epoch: 1,
            },
        )
        .await
        .unwrap();
    assert!(!leader.accepted);
    assert_eq!(leader.metadata_offset, metadata_offset);

    let replication = runtime
        .route_update_partition_replication_via_tcp(
            &transport,
            UpdatePartitionReplicationRequest {
                topic_name: "tcp-route.topic".to_string(),
                partition_index: 0,
                replicas: vec![1, 2],
                isr: vec![1],
                leader_epoch: 1,
            },
        )
        .await
        .unwrap();
    assert!(!replication.accepted);
    assert_eq!(replication.metadata_offset, metadata_offset);

    let reassignment = runtime
        .route_begin_partition_reassignment_via_tcp(
            &transport,
            BeginPartitionReassignmentRequest {
                topic_name: "tcp-route.topic".to_string(),
                partition_index: 0,
                target_replicas: vec![2],
            },
        )
        .await
        .unwrap();
    assert!(!reassignment.accepted);
    assert_eq!(reassignment.metadata_offset, metadata_offset);
}

#[tokio::test]
async fn tcp_route_methods_use_local_fast_path_when_runtime_is_writable() {
    let dir = tempdir().unwrap();
    let mut config = Config::single_node(dir.path().join("data"), 19092, 1);
    config.cluster.process_roles = vec![ProcessRole::Broker, ProcessRole::Controller];
    let runtime = ClusterRuntime::from_config(&config).unwrap();
    runtime
        .sync_local_topics(
            &[kafkalite_server::store::TopicMetadata {
                name: "tcp-local.topic".to_string(),
                partitions: vec![kafkalite_server::store::PartitionMetadata { partition: 0 }],
            }],
            1,
        )
        .unwrap();
    let transport = TcpClusterRpcTransport;

    let leader = runtime
        .route_update_partition_leader_via_tcp(
            &transport,
            UpdatePartitionLeaderRequest {
                topic_name: "tcp-local.topic".to_string(),
                partition_index: 0,
                leader_id: 1,
                leader_epoch: 1,
            },
        )
        .await
        .unwrap();
    assert!(leader.accepted);

    let replication = runtime
        .route_update_partition_replication_via_tcp(
            &transport,
            UpdatePartitionReplicationRequest {
                topic_name: "tcp-local.topic".to_string(),
                partition_index: 0,
                replicas: vec![1],
                isr: vec![1],
                leader_epoch: 1,
            },
        )
        .await
        .unwrap();
    assert!(replication.accepted);

    let reassignment = runtime
        .route_begin_partition_reassignment_via_tcp(
            &transport,
            BeginPartitionReassignmentRequest {
                topic_name: "tcp-local.topic".to_string(),
                partition_index: 0,
                target_replicas: vec![1],
            },
        )
        .await
        .unwrap();
    assert!(reassignment.accepted);
}

#[tokio::test]
async fn tcp_route_methods_forward_to_known_remote_controller() {
    let node1_dir = tempdir().unwrap();
    let node2_dir = tempdir().unwrap();
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let addr = listener.local_addr().unwrap();
    let voters = vec![
        ControllerQuorumVoter {
            node_id: 1,
            host: "127.0.0.1".to_string(),
            port: 19093,
        },
        ControllerQuorumVoter {
            node_id: 2,
            host: "127.0.0.1".to_string(),
            port: addr.port(),
        },
    ];

    let mut node1_config = Config::single_node(node1_dir.path().join("data"), 19092, 1);
    node1_config.cluster.node_id = 1;
    node1_config.cluster.process_roles = vec![ProcessRole::Controller];
    node1_config.cluster.controller_quorum_voters = voters.clone();
    let node1 = ClusterRuntime::from_config(&node1_config).unwrap();

    let mut node2_config = Config::single_node(node2_dir.path().join("data"), 19094, 2);
    node2_config.cluster.node_id = 2;
    node2_config.cluster.process_roles = vec![ProcessRole::Controller];
    node2_config.cluster.controller_quorum_voters = voters;
    let node2 = ClusterRuntime::from_config(&node2_config).unwrap();

    set_controller_leader(&node1, 2);
    set_controller_leader(&node2, 2);
    node2
        .sync_local_topics(
            &[kafkalite_server::store::TopicMetadata {
                name: "tcp-remote.topic".to_string(),
                partitions: vec![kafkalite_server::store::PartitionMetadata { partition: 0 }],
            }],
            2,
        )
        .unwrap();

    let server_runtime = node2.clone();
    let server = tokio::spawn(async move {
        TcpClusterRpcTransport::serve_runtime_forever(listener, server_runtime)
            .await
            .unwrap();
    });

    let transport = TcpClusterRpcTransport;
    let leader = node1
        .route_update_partition_leader_via_tcp(
            &transport,
            UpdatePartitionLeaderRequest {
                topic_name: "tcp-remote.topic".to_string(),
                partition_index: 0,
                leader_id: 2,
                leader_epoch: 1,
            },
        )
        .await
        .unwrap();
    assert!(leader.accepted);

    let replication = node1
        .route_update_partition_replication_via_tcp(
            &transport,
            UpdatePartitionReplicationRequest {
                topic_name: "tcp-remote.topic".to_string(),
                partition_index: 0,
                replicas: vec![2, 3],
                isr: vec![2],
                leader_epoch: 1,
            },
        )
        .await
        .unwrap();
    assert!(replication.accepted);

    let reassignment = node1
        .route_begin_partition_reassignment_via_tcp(
            &transport,
            BeginPartitionReassignmentRequest {
                topic_name: "tcp-remote.topic".to_string(),
                partition_index: 0,
                target_replicas: vec![2, 3],
            },
        )
        .await
        .unwrap();
    assert!(reassignment.accepted);

    assert_eq!(
        node2
            .metadata_image()
            .partition_leader_id("tcp-remote.topic", 0),
        Some(2)
    );
    assert_eq!(
        node2.metadata_image().topics[0].partitions[0].replicas,
        vec![2, 3]
    );
    assert_eq!(
        node2
            .metadata_image()
            .partition_reassignment("tcp-remote.topic", 0)
            .unwrap()
            .target_replicas,
        vec![2, 3]
    );

    server.abort();
}
