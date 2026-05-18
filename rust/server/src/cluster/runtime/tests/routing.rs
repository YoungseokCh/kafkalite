use tokio::net::TcpListener;

use crate::cluster::{
    BeginPartitionReassignmentRequest, UpdatePartitionLeaderRequest,
    UpdatePartitionReplicationRequest, test_support::TwoNodeClusterHarness,
};

#[test]
fn non_leader_controller_routes_partition_leader_update_to_elected_controller() {
    let harness = TwoNodeClusterHarness::new_controller_pair();
    let transport = harness.transport_from_node1();
    let _ = harness
        .node1
        .runtime
        .handle_append_metadata(crate::cluster::AppendMetadataRequest {
            term: 1,
            leader_id: 2,
            prev_metadata_offset: harness.node1.runtime.metadata_image().metadata_offset,
            records: vec![crate::cluster::MetadataRecord::SetController { controller_id: 2 }],
        })
        .unwrap();
    let _ = harness
        .node2
        .runtime
        .handle_append_metadata(crate::cluster::AppendMetadataRequest {
            term: 1,
            leader_id: 2,
            prev_metadata_offset: harness.node2.runtime.metadata_image().metadata_offset,
            records: vec![crate::cluster::MetadataRecord::SetController { controller_id: 2 }],
        })
        .unwrap();
    harness
        .node2
        .runtime
        .sync_local_topics(
            &[crate::store::TopicMetadata {
                name: "route.topic".to_string(),
                partitions: vec![crate::store::PartitionMetadata { partition: 0 }],
            }],
            2,
        )
        .unwrap();

    let response = harness
        .node1
        .runtime
        .route_update_partition_leader(
            &transport,
            UpdatePartitionLeaderRequest {
                topic_name: "route.topic".to_string(),
                partition_index: 0,
                leader_id: 2,
                leader_epoch: 1,
            },
        )
        .unwrap();

    assert!(response.accepted);
    assert_eq!(
        harness
            .node2
            .runtime
            .metadata_image()
            .partition_leader_id("route.topic", 0),
        Some(2)
    );
}

#[tokio::test]
async fn non_leader_controller_routes_partition_leader_update_via_tcp() {
    let harness = TwoNodeClusterHarness::new_controller_pair();
    let _ = harness
        .node2
        .runtime
        .handle_append_metadata(crate::cluster::AppendMetadataRequest {
            term: 1,
            leader_id: 2,
            prev_metadata_offset: harness.node2.runtime.metadata_image().metadata_offset,
            records: vec![crate::cluster::MetadataRecord::SetController { controller_id: 2 }],
        })
        .unwrap();
    harness
        .node2
        .runtime
        .sync_local_topics(
            &[crate::store::TopicMetadata {
                name: "tcp.route.topic".to_string(),
                partitions: vec![crate::store::PartitionMetadata { partition: 0 }],
            }],
            2,
        )
        .unwrap();

    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let server_runtime = harness.node2.runtime.clone();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        crate::cluster::TcpClusterRpcTransport::serve_runtime_once(&listener, server_runtime)
            .await
            .unwrap();
    });

    let transport = crate::cluster::TcpClusterRpcTransport;
    let response = transport
        .update_partition_leader_to(
            &crate::cluster::ClusterRpcTarget {
                node_id: 2,
                host: addr.ip().to_string(),
                port: addr.port(),
            },
            UpdatePartitionLeaderRequest {
                topic_name: "tcp.route.topic".to_string(),
                partition_index: 0,
                leader_id: 2,
                leader_epoch: 1,
            },
        )
        .await
        .unwrap();

    assert!(response.accepted);
    assert_eq!(
        harness
            .node2
            .runtime
            .metadata_image()
            .partition_leader_id("tcp.route.topic", 0),
        Some(2)
    );
    server.await.unwrap();
}

#[tokio::test]
async fn non_leader_controller_routes_partition_replication_via_tcp() {
    let harness = TwoNodeClusterHarness::new_controller_pair();
    let _ = harness
        .node2
        .runtime
        .handle_append_metadata(crate::cluster::AppendMetadataRequest {
            term: 1,
            leader_id: 2,
            prev_metadata_offset: harness.node2.runtime.metadata_image().metadata_offset,
            records: vec![crate::cluster::MetadataRecord::SetController { controller_id: 2 }],
        })
        .unwrap();
    let _ = harness
        .node1
        .runtime
        .handle_append_metadata(crate::cluster::AppendMetadataRequest {
            term: 1,
            leader_id: 2,
            prev_metadata_offset: harness.node1.runtime.metadata_image().metadata_offset,
            records: vec![crate::cluster::MetadataRecord::SetController { controller_id: 2 }],
        })
        .unwrap();
    harness
        .node2
        .runtime
        .sync_local_topics(
            &[crate::store::TopicMetadata {
                name: "tcp.replication.topic".to_string(),
                partitions: vec![crate::store::PartitionMetadata { partition: 0 }],
            }],
            2,
        )
        .unwrap();

    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let server_runtime = harness.node2.runtime.clone();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        crate::cluster::TcpClusterRpcTransport::serve_runtime_once(&listener, server_runtime)
            .await
            .unwrap();
    });

    let transport = crate::cluster::TcpClusterRpcTransport;
    let response = transport
        .update_partition_replication_to(
            &crate::cluster::ClusterRpcTarget {
                node_id: 2,
                host: addr.ip().to_string(),
                port: addr.port(),
            },
            UpdatePartitionReplicationRequest {
                topic_name: "tcp.replication.topic".to_string(),
                partition_index: 0,
                replicas: vec![2, 3],
                isr: vec![2],
                leader_epoch: 1,
            },
        )
        .await
        .unwrap();

    assert!(response.accepted);
    assert_eq!(
        harness.node2.runtime.metadata_image().topics[0].partitions[0].replicas,
        vec![2, 3]
    );
    server.await.unwrap();
}

#[tokio::test]
async fn non_leader_controller_routes_reassignment_via_tcp() {
    let harness = TwoNodeClusterHarness::new_controller_pair();
    let _ = harness
        .node2
        .runtime
        .handle_append_metadata(crate::cluster::AppendMetadataRequest {
            term: 1,
            leader_id: 2,
            prev_metadata_offset: harness.node2.runtime.metadata_image().metadata_offset,
            records: vec![crate::cluster::MetadataRecord::SetController { controller_id: 2 }],
        })
        .unwrap();
    let _ = harness
        .node1
        .runtime
        .handle_append_metadata(crate::cluster::AppendMetadataRequest {
            term: 1,
            leader_id: 2,
            prev_metadata_offset: harness.node1.runtime.metadata_image().metadata_offset,
            records: vec![crate::cluster::MetadataRecord::SetController { controller_id: 2 }],
        })
        .unwrap();
    harness
        .node2
        .runtime
        .sync_local_topics(
            &[crate::store::TopicMetadata {
                name: "tcp.reassign.topic".to_string(),
                partitions: vec![crate::store::PartitionMetadata { partition: 0 }],
            }],
            2,
        )
        .unwrap();

    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server_runtime = harness.node2.runtime.clone();
    let server = tokio::spawn(async move {
        crate::cluster::TcpClusterRpcTransport::serve_runtime_once(&listener, server_runtime)
            .await
            .unwrap();
    });

    let transport = crate::cluster::TcpClusterRpcTransport;
    let response = transport
        .begin_partition_reassignment_to(
            &crate::cluster::ClusterRpcTarget {
                node_id: 2,
                host: addr.ip().to_string(),
                port: addr.port(),
            },
            BeginPartitionReassignmentRequest {
                topic_name: "tcp.reassign.topic".to_string(),
                partition_index: 0,
                target_replicas: vec![2, 3],
            },
        )
        .await
        .unwrap();

    assert!(response.accepted);
    assert_eq!(
        harness
            .node2
            .runtime
            .metadata_image()
            .partition_reassignment("tcp.reassign.topic", 0)
            .unwrap()
            .target_replicas,
        vec![2, 3]
    );
    server.await.unwrap();
}
