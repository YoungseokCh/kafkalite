use super::*;

#[test]
fn remote_transport_resolves_target_from_quorum_voters() {
    let mut config = ClusterConfig {
        node_id: 1,
        ..ClusterConfig::default()
    };
    config.controller_quorum_voters = vec![
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

    let transport = RemoteClusterRpcTransport::new(&config);
    let target = transport.resolve_target(2).unwrap();

    assert_eq!(transport.local_node_id(), 1);
    assert_eq!(target.node_id, 2);
    assert_eq!(target.host, "node2");
    assert_eq!(target.port, 9093);
}

#[test]
fn remote_transport_rejects_unknown_target() {
    let transport = RemoteClusterRpcTransport::new(&ClusterConfig::default());
    let err = transport.resolve_target(99).unwrap_err().to_string();

    assert!(err.contains("unknown cluster RPC target node 99"));
}

#[test]
fn in_memory_remote_transport_dispatches_to_registered_runtime() {
    let harness = TwoNodeClusterHarness::new_controller_pair();
    let mut config1 = Config::single_node(tempdir().unwrap().path().join("node1-client"), 19092, 1);
    config1.cluster.node_id = 1;
    config1.cluster.process_roles = vec![ProcessRole::Controller];
    config1.cluster.controller_quorum_voters = vec![
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
    let transport = InMemoryRemoteClusterRpcTransport::new(&config1.cluster, harness.network);
    let target = transport.remote.resolve_target(2).unwrap();

    let response = transport
        .send_to(
            &target,
            ClusterRpcRequest::AppendMetadata(AppendMetadataRequest {
                term: 2,
                leader_id: 1,
                prev_metadata_offset: harness.node2.runtime.metadata_image().metadata_offset,
                records: vec![crate::cluster::MetadataRecord::SetController { controller_id: 1 }],
            }),
        )
        .unwrap();

    assert!(matches!(response, ClusterRpcResponse::AppendMetadata(_)));
    assert_eq!(harness.node2.runtime.metadata_image().controller_id, 1);
}

#[test]
fn in_memory_remote_transport_propagates_replication_scenario() {
    let harness = TwoNodeClusterHarness::new_controller_pair();
    harness
        .node2
        .runtime
        .sync_local_topics(
            &[crate::store::TopicMetadata {
                name: "replicated.topic".to_string(),
                partitions: vec![crate::store::PartitionMetadata { partition: 0 }],
            }],
            2,
        )
        .unwrap();
    let _ = harness
        .node2
        .runtime
        .handle_append_metadata(AppendMetadataRequest {
            term: 1,
            leader_id: 2,
            prev_metadata_offset: harness.node2.runtime.metadata_image().metadata_offset,
            records: vec![crate::cluster::MetadataRecord::SetController { controller_id: 2 }],
        })
        .unwrap();

    let mut config1 = Config::single_node(tempdir().unwrap().path().join("node1-client"), 19092, 1);
    config1.cluster.node_id = 1;
    config1.cluster.process_roles = vec![ProcessRole::Controller];
    config1.cluster.controller_quorum_voters = vec![
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
    let transport = InMemoryRemoteClusterRpcTransport::new(&config1.cluster, harness.network);
    let target = transport.remote.resolve_target(2).unwrap();

    let _ = transport
        .send_to(
            &target,
            ClusterRpcRequest::UpdatePartitionLeader(UpdatePartitionLeaderRequest {
                topic_name: "replicated.topic".to_string(),
                partition_index: 0,
                leader_id: 1,
                leader_epoch: 1,
            }),
        )
        .unwrap();
    let _ = transport
        .send_to(
            &target,
            ClusterRpcRequest::UpdatePartitionReplication(UpdatePartitionReplicationRequest {
                topic_name: "replicated.topic".to_string(),
                partition_index: 0,
                replicas: vec![1, 2],
                isr: vec![1, 2],
                leader_epoch: 1,
            }),
        )
        .unwrap();
    let _ = transport
        .send_to(
            &target,
            ClusterRpcRequest::UpdateReplicaProgress(UpdateReplicaProgressRequest {
                topic_name: "replicated.topic".to_string(),
                partition_index: 0,
                leader_epoch: 1,
                broker_id: 1,
                log_end_offset: 11,
                last_caught_up_ms: 100,
            }),
        )
        .unwrap();
    let response = transport
        .send_to(
            &target,
            ClusterRpcRequest::UpdateReplicaProgress(UpdateReplicaProgressRequest {
                topic_name: "replicated.topic".to_string(),
                partition_index: 0,
                leader_epoch: 1,
                broker_id: 2,
                log_end_offset: 9,
                last_caught_up_ms: 100,
            }),
        )
        .unwrap();

    let ClusterRpcResponse::UpdateReplicaProgress(response) = response else {
        panic!("unexpected response variant");
    };
    assert_eq!(response.high_watermark, 11);
    assert_eq!(
        harness
            .node2
            .runtime
            .metadata_image()
            .partition_high_watermark("replicated.topic", 0),
        Some(11)
    );
}

#[test]
fn in_memory_remote_transport_replica_fetch_without_store_reports_missing() {
    let harness = TwoNodeClusterHarness::new_controller_pair();
    let transport = harness.transport_from_node1();
    let target = transport.resolve_target(2).unwrap();

    let response = transport
        .replica_fetch_to(
            &target,
            ReplicaFetchRequest {
                topic_name: "missing.topic".to_string(),
                partition_index: 0,
                start_offset: 0,
                max_records: 10,
            },
        )
        .unwrap();

    assert!(!response.found);
    assert!(response.records.is_empty());
}

#[test]
fn in_memory_remote_transport_replica_fetch_unknown_topic_reports_missing() {
    let harness = TwoNodeClusterHarness::new_controller_pair();
    let transport = harness.transport_from_node1();
    let dir = tempdir().unwrap();
    harness.network.register_store(
        2,
        Arc::new(FileStore::open(dir.path().join("node-2-data")).unwrap()),
    );
    let target = transport.resolve_target(2).unwrap();

    let response = transport
        .replica_fetch_to(
            &target,
            ReplicaFetchRequest {
                topic_name: "missing.topic".to_string(),
                partition_index: 0,
                start_offset: 0,
                max_records: 10,
            },
        )
        .unwrap();

    assert!(!response.found);
    assert_eq!(response.leader_log_end_offset, -1);
}

#[test]
fn remote_transports_require_explicit_target_nodes() {
    let remote = RemoteClusterRpcTransport::new(&ClusterConfig::default());
    let err = remote
        .send(ClusterRpcRequest::Vote(VoteRequest {
            term: 1,
            candidate_id: 1,
            last_metadata_offset: 0,
        }))
        .unwrap_err()
        .to_string();
    assert!(err.contains("requires a target node"));

    let err = remote
        .send_to(
            &ClusterRpcTarget {
                node_id: 1,
                host: "node1".to_string(),
                port: 9093,
            },
            ClusterRpcRequest::Vote(VoteRequest {
                term: 1,
                candidate_id: 1,
                last_metadata_offset: 0,
            }),
        )
        .unwrap_err()
        .to_string();
    assert!(err.contains("remote cluster rpc not implemented yet"));

    let in_memory = InMemoryRemoteClusterRpcTransport::new(
        &ClusterConfig::default(),
        InMemoryClusterNetwork::default(),
    );
    let err = in_memory
        .send(ClusterRpcRequest::Vote(VoteRequest {
            term: 1,
            candidate_id: 1,
            last_metadata_offset: 0,
        }))
        .unwrap_err()
        .to_string();
    assert!(err.contains("requires a target node"));
}
