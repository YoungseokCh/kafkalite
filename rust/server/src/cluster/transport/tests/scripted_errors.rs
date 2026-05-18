use super::*;

#[test]
fn scripted_transport_rejects_unexpected_remaining_wrapper_variants() {
    let target = ClusterRpcTarget {
        node_id: 2,
        host: "node2".to_string(),
        port: 9093,
    };
    let transport = ScriptedTransport::new([
        ClusterRpcResponse::Vote(VoteResponse {
            term: 1,
            vote_granted: true,
        }),
        ClusterRpcResponse::Vote(VoteResponse {
            term: 1,
            vote_granted: true,
        }),
        ClusterRpcResponse::Vote(VoteResponse {
            term: 1,
            vote_granted: true,
        }),
        ClusterRpcResponse::Vote(VoteResponse {
            term: 1,
            vote_granted: true,
        }),
        ClusterRpcResponse::Vote(VoteResponse {
            term: 1,
            vote_granted: true,
        }),
        ClusterRpcResponse::AppendMetadata(AppendMetadataResponse {
            term: 1,
            accepted: true,
            last_metadata_offset: 1,
        }),
    ]);

    for err in [
        transport
            .get_partition_state(GetPartitionStateRequest {
                topic_name: "scripted.topic".to_string(),
                partition_index: 0,
            })
            .unwrap_err()
            .to_string(),
        transport
            .replica_fetch_to(
                &target,
                ReplicaFetchRequest {
                    topic_name: "scripted.topic".to_string(),
                    partition_index: 0,
                    start_offset: 0,
                    max_records: 1,
                },
            )
            .unwrap_err()
            .to_string(),
        transport
            .begin_partition_reassignment(BeginPartitionReassignmentRequest {
                topic_name: "scripted.topic".to_string(),
                partition_index: 0,
                target_replicas: vec![2, 3],
            })
            .unwrap_err()
            .to_string(),
        transport
            .advance_partition_reassignment(AdvancePartitionReassignmentRequest {
                topic_name: "scripted.topic".to_string(),
                partition_index: 0,
                step: crate::cluster::ReassignmentStep::ExpandingIsr,
            })
            .unwrap_err()
            .to_string(),
        transport
            .advance_partition_reassignment_to(
                &target,
                AdvancePartitionReassignmentRequest {
                    topic_name: "scripted.topic".to_string(),
                    partition_index: 0,
                    step: crate::cluster::ReassignmentStep::LeaderSwitch,
                },
            )
            .unwrap_err()
            .to_string(),
        transport
            .vote_to(
                &target,
                VoteRequest {
                    term: 7,
                    candidate_id: 2,
                    last_metadata_offset: 8,
                },
            )
            .unwrap_err()
            .to_string(),
    ] {
        assert!(err.contains("unexpected RPC response"));
    }
}

#[test]
fn scripted_transport_rejects_unexpected_core_wrapper_variants() {
    let target = ClusterRpcTarget {
        node_id: 2,
        host: "node2".to_string(),
        port: 9093,
    };
    let transport = ScriptedTransport::new([
        ClusterRpcResponse::Vote(VoteResponse {
            term: 1,
            vote_granted: true,
        }),
        ClusterRpcResponse::Vote(VoteResponse {
            term: 1,
            vote_granted: true,
        }),
        ClusterRpcResponse::Vote(VoteResponse {
            term: 1,
            vote_granted: true,
        }),
        ClusterRpcResponse::Vote(VoteResponse {
            term: 1,
            vote_granted: true,
        }),
        ClusterRpcResponse::Vote(VoteResponse {
            term: 1,
            vote_granted: true,
        }),
        ClusterRpcResponse::Vote(VoteResponse {
            term: 1,
            vote_granted: true,
        }),
        ClusterRpcResponse::Vote(VoteResponse {
            term: 1,
            vote_granted: true,
        }),
        ClusterRpcResponse::Vote(VoteResponse {
            term: 1,
            vote_granted: true,
        }),
    ]);

    for err in [
        transport
            .register_broker(RegisterBrokerRequest {
                node_id: 2,
                advertised_host: "node2".to_string(),
                advertised_port: 9092,
            })
            .unwrap_err()
            .to_string(),
        transport
            .append_metadata(AppendMetadataRequest {
                term: 1,
                leader_id: 1,
                prev_metadata_offset: -1,
                records: vec![],
            })
            .unwrap_err()
            .to_string(),
        transport
            .broker_heartbeat(BrokerHeartbeatRequest {
                node_id: 2,
                broker_epoch: 1,
                timestamp_ms: 1,
            })
            .unwrap_err()
            .to_string(),
        transport
            .update_partition_leader(UpdatePartitionLeaderRequest {
                topic_name: "scripted.topic".to_string(),
                partition_index: 0,
                leader_id: 2,
                leader_epoch: 1,
            })
            .unwrap_err()
            .to_string(),
        transport
            .update_partition_leader_to(
                &target,
                UpdatePartitionLeaderRequest {
                    topic_name: "scripted.topic".to_string(),
                    partition_index: 0,
                    leader_id: 2,
                    leader_epoch: 1,
                },
            )
            .unwrap_err()
            .to_string(),
        transport
            .update_partition_replication(UpdatePartitionReplicationRequest {
                topic_name: "scripted.topic".to_string(),
                partition_index: 0,
                replicas: vec![2],
                isr: vec![2],
                leader_epoch: 1,
            })
            .unwrap_err()
            .to_string(),
        transport
            .update_partition_replication_to(
                &target,
                UpdatePartitionReplicationRequest {
                    topic_name: "scripted.topic".to_string(),
                    partition_index: 0,
                    replicas: vec![2],
                    isr: vec![2],
                    leader_epoch: 1,
                },
            )
            .unwrap_err()
            .to_string(),
        transport
            .update_replica_progress(UpdateReplicaProgressRequest {
                topic_name: "scripted.topic".to_string(),
                partition_index: 0,
                leader_epoch: 1,
                broker_id: 2,
                log_end_offset: 1,
                last_caught_up_ms: 1,
            })
            .unwrap_err()
            .to_string(),
    ] {
        assert!(err.contains("unexpected RPC response"));
    }
}

#[test]
fn scripted_transport_rejects_unexpected_targeted_wrapper_variants() {
    let target = ClusterRpcTarget {
        node_id: 2,
        host: "node2".to_string(),
        port: 9093,
    };
    let transport = ScriptedTransport::new([
        ClusterRpcResponse::Vote(VoteResponse {
            term: 1,
            vote_granted: true,
        }),
        ClusterRpcResponse::Vote(VoteResponse {
            term: 1,
            vote_granted: true,
        }),
        ClusterRpcResponse::Vote(VoteResponse {
            term: 1,
            vote_granted: true,
        }),
    ]);

    for err in [
        transport
            .update_replica_progress_to(
                &target,
                UpdateReplicaProgressRequest {
                    topic_name: "scripted.topic".to_string(),
                    partition_index: 0,
                    leader_epoch: 1,
                    broker_id: 2,
                    log_end_offset: 1,
                    last_caught_up_ms: 1,
                },
            )
            .unwrap_err()
            .to_string(),
        transport
            .begin_partition_reassignment_to(
                &target,
                BeginPartitionReassignmentRequest {
                    topic_name: "scripted.topic".to_string(),
                    partition_index: 0,
                    target_replicas: vec![2, 3],
                },
            )
            .unwrap_err()
            .to_string(),
        transport
            .update_partition_replication_to(
                &target,
                UpdatePartitionReplicationRequest {
                    topic_name: "scripted.topic".to_string(),
                    partition_index: 0,
                    replicas: vec![2],
                    isr: vec![2],
                    leader_epoch: 1,
                },
            )
            .unwrap_err()
            .to_string(),
    ] {
        assert!(err.contains("unexpected RPC response"));
    }
}
