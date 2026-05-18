use super::*;

#[test]
fn scripted_transport_covers_remaining_wrapper_variants() {
    let target = ClusterRpcTarget {
        node_id: 2,
        host: "node2".to_string(),
        port: 9093,
    };
    let transport = ScriptedTransport::new([
        ClusterRpcResponse::GetPartitionState(GetPartitionStateResponse {
            found: true,
            leader_id: 2,
            leader_epoch: 3,
            high_watermark: 5,
            leader_log_end_offset: 8,
        }),
        ClusterRpcResponse::ReplicaFetch(ReplicaFetchResponse {
            found: true,
            leader_id: 2,
            leader_epoch: 3,
            high_watermark: 5,
            leader_log_end_offset: 8,
            records: Vec::new(),
        }),
        ClusterRpcResponse::BeginPartitionReassignment(PartitionReassignmentResponse {
            accepted: true,
            metadata_offset: 21,
        }),
        ClusterRpcResponse::AdvancePartitionReassignment(PartitionReassignmentResponse {
            accepted: true,
            metadata_offset: 22,
        }),
        ClusterRpcResponse::AdvancePartitionReassignment(PartitionReassignmentResponse {
            accepted: true,
            metadata_offset: 23,
        }),
        ClusterRpcResponse::Vote(VoteResponse {
            term: 7,
            vote_granted: true,
        }),
    ]);

    let state = transport
        .get_partition_state(GetPartitionStateRequest {
            topic_name: "scripted.topic".to_string(),
            partition_index: 0,
        })
        .unwrap();
    assert!(state.found);
    assert_eq!(state.leader_epoch, 3);

    let fetched = transport
        .replica_fetch_to(
            &target,
            ReplicaFetchRequest {
                topic_name: "scripted.topic".to_string(),
                partition_index: 0,
                start_offset: 0,
                max_records: 1,
            },
        )
        .unwrap();
    assert!(fetched.found);
    assert_eq!(fetched.leader_log_end_offset, 8);

    let begin = transport
        .begin_partition_reassignment(BeginPartitionReassignmentRequest {
            topic_name: "scripted.topic".to_string(),
            partition_index: 0,
            target_replicas: vec![2, 3],
        })
        .unwrap();
    assert!(begin.accepted);
    assert_eq!(begin.metadata_offset, 21);

    let advance = transport
        .advance_partition_reassignment(AdvancePartitionReassignmentRequest {
            topic_name: "scripted.topic".to_string(),
            partition_index: 0,
            step: crate::cluster::ReassignmentStep::ExpandingIsr,
        })
        .unwrap();
    assert!(advance.accepted);
    assert_eq!(advance.metadata_offset, 22);

    let advance_to = transport
        .advance_partition_reassignment_to(
            &target,
            AdvancePartitionReassignmentRequest {
                topic_name: "scripted.topic".to_string(),
                partition_index: 0,
                step: crate::cluster::ReassignmentStep::LeaderSwitch,
            },
        )
        .unwrap();
    assert!(advance_to.accepted);
    assert_eq!(advance_to.metadata_offset, 23);

    let vote = transport
        .vote_to(
            &target,
            VoteRequest {
                term: 7,
                candidate_id: 2,
                last_metadata_offset: 8,
            },
        )
        .unwrap();
    assert_eq!(vote.term, 7);
    assert!(vote.vote_granted);
}

#[test]
fn scripted_transport_covers_targeted_wrapper_success_variants() {
    let target = ClusterRpcTarget {
        node_id: 2,
        host: "node2".to_string(),
        port: 9093,
    };
    let transport = ScriptedTransport::new([
        ClusterRpcResponse::UpdatePartitionReplication(UpdatePartitionReplicationResponse {
            accepted: true,
            metadata_offset: 31,
        }),
        ClusterRpcResponse::UpdateReplicaProgress(UpdateReplicaProgressResponse {
            accepted: true,
            metadata_offset: 32,
            high_watermark: 5,
        }),
        ClusterRpcResponse::UpdateReplicaProgress(UpdateReplicaProgressResponse {
            accepted: true,
            metadata_offset: 33,
            high_watermark: 6,
        }),
        ClusterRpcResponse::BeginPartitionReassignment(PartitionReassignmentResponse {
            accepted: true,
            metadata_offset: 34,
        }),
        ClusterRpcResponse::AdvancePartitionReassignment(PartitionReassignmentResponse {
            accepted: true,
            metadata_offset: 35,
        }),
    ]);

    let replication = transport
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
        .unwrap();
    assert!(replication.accepted);
    assert_eq!(replication.metadata_offset, 31);

    let progress = transport
        .update_replica_progress(UpdateReplicaProgressRequest {
            topic_name: "scripted.topic".to_string(),
            partition_index: 0,
            leader_epoch: 1,
            broker_id: 2,
            log_end_offset: 5,
            last_caught_up_ms: 100,
        })
        .unwrap();
    assert!(progress.accepted);
    assert_eq!(progress.high_watermark, 5);

    let progress_to = transport
        .update_replica_progress_to(
            &target,
            UpdateReplicaProgressRequest {
                topic_name: "scripted.topic".to_string(),
                partition_index: 0,
                leader_epoch: 1,
                broker_id: 2,
                log_end_offset: 6,
                last_caught_up_ms: 101,
            },
        )
        .unwrap();
    assert!(progress_to.accepted);
    assert_eq!(progress_to.high_watermark, 6);

    let begin = transport
        .begin_partition_reassignment_to(
            &target,
            BeginPartitionReassignmentRequest {
                topic_name: "scripted.topic".to_string(),
                partition_index: 0,
                target_replicas: vec![2, 3],
            },
        )
        .unwrap();
    assert!(begin.accepted);
    assert_eq!(begin.metadata_offset, 34);

    let advance = transport
        .advance_partition_reassignment_to(
            &target,
            AdvancePartitionReassignmentRequest {
                topic_name: "scripted.topic".to_string(),
                partition_index: 0,
                step: crate::cluster::ReassignmentStep::LeaderSwitch,
            },
        )
        .unwrap();
    assert!(advance.accepted);
    assert_eq!(advance.metadata_offset, 35);
}
