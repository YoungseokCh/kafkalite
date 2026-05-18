use super::*;

#[test]
fn in_memory_remote_transport_applies_controller_failover() {
    let harness = TwoNodeClusterHarness::new_controller_pair();
    let transport = harness.transport_from_node1();
    let target = transport.resolve_target(2).unwrap();

    let _ = transport
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
    let after_first = harness.node2.runtime.quorum_snapshot();
    assert_eq!(after_first.leader_id, Some(1));
    assert_eq!(after_first.current_term, 2);

    let _ = transport
        .send_to(
            &target,
            ClusterRpcRequest::AppendMetadata(AppendMetadataRequest {
                term: 3,
                leader_id: 2,
                prev_metadata_offset: harness.node2.runtime.metadata_image().metadata_offset,
                records: vec![crate::cluster::MetadataRecord::SetController { controller_id: 2 }],
            }),
        )
        .unwrap();
    let after_failover = harness.node2.runtime.quorum_snapshot();
    assert_eq!(after_failover.leader_id, Some(2));
    assert_eq!(after_failover.current_term, 3);
    assert_eq!(harness.node2.runtime.metadata_image().controller_id, 2);
}

#[test]
fn in_memory_remote_transport_keeps_partition_state_coherent_across_failover() {
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
    harness
        .node2
        .runtime
        .handle_update_partition_leader(UpdatePartitionLeaderRequest {
            topic_name: "replicated.topic".to_string(),
            partition_index: 0,
            leader_id: 1,
            leader_epoch: 1,
        })
        .unwrap();
    harness
        .node2
        .runtime
        .handle_update_partition_replication(UpdatePartitionReplicationRequest {
            topic_name: "replicated.topic".to_string(),
            partition_index: 0,
            replicas: vec![1, 2],
            isr: vec![1, 2],
            leader_epoch: 1,
        })
        .unwrap();
    harness
        .node2
        .runtime
        .handle_update_replica_progress(UpdateReplicaProgressRequest {
            topic_name: "replicated.topic".to_string(),
            partition_index: 0,
            leader_epoch: 1,
            broker_id: 1,
            log_end_offset: 7,
            last_caught_up_ms: 100,
        })
        .unwrap();
    harness
        .node2
        .runtime
        .handle_update_replica_progress(UpdateReplicaProgressRequest {
            topic_name: "replicated.topic".to_string(),
            partition_index: 0,
            leader_epoch: 1,
            broker_id: 2,
            log_end_offset: 5,
            last_caught_up_ms: 100,
        })
        .unwrap();

    let transport = harness.transport_from_node1();
    let target = transport.resolve_target(2).unwrap();
    let before_failover = transport
        .send_to(
            &target,
            ClusterRpcRequest::GetPartitionState(GetPartitionStateRequest {
                topic_name: "replicated.topic".to_string(),
                partition_index: 0,
            }),
        )
        .unwrap();
    let ClusterRpcResponse::GetPartitionState(before_failover) = before_failover else {
        panic!("unexpected response variant");
    };

    let _ = transport
        .send_to(
            &target,
            ClusterRpcRequest::AppendMetadata(AppendMetadataRequest {
                term: 3,
                leader_id: 2,
                prev_metadata_offset: harness.node2.runtime.metadata_image().metadata_offset,
                records: vec![crate::cluster::MetadataRecord::SetController { controller_id: 2 }],
            }),
        )
        .unwrap();
    let after_failover = transport
        .send_to(
            &target,
            ClusterRpcRequest::GetPartitionState(GetPartitionStateRequest {
                topic_name: "replicated.topic".to_string(),
                partition_index: 0,
            }),
        )
        .unwrap();
    let ClusterRpcResponse::GetPartitionState(after_failover) = after_failover else {
        panic!("unexpected response variant");
    };

    assert!(before_failover.found);
    assert_eq!(before_failover.leader_id, 1);
    assert_eq!(before_failover.high_watermark, 7);
    assert_eq!(before_failover.leader_log_end_offset, 7);
    assert_eq!(after_failover, before_failover);
    assert_eq!(harness.node2.runtime.metadata_image().controller_id, 2);
}

#[test]
fn in_memory_remote_transport_advances_reassignment_lifecycle() {
    let harness = TwoNodeClusterHarness::new_controller_pair();
    harness
        .node2
        .runtime
        .sync_local_topics(
            &[crate::store::TopicMetadata {
                name: "reassign.topic".to_string(),
                partitions: vec![crate::store::PartitionMetadata { partition: 0 }],
            }],
            1,
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
    let transport = harness.transport_from_node1();
    let target = transport.resolve_target(2).unwrap();

    let _ = transport
        .send_to(
            &target,
            ClusterRpcRequest::BeginPartitionReassignment(BeginPartitionReassignmentRequest {
                topic_name: "reassign.topic".to_string(),
                partition_index: 0,
                target_replicas: vec![2, 3],
            }),
        )
        .unwrap();
    let _ = transport
        .send_to(
            &target,
            ClusterRpcRequest::UpdateReplicaProgress(UpdateReplicaProgressRequest {
                topic_name: "reassign.topic".to_string(),
                partition_index: 0,
                leader_epoch: 0,
                broker_id: 2,
                log_end_offset: 0,
                last_caught_up_ms: 100,
            }),
        )
        .unwrap();
    let _ = transport
        .send_to(
            &target,
            ClusterRpcRequest::UpdateReplicaProgress(UpdateReplicaProgressRequest {
                topic_name: "reassign.topic".to_string(),
                partition_index: 0,
                leader_epoch: 0,
                broker_id: 3,
                log_end_offset: 0,
                last_caught_up_ms: 100,
            }),
        )
        .unwrap();
    let _ = transport
        .send_to(
            &target,
            ClusterRpcRequest::AdvancePartitionReassignment(AdvancePartitionReassignmentRequest {
                topic_name: "reassign.topic".to_string(),
                partition_index: 0,
                step: crate::cluster::ReassignmentStep::ExpandingIsr,
            }),
        )
        .unwrap();
    let _ = transport
        .send_to(
            &target,
            ClusterRpcRequest::AdvancePartitionReassignment(AdvancePartitionReassignmentRequest {
                topic_name: "reassign.topic".to_string(),
                partition_index: 0,
                step: crate::cluster::ReassignmentStep::LeaderSwitch,
            }),
        )
        .unwrap();

    let reassignment = harness
        .node2
        .runtime
        .metadata_image()
        .partition_reassignment("reassign.topic", 0)
        .unwrap();
    assert_eq!(
        reassignment.step,
        crate::cluster::ReassignmentStep::LeaderSwitch
    );
    assert_eq!(
        harness.node2.runtime.metadata_image().topics[0].partitions[0].leader_id,
        2
    );
}

#[test]
fn in_memory_remote_transport_rejects_stale_append_after_failover() {
    let harness = TwoNodeClusterHarness::new_controller_pair();
    let transport = harness.transport_from_node1();
    let target = transport.resolve_target(2).unwrap();

    let initial_offset = harness.node2.runtime.metadata_image().metadata_offset;
    let _ = transport
        .send_to(
            &target,
            ClusterRpcRequest::AppendMetadata(AppendMetadataRequest {
                term: 3,
                leader_id: 2,
                prev_metadata_offset: initial_offset,
                records: vec![crate::cluster::MetadataRecord::SetController { controller_id: 2 }],
            }),
        )
        .unwrap();

    let response = transport
        .send_to(
            &target,
            ClusterRpcRequest::AppendMetadata(AppendMetadataRequest {
                term: 2,
                leader_id: 1,
                prev_metadata_offset: initial_offset,
                records: vec![crate::cluster::MetadataRecord::SetController { controller_id: 1 }],
            }),
        )
        .unwrap();

    let ClusterRpcResponse::AppendMetadata(response) = response else {
        panic!("unexpected response variant");
    };
    assert!(!response.accepted);
    assert_eq!(harness.node2.runtime.metadata_image().controller_id, 2);
}
