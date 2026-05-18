use tempfile::tempdir;

use kafkalite_server::cluster::{
    AdvancePartitionReassignmentRequest, BeginPartitionReassignmentRequest, ClusterRuntime,
    ProcessRole, ReassignmentStep, UpdatePartitionLeaderRequest, UpdatePartitionReplicationRequest,
};
use kafkalite_server::config::Config;

#[test]
fn routed_mutations_fail_closed_without_known_controller_target() {
    let dir = tempdir().unwrap();
    let mut config = Config::single_node(dir.path().join("data"), 19092, 1);
    config.cluster.process_roles = vec![ProcessRole::Broker];
    config.cluster.controller_quorum_voters = super::voters(&[1, 2]);
    let runtime = ClusterRuntime::from_config(&config).unwrap();
    let transport = runtime.local_transport();

    let leader_response = runtime
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
    assert!(!leader_response.accepted);

    let replication_response = runtime
        .route_update_partition_replication(
            &transport,
            UpdatePartitionReplicationRequest {
                topic_name: "route.topic".to_string(),
                partition_index: 0,
                replicas: vec![1, 2],
                isr: vec![1],
                leader_epoch: 1,
            },
        )
        .unwrap();
    assert!(!replication_response.accepted);

    let reassignment_response = runtime
        .route_begin_partition_reassignment(
            &transport,
            BeginPartitionReassignmentRequest {
                topic_name: "route.topic".to_string(),
                partition_index: 0,
                target_replicas: vec![2],
            },
        )
        .unwrap();
    assert!(!reassignment_response.accepted);
}

#[test]
fn advance_reassignment_rejects_when_no_reassignment_exists() {
    let dir = tempdir().unwrap();
    let mut config = Config::single_node(dir.path().join("data"), 19092, 1);
    config.cluster.process_roles = vec![ProcessRole::Broker, ProcessRole::Controller];
    let runtime = ClusterRuntime::from_config(&config).unwrap();
    runtime
        .sync_local_topics(
            &[kafkalite_server::store::TopicMetadata {
                name: "reassign-missing.topic".to_string(),
                partitions: vec![kafkalite_server::store::PartitionMetadata { partition: 0 }],
            }],
            1,
        )
        .unwrap();

    let response = runtime
        .handle_advance_partition_reassignment(AdvancePartitionReassignmentRequest {
            topic_name: "reassign-missing.topic".to_string(),
            partition_index: 0,
            step: ReassignmentStep::Copying,
        })
        .unwrap();
    assert!(!response.accepted);
}

#[test]
fn reassignment_mutations_reject_when_runtime_is_not_metadata_leader() {
    let dir = tempdir().unwrap();
    let mut config = Config::single_node(dir.path().join("data"), 19092, 1);
    config.cluster.process_roles = vec![ProcessRole::Broker];
    config.cluster.controller_quorum_voters = super::voters(&[1, 2]);
    let runtime = ClusterRuntime::from_config(&config).unwrap();

    let metadata_offset = runtime.metadata_image().metadata_offset;
    let begin = runtime
        .handle_begin_partition_reassignment(BeginPartitionReassignmentRequest {
            topic_name: "reassign-blocked.topic".to_string(),
            partition_index: 0,
            target_replicas: vec![2],
        })
        .unwrap();
    assert!(!begin.accepted);
    assert_eq!(begin.metadata_offset, metadata_offset);

    let advance = runtime
        .handle_advance_partition_reassignment(AdvancePartitionReassignmentRequest {
            topic_name: "reassign-blocked.topic".to_string(),
            partition_index: 0,
            step: ReassignmentStep::Copying,
        })
        .unwrap();
    assert!(!advance.accepted);
    assert_eq!(advance.metadata_offset, metadata_offset);
}

#[test]
fn update_partition_leader_rejects_when_runtime_is_not_metadata_leader() {
    let dir = tempdir().unwrap();
    let mut config = Config::single_node(dir.path().join("data"), 19092, 1);
    config.cluster.process_roles = vec![ProcessRole::Broker];
    config.cluster.controller_quorum_voters = super::voters(&[1, 2]);
    let runtime = ClusterRuntime::from_config(&config).unwrap();

    let metadata_offset = runtime.metadata_image().metadata_offset;
    let response = runtime
        .handle_update_partition_leader(UpdatePartitionLeaderRequest {
            topic_name: "leader-blocked.topic".to_string(),
            partition_index: 0,
            leader_id: 2,
            leader_epoch: 1,
        })
        .unwrap();

    assert!(!response.accepted);
    assert_eq!(response.metadata_offset, metadata_offset);
}

#[test]
fn begin_reassignment_rejects_missing_partition_when_runtime_is_writable() {
    let dir = tempdir().unwrap();
    let mut config = Config::single_node(dir.path().join("data"), 19092, 1);
    config.cluster.process_roles = vec![ProcessRole::Broker, ProcessRole::Controller];
    let runtime = ClusterRuntime::from_config(&config).unwrap();

    let metadata_offset = runtime.metadata_image().metadata_offset;
    let response = runtime
        .handle_begin_partition_reassignment(BeginPartitionReassignmentRequest {
            topic_name: "reassign-missing-preview.topic".to_string(),
            partition_index: 0,
            target_replicas: vec![2],
        })
        .unwrap();

    assert!(!response.accepted);
    assert_eq!(response.metadata_offset, metadata_offset);
}

#[test]
fn route_methods_take_local_fast_path_when_runtime_is_writable() {
    let dir = tempdir().unwrap();
    let mut config = Config::single_node(dir.path().join("data"), 19092, 1);
    config.cluster.process_roles = vec![ProcessRole::Broker, ProcessRole::Controller];
    let runtime = ClusterRuntime::from_config(&config).unwrap();
    runtime
        .sync_local_topics(
            &[kafkalite_server::store::TopicMetadata {
                name: "route-local.topic".to_string(),
                partitions: vec![kafkalite_server::store::PartitionMetadata { partition: 0 }],
            }],
            1,
        )
        .unwrap();

    let transport = runtime.local_transport();
    let leader = runtime
        .route_update_partition_leader(
            &transport,
            UpdatePartitionLeaderRequest {
                topic_name: "route-local.topic".to_string(),
                partition_index: 0,
                leader_id: 1,
                leader_epoch: 1,
            },
        )
        .unwrap();
    assert!(leader.accepted);

    let replication = runtime
        .route_update_partition_replication(
            &transport,
            UpdatePartitionReplicationRequest {
                topic_name: "route-local.topic".to_string(),
                partition_index: 0,
                replicas: vec![1],
                isr: vec![1],
                leader_epoch: 1,
            },
        )
        .unwrap();
    assert!(replication.accepted);

    let reassignment = runtime
        .route_begin_partition_reassignment(
            &transport,
            BeginPartitionReassignmentRequest {
                topic_name: "route-local.topic".to_string(),
                partition_index: 0,
                target_replicas: vec![1],
            },
        )
        .unwrap();
    assert!(reassignment.accepted);
}
