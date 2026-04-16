use kafkalite_server::cluster::ApplyReplicaRecordsRequest;
use kafkalite_server::cluster::{
    AdvancePartitionReassignmentRequest, AppendMetadataRequest, BeginPartitionReassignmentRequest,
    BrokerHeartbeatRequest, ClusterRpcRequest, ClusterRuntime, ControllerQuorumVoter,
    GetPartitionStateRequest, MetadataRecord, ProcessRole, ReassignmentStep, RegisterBrokerRequest,
    ReplicaFetchRequest, UpdatePartitionLeaderRequest, UpdatePartitionReplicationRequest,
    UpdateReplicaProgressRequest, VoteRequest,
};
use kafkalite_server::config::Config;
use tempfile::tempdir;

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

#[test]
fn write_and_auto_create_flags_follow_role_and_quorum_shape() {
    let broker_dir = tempdir().unwrap();
    let mut broker_config = Config::single_node(broker_dir.path().join("data"), 19092, 1);
    broker_config.cluster.process_roles = vec![ProcessRole::Broker];
    broker_config.cluster.controller_quorum_voters = voters(&[1, 2]);
    let broker_runtime = ClusterRuntime::from_config(&broker_config).unwrap();
    assert!(!broker_runtime.can_write_metadata_locally());
    assert!(!broker_runtime.can_auto_create_topics_locally());

    let single_dir = tempdir().unwrap();
    let mut single_config = Config::single_node(single_dir.path().join("data"), 19093, 1);
    single_config.cluster.process_roles = vec![ProcessRole::Broker];
    let single_runtime = ClusterRuntime::from_config(&single_config).unwrap();
    assert!(single_runtime.can_write_metadata_locally());
    assert!(single_runtime.can_auto_create_topics_locally());
}

#[test]
fn register_and_heartbeat_are_rejected_when_node_is_not_leader() {
    let node1_dir = tempdir().unwrap();
    let mut node1_config = Config::single_node(node1_dir.path().join("data"), 19092, 1);
    node1_config.cluster.node_id = 1;
    node1_config.cluster.process_roles = vec![ProcessRole::Controller];
    node1_config.cluster.controller_quorum_voters = voters(&[1, 2]);
    let node1 = ClusterRuntime::from_config(&node1_config).unwrap();

    let _ = node1
        .handle_append_metadata(AppendMetadataRequest {
            term: 1,
            leader_id: 2,
            prev_metadata_offset: node1.metadata_image().metadata_offset,
            records: vec![MetadataRecord::SetController { controller_id: 2 }],
        })
        .unwrap();

    let registration = node1
        .handle_register_broker(
            RegisterBrokerRequest {
                node_id: 99,
                advertised_host: "broker-99.local".to_string(),
                advertised_port: 39092,
            },
            1_000,
        )
        .unwrap();
    assert!(!registration.accepted);

    let heartbeat = node1
        .handle_broker_heartbeat(BrokerHeartbeatRequest {
            node_id: 1,
            broker_epoch: 1,
            timestamp_ms: 1_001,
        })
        .unwrap();
    assert!(!heartbeat.accepted);
    assert_eq!(heartbeat.leader_id, Some(2));
}

#[test]
fn append_and_vote_reject_non_voter_and_stale_offsets() {
    let dir = tempdir().unwrap();
    let mut config = Config::single_node(dir.path().join("data"), 19092, 1);
    config.cluster.node_id = 1;
    config.cluster.process_roles = vec![ProcessRole::Controller];
    config.cluster.controller_quorum_voters = voters(&[1, 2]);
    let runtime = ClusterRuntime::from_config(&config).unwrap();

    let non_voter_append = runtime
        .handle_append_metadata(AppendMetadataRequest {
            term: 1,
            leader_id: 9,
            prev_metadata_offset: runtime.metadata_image().metadata_offset,
            records: vec![MetadataRecord::SetController { controller_id: 9 }],
        })
        .unwrap();
    assert!(!non_voter_append.accepted);

    let accepted = runtime
        .handle_append_metadata(AppendMetadataRequest {
            term: 1,
            leader_id: 1,
            prev_metadata_offset: runtime.metadata_image().metadata_offset,
            records: vec![MetadataRecord::SetController { controller_id: 1 }],
        })
        .unwrap();
    assert!(accepted.accepted);

    let stale_vote = runtime
        .handle_vote(VoteRequest {
            term: 2,
            candidate_id: 1,
            last_metadata_offset: -1,
        })
        .unwrap();
    assert!(!stale_vote.vote_granted);
}

#[test]
fn routed_mutations_fail_closed_without_known_controller_target() {
    let dir = tempdir().unwrap();
    let mut config = Config::single_node(dir.path().join("data"), 19092, 1);
    config.cluster.process_roles = vec![ProcessRole::Broker];
    config.cluster.controller_quorum_voters = voters(&[1, 2]);
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
fn replica_progress_reports_current_high_watermark_when_not_leader() {
    let dir = tempdir().unwrap();
    let mut config = Config::single_node(dir.path().join("data"), 19092, 1);
    config.cluster.node_id = 1;
    config.cluster.process_roles = vec![ProcessRole::Controller];
    config.cluster.controller_quorum_voters = voters(&[1, 2]);
    let runtime = ClusterRuntime::from_config(&config).unwrap();

    runtime
        .sync_local_topics(
            &[kafkalite_server::store::TopicMetadata {
                name: "progress.topic".to_string(),
                partitions: vec![kafkalite_server::store::PartitionMetadata { partition: 0 }],
            }],
            1,
        )
        .unwrap();

    let _ = runtime
        .handle_append_metadata(AppendMetadataRequest {
            term: 1,
            leader_id: 2,
            prev_metadata_offset: runtime.metadata_image().metadata_offset,
            records: vec![
                MetadataRecord::SetController { controller_id: 2 },
                MetadataRecord::UpdatePartitionReplication {
                    topic_name: "progress.topic".to_string(),
                    partition_index: 0,
                    replicas: vec![1, 2],
                    isr: vec![1],
                    leader_epoch: 1,
                },
                MetadataRecord::UpdateReplicaProgress {
                    topic_name: "progress.topic".to_string(),
                    partition_index: 0,
                    leader_epoch: 1,
                    progress: kafkalite_server::cluster::ReplicaProgress {
                        broker_id: 1,
                        log_end_offset: 7,
                        last_caught_up_ms: 100,
                    },
                },
            ],
        })
        .unwrap();

    let response = runtime
        .handle_update_replica_progress(UpdateReplicaProgressRequest {
            topic_name: "progress.topic".to_string(),
            partition_index: 0,
            leader_epoch: 1,
            broker_id: 2,
            log_end_offset: 7,
            last_caught_up_ms: 100,
        })
        .unwrap();
    assert!(!response.accepted);
    assert_eq!(response.high_watermark, 7);
}

#[test]
fn replica_fetch_reports_not_implemented_error() {
    let dir = tempdir().unwrap();
    let config = Config::single_node(dir.path().join("data"), 19092, 1);
    let runtime = ClusterRuntime::from_config(&config).unwrap();

    let err = runtime
        .handle_replica_fetch(ReplicaFetchRequest {
            topic_name: "topic-a".to_string(),
            partition_index: 0,
            start_offset: 0,
            max_records: 1,
        })
        .unwrap_err()
        .to_string();
    assert!(err.contains("replica fetch requires broker data-plane transport"));
}

#[test]
fn run_election_self_majority_promotes_local_leader() {
    let dir = tempdir().unwrap();
    let mut config = Config::single_node(dir.path().join("data"), 19092, 1);
    config.cluster.process_roles = vec![ProcessRole::Controller];
    let runtime = ClusterRuntime::from_config(&config).unwrap();

    let elected = runtime
        .run_election(&runtime.local_transport(), &[])
        .unwrap();

    assert!(elected);
    assert_eq!(
        runtime.quorum_snapshot().leader_id,
        Some(config.cluster.node_id)
    );
    assert_eq!(
        runtime.metadata_image().controller_id,
        config.cluster.node_id
    );
}

#[test]
fn replica_progress_rejects_missing_partition_and_epoch_mismatch() {
    let dir = tempdir().unwrap();
    let mut config = Config::single_node(dir.path().join("data"), 19092, 1);
    config.cluster.process_roles = vec![ProcessRole::Broker, ProcessRole::Controller];
    let runtime = ClusterRuntime::from_config(&config).unwrap();

    let missing = runtime
        .handle_update_replica_progress(UpdateReplicaProgressRequest {
            topic_name: "missing".to_string(),
            partition_index: 0,
            leader_epoch: 1,
            broker_id: 1,
            log_end_offset: 1,
            last_caught_up_ms: 1,
        })
        .unwrap();
    assert!(!missing.accepted);
    assert_eq!(missing.high_watermark, 0);

    runtime
        .sync_local_topics(
            &[kafkalite_server::store::TopicMetadata {
                name: "progress-epoch.topic".to_string(),
                partitions: vec![kafkalite_server::store::PartitionMetadata { partition: 0 }],
            }],
            1,
        )
        .unwrap();
    runtime
        .handle_update_partition_replication(UpdatePartitionReplicationRequest {
            topic_name: "progress-epoch.topic".to_string(),
            partition_index: 0,
            replicas: vec![1],
            isr: vec![1],
            leader_epoch: 2,
        })
        .unwrap();

    let stale_epoch = runtime
        .handle_update_replica_progress(UpdateReplicaProgressRequest {
            topic_name: "progress-epoch.topic".to_string(),
            partition_index: 0,
            leader_epoch: 1,
            broker_id: 1,
            log_end_offset: 1,
            last_caught_up_ms: 1,
        })
        .unwrap();
    assert!(!stale_epoch.accepted);
}

#[test]
fn partition_state_reports_missing_and_found_paths() {
    let dir = tempdir().unwrap();
    let mut config = Config::single_node(dir.path().join("data"), 19092, 1);
    config.cluster.process_roles = vec![ProcessRole::Broker, ProcessRole::Controller];
    let runtime = ClusterRuntime::from_config(&config).unwrap();

    let missing = runtime
        .handle_get_partition_state(GetPartitionStateRequest {
            topic_name: "missing".to_string(),
            partition_index: 0,
        })
        .unwrap();
    assert!(!missing.found);

    runtime
        .sync_local_topics(
            &[kafkalite_server::store::TopicMetadata {
                name: "state.topic".to_string(),
                partitions: vec![kafkalite_server::store::PartitionMetadata { partition: 0 }],
            }],
            1,
        )
        .unwrap();

    let found = runtime
        .handle_get_partition_state(GetPartitionStateRequest {
            topic_name: "state.topic".to_string(),
            partition_index: 0,
        })
        .unwrap();
    assert!(found.found);
    assert_eq!(found.leader_id, 1);
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

#[test]
fn elected_multi_voter_controller_can_write_and_auto_create() {
    let dir = tempdir().unwrap();
    let mut config = Config::single_node(dir.path().join("data"), 19092, 1);
    config.cluster.node_id = 1;
    config.cluster.process_roles = vec![ProcessRole::Controller];
    config.cluster.controller_quorum_voters = voters(&[1, 2]);
    let runtime = ClusterRuntime::from_config(&config).unwrap();

    assert!(!runtime.can_write_metadata_locally());
    assert!(!runtime.can_auto_create_topics_locally());

    let _ = runtime
        .handle_append_metadata(AppendMetadataRequest {
            term: 1,
            leader_id: 1,
            prev_metadata_offset: runtime.metadata_image().metadata_offset,
            records: vec![MetadataRecord::SetController { controller_id: 1 }],
        })
        .unwrap();

    assert!(runtime.can_write_metadata_locally());
    assert!(runtime.can_auto_create_topics_locally());
}

#[test]
fn vote_rejects_non_voter_candidate() {
    let dir = tempdir().unwrap();
    let mut config = Config::single_node(dir.path().join("data"), 19092, 1);
    config.cluster.node_id = 1;
    config.cluster.process_roles = vec![ProcessRole::Controller];
    config.cluster.controller_quorum_voters = voters(&[1, 2]);
    let runtime = ClusterRuntime::from_config(&config).unwrap();

    let response = runtime
        .handle_vote(VoteRequest {
            term: 1,
            candidate_id: 9,
            last_metadata_offset: 0,
        })
        .unwrap();
    assert!(!response.vote_granted);
}

#[test]
fn dispatch_replica_fetch_bubbles_runtime_transport_error() {
    let dir = tempdir().unwrap();
    let config = Config::single_node(dir.path().join("data"), 19092, 1);
    let runtime = ClusterRuntime::from_config(&config).unwrap();

    let err = runtime
        .dispatch(ClusterRpcRequest::ReplicaFetch(ReplicaFetchRequest {
            topic_name: "topic-a".to_string(),
            partition_index: 0,
            start_offset: 0,
            max_records: 1,
        }))
        .unwrap_err()
        .to_string();

    assert!(err.contains("replica fetch requires broker data-plane transport"));
}

#[test]
fn dispatch_apply_replica_records_requires_broker_data_plane_transport() {
    let dir = tempdir().unwrap();
    let config = Config::single_node(dir.path().join("data"), 19092, 1);
    let runtime = ClusterRuntime::from_config(&config).unwrap();

    let err = runtime
        .dispatch(ClusterRpcRequest::ApplyReplicaRecords(
            ApplyReplicaRecordsRequest {
                topic_name: "topic-a".to_string(),
                partition_index: 0,
                records: vec![],
                now_ms: 0,
            },
        ))
        .unwrap_err()
        .to_string();

    assert!(err.contains("apply replica records requires broker data-plane transport"));
}
