use tempfile::tempdir;

use kafkalite_server::cluster::{
    AppendMetadataRequest, ApplyReplicaRecordsRequest, ClusterRpcRequest, ClusterRuntime,
    GetPartitionStateRequest, MetadataRecord, ProcessRole, ReplicaFetchRequest,
    UpdatePartitionReplicationRequest, UpdateReplicaProgressRequest,
};
use kafkalite_server::config::Config;

#[test]
fn replica_progress_reports_current_high_watermark_when_not_leader() {
    let dir = tempdir().unwrap();
    let mut config = Config::single_node(dir.path().join("data"), 19092, 1);
    config.cluster.node_id = 1;
    config.cluster.process_roles = vec![ProcessRole::Controller];
    config.cluster.controller_quorum_voters = super::voters(&[1, 2]);
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
