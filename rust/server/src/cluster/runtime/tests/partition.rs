use tempfile::tempdir;

use crate::cluster::{
    ClusterRuntime, ProcessRole, UpdatePartitionLeaderRequest, UpdatePartitionReplicationRequest,
    UpdateReplicaProgressRequest,
};
use crate::config::Config;

#[test]
fn update_partition_leader_changes_metadata_image() {
    let dir = tempdir().unwrap();
    let mut config = Config::single_node(dir.path().join("data"), 19092, 1);
    config.cluster.node_id = 4;
    config.cluster.process_roles = vec![ProcessRole::Broker, ProcessRole::Controller];

    let runtime = ClusterRuntime::from_config(&config).unwrap();
    runtime
        .sync_local_topics(
            &[crate::store::TopicMetadata {
                name: "leader.topic".to_string(),
                partitions: vec![crate::store::PartitionMetadata { partition: 0 }],
            }],
            1,
        )
        .unwrap();

    let response = runtime
        .handle_update_partition_leader(UpdatePartitionLeaderRequest {
            topic_name: "leader.topic".to_string(),
            partition_index: 0,
            leader_id: 9,
            leader_epoch: 1,
        })
        .unwrap();

    assert!(response.accepted);
    let image = runtime.metadata_image();
    assert_eq!(image.partition_leader_id("leader.topic", 0), Some(9));
    assert_eq!(image.topics[0].partitions[0].leader_epoch, 1);
}

#[test]
fn update_partition_leader_preserves_replication_state() {
    let dir = tempdir().unwrap();
    let mut config = Config::single_node(dir.path().join("data"), 19092, 1);
    config.cluster.node_id = 4;
    config.cluster.process_roles = vec![ProcessRole::Broker, ProcessRole::Controller];
    let runtime = ClusterRuntime::from_config(&config).unwrap();
    runtime
        .sync_local_topics(
            &[crate::store::TopicMetadata {
                name: "leader.topic".to_string(),
                partitions: vec![crate::store::PartitionMetadata { partition: 0 }],
            }],
            1,
        )
        .unwrap();
    runtime
        .handle_update_partition_replication(UpdatePartitionReplicationRequest {
            topic_name: "leader.topic".to_string(),
            partition_index: 0,
            replicas: vec![1, 2],
            isr: vec![1, 2],
            leader_epoch: 1,
        })
        .unwrap();
    runtime
        .handle_update_replica_progress(UpdateReplicaProgressRequest {
            topic_name: "leader.topic".to_string(),
            partition_index: 0,
            leader_epoch: 1,
            broker_id: 1,
            log_end_offset: 10,
            last_caught_up_ms: 100,
        })
        .unwrap();

    runtime
        .handle_update_partition_leader(UpdatePartitionLeaderRequest {
            topic_name: "leader.topic".to_string(),
            partition_index: 0,
            leader_id: 2,
            leader_epoch: 2,
        })
        .unwrap();

    let partition = &runtime.metadata_image().topics[0].partitions[0];
    assert_eq!(partition.replicas, vec![1, 2]);
    assert!(partition.replica_progress.iter().any(|p| p.broker_id == 1));
    assert_eq!(partition.high_watermark, 10);
}

#[test]
fn update_partition_leader_rejects_older_epoch() {
    let dir = tempdir().unwrap();
    let mut config = Config::single_node(dir.path().join("data"), 19092, 1);
    config.cluster.node_id = 4;
    config.cluster.process_roles = vec![ProcessRole::Broker, ProcessRole::Controller];
    let runtime = ClusterRuntime::from_config(&config).unwrap();
    runtime
        .sync_local_topics(
            &[crate::store::TopicMetadata {
                name: "leader.topic".to_string(),
                partitions: vec![crate::store::PartitionMetadata { partition: 0 }],
            }],
            1,
        )
        .unwrap();
    let _ = runtime
        .handle_update_partition_leader(UpdatePartitionLeaderRequest {
            topic_name: "leader.topic".to_string(),
            partition_index: 0,
            leader_id: 9,
            leader_epoch: 3,
        })
        .unwrap();

    let rejected = runtime
        .handle_update_partition_leader(UpdatePartitionLeaderRequest {
            topic_name: "leader.topic".to_string(),
            partition_index: 0,
            leader_id: 8,
            leader_epoch: 2,
        })
        .unwrap();

    assert!(!rejected.accepted);
    assert_eq!(
        runtime
            .metadata_image()
            .partition_leader_id("leader.topic", 0),
        Some(9)
    );
    assert_eq!(
        runtime.metadata_image().topics[0].partitions[0].leader_epoch,
        3
    );
}

#[test]
fn update_partition_replication_changes_isr_and_replicas() {
    let dir = tempdir().unwrap();
    let mut config = Config::single_node(dir.path().join("data"), 19092, 1);
    config.cluster.node_id = 4;
    config.cluster.process_roles = vec![ProcessRole::Broker, ProcessRole::Controller];

    let runtime = ClusterRuntime::from_config(&config).unwrap();
    runtime
        .sync_local_topics(
            &[crate::store::TopicMetadata {
                name: "replication.topic".to_string(),
                partitions: vec![crate::store::PartitionMetadata { partition: 0 }],
            }],
            1,
        )
        .unwrap();

    let response = runtime
        .handle_update_partition_replication(UpdatePartitionReplicationRequest {
            topic_name: "replication.topic".to_string(),
            partition_index: 0,
            replicas: vec![1, 2, 3],
            isr: vec![1, 2],
            leader_epoch: 2,
        })
        .unwrap();

    assert!(response.accepted);
    let image = runtime.metadata_image();
    assert_eq!(image.topics[0].partitions[0].replicas, vec![1, 2, 3]);
    assert_eq!(image.topics[0].partitions[0].isr, vec![1, 2]);
    assert_eq!(image.topics[0].partitions[0].leader_epoch, 2);
}

#[test]
fn update_replica_progress_computes_high_watermark_from_isr() {
    let dir = tempdir().unwrap();
    let mut config = Config::single_node(dir.path().join("data"), 19092, 1);
    config.cluster.node_id = 4;
    config.cluster.process_roles = vec![ProcessRole::Broker, ProcessRole::Controller];

    let runtime = ClusterRuntime::from_config(&config).unwrap();
    runtime
        .sync_local_topics(
            &[crate::store::TopicMetadata {
                name: "progress.topic".to_string(),
                partitions: vec![crate::store::PartitionMetadata { partition: 0 }],
            }],
            1,
        )
        .unwrap();
    runtime
        .handle_update_partition_replication(UpdatePartitionReplicationRequest {
            topic_name: "progress.topic".to_string(),
            partition_index: 0,
            replicas: vec![1, 2],
            isr: vec![1, 2],
            leader_epoch: 1,
        })
        .unwrap();
    runtime
        .handle_update_replica_progress(UpdateReplicaProgressRequest {
            topic_name: "progress.topic".to_string(),
            partition_index: 0,
            leader_epoch: 1,
            broker_id: 1,
            log_end_offset: 10,
            last_caught_up_ms: 100,
        })
        .unwrap();
    let response = runtime
        .handle_update_replica_progress(UpdateReplicaProgressRequest {
            topic_name: "progress.topic".to_string(),
            partition_index: 0,
            leader_epoch: 1,
            broker_id: 2,
            log_end_offset: 8,
            last_caught_up_ms: 100,
        })
        .unwrap();

    assert!(response.accepted);
    assert_eq!(response.high_watermark, 10);
    assert_eq!(
        runtime
            .metadata_image()
            .partition_high_watermark("progress.topic", 0),
        Some(10)
    );
}

#[test]
fn replica_progress_reconciles_isr_by_lag() {
    let dir = tempdir().unwrap();
    let mut config = Config::single_node(dir.path().join("data"), 19092, 1);
    config.cluster.node_id = 4;
    config.cluster.process_roles = vec![ProcessRole::Broker, ProcessRole::Controller];

    let runtime = ClusterRuntime::from_config(&config).unwrap();
    runtime
        .sync_local_topics(
            &[crate::store::TopicMetadata {
                name: "isr.topic".to_string(),
                partitions: vec![crate::store::PartitionMetadata { partition: 0 }],
            }],
            1,
        )
        .unwrap();
    runtime
        .handle_update_partition_replication(UpdatePartitionReplicationRequest {
            topic_name: "isr.topic".to_string(),
            partition_index: 0,
            replicas: vec![1, 2, 3],
            isr: vec![1, 2, 3],
            leader_epoch: 1,
        })
        .unwrap();
    runtime
        .handle_update_replica_progress(UpdateReplicaProgressRequest {
            topic_name: "isr.topic".to_string(),
            partition_index: 0,
            leader_epoch: 1,
            broker_id: 1,
            log_end_offset: 10,
            last_caught_up_ms: 100,
        })
        .unwrap();
    runtime
        .handle_update_replica_progress(UpdateReplicaProgressRequest {
            topic_name: "isr.topic".to_string(),
            partition_index: 0,
            leader_epoch: 1,
            broker_id: 2,
            log_end_offset: 10,
            last_caught_up_ms: 100,
        })
        .unwrap();
    runtime
        .handle_update_replica_progress(UpdateReplicaProgressRequest {
            topic_name: "isr.topic".to_string(),
            partition_index: 0,
            leader_epoch: 1,
            broker_id: 3,
            log_end_offset: 5,
            last_caught_up_ms: 100,
        })
        .unwrap();

    let image = runtime.metadata_image();
    assert_eq!(image.topics[0].partitions[0].isr, vec![1, 2]);
    assert_eq!(image.topics[0].partitions[0].high_watermark, 10);
}
