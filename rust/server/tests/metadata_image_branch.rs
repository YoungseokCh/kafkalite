use kafkalite_server::cluster::{
    BrokerMetadata, ClusterMetadataImage, MetadataRecord, PartitionMetadataImage,
    PartitionReassignment, ReassignmentStep, ReplicaProgress, TopicMetadataImage,
};
use kafkalite_server::store::{PartitionMetadata, TopicMetadata};

fn image_with_partition() -> ClusterMetadataImage {
    ClusterMetadataImage {
        cluster_id: "cluster-a".to_string(),
        controller_id: 1,
        metadata_offset: -1,
        brokers: vec![],
        topics: vec![TopicMetadataImage {
            name: "topic-a".to_string(),
            partitions: vec![PartitionMetadataImage {
                partition: 0,
                leader_id: 1,
                leader_epoch: 1,
                high_watermark: 5,
                replicas: vec![1, 2],
                isr: vec![1, 2],
                replica_progress: vec![
                    ReplicaProgress {
                        broker_id: 1,
                        log_end_offset: 6,
                        last_caught_up_ms: 100,
                    },
                    ReplicaProgress {
                        broker_id: 2,
                        log_end_offset: 5,
                        last_caught_up_ms: 100,
                    },
                ],
                reassignment: None,
            }],
        }],
    }
}

#[test]
fn upsert_broker_and_topic_detect_noop_updates() {
    let mut image = ClusterMetadataImage::new("cluster-a".to_string(), 1);
    let broker = BrokerMetadata {
        node_id: 1,
        host: "node1".to_string(),
        port: 9092,
    };
    assert!(image.upsert_broker(broker.clone()));
    assert!(!image.upsert_broker(broker));

    let topic = TopicMetadataImage {
        name: "topic-a".to_string(),
        partitions: vec![],
    };
    assert!(image.upsert_topic(topic.clone()));
    assert!(!image.upsert_topic(topic));
}

#[test]
fn upsert_broker_and_topic_replace_changed_existing_entries() {
    let mut image = ClusterMetadataImage::new("cluster-a".to_string(), 1);

    assert!(image.upsert_broker(BrokerMetadata {
        node_id: 1,
        host: "node1".to_string(),
        port: 9092,
    }));
    assert!(image.upsert_broker(BrokerMetadata {
        node_id: 1,
        host: "node1-updated".to_string(),
        port: 19092,
    }));
    assert_eq!(image.brokers[0].host, "node1-updated");
    assert_eq!(image.brokers[0].port, 19092);

    assert!(image.upsert_topic(TopicMetadataImage {
        name: "topic-a".to_string(),
        partitions: vec![PartitionMetadataImage {
            partition: 0,
            leader_id: 1,
            leader_epoch: 0,
            high_watermark: 0,
            replicas: vec![1],
            isr: vec![1],
            replica_progress: vec![],
            reassignment: None,
        }],
    }));
    assert!(image.upsert_topic(TopicMetadataImage {
        name: "topic-a".to_string(),
        partitions: vec![PartitionMetadataImage {
            partition: 0,
            leader_id: 2,
            leader_epoch: 1,
            high_watermark: 3,
            replicas: vec![2],
            isr: vec![2],
            replica_progress: vec![],
            reassignment: None,
        }],
    }));
    assert_eq!(image.topics[0].partitions[0].leader_id, 2);
    assert_eq!(image.topics[0].partitions[0].high_watermark, 3);
}

#[test]
fn merge_store_topic_only_adds_missing_partitions() {
    let mut image = ClusterMetadataImage {
        cluster_id: "cluster-a".to_string(),
        controller_id: 1,
        metadata_offset: -1,
        brokers: vec![],
        topics: vec![TopicMetadataImage {
            name: "topic-a".to_string(),
            partitions: vec![PartitionMetadataImage {
                partition: 0,
                leader_id: 1,
                leader_epoch: 0,
                high_watermark: 0,
                replicas: vec![1],
                isr: vec![1],
                replica_progress: vec![],
                reassignment: None,
            }],
        }],
    };

    assert!(!image.merge_store_topic(&TopicMetadata {
        name: "missing-topic".to_string(),
        partitions: vec![PartitionMetadata { partition: 0 }],
    }));

    assert!(!image.merge_store_topic(&TopicMetadata {
        name: "topic-a".to_string(),
        partitions: vec![PartitionMetadata { partition: 0 }],
    }));

    assert!(image.merge_store_topic(&TopicMetadata {
        name: "topic-a".to_string(),
        partitions: vec![
            PartitionMetadata { partition: 2 },
            PartitionMetadata { partition: 1 }
        ],
    }));
    assert_eq!(
        image.topics[0]
            .partitions
            .iter()
            .map(|partition| partition.partition)
            .collect::<Vec<_>>(),
        vec![0, 1, 2]
    );
}

#[test]
fn update_partition_leader_validates_epoch_and_noop() {
    let mut image = image_with_partition();

    assert!(!image.update_partition_leader("missing", 0, 2, 2));
    assert!(!image.update_partition_leader("topic-a", 99, 2, 2));
    assert!(!image.update_partition_leader("topic-a", 0, 2, 0));
    assert!(!image.update_partition_leader("topic-a", 0, 1, 1));
    assert!(image.update_partition_leader("topic-a", 0, 3, 2));

    let partition = &image.topics[0].partitions[0];
    assert_eq!(partition.leader_id, 3);
    assert_eq!(partition.leader_epoch, 2);
    assert_eq!(partition.replicas[0], 3);
    assert_eq!(partition.isr[0], 3);
}

#[test]
fn update_partition_replication_recomputes_high_watermark() {
    let mut image = image_with_partition();

    assert!(!image.update_partition_replication("missing", 0, vec![1, 2], vec![1, 2], 1));
    assert!(!image.update_partition_replication("topic-a", 99, vec![1, 2], vec![1, 2], 1));
    assert!(!image.update_partition_replication("topic-a", 0, vec![1, 2], vec![1, 2], 0));
    assert!(!image.update_partition_replication("topic-a", 0, vec![1, 2], vec![1, 2], 1));

    assert!(image.update_partition_replication("topic-a", 0, vec![1, 2, 3], vec![1, 2], 2));
    let partition = &image.topics[0].partitions[0];
    assert_eq!(partition.leader_epoch, 2);
    assert_eq!(partition.high_watermark, 5);
}

#[test]
fn update_replica_progress_reconciles_isr_and_high_watermark() {
    let mut image = image_with_partition();

    assert!(!image.update_replica_progress(
        "missing",
        0,
        1,
        ReplicaProgress {
            broker_id: 2,
            log_end_offset: 6,
            last_caught_up_ms: 100,
        },
    ));
    assert!(!image.update_replica_progress(
        "topic-a",
        99,
        1,
        ReplicaProgress {
            broker_id: 2,
            log_end_offset: 6,
            last_caught_up_ms: 100,
        },
    ));
    assert!(!image.update_replica_progress(
        "topic-a",
        0,
        9,
        ReplicaProgress {
            broker_id: 2,
            log_end_offset: 6,
            last_caught_up_ms: 100,
        },
    ));

    assert!(image.update_replica_progress(
        "topic-a",
        0,
        1,
        ReplicaProgress {
            broker_id: 2,
            log_end_offset: 1,
            last_caught_up_ms: 100,
        },
    ));
    let partition = &image.topics[0].partitions[0];
    assert_eq!(partition.isr, vec![1]);
    assert_eq!(partition.high_watermark, 6);

    assert!(!image.update_replica_progress(
        "topic-a",
        0,
        1,
        ReplicaProgress {
            broker_id: 2,
            log_end_offset: 1,
            last_caught_up_ms: 100,
        },
    ));
}

#[test]
fn reassignment_lifecycle_enforces_preconditions() {
    let mut image = image_with_partition();

    assert!(!image.begin_partition_reassignment("topic-a", 0, vec![]));
    assert!(!image.advance_partition_reassignment("topic-a", 9, ReassignmentStep::ExpandingIsr,));
    assert!(!image.advance_partition_reassignment("topic-a", 0, ReassignmentStep::ExpandingIsr,));

    assert!(image.begin_partition_reassignment("topic-a", 0, vec![3, 1]));
    assert!(!image.begin_partition_reassignment("topic-a", 0, vec![3, 1]));
    assert!(!image.advance_partition_reassignment("topic-a", 0, ReassignmentStep::Planned,));
    assert!(!image.advance_partition_reassignment("topic-a", 0, ReassignmentStep::ExpandingIsr,));
    assert!(!image.advance_partition_reassignment("topic-a", 0, ReassignmentStep::LeaderSwitch,));
    assert!(!image.advance_partition_reassignment("topic-a", 0, ReassignmentStep::Shrinking,));
    assert!(!image.advance_partition_reassignment("topic-a", 0, ReassignmentStep::Complete,));

    assert!(image.update_replica_progress(
        "topic-a",
        0,
        1,
        ReplicaProgress {
            broker_id: 3,
            log_end_offset: 6,
            last_caught_up_ms: 100,
        },
    ));
    assert!(image.advance_partition_reassignment("topic-a", 0, ReassignmentStep::ExpandingIsr));
    assert!(image.advance_partition_reassignment("topic-a", 0, ReassignmentStep::LeaderSwitch));
    assert!(image.advance_partition_reassignment("topic-a", 0, ReassignmentStep::Shrinking));
    assert!(image.complete_partition_reassignment("topic-a", 0));
    assert!(image.partition_reassignment("topic-a", 0).is_none());
}

#[test]
fn leader_switch_with_empty_target_replicas_keeps_current_leader() {
    let mut image = image_with_partition();
    image.topics[0].partitions[0].reassignment = Some(PartitionReassignment {
        target_replicas: vec![],
        step: ReassignmentStep::Copying,
    });

    assert!(image.advance_partition_reassignment("topic-a", 0, ReassignmentStep::LeaderSwitch));

    let partition = &image.topics[0].partitions[0];
    assert_eq!(partition.leader_id, 1);
    assert_eq!(partition.leader_epoch, 1);
    assert_eq!(
        partition.reassignment.as_ref().unwrap().step,
        ReassignmentStep::LeaderSwitch
    );
}

#[test]
fn state_view_and_progress_presence_follow_partition_state() {
    let mut image = image_with_partition();
    image.topics[0].partitions[0].replica_progress.clear();

    assert!(!image.partition_has_replica_progress("topic-a", 0));
    let state = image.partition_state_view("topic-a", 0).unwrap();
    assert_eq!(state, (1, 1, 5, 0));
}

#[test]
fn apply_updates_controller_and_metadata_offset() {
    let mut image = ClusterMetadataImage::new("cluster-a".to_string(), 1);
    image.apply(MetadataRecord::SetController { controller_id: 2 });
    assert_eq!(image.controller_id, 2);
    assert_eq!(image.metadata_offset, 0);

    image.apply(MetadataRecord::RegisterBroker(BrokerMetadata {
        node_id: 1,
        host: "node1".to_string(),
        port: 9092,
    }));
    assert_eq!(image.metadata_offset, 1);
    assert_eq!(image.brokers.len(), 1);
}
