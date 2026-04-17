use kafkalite_server::cluster::{
    ClusterMetadataImage, MetadataRecord, PartitionMetadataImage, ReassignmentStep,
    ReplicaProgress, TopicMetadataImage,
};

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
fn partition_lookup_helpers_cover_present_and_missing_paths() {
    let image = image_with_partition();

    assert_eq!(image.partition_leader_id("topic-a", 0), Some(1));
    assert_eq!(image.partition_high_watermark("topic-a", 0), Some(5));
    assert!(image.partition_has_replica_progress("topic-a", 0));
    assert_eq!(image.partition_leader_id("missing", 0), None);
    assert_eq!(image.partition_leader_id("topic-a", 9), None);
    assert_eq!(image.partition_high_watermark("missing", 0), None);
    assert_eq!(image.partition_high_watermark("topic-a", 9), None);
    assert_eq!(image.partition_state_view("missing", 0), None);
    assert_eq!(image.partition_state_view("topic-a", 9), None);
    assert_eq!(image.partition_reassignment("missing", 0), None);
    assert_eq!(image.partition_reassignment("topic-a", 9), None);
    assert!(!image.partition_has_replica_progress("missing", 0));
    assert!(!image.partition_has_replica_progress("topic-a", 9));
}

#[test]
fn apply_covers_topic_progress_and_reassignment_variants() {
    let mut image = ClusterMetadataImage::new("cluster-a".to_string(), 1);
    image.apply(MetadataRecord::UpsertTopic(TopicMetadataImage {
        name: "topic-a".to_string(),
        partitions: vec![PartitionMetadataImage {
            partition: 0,
            leader_id: 1,
            leader_epoch: 1,
            high_watermark: 0,
            replicas: vec![1, 2],
            isr: vec![1],
            replica_progress: vec![ReplicaProgress {
                broker_id: 1,
                log_end_offset: 1,
                last_caught_up_ms: 100,
            }],
            reassignment: None,
        }],
    }));
    image.apply(MetadataRecord::UpdatePartitionLeader {
        topic_name: "topic-a".to_string(),
        partition_index: 0,
        leader_id: 1,
        leader_epoch: 2,
    });
    image.apply(MetadataRecord::UpdatePartitionReplication {
        topic_name: "topic-a".to_string(),
        partition_index: 0,
        replicas: vec![1, 2, 3],
        isr: vec![1],
        leader_epoch: 2,
    });
    image.apply(MetadataRecord::UpdateReplicaProgress {
        topic_name: "topic-a".to_string(),
        partition_index: 0,
        leader_epoch: 2,
        progress: ReplicaProgress {
            broker_id: 3,
            log_end_offset: 1,
            last_caught_up_ms: 101,
        },
    });
    image.apply(MetadataRecord::BeginPartitionReassignment {
        topic_name: "topic-a".to_string(),
        partition_index: 0,
        target_replicas: vec![3, 1],
    });
    image.apply(MetadataRecord::AdvancePartitionReassignment {
        topic_name: "topic-a".to_string(),
        partition_index: 0,
        step: ReassignmentStep::ExpandingIsr,
    });
    image.apply(MetadataRecord::AdvancePartitionReassignment {
        topic_name: "topic-a".to_string(),
        partition_index: 0,
        step: ReassignmentStep::LeaderSwitch,
    });
    image.apply(MetadataRecord::AdvancePartitionReassignment {
        topic_name: "topic-a".to_string(),
        partition_index: 0,
        step: ReassignmentStep::Shrinking,
    });
    image.apply(MetadataRecord::CompletePartitionReassignment {
        topic_name: "topic-a".to_string(),
        partition_index: 0,
    });

    let partition = &image.topics[0].partitions[0];
    assert_eq!(image.metadata_offset, 8);
    assert_eq!(partition.leader_id, 3);
    assert_eq!(partition.leader_epoch, 3);
    assert_eq!(partition.replicas, vec![3, 1]);
    assert_eq!(partition.isr, vec![1, 3]);
    assert!(partition.reassignment.is_none());
}

#[test]
fn reassignment_can_advance_to_copying_before_expanding() {
    let mut image = image_with_partition();

    assert!(image.begin_partition_reassignment("topic-a", 0, vec![3, 1]));
    assert!(image.advance_partition_reassignment("topic-a", 0, ReassignmentStep::Copying));

    let reassignment = image.partition_reassignment("topic-a", 0).unwrap();
    assert_eq!(reassignment.step, ReassignmentStep::Copying);
}
