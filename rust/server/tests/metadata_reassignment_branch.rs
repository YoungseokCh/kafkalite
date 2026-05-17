use kafkalite_server::cluster::{
    ClusterMetadataImage, PartitionMetadataImage, PartitionReassignment, ReassignmentStep,
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
