use crate::cluster::{
    ClusterMetadataImage, UpdatePartitionLeaderRequest, UpdatePartitionReplicationRequest,
};

pub(super) fn partition_leader_matches(
    image: &ClusterMetadataImage,
    request: &UpdatePartitionLeaderRequest,
) -> bool {
    image
        .partition_state_view(&request.topic_name, request.partition_index)
        .is_some_and(|(leader_id, leader_epoch, _, _)| {
            leader_id == request.leader_id && leader_epoch == request.leader_epoch
        })
}

pub(super) fn partition_replication_matches(
    image: &ClusterMetadataImage,
    request: &UpdatePartitionReplicationRequest,
) -> bool {
    image
        .topics
        .iter()
        .find(|topic| topic.name == request.topic_name)
        .and_then(|topic| {
            topic
                .partitions
                .iter()
                .find(|partition| partition.partition == request.partition_index)
        })
        .is_some_and(|partition| {
            partition.replicas == request.replicas
                && partition.isr == request.isr
                && partition.leader_epoch == request.leader_epoch
        })
}

pub(super) fn rejected_replica_progress_high_watermark(
    image: &ClusterMetadataImage,
    topic_name: &str,
    partition_index: i32,
) -> i64 {
    image
        .topics
        .iter()
        .find(|topic| topic.name == topic_name)
        .and_then(|topic| {
            topic
                .partitions
                .iter()
                .find(|partition| partition.partition == partition_index)
        })
        .map(|partition| {
            if partition.replicas.len() <= 1 {
                0
            } else {
                partition.high_watermark
            }
        })
        .unwrap_or(0)
}
