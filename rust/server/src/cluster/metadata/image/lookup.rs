use super::{ClusterMetadataImage, PartitionReassignment};

impl ClusterMetadataImage {
    pub fn partition_leader_id(&self, topic_name: &str, partition_index: i32) -> Option<i32> {
        self.topics
            .iter()
            .find(|topic| topic.name == topic_name)
            .and_then(|topic| {
                topic
                    .partitions
                    .iter()
                    .find(|partition| partition.partition == partition_index)
            })
            .map(|partition| partition.leader_id)
    }

    pub fn partition_high_watermark(&self, topic_name: &str, partition_index: i32) -> Option<i64> {
        self.topics
            .iter()
            .find(|topic| topic.name == topic_name)
            .and_then(|topic| {
                topic
                    .partitions
                    .iter()
                    .find(|partition| partition.partition == partition_index)
            })
            .map(|partition| partition.high_watermark)
    }

    pub fn partition_state_view(
        &self,
        topic_name: &str,
        partition_index: i32,
    ) -> Option<(i32, i32, i64, i64)> {
        self.topics
            .iter()
            .find(|topic| topic.name == topic_name)
            .and_then(|topic| {
                topic
                    .partitions
                    .iter()
                    .find(|partition| partition.partition == partition_index)
            })
            .map(|partition| {
                let leader_log_end_offset = partition
                    .replica_progress
                    .iter()
                    .find(|progress| progress.broker_id == partition.leader_id)
                    .map(|progress| progress.log_end_offset)
                    .unwrap_or(0);
                (
                    partition.leader_id,
                    partition.leader_epoch,
                    partition.high_watermark,
                    leader_log_end_offset,
                )
            })
    }

    pub fn partition_reassignment(
        &self,
        topic_name: &str,
        partition_index: i32,
    ) -> Option<PartitionReassignment> {
        self.topics
            .iter()
            .find(|topic| topic.name == topic_name)
            .and_then(|topic| {
                topic
                    .partitions
                    .iter()
                    .find(|p| p.partition == partition_index)
            })
            .and_then(|partition| partition.reassignment.clone())
    }

    pub fn partition_has_replica_progress(&self, topic_name: &str, partition_index: i32) -> bool {
        self.topics
            .iter()
            .find(|topic| topic.name == topic_name)
            .and_then(|topic| {
                topic
                    .partitions
                    .iter()
                    .find(|p| p.partition == partition_index)
            })
            .is_some_and(|partition| !partition.replica_progress.is_empty())
    }
}
