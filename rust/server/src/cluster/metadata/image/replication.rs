use crate::cluster::ReplicaProgress;

use super::{ClusterMetadataImage, PartitionMetadataImage, compute_high_watermark, reconcile_isr};

impl ClusterMetadataImage {
    pub fn update_partition_leader(
        &mut self,
        topic_name: &str,
        partition_index: i32,
        leader_id: i32,
        leader_epoch: i32,
    ) -> bool {
        let Some(partition) = self.partition_mut(topic_name, partition_index) else {
            return false;
        };
        if leader_epoch < partition.leader_epoch {
            return false;
        }
        if partition.leader_id == leader_id && partition.leader_epoch == leader_epoch {
            return false;
        }
        partition.leader_id = leader_id;
        partition.leader_epoch = leader_epoch;
        if !partition.replicas.contains(&leader_id) {
            partition.replicas.insert(0, leader_id);
        }
        if !partition.isr.contains(&leader_id) {
            partition.isr.insert(0, leader_id);
        }
        true
    }

    pub fn update_partition_replication(
        &mut self,
        topic_name: &str,
        partition_index: i32,
        replicas: Vec<i32>,
        isr: Vec<i32>,
        leader_epoch: i32,
    ) -> bool {
        let Some(partition) = self.partition_mut(topic_name, partition_index) else {
            return false;
        };
        if leader_epoch < partition.leader_epoch {
            return false;
        }
        if partition.replicas == replicas
            && partition.isr == isr
            && partition.leader_epoch == leader_epoch
        {
            return false;
        }
        partition.replicas = replicas;
        partition.isr = isr;
        partition.leader_epoch = leader_epoch;
        partition.high_watermark =
            compute_high_watermark(&partition.isr, &partition.replica_progress)
                .unwrap_or(partition.high_watermark);
        true
    }

    pub fn update_replica_progress(
        &mut self,
        topic_name: &str,
        partition_index: i32,
        leader_epoch: i32,
        progress: ReplicaProgress,
    ) -> bool {
        let Some(partition) = self.partition_mut(topic_name, partition_index) else {
            return false;
        };
        if leader_epoch != partition.leader_epoch {
            return false;
        }
        match partition
            .replica_progress
            .iter_mut()
            .find(|entry| entry.broker_id == progress.broker_id)
        {
            Some(current) if *current == progress => return false,
            Some(current) => *current = progress,
            None => partition.replica_progress.push(progress),
        }
        partition
            .replica_progress
            .sort_by_key(|entry| entry.broker_id);
        reconcile_isr(partition);
        partition.high_watermark =
            compute_high_watermark(&partition.isr, &partition.replica_progress)
                .unwrap_or(partition.high_watermark);
        true
    }

    fn partition_mut(
        &mut self,
        topic_name: &str,
        partition_index: i32,
    ) -> Option<&mut PartitionMetadataImage> {
        self.topics
            .iter_mut()
            .find(|topic| topic.name == topic_name)
            .and_then(|topic| {
                topic
                    .partitions
                    .iter_mut()
                    .find(|partition| partition.partition == partition_index)
            })
    }
}
