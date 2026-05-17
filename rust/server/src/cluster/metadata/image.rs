use serde::{Deserialize, Serialize};

use crate::cluster::ReplicaProgress;
use crate::store::TopicMetadata;

use super::record::MetadataRecord;
pub use types::{
    BrokerMetadata, PartitionMetadataImage, PartitionReassignment, ReassignmentStep,
    TopicMetadataImage,
};

mod lookup;
mod reassignment;
mod replication;
mod types;

const ISR_LAG_TOLERANCE: i64 = 1;

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClusterMetadataImage {
    pub cluster_id: String,
    pub controller_id: i32,
    pub metadata_offset: i64,
    pub brokers: Vec<BrokerMetadata>,
    pub topics: Vec<TopicMetadataImage>,
}

impl ClusterMetadataImage {
    pub fn new(cluster_id: String, controller_id: i32) -> Self {
        Self {
            cluster_id,
            controller_id,
            metadata_offset: -1,
            brokers: Vec::new(),
            topics: Vec::new(),
        }
    }

    pub fn apply(&mut self, record: MetadataRecord) {
        self.metadata_offset += 1;
        match record {
            MetadataRecord::SetController { controller_id } => {
                self.controller_id = controller_id;
            }
            MetadataRecord::RegisterBroker(broker) => {
                self.upsert_broker(broker);
            }
            MetadataRecord::UpdatePartitionLeader {
                topic_name,
                partition_index,
                leader_id,
                leader_epoch,
            } => {
                self.update_partition_leader(&topic_name, partition_index, leader_id, leader_epoch);
            }
            MetadataRecord::UpdatePartitionReplication {
                topic_name,
                partition_index,
                replicas,
                isr,
                leader_epoch,
            } => {
                self.update_partition_replication(
                    &topic_name,
                    partition_index,
                    replicas,
                    isr,
                    leader_epoch,
                );
            }
            MetadataRecord::UpdateReplicaProgress {
                topic_name,
                partition_index,
                leader_epoch,
                progress,
            } => {
                self.update_replica_progress(&topic_name, partition_index, leader_epoch, progress);
            }
            MetadataRecord::BeginPartitionReassignment {
                topic_name,
                partition_index,
                target_replicas,
            } => {
                self.begin_partition_reassignment(&topic_name, partition_index, target_replicas);
            }
            MetadataRecord::AdvancePartitionReassignment {
                topic_name,
                partition_index,
                step,
            } => {
                self.advance_partition_reassignment(&topic_name, partition_index, step);
            }
            MetadataRecord::CompletePartitionReassignment {
                topic_name,
                partition_index,
            } => {
                self.complete_partition_reassignment(&topic_name, partition_index);
            }
            MetadataRecord::UpsertTopic(topic) => {
                self.upsert_topic(topic);
            }
        }
    }

    pub fn upsert_broker(&mut self, next: BrokerMetadata) -> bool {
        match self
            .brokers
            .iter_mut()
            .find(|broker| broker.node_id == next.node_id)
        {
            Some(current) if *current == next => false,
            Some(current) => {
                *current = next;
                true
            }
            None => {
                self.brokers.push(next);
                self.brokers.sort_by_key(|broker| broker.node_id);
                true
            }
        }
    }

    pub fn upsert_topic(&mut self, next: TopicMetadataImage) -> bool {
        match self.topics.iter_mut().find(|topic| topic.name == next.name) {
            Some(current) if *current == next => false,
            Some(current) => {
                *current = next;
                true
            }
            None => {
                self.topics.push(next);
                self.topics
                    .sort_by(|left, right| left.name.cmp(&right.name));
                true
            }
        }
    }

    pub fn merge_store_topic(&mut self, topic: &TopicMetadata) -> bool {
        let Some(existing) = self
            .topics
            .iter_mut()
            .find(|existing| existing.name == topic.name)
        else {
            return false;
        };
        let mut changed = false;
        for partition in &topic.partitions {
            if existing
                .partitions
                .iter()
                .any(|current| current.partition == partition.partition)
            {
                continue;
            }
            existing.partitions.push(PartitionMetadataImage {
                partition: partition.partition,
                leader_id: 0,
                leader_epoch: 0,
                high_watermark: 0,
                replicas: Vec::new(),
                isr: Vec::new(),
                replica_progress: Vec::new(),
                reassignment: None,
            });
            changed = true;
        }
        if changed {
            existing
                .partitions
                .sort_by_key(|partition| partition.partition);
        }
        changed
    }
}

pub(super) fn compute_high_watermark(
    isr: &[i32],
    replica_progress: &[ReplicaProgress],
) -> Option<i64> {
    isr.iter()
        .filter_map(|broker_id| {
            replica_progress
                .iter()
                .find(|progress| &progress.broker_id == broker_id)
                .map(|progress| progress.log_end_offset)
        })
        .min()
}

pub(super) fn reconcile_isr(partition: &mut PartitionMetadataImage) {
    let leader_leo = partition
        .replica_progress
        .iter()
        .find(|progress| progress.broker_id == partition.leader_id)
        .map(|progress| progress.log_end_offset);
    let Some(leader_leo) = leader_leo else {
        partition.isr = vec![partition.leader_id];
        return;
    };
    partition.isr = partition
        .replicas
        .iter()
        .copied()
        .filter(|broker_id| {
            *broker_id == partition.leader_id
                || partition
                    .replica_progress
                    .iter()
                    .find(|progress| progress.broker_id == *broker_id)
                    .is_some_and(|progress| {
                        leader_leo - progress.log_end_offset <= ISR_LAG_TOLERANCE
                    })
        })
        .collect();
}
