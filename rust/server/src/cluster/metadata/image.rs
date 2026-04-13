use serde::{Deserialize, Serialize};

use crate::cluster::ReplicaProgress;
use crate::store::TopicMetadata;

use super::record::MetadataRecord;

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
                progress,
            } => {
                self.update_replica_progress(&topic_name, partition_index, progress);
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

    pub fn update_partition_leader(
        &mut self,
        topic_name: &str,
        partition_index: i32,
        leader_id: i32,
        leader_epoch: i32,
    ) -> bool {
        let Some(topic) = self
            .topics
            .iter_mut()
            .find(|topic| topic.name == topic_name)
        else {
            return false;
        };
        let Some(partition) = topic
            .partitions
            .iter_mut()
            .find(|partition| partition.partition == partition_index)
        else {
            return false;
        };
        if partition.leader_id == leader_id && partition.leader_epoch == leader_epoch {
            return false;
        }
        partition.leader_id = leader_id;
        partition.leader_epoch = leader_epoch;
        partition.replicas = vec![leader_id];
        partition.isr = vec![leader_id];
        partition.high_watermark = 0;
        partition.replica_progress.clear();
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
        let Some(topic) = self
            .topics
            .iter_mut()
            .find(|topic| topic.name == topic_name)
        else {
            return false;
        };
        let Some(partition) = topic
            .partitions
            .iter_mut()
            .find(|partition| partition.partition == partition_index)
        else {
            return false;
        };
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
        progress: ReplicaProgress,
    ) -> bool {
        let Some(topic) = self
            .topics
            .iter_mut()
            .find(|topic| topic.name == topic_name)
        else {
            return false;
        };
        let Some(partition) = topic
            .partitions
            .iter_mut()
            .find(|partition| partition.partition == partition_index)
        else {
            return false;
        };
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

    pub fn begin_partition_reassignment(
        &mut self,
        topic_name: &str,
        partition_index: i32,
        target_replicas: Vec<i32>,
    ) -> bool {
        let Some(partition) = self.partition_mut(topic_name, partition_index) else {
            return false;
        };
        if partition.reassignment.is_some() {
            return false;
        }
        partition.reassignment = Some(PartitionReassignment {
            target_replicas,
            step: ReassignmentStep::Planned,
        });
        true
    }

    pub fn advance_partition_reassignment(
        &mut self,
        topic_name: &str,
        partition_index: i32,
        step: ReassignmentStep,
    ) -> bool {
        let Some(partition) = self.partition_mut(topic_name, partition_index) else {
            return false;
        };
        let Some(reassignment) = partition.reassignment.as_mut() else {
            return false;
        };
        if reassignment.step == step {
            return false;
        }
        reassignment.step = step.clone();
        match step {
            ReassignmentStep::ExpandingIsr => {
                partition.replicas = reassignment.target_replicas.clone();
                partition.isr = reassignment.target_replicas.clone();
            }
            ReassignmentStep::LeaderSwitch => {
                if let Some(new_leader) = reassignment.target_replicas.first().copied() {
                    partition.leader_id = new_leader;
                    partition.leader_epoch += 1;
                }
            }
            ReassignmentStep::Shrinking => {
                partition.replicas = reassignment.target_replicas.clone();
                partition
                    .isr
                    .retain(|id| reassignment.target_replicas.contains(id));
            }
            ReassignmentStep::Complete => {
                partition.replicas = reassignment.target_replicas.clone();
                partition
                    .isr
                    .retain(|id| reassignment.target_replicas.contains(id));
                partition.reassignment = None;
            }
            ReassignmentStep::Planned | ReassignmentStep::Copying => {}
        }
        true
    }

    pub fn complete_partition_reassignment(
        &mut self,
        topic_name: &str,
        partition_index: i32,
    ) -> bool {
        self.advance_partition_reassignment(topic_name, partition_index, ReassignmentStep::Complete)
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
                    .find(|p| p.partition == partition_index)
            })
    }
}

fn compute_high_watermark(isr: &[i32], replica_progress: &[ReplicaProgress]) -> Option<i64> {
    isr.iter()
        .filter_map(|broker_id| {
            replica_progress
                .iter()
                .find(|progress| &progress.broker_id == broker_id)
                .map(|progress| progress.log_end_offset)
        })
        .min()
}

fn reconcile_isr(partition: &mut PartitionMetadataImage) {
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrokerMetadata {
    pub node_id: i32,
    pub host: String,
    pub port: u16,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TopicMetadataImage {
    pub name: String,
    pub partitions: Vec<PartitionMetadataImage>,
}

impl TopicMetadataImage {
    pub fn from_store_topic(topic: &TopicMetadata, broker_id: i32) -> Self {
        Self {
            name: topic.name.clone(),
            partitions: topic
                .partitions
                .iter()
                .map(|partition| PartitionMetadataImage {
                    partition: partition.partition,
                    leader_id: broker_id,
                    leader_epoch: 0,
                    high_watermark: 0,
                    replicas: vec![broker_id],
                    isr: vec![broker_id],
                    replica_progress: vec![],
                    reassignment: None,
                })
                .collect(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartitionMetadataImage {
    pub partition: i32,
    pub leader_id: i32,
    pub leader_epoch: i32,
    pub high_watermark: i64,
    pub replicas: Vec<i32>,
    pub isr: Vec<i32>,
    pub replica_progress: Vec<ReplicaProgress>,
    pub reassignment: Option<PartitionReassignment>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartitionReassignment {
    pub target_replicas: Vec<i32>,
    pub step: ReassignmentStep,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReassignmentStep {
    Planned,
    Copying,
    ExpandingIsr,
    LeaderSwitch,
    Shrinking,
    Complete,
}
