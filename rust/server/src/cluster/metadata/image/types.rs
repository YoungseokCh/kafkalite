use serde::{Deserialize, Serialize};

use crate::cluster::ReplicaProgress;
use crate::store::TopicMetadata;

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
