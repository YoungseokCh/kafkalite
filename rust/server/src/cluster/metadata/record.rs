use serde::{Deserialize, Serialize};

use crate::cluster::ReplicaProgress;

use super::image::{BrokerMetadata, TopicMetadataImage};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum MetadataRecord {
    SetController {
        controller_id: i32,
    },
    RegisterBroker(BrokerMetadata),
    UpdatePartitionLeader {
        topic_name: String,
        partition_index: i32,
        leader_id: i32,
        leader_epoch: i32,
    },
    UpdatePartitionReplication {
        topic_name: String,
        partition_index: i32,
        replicas: Vec<i32>,
        isr: Vec<i32>,
        leader_epoch: i32,
    },
    UpdateReplicaProgress {
        topic_name: String,
        partition_index: i32,
        progress: ReplicaProgress,
    },
    UpsertTopic(TopicMetadataImage),
}
