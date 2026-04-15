use serde::{Deserialize, Serialize};

use crate::cluster::ReplicaProgress;

use super::image::{BrokerMetadata, ReassignmentStep, TopicMetadataImage};

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
        leader_epoch: i32,
        progress: ReplicaProgress,
    },
    BeginPartitionReassignment {
        topic_name: String,
        partition_index: i32,
        target_replicas: Vec<i32>,
    },
    AdvancePartitionReassignment {
        topic_name: String,
        partition_index: i32,
        step: ReassignmentStep,
    },
    CompletePartitionReassignment {
        topic_name: String,
        partition_index: i32,
    },
    UpsertTopic(TopicMetadataImage),
}
