use serde::{Deserialize, Serialize};

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
    UpsertTopic(TopicMetadataImage),
}
