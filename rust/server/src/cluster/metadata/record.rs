use serde::{Deserialize, Serialize};

use super::image::{BrokerMetadata, TopicMetadataImage};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum MetadataRecord {
    SetController { controller_id: i32 },
    RegisterBroker(BrokerMetadata),
    UpsertTopic(TopicMetadataImage),
}
