use serde::{Deserialize, Serialize};

use crate::store::TopicMetadata;

use super::record::MetadataRecord;

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
                    replicas: vec![broker_id],
                    isr: vec![broker_id],
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
    pub replicas: Vec<i32>,
    pub isr: Vec<i32>,
}
