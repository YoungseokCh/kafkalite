use std::collections::BTreeMap;

use crate::store::DEFAULT_PARTITION;

use super::state::{PartitionState, ProducerSequenceState, ProducerState, TopicState};

pub struct TopicCatalog {
    topics: BTreeMap<String, TopicRuntime>,
}

pub struct TopicRuntime {
    pub name: String,
    pub partitions: BTreeMap<i32, PartitionRuntime>,
    pub created_at_unix_ms: i64,
    pub updated_at_unix_ms: i64,
}

pub struct PartitionRuntime {
    pub state: PartitionState,
    pub producer_sequences: BTreeMap<i64, ProducerSequenceState>,
}

impl TopicCatalog {
    pub fn from_persisted(
        topics: BTreeMap<String, TopicState>,
        producer_sequences: &BTreeMap<String, ProducerSequenceState>,
    ) -> Self {
        let mut runtimes = BTreeMap::new();
        for (name, topic) in topics {
            let mut partitions = BTreeMap::new();
            for (partition_id, partition_state) in topic.partitions {
                let prefix = format!("{name}:{partition_id}:");
                let sequences = producer_sequences
                    .iter()
                    .filter_map(|(key, value)| {
                        key.strip_prefix(&prefix)
                            .and_then(|id| id.parse::<i64>().ok())
                            .map(|producer_id| (producer_id, value.clone()))
                    })
                    .collect();
                partitions.insert(
                    partition_id,
                    PartitionRuntime {
                        state: partition_state,
                        producer_sequences: sequences,
                    },
                );
            }
            runtimes.insert(
                name.clone(),
                TopicRuntime {
                    name,
                    partitions,
                    created_at_unix_ms: topic.created_at_unix_ms,
                    updated_at_unix_ms: topic.updated_at_unix_ms,
                },
            );
        }
        Self { topics: runtimes }
    }

    pub fn contains(&self, topic: &str) -> bool {
        self.topics.contains_key(topic)
    }

    pub fn topic_names(&self) -> impl Iterator<Item = String> + '_ {
        self.topics.keys().cloned()
    }

    pub fn ensure_topic_runtime(&mut self, topic: &str, now_ms: i64) -> &mut TopicRuntime {
        self.topics
            .entry(topic.to_string())
            .or_insert_with(|| TopicRuntime {
                name: topic.to_string(),
                partitions: BTreeMap::from([(DEFAULT_PARTITION, PartitionRuntime::new(now_ms))]),
                created_at_unix_ms: now_ms,
                updated_at_unix_ms: now_ms,
            })
    }

    pub fn partition_state(&self, topic: &str, partition: i32) -> Option<&PartitionRuntime> {
        self.topics
            .get(topic)
            .and_then(|topic| topic.partitions.get(&partition))
    }

    pub fn partition_state_mut(
        &mut self,
        topic: &str,
        partition: i32,
        now_ms: i64,
    ) -> &mut PartitionRuntime {
        self.ensure_topic_runtime(topic, now_ms)
            .partitions
            .entry(partition)
            .or_insert_with(|| PartitionRuntime::new(now_ms))
    }

    pub fn topic_runtime_mut(&mut self, topic: &str, now_ms: i64) -> &mut TopicRuntime {
        self.ensure_topic_runtime(topic, now_ms)
    }

    pub fn to_persisted(&self) -> BTreeMap<String, TopicState> {
        self.topics
            .iter()
            .map(|(name, topic)| {
                (
                    name.clone(),
                    TopicState {
                        name: topic.name.clone(),
                        partitions: topic
                            .partitions
                            .iter()
                            .map(|(partition_id, runtime)| (*partition_id, runtime.state.clone()))
                            .collect(),
                        created_at_unix_ms: topic.created_at_unix_ms,
                        updated_at_unix_ms: topic.updated_at_unix_ms,
                    },
                )
            })
            .collect()
    }

    pub fn to_producer_state(&self, next_producer_id: i64) -> ProducerState {
        let mut sequences = BTreeMap::new();
        for (topic_name, topic) in &self.topics {
            for (partition_id, runtime) in &topic.partitions {
                for (producer_id, state) in &runtime.producer_sequences {
                    sequences.insert(
                        format!("{topic_name}:{partition_id}:{producer_id}"),
                        state.clone(),
                    );
                }
            }
        }
        ProducerState {
            next_producer_id,
            sequences,
        }
    }
}

impl PartitionRuntime {
    pub fn new(now_ms: i64) -> Self {
        Self {
            state: PartitionState::new(now_ms),
            producer_sequences: BTreeMap::new(),
        }
    }

    pub fn producer_sequences_ref(&self) -> &BTreeMap<i64, ProducerSequenceState> {
        &self.producer_sequences
    }
}
