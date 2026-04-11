use std::collections::BTreeMap;

use crate::store::PartitionMetadata;

use super::{TopicPartitionSummary, TopicSummary};

use super::state::{PartitionState, ProducerSequenceState, ProducerState, TopicState};

pub struct TopicCatalog {
    topics: BTreeMap<String, TopicRuntime>,
}

pub struct TopicRuntime {
    pub name: String,
    pub partitions: BTreeMap<i32, PartitionRuntime>,
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

    pub fn topic_count(&self) -> usize {
        self.topics.len()
    }

    pub fn describe_topic(&self, topic: &str) -> Option<TopicSummary> {
        let topic = self.topics.get(topic)?;
        let partitions = topic
            .partitions
            .iter()
            .map(|(partition, runtime)| TopicPartitionSummary {
                partition: *partition,
                next_offset: runtime.state.next_offset,
                log_start_offset: runtime.state.log_start_offset,
                active_segment_base_offset: runtime.state.active_segment_base_offset,
            })
            .collect::<Vec<_>>();
        Some(TopicSummary {
            name: topic.name.clone(),
            partition_count: partitions.len(),
            partitions,
        })
    }

    pub fn topic_metadata(&self, topic: &str) -> Option<Vec<PartitionMetadata>> {
        let topic = self.topics.get(topic)?;
        Some(
            topic
                .partitions
                .keys()
                .map(|partition| PartitionMetadata {
                    partition: *partition,
                })
                .collect(),
        )
    }

    pub fn ensure_topic_runtime(
        &mut self,
        topic: &str,
        partition_count: i32,
        now_ms: i64,
    ) -> &mut TopicRuntime {
        self.topics
            .entry(topic.to_string())
            .or_insert_with(|| TopicRuntime::new(topic, partition_count, now_ms))
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
    ) -> Option<&mut PartitionRuntime> {
        self.topics
            .get_mut(topic)
            .and_then(|topic| topic.partitions.get_mut(&partition))
    }

    pub fn topic_runtime_mut(&mut self, topic: &str) -> Option<&mut TopicRuntime> {
        self.topics.get_mut(topic)
    }

    pub fn ensure_known_partitions(&mut self, topic: &str, partitions: &[i32], now_ms: i64) {
        let runtime = self
            .topics
            .entry(topic.to_string())
            .or_insert_with(|| TopicRuntime::new(topic, 0, now_ms));
        for partition in partitions {
            runtime
                .partitions
                .entry(*partition)
                .or_insert_with(|| PartitionRuntime::new(now_ms));
        }
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

impl TopicRuntime {
    fn new(topic: &str, partition_count: i32, now_ms: i64) -> Self {
        let mut partitions = BTreeMap::new();
        for partition in 0..partition_count.max(0) {
            partitions.insert(partition, PartitionRuntime::new(now_ms));
        }
        Self {
            name: topic.to_string(),
            partitions,
            updated_at_unix_ms: now_ms,
        }
    }
}
