use std::collections::BTreeMap;

mod append;

use crate::store::{
    BrokerRecord, ProducerSession, ReplicaApplyResult, Result, StoreError, TopicMetadata,
};

use super::TopicSummary;
use super::internal_topics::is_internal_topic_name;
use super::log::RecordLog;
use super::state::{ProducerSequenceState, ProducerState, TopicState, TransactionMarkerState};
use super::topic_catalog::{PartitionRuntime, TopicCatalog, TopicRuntime};

pub struct DataPlaneState {
    catalog: TopicCatalog,
    next_producer_id: i64,
}

pub enum AppendDecision {
    Duplicate { base_offset: i64, last_offset: i64 },
    Append(PreparedAppend),
}

pub struct PreparedAppend {
    pub topic: String,
    pub partition: i32,
    pub base_offset: i64,
    pub last_offset: i64,
    pub records: Vec<BrokerRecord>,
}

impl DataPlaneState {
    pub fn new(topics: BTreeMap<String, TopicState>, producers: ProducerState) -> Self {
        Self {
            catalog: TopicCatalog::from_persisted(topics, &producers.sequences),
            next_producer_id: producers.next_producer_id,
        }
    }

    pub fn topic_metadata(&self, topics: Option<&[String]>) -> Vec<TopicMetadata> {
        if let Some(requested) = topics {
            return requested
                .iter()
                .filter(|topic| !is_internal_topic_name(topic))
                .filter(|topic| self.catalog.contains(topic))
                .map(|topic| self.topic_metadata_for(topic))
                .collect();
        }
        self.catalog
            .topic_names()
            .filter(|name| !is_internal_topic_name(name))
            .map(|name| self.topic_metadata_for(&name))
            .collect()
    }

    pub fn ensure_topic(&mut self, topic: &str, partition_count: i32, now_ms: i64) -> Result<()> {
        if is_internal_topic_name(topic) {
            return Ok(());
        }
        self.ensure_topic_runtime(topic, partition_count, now_ms);
        Ok(())
    }

    pub fn init_producer(&mut self) -> Result<ProducerSession> {
        let session = ProducerSession {
            producer_id: self.next_producer_id,
            producer_epoch: 0,
        };
        self.next_producer_id += 1;
        Ok(session)
    }

    pub fn finish_append(&mut self, prepared: &PreparedAppend, now_ms: i64) -> Result<()> {
        self.apply_prepared_append(prepared, now_ms)?;
        self.update_high_watermark(&prepared.topic, prepared.partition, i64::MAX)?;
        Ok(())
    }

    pub fn high_watermark(&self, topic: &str, partition: i32) -> Result<i64> {
        self.partition_state(topic, partition)
            .map(|partition| partition.high_watermark)
            .ok_or_else(|| StoreError::UnknownTopicOrPartition {
                topic: topic.to_string(),
                partition,
            })
    }

    pub fn latest_offset(&self, topic: &str, partition: i32) -> Result<i64> {
        self.partition_state(topic, partition)
            .map(|partition| partition.state.next_offset)
            .ok_or_else(|| StoreError::UnknownTopicOrPartition {
                topic: topic.to_string(),
                partition,
            })
    }

    pub fn replica_progress(&self, topic: &str, partition: i32) -> Result<(i64, i64)> {
        self.partition_state(topic, partition)
            .map(|partition| (partition.high_watermark, partition.state.next_offset))
            .ok_or_else(|| StoreError::UnknownTopicOrPartition {
                topic: topic.to_string(),
                partition,
            })
    }

    pub fn reconcile_partition_offset(
        &mut self,
        topic: &str,
        partition: i32,
        next_offset: i64,
    ) -> Result<()> {
        let runtime = self.partition_state_mut(topic, partition).ok_or_else(|| {
            StoreError::UnknownTopicOrPartition {
                topic: topic.to_string(),
                partition,
            }
        })?;
        runtime.state.next_offset = next_offset;
        runtime.producer_sequences.clear();
        Ok(())
    }

    pub fn topic_count(&self) -> usize {
        self.catalog.topic_count()
    }

    pub fn describe_topic(&self, topic: &str) -> Option<TopicSummary> {
        if is_internal_topic_name(topic) {
            return None;
        }
        self.catalog.describe_topic(topic)
    }

    pub fn has_partition(&self, topic: &str, partition: i32) -> bool {
        self.catalog.has_partition(topic, partition)
    }

    pub fn ensure_known_partitions(&mut self, topic: &str, partitions: &[i32], now_ms: i64) {
        self.catalog
            .ensure_known_partitions(topic, partitions, now_ms)
    }

    pub fn set_active_segment_base_offset(
        &mut self,
        topic: &str,
        partition: i32,
        active_segment_base_offset: i64,
    ) -> Result<()> {
        let runtime = self.partition_state_mut(topic, partition).ok_or_else(|| {
            StoreError::UnknownTopicOrPartition {
                topic: topic.to_string(),
                partition,
            }
        })?;
        runtime.state.active_segment_base_offset = active_segment_base_offset;
        Ok(())
    }

    pub fn set_log_start_offset(
        &mut self,
        topic: &str,
        partition: i32,
        log_start_offset: i64,
    ) -> Result<()> {
        let runtime = self.partition_state_mut(topic, partition).ok_or_else(|| {
            StoreError::UnknownTopicOrPartition {
                topic: topic.to_string(),
                partition,
            }
        })?;
        runtime.state.log_start_offset = log_start_offset;
        Ok(())
    }

    pub fn finish_replica_append(
        &mut self,
        prepared: Option<&PreparedAppend>,
        topic: &str,
        partition: i32,
        leader_high_watermark: i64,
        now_ms: i64,
    ) -> Result<ReplicaApplyResult> {
        if let Some(prepared) = prepared {
            self.apply_prepared_append(prepared, now_ms)?;
        }
        let high_watermark = self.update_high_watermark(topic, partition, leader_high_watermark)?;
        let log_end_offset = self.latest_offset(topic, partition)?;
        Ok(ReplicaApplyResult {
            high_watermark,
            log_end_offset,
        })
    }

    pub fn recover_producer_state(&mut self, logs: &RecordLog) -> Result<()> {
        let mut max_producer_id = self.next_producer_id - 1;
        for (topic, partition) in logs.discover_user_partitions()? {
            let records = logs.read_all_records(&topic, partition)?;
            for record in records {
                if record.producer_id < 0 {
                    continue;
                }
                max_producer_id = max_producer_id.max(record.producer_id);
                let runtime = self.partition_state_mut(&topic, partition).ok_or_else(|| {
                    StoreError::UnknownTopicOrPartition {
                        topic: topic.clone(),
                        partition,
                    }
                })?;
                let entry = runtime
                    .producer_sequences
                    .entry(record.producer_id)
                    .or_insert(ProducerSequenceState {
                        producer_epoch: record.producer_epoch,
                        first_sequence: record.sequence,
                        last_sequence: record.sequence,
                        base_offset: record.offset,
                        last_offset: record.offset,
                        last_transaction_marker: transaction_marker_state(&record),
                    });
                if record.producer_epoch > entry.producer_epoch {
                    entry.producer_epoch = record.producer_epoch;
                    entry.first_sequence = record.sequence;
                    entry.base_offset = record.offset;
                }
                if record.offset >= entry.last_offset {
                    entry.last_sequence = record.sequence;
                    entry.last_offset = record.offset;
                    entry.last_transaction_marker = transaction_marker_state(&record);
                }
            }
        }
        self.next_producer_id = self.next_producer_id.max(max_producer_id + 1);
        Ok(())
    }

    fn ensure_topic_runtime(
        &mut self,
        topic: &str,
        partition_count: i32,
        now_ms: i64,
    ) -> &mut TopicRuntime {
        self.catalog
            .ensure_topic_runtime(topic, partition_count, now_ms)
    }

    fn partition_state(&self, topic: &str, partition: i32) -> Option<&PartitionRuntime> {
        self.catalog.partition_state(topic, partition)
    }

    pub(super) fn partition_state_mut(
        &mut self,
        topic: &str,
        partition: i32,
    ) -> Option<&mut PartitionRuntime> {
        self.catalog.partition_state_mut(topic, partition)
    }

    fn topic_metadata_for(&self, topic: &str) -> TopicMetadata {
        TopicMetadata {
            name: topic.to_string(),
            partitions: self.catalog.topic_metadata(topic).unwrap_or_default(),
        }
    }

    fn update_high_watermark(
        &mut self,
        topic: &str,
        partition: i32,
        leader_high_watermark: i64,
    ) -> Result<i64> {
        let runtime = self.partition_state_mut(topic, partition).ok_or_else(|| {
            StoreError::UnknownTopicOrPartition {
                topic: topic.to_string(),
                partition,
            }
        })?;
        runtime.high_watermark = leader_high_watermark.min(runtime.state.next_offset);
        Ok(runtime.high_watermark)
    }
}

fn transaction_marker_state(record: &BrokerRecord) -> Option<TransactionMarkerState> {
    if !record.control {
        return None;
    }
    let committed = parse_transaction_marker_key(record.key.as_deref()?)?;
    let coordinator_epoch = parse_transaction_marker_value(record.value.as_deref()?)?;
    Some(TransactionMarkerState {
        committed,
        coordinator_epoch,
    })
}

fn parse_transaction_marker_key(key: &[u8]) -> Option<bool> {
    if key.len() < 4 {
        return None;
    }
    match i16::from_be_bytes([key[2], key[3]]) {
        0 => Some(false),
        1 => Some(true),
        _ => None,
    }
}

fn parse_transaction_marker_value(value: &[u8]) -> Option<i32> {
    if value.len() < 6 {
        return None;
    }
    Some(i32::from_be_bytes([value[2], value[3], value[4], value[5]]))
}
