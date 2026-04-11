use std::collections::BTreeMap;

use crate::store::{
    BrokerRecord, DEFAULT_PARTITION, ProducerSession, Result, StoreError, TopicMetadata,
};

use super::TopicSummary;
use super::state::{ProducerSequenceState, ProducerState, StateJournal, TopicState};
use super::topic_catalog::{PartitionRuntime, TopicCatalog, TopicRuntime};

pub struct DataPlaneState {
    catalog: TopicCatalog,
    next_producer_id: i64,
    journal: StateJournal,
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
    pub fn new(
        topics: BTreeMap<String, TopicState>,
        producers: ProducerState,
        journal: StateJournal,
    ) -> Self {
        Self {
            catalog: TopicCatalog::from_persisted(topics, &producers.sequences),
            next_producer_id: producers.next_producer_id,
            journal,
        }
    }

    pub fn topic_metadata(&self, topics: Option<&[String]>) -> Vec<TopicMetadata> {
        if let Some(requested) = topics {
            return requested
                .iter()
                .filter(|topic| self.catalog.contains(topic))
                .map(|topic| TopicMetadata {
                    name: topic.clone(),
                })
                .collect();
        }
        self.catalog
            .topic_names()
            .map(|name| TopicMetadata { name })
            .collect()
    }

    pub fn ensure_topic(&mut self, topic: &str, now_ms: i64) -> Result<()> {
        self.ensure_topic_runtime(topic, now_ms);
        self.persist_topics()
    }

    pub fn init_producer(&mut self, now_ms: i64) -> Result<ProducerSession> {
        let session = ProducerSession {
            producer_id: self.next_producer_id,
            producer_epoch: 0,
        };
        self.next_producer_id += 1;
        self.persist_producers(now_ms)?;
        Ok(session)
    }

    pub fn prepare_append(
        &mut self,
        topic: &str,
        records: &[BrokerRecord],
        now_ms: i64,
    ) -> Result<AppendDecision> {
        let next_producer_id = self.next_producer_id;
        let partition = self.ensure_partition_runtime(topic, DEFAULT_PARTITION, now_ms);
        let batch = ProducerBatchInfo::from_records(records);
        if let Some(batch) = batch.as_ref() {
            validate_producer_state(next_producer_id, partition.producer_sequences_ref(), batch)?;
            if let Some((base_offset, last_offset)) =
                duplicate_append_result(partition.producer_sequences_ref(), batch)
            {
                return Ok(AppendDecision::Duplicate {
                    base_offset,
                    last_offset,
                });
            }
        }

        let base_offset = partition.state.next_offset;
        let mut appended = Vec::new();
        for (index, record) in records.iter().enumerate() {
            appended.push(BrokerRecord {
                offset: base_offset + index as i64,
                timestamp_ms: record.timestamp_ms,
                producer_id: record.producer_id,
                producer_epoch: record.producer_epoch,
                sequence: record.sequence,
                key: record.key.clone(),
                value: record.value.clone(),
                headers_json: record.headers_json.clone(),
            });
        }
        let last_offset = appended
            .last()
            .map(|record| record.offset)
            .unwrap_or(base_offset);
        Ok(AppendDecision::Append(PreparedAppend {
            topic: topic.to_string(),
            partition: DEFAULT_PARTITION,
            base_offset,
            last_offset,
            records: appended,
        }))
    }

    pub fn finish_append(&mut self, prepared: &PreparedAppend, now_ms: i64) -> Result<()> {
        let partition = self.ensure_partition_runtime(&prepared.topic, prepared.partition, now_ms);
        partition.state.next_offset = prepared.last_offset + 1;
        for record in &prepared.records {
            partition.producer_sequences.insert(
                record.producer_id,
                ProducerSequenceState {
                    producer_epoch: record.producer_epoch,
                    first_sequence: prepared
                        .records
                        .first()
                        .map(|r| r.sequence)
                        .unwrap_or(record.sequence),
                    last_sequence: record.sequence,
                    base_offset: prepared.base_offset,
                    last_offset: record.offset,
                },
            );
        }
        let topic = self.catalog.topic_runtime_mut(&prepared.topic, now_ms);
        topic.updated_at_unix_ms = now_ms;
        self.persist_topics()?;
        self.persist_producers(now_ms)
    }

    pub fn high_watermark(&self, topic: &str) -> i64 {
        self.partition_state(topic)
            .map(|partition| partition.state.next_offset)
            .unwrap_or(0)
    }

    pub fn latest_offset(&self, topic: &str) -> i64 {
        self.high_watermark(topic)
    }

    pub fn topic_count(&self) -> usize {
        self.catalog.topic_count()
    }

    pub fn describe_topic(&self, topic: &str) -> Option<TopicSummary> {
        self.catalog.describe_topic(topic)
    }

    fn ensure_topic_runtime(&mut self, topic: &str, now_ms: i64) -> &mut TopicRuntime {
        self.catalog.ensure_topic_runtime(topic, now_ms)
    }

    fn ensure_partition_runtime(
        &mut self,
        topic: &str,
        partition: i32,
        now_ms: i64,
    ) -> &mut PartitionRuntime {
        self.catalog.partition_state_mut(topic, partition, now_ms)
    }

    fn partition_state(&self, topic: &str) -> Option<&PartitionRuntime> {
        self.catalog.partition_state(topic, DEFAULT_PARTITION)
    }

    fn persist_topics(&self) -> Result<()> {
        self.journal.append_topics(&self.catalog.to_persisted())
    }

    fn persist_producers(&self, now_ms: i64) -> Result<()> {
        self.journal.append_producer_state(
            &self.catalog.to_producer_state(self.next_producer_id),
            now_ms,
        )
    }
}

fn validate_producer_state(
    next_producer_id: i64,
    producer_sequences: &BTreeMap<i64, ProducerSequenceState>,
    batch: &ProducerBatchInfo,
) -> Result<()> {
    if batch.producer_id < 0 {
        return Ok(());
    }
    if batch.producer_id >= next_producer_id {
        return Err(StoreError::UnknownProducerId {
            producer_id: batch.producer_id,
        });
    }
    if let Some(sequence) = producer_sequences.get(&batch.producer_id) {
        if batch.producer_epoch < sequence.producer_epoch {
            return Err(StoreError::StaleProducerEpoch {
                producer_id: batch.producer_id,
                expected: sequence.producer_epoch,
                actual: batch.producer_epoch,
            });
        }
        if batch.producer_epoch == sequence.producer_epoch {
            if batch.first_sequence == sequence.first_sequence
                && batch.last_sequence == sequence.last_sequence
            {
                return Ok(());
            }
            let expected = sequence.last_sequence + 1;
            if batch.first_sequence != expected {
                return Err(StoreError::InvalidProducerSequence {
                    producer_id: batch.producer_id,
                    expected,
                    actual: batch.first_sequence,
                });
            }
        }
    }
    Ok(())
}

fn duplicate_append_result(
    producer_sequences: &BTreeMap<i64, ProducerSequenceState>,
    batch: &ProducerBatchInfo,
) -> Option<(i64, i64)> {
    let state = producer_sequences.get(&batch.producer_id)?;
    if batch.producer_epoch == state.producer_epoch
        && batch.first_sequence == state.first_sequence
        && batch.last_sequence == state.last_sequence
    {
        Some((state.base_offset, state.last_offset))
    } else {
        None
    }
}

struct ProducerBatchInfo {
    producer_id: i64,
    producer_epoch: i16,
    first_sequence: i32,
    last_sequence: i32,
}

impl ProducerBatchInfo {
    fn from_records(records: &[BrokerRecord]) -> Option<Self> {
        let first = records.first()?;
        let last = records.last()?;
        Some(Self {
            producer_id: first.producer_id,
            producer_epoch: first.producer_epoch,
            first_sequence: first.sequence,
            last_sequence: last.sequence,
        })
    }
}
