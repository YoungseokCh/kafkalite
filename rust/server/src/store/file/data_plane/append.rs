use std::collections::BTreeMap;

use crate::store::{BrokerRecord, Result, StoreError};

use super::super::state::ProducerSequenceState;
use super::{AppendDecision, DataPlaneState, PreparedAppend};

impl DataPlaneState {
    pub fn prepare_append(
        &mut self,
        topic: &str,
        partition: i32,
        records: &[BrokerRecord],
        _now_ms: i64,
    ) -> Result<AppendDecision> {
        let next_producer_id = self.next_producer_id;
        let runtime = self.partition_state_mut(topic, partition).ok_or_else(|| {
            StoreError::UnknownTopicOrPartition {
                topic: topic.to_string(),
                partition,
            }
        })?;
        let batch_info = ProducerBatchInfo::from_records(records);
        if let Some(batch) = batch_info.as_ref() {
            validate_producer_state(next_producer_id, runtime.producer_sequences_ref(), batch)?;
            if let Some((base_offset, last_offset)) =
                duplicate_append_result(runtime.producer_sequences_ref(), batch)
            {
                return Ok(AppendDecision::Duplicate {
                    base_offset,
                    last_offset,
                });
            }
        }

        let base_offset = runtime.state.next_offset;
        let appended = records
            .iter()
            .enumerate()
            .map(|(index, record)| BrokerRecord {
                offset: base_offset + index as i64,
                timestamp_ms: record.timestamp_ms,
                producer_id: record.producer_id,
                producer_epoch: record.producer_epoch,
                sequence: record.sequence,
                key: record.key.clone(),
                value: record.value.clone(),
                headers_json: record.headers_json.clone(),
            })
            .collect::<Vec<_>>();
        let last_offset = appended
            .last()
            .map(|record| record.offset)
            .unwrap_or(base_offset);
        Ok(AppendDecision::Append(PreparedAppend {
            topic: topic.to_string(),
            partition,
            base_offset,
            last_offset,
            records: appended,
        }))
    }

    pub fn prepare_replica_append(
        &mut self,
        topic: &str,
        partition: i32,
        records: &[BrokerRecord],
    ) -> Result<Option<PreparedAppend>> {
        let runtime = self.partition_state_mut(topic, partition).ok_or_else(|| {
            StoreError::UnknownTopicOrPartition {
                topic: topic.to_string(),
                partition,
            }
        })?;
        let next_offset = runtime.state.next_offset;
        let appended = records
            .iter()
            .filter(|record| record.offset >= next_offset)
            .cloned()
            .collect::<Vec<_>>();
        if appended.is_empty() {
            return Ok(None);
        }
        validate_replica_offsets(next_offset, &appended)?;
        let base_offset = appended[0].offset;
        let last_offset = appended
            .last()
            .map(|record| record.offset)
            .unwrap_or(base_offset);
        Ok(Some(PreparedAppend {
            topic: topic.to_string(),
            partition,
            base_offset,
            last_offset,
            records: appended,
        }))
    }

    pub(super) fn apply_prepared_append(
        &mut self,
        prepared: &PreparedAppend,
        now_ms: i64,
    ) -> Result<()> {
        let partition = self
            .partition_state_mut(&prepared.topic, prepared.partition)
            .ok_or_else(|| StoreError::UnknownTopicOrPartition {
                topic: prepared.topic.clone(),
                partition: prepared.partition,
            })?;
        partition.state.next_offset = prepared.last_offset + 1;
        for record in &prepared.records {
            if !tracks_producer_state(record.producer_id) {
                continue;
            }
            partition.producer_sequences.insert(
                record.producer_id,
                ProducerSequenceState {
                    producer_epoch: record.producer_epoch,
                    first_sequence: prepared
                        .records
                        .first()
                        .map(|first| first.sequence)
                        .unwrap_or(record.sequence),
                    last_sequence: record.sequence,
                    base_offset: prepared.base_offset,
                    last_offset: record.offset,
                },
            );
        }
        let topic = self
            .catalog
            .topic_runtime_mut(&prepared.topic)
            .ok_or_else(|| StoreError::UnknownTopicOrPartition {
                topic: prepared.topic.clone(),
                partition: prepared.partition,
            })?;
        topic.updated_at_unix_ms = now_ms;
        Ok(())
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
    if !tracks_producer_state(batch.producer_id) {
        return None;
    }
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

fn tracks_producer_state(producer_id: i64) -> bool {
    producer_id >= 0
}

fn validate_replica_offsets(next_offset: i64, records: &[BrokerRecord]) -> Result<()> {
    debug_assert!(!records.is_empty(), "replica append must contain records");
    let first = &records[0];
    if first.offset != next_offset {
        return Err(StoreError::ReplicaOffsetMismatch {
            expected: next_offset,
            actual: first.offset,
        });
    }
    for window in records.windows(2) {
        if window[1].offset != window[0].offset + 1 {
            return Err(StoreError::Protocol(format!(
                "replica append offsets must be contiguous: {} followed by {}",
                window[0].offset, window[1].offset
            )));
        }
    }
    Ok(())
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
