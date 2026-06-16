use std::collections::BTreeMap;

use bytes::Bytes;

use crate::store::{BrokerRecord, Result, StoreError, TransactionMarkerRequest};

use super::super::state::{ProducerSequenceState, TransactionMarkerState};
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
        let current_leader_epoch = runtime.state.current_leader_epoch;
        let appended = records
            .iter()
            .enumerate()
            .map(|(index, record)| BrokerRecord {
                offset: base_offset + index as i64,
                timestamp_ms: record.timestamp_ms,
                producer_id: record.producer_id,
                producer_epoch: record.producer_epoch,
                sequence: record.sequence,
                partition_leader_epoch: current_leader_epoch,
                transactional: record.transactional,
                control: record.control,
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

    pub fn prepare_transaction_marker(
        &mut self,
        request: TransactionMarkerRequest<'_>,
    ) -> Result<AppendDecision> {
        let producer_sequences = self
            .partition_state(request.topic, request.partition)
            .ok_or_else(|| StoreError::UnknownTopicOrPartition {
                topic: request.topic.to_string(),
                partition: request.partition,
            })?
            .producer_sequences_ref();
        if request.producer_id >= 0 {
            if request.producer_id >= self.next_producer_id {
                return Err(StoreError::UnknownProducerId {
                    producer_id: request.producer_id,
                });
            }
            if let Some(sequence) = producer_sequences.get(&request.producer_id) {
                if request.producer_epoch < sequence.producer_epoch {
                    return Err(StoreError::StaleProducerEpoch {
                        producer_id: request.producer_id,
                        expected: sequence.producer_epoch,
                        actual: request.producer_epoch,
                    });
                }
                if request.producer_epoch == sequence.producer_epoch
                    && sequence.last_transaction_marker.as_ref()
                        == Some(&TransactionMarkerState {
                            committed: request.committed,
                            coordinator_epoch: request.coordinator_epoch,
                        })
                {
                    return Ok(AppendDecision::Duplicate {
                        base_offset: sequence.last_offset,
                        last_offset: sequence.last_offset,
                    });
                }
            }
        }
        let sequence = next_transaction_marker_sequence(
            self.next_producer_id,
            producer_sequences,
            request.producer_id,
            request.producer_epoch,
            request.committed,
            request.coordinator_epoch,
        )?;
        let marker = BrokerRecord {
            offset: 0,
            timestamp_ms: request.now_ms,
            producer_id: request.producer_id,
            producer_epoch: request.producer_epoch,
            sequence,
            partition_leader_epoch: request.partition_leader_epoch,
            transactional: true,
            control: true,
            key: Some(transaction_marker_key(request.committed)),
            value: Some(transaction_marker_value(request.coordinator_epoch)),
            headers_json: b"[]".to_vec(),
        };
        self.prepare_append(request.topic, request.partition, &[marker], request.now_ms)
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
                    last_transaction_marker: transaction_marker_state(record),
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

fn next_transaction_marker_sequence(
    next_producer_id: i64,
    producer_sequences: &BTreeMap<i64, ProducerSequenceState>,
    producer_id: i64,
    producer_epoch: i16,
    committed: bool,
    coordinator_epoch: i32,
) -> Result<i32> {
    if producer_id < 0 {
        return Ok(0);
    }
    if producer_id >= next_producer_id {
        return Err(StoreError::UnknownProducerId { producer_id });
    }
    match producer_sequences.get(&producer_id) {
        Some(sequence) if producer_epoch < sequence.producer_epoch => {
            Err(StoreError::StaleProducerEpoch {
                producer_id,
                expected: sequence.producer_epoch,
                actual: producer_epoch,
            })
        }
        Some(sequence) if producer_epoch == sequence.producer_epoch => {
            if sequence.last_transaction_marker.as_ref()
                == Some(&TransactionMarkerState {
                    committed,
                    coordinator_epoch,
                })
            {
                Ok(sequence.last_sequence)
            } else {
                Ok(sequence.last_sequence + 1)
            }
        }
        _ => Ok(0),
    }
}

fn transaction_marker_key(committed: bool) -> Bytes {
    let marker_type = if committed { 1_i16 } else { 0_i16 };
    let mut key = Vec::with_capacity(4);
    key.extend_from_slice(&0_i16.to_be_bytes());
    key.extend_from_slice(&marker_type.to_be_bytes());
    Bytes::from(key)
}

fn transaction_marker_value(coordinator_epoch: i32) -> Bytes {
    let mut value = Vec::with_capacity(6);
    value.extend_from_slice(&0_i16.to_be_bytes());
    value.extend_from_slice(&coordinator_epoch.to_be_bytes());
    Bytes::from(value)
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
