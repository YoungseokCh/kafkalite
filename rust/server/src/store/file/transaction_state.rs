use std::collections::BTreeMap;

use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::store::{Result, StoreError, TransactionSessionState, TransactionStatus};

use super::internal_hash;
use super::log::{RecordLog, StoredBatch};
const TRANSACTION_STATE_TOPIC: &str = "__transaction_state";
const DEFAULT_TRANSACTION_STATE_PARTITIONS: i32 = 50;
const TRANSACTION_STATE_KEY_VERSION: i16 = 0;
const TRANSACTION_STATE_VALUE_VERSION_V0: i16 = 0;

pub(super) struct ReplayState {
    pub sessions: BTreeMap<String, TransactionSessionState>,
    pub next_record_offsets: BTreeMap<i32, i64>,
}

pub(super) fn replay(logs: &RecordLog) -> Result<ReplayState> {
    let mut partitions = logs.internal_topic_partitions(TRANSACTION_STATE_TOPIC)?;
    if partitions.is_empty() {
        partitions.push(0);
    }
    let mut sessions = BTreeMap::new();
    let mut next_record_offsets = BTreeMap::new();
    for partition in partitions {
        logs.recover_internal_partition(TRANSACTION_STATE_TOPIC, partition)?;
        let records = logs.read_all_records(TRANSACTION_STATE_TOPIC, partition)?;
        let next_record_offset = records.last().map(|record| record.offset + 1).unwrap_or(0);
        for record in records {
            let Some(key) = record.key else {
                continue;
            };
            let Some(transactional_id) = decode_key(&key)? else {
                continue;
            };
            let Some(value) = record.value else {
                sessions.remove(&transactional_id);
                continue;
            };
            if let Some(session) = decode_value(&value)? {
                sessions.insert(transactional_id, session);
            }
        }
        next_record_offsets.insert(partition, next_record_offset);
    }
    Ok(ReplayState {
        sessions,
        next_record_offsets,
    })
}

pub(super) fn append_session(
    logs: &RecordLog,
    record_offset: i64,
    offset_topic_partition: i32,
    transactional_id: &str,
    session: &TransactionSessionState,
    now_ms: i64,
) -> Result<()> {
    let record = crate::store::BrokerRecord {
        offset: record_offset,
        timestamp_ms: now_ms,
        producer_id: -1,
        producer_epoch: -1,
        sequence: record_offset as i32,
        key: Some(Bytes::from(encode_key(transactional_id))),
        value: Some(Bytes::from(encode_value(session)?)),
        headers_json: b"[]".to_vec(),
        partition_leader_epoch: 0,
        transactional: false,
        control: false,
    };
    logs.append_batch(
        TRANSACTION_STATE_TOPIC,
        offset_topic_partition,
        &StoredBatch::from_records(&[record]),
    )
}

pub(super) fn append_tombstone(
    logs: &RecordLog,
    record_offset: i64,
    offset_topic_partition: i32,
    transactional_id: &str,
    now_ms: i64,
) -> Result<()> {
    let record = crate::store::BrokerRecord {
        offset: record_offset,
        timestamp_ms: now_ms,
        producer_id: -1,
        producer_epoch: -1,
        sequence: record_offset as i32,
        key: Some(Bytes::from(encode_key(transactional_id))),
        value: None,
        headers_json: b"[]".to_vec(),
        partition_leader_epoch: 0,
        transactional: false,
        control: false,
    };
    logs.append_batch(
        TRANSACTION_STATE_TOPIC,
        offset_topic_partition,
        &StoredBatch::from_records(&[record]),
    )
}

pub(super) fn partition_for_transactional_id(transactional_id: &str) -> i32 {
    internal_hash::partition_for_key(transactional_id, DEFAULT_TRANSACTION_STATE_PARTITIONS)
}

fn encode_key(transactional_id: &str) -> Vec<u8> {
    let mut bytes = BytesMut::new();
    bytes.put_i16(TRANSACTION_STATE_KEY_VERSION);
    put_string(&mut bytes, transactional_id);
    bytes.to_vec()
}

fn decode_key(bytes: &[u8]) -> Result<Option<String>> {
    let mut bytes = bytes;
    if bytes.remaining() < 2 {
        return Ok(None);
    }
    if bytes.get_i16() != TRANSACTION_STATE_KEY_VERSION {
        return Ok(None);
    }
    get_string(&mut bytes)
}

fn encode_value(session: &TransactionSessionState) -> Result<Vec<u8>> {
    let mut bytes = BytesMut::new();
    bytes.put_i16(TRANSACTION_STATE_VALUE_VERSION_V0);
    bytes.put_i64(session.producer_id);
    bytes.put_i16(session.producer_epoch);
    bytes.put_i32(session.transaction_timeout_ms);
    bytes.put_i8(encode_status(session.status));
    encode_kafka_topic_partitions(&mut bytes, &session.partitions);
    bytes.put_i64(session.last_updated_ms);
    bytes.put_i64(session.transaction_start_timestamp_ms);
    Ok(bytes.to_vec())
}

fn decode_value(bytes: &[u8]) -> Result<Option<TransactionSessionState>> {
    let mut bytes = bytes;
    if bytes.remaining() < 2 {
        return Ok(None);
    }
    match bytes.get_i16() {
        TRANSACTION_STATE_VALUE_VERSION_V0 => {
            if bytes.remaining() == 0 {
                return Ok(None);
            }
            decode_value_v0(&mut bytes)
        }
        _ => Ok(None),
    }
}

fn put_string(bytes: &mut BytesMut, value: &str) {
    bytes.put_i16(value.len() as i16);
    bytes.put_slice(value.as_bytes());
}

fn get_string(bytes: &mut &[u8]) -> Result<Option<String>> {
    if bytes.remaining() < 2 {
        return Ok(None);
    }
    let len = bytes.get_i16();
    if len < 0 {
        return Ok(None);
    }
    read_utf8(bytes, len as usize)
}

fn read_utf8(bytes: &mut &[u8], len: usize) -> Result<Option<String>> {
    if bytes.remaining() < len {
        return Ok(None);
    }
    let raw = bytes.copy_to_bytes(len);
    String::from_utf8(raw.to_vec())
        .map(Some)
        .map_err(|err| StoreError::Protocol(err.to_string()))
}

fn decode_value_v0(bytes: &mut &[u8]) -> Result<Option<TransactionSessionState>> {
    if bytes.remaining() < 31 {
        return Ok(None);
    }
    let producer_id = bytes.get_i64();
    let producer_epoch = bytes.get_i16();
    let transaction_timeout_ms = bytes.get_i32();
    let Some(status) = decode_status(bytes.get_i8()) else {
        return Ok(None);
    };
    let Some(partitions) = decode_kafka_topic_partitions(bytes)? else {
        return Ok(None);
    };
    if bytes.remaining() < 16 {
        return Ok(None);
    }
    let last_updated_ms = bytes.get_i64();
    let transaction_start_timestamp_ms = bytes.get_i64();
    Ok(Some(TransactionSessionState {
        producer_id,
        producer_epoch,
        transaction_timeout_ms,
        last_updated_ms,
        transaction_start_timestamp_ms,
        status,
        partitions,
    }))
}

fn decode_kafka_topic_partitions(bytes: &mut &[u8]) -> Result<Option<Vec<(String, i32)>>> {
    if bytes.remaining() < 4 {
        return Ok(None);
    }
    let topic_count = bytes.get_i32();
    if topic_count < -1 {
        return Ok(None);
    }
    if topic_count == -1 {
        return Ok(Some(Vec::new()));
    }
    let mut partitions = Vec::new();
    for _ in 0..topic_count {
        let Some(topic) = get_string(bytes)? else {
            return Ok(None);
        };
        if bytes.remaining() < 4 {
            return Ok(None);
        }
        let partition_count = bytes.get_i32();
        if partition_count < 0 {
            return Ok(None);
        }
        for _ in 0..partition_count {
            if bytes.remaining() < 4 {
                return Ok(None);
            }
            partitions.push((topic.clone(), bytes.get_i32()));
        }
    }
    Ok(Some(partitions))
}

fn encode_kafka_topic_partitions(bytes: &mut BytesMut, partitions: &[(String, i32)]) {
    if partitions.is_empty() {
        bytes.put_i32(-1);
        return;
    }
    let partitions_by_topic = group_partitions_by_topic(partitions);
    bytes.put_i32(partitions_by_topic.len() as i32);
    for (topic, partitions) in partitions_by_topic {
        put_string(bytes, topic);
        bytes.put_i32(partitions.len() as i32);
        for partition in partitions {
            bytes.put_i32(partition);
        }
    }
}

fn encode_status(status: TransactionStatus) -> i8 {
    match status {
        TransactionStatus::Empty => 0,
        TransactionStatus::Ongoing => 1,
        TransactionStatus::PrepareCommit => 2,
        TransactionStatus::PrepareAbort => 3,
        TransactionStatus::CompleteCommit => 4,
        TransactionStatus::CompleteAbort => 5,
    }
}

fn decode_status(status: i8) -> Option<TransactionStatus> {
    match status {
        0 => Some(TransactionStatus::Empty),
        1 => Some(TransactionStatus::Ongoing),
        2 => Some(TransactionStatus::PrepareCommit),
        3 => Some(TransactionStatus::PrepareAbort),
        4 => Some(TransactionStatus::CompleteCommit),
        5 => Some(TransactionStatus::CompleteAbort),
        _ => None,
    }
}

fn group_partitions_by_topic(partitions: &[(String, i32)]) -> BTreeMap<&str, Vec<i32>> {
    let mut grouped = BTreeMap::new();
    for (topic, partition) in partitions {
        grouped
            .entry(topic.as_str())
            .or_insert_with(Vec::new)
            .push(*partition);
    }
    grouped
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transaction_state_binary_round_trips() {
        let session = TransactionSessionState {
            producer_id: 7,
            producer_epoch: 2,
            transaction_timeout_ms: 45_000,
            last_updated_ms: 123_456,
            transaction_start_timestamp_ms: 123_000,
            status: TransactionStatus::PrepareCommit,
            partitions: vec![("a".to_string(), 0), ("b".to_string(), 2)],
        };

        let encoded = encode_value(&session).unwrap();
        let decoded = decode_value(&encoded).unwrap().unwrap();
        assert_eq!(decoded.producer_id, session.producer_id);
        assert_eq!(decoded.producer_epoch, session.producer_epoch);
        assert_eq!(
            decoded.transaction_timeout_ms,
            session.transaction_timeout_ms
        );
        assert_eq!(decoded.last_updated_ms, session.last_updated_ms);
        assert_eq!(
            decoded.transaction_start_timestamp_ms,
            session.transaction_start_timestamp_ms
        );
        assert_eq!(decoded.status, session.status);
        assert_eq!(decoded.partitions, session.partitions);
    }

    #[test]
    fn transaction_state_kafka_v0_value_decodes() {
        let session = TransactionSessionState {
            producer_id: 9,
            producer_epoch: 1,
            transaction_timeout_ms: 60_000,
            last_updated_ms: 987_654,
            transaction_start_timestamp_ms: 987_000,
            status: TransactionStatus::Ongoing,
            partitions: vec![("topic".to_string(), 1), ("topic".to_string(), 3)],
        };

        let mut bytes = BytesMut::new();
        bytes.put_i16(TRANSACTION_STATE_VALUE_VERSION_V0);
        bytes.put_i64(session.producer_id);
        bytes.put_i16(session.producer_epoch);
        bytes.put_i32(session.transaction_timeout_ms);
        bytes.put_i8(encode_status(session.status));
        bytes.put_i32(1);
        put_string(&mut bytes, "topic");
        bytes.put_i32(2);
        bytes.put_i32(1);
        bytes.put_i32(3);
        bytes.put_i64(session.last_updated_ms);
        bytes.put_i64(session.transaction_start_timestamp_ms);

        let decoded = decode_value(&bytes).unwrap().unwrap();
        assert_eq!(decoded, session);
    }

    #[test]
    fn transaction_state_kafka_v0_empty_state_decodes_with_null_partitions() {
        let mut bytes = BytesMut::new();
        bytes.put_i16(TRANSACTION_STATE_VALUE_VERSION_V0);
        bytes.put_i64(11);
        bytes.put_i16(0);
        bytes.put_i32(30_000);
        bytes.put_i8(encode_status(TransactionStatus::Empty));
        bytes.put_i32(-1);
        bytes.put_i64(123);
        bytes.put_i64(-1);

        let decoded = decode_value(&bytes).unwrap().unwrap();
        assert_eq!(decoded.status, TransactionStatus::Empty);
        assert!(decoded.partitions.is_empty());
    }
}
