use std::collections::BTreeMap;

use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::store::{BrokerRecord, Result, StoreError, TransactionalOffsetCommit};

use super::internal_hash;
use super::log::{RecordLog, StoredBatch};
use super::state::GroupState;

mod group_metadata;

const CONSUMER_OFFSETS_TOPIC: &str = "__consumer_offsets";
const DEFAULT_CONSUMER_OFFSETS_PARTITIONS: i32 = 50;
const OFFSET_COMMIT_KEY_VERSION: i16 = 1;
const OFFSET_COMMIT_VALUE_VERSION: i16 = 1;
const NO_EXPIRATION_TIMESTAMP: i64 = -1;
type PendingTransactionalOffsets = BTreeMap<(i64, i32), Vec<TransactionalOffsetCommit>>;

pub(super) struct ReplayState {
    pub offsets: BTreeMap<String, i64>,
    pub groups: BTreeMap<String, GroupState>,
    pub pending_transactional_offsets: PendingTransactionalOffsets,
    pub next_record_offsets: BTreeMap<i32, i64>,
}

pub(super) struct OffsetCommitRecord<'a> {
    pub producer_id: i64,
    pub producer_epoch: i16,
    pub group_id: &'a str,
    pub offset_topic_partition: i32,
    pub topic: &'a str,
    pub partition: i32,
    pub next_offset: i64,
    pub now_ms: i64,
}

pub(super) struct GroupStateRecord<'a> {
    pub group_id: &'a str,
    pub offset_topic_partition: i32,
    pub group: &'a GroupState,
    pub now_ms: i64,
}

enum ConsumerOffsetsKey {
    OffsetCommit(String),
    GroupMetadata(String),
}

pub(super) fn replay(logs: &RecordLog) -> Result<ReplayState> {
    let mut partitions = logs.internal_topic_partitions(CONSUMER_OFFSETS_TOPIC)?;
    if partitions.is_empty() {
        partitions.push(0);
    }
    let mut offsets = BTreeMap::new();
    let mut groups = BTreeMap::new();
    let mut pending_transactional_offsets = BTreeMap::new();
    let mut next_record_offsets = BTreeMap::new();
    for partition in partitions {
        logs.recover_internal_partition(CONSUMER_OFFSETS_TOPIC, partition)?;
        let records = logs.read_all_records(CONSUMER_OFFSETS_TOPIC, partition)?;
        let next_record_offset = records.last().map(|record| record.offset + 1).unwrap_or(0);
        for record in records {
            apply_record(
                &mut offsets,
                &mut groups,
                &mut pending_transactional_offsets,
                partition,
                record,
            )?;
        }
        next_record_offsets.insert(partition, next_record_offset);
    }
    Ok(ReplayState {
        offsets,
        groups,
        pending_transactional_offsets,
        next_record_offsets,
    })
}

pub(super) fn append_commit(
    logs: &RecordLog,
    record_offset: i64,
    commit: OffsetCommitRecord<'_>,
) -> Result<()> {
    let record = BrokerRecord {
        offset: record_offset,
        timestamp_ms: commit.now_ms,
        producer_id: commit.producer_id,
        producer_epoch: commit.producer_epoch,
        sequence: record_offset as i32,
        key: Some(Bytes::from(encode_offset_key(
            commit.group_id,
            commit.topic,
            commit.partition,
        ))),
        value: Some(Bytes::from(encode_offset_value(
            commit.next_offset,
            commit.now_ms,
        ))),
        headers_json: b"[]".to_vec(),
        partition_leader_epoch: 0,
        transactional: commit.producer_id >= 0,
        control: false,
    };
    logs.append_batch(
        CONSUMER_OFFSETS_TOPIC,
        commit.offset_topic_partition,
        &StoredBatch::from_records(&[record]),
    )
}

pub(super) fn append_transaction_marker(
    logs: &RecordLog,
    record_offset: i64,
    offset_topic_partition: i32,
    producer_id: i64,
    producer_epoch: i16,
    committed: bool,
    now_ms: i64,
) -> Result<()> {
    let record = BrokerRecord {
        offset: record_offset,
        timestamp_ms: now_ms,
        producer_id,
        producer_epoch,
        sequence: record_offset as i32,
        key: Some(transaction_marker_key(committed)),
        value: Some(transaction_marker_value(0)),
        headers_json: b"[]".to_vec(),
        partition_leader_epoch: 0,
        transactional: true,
        control: true,
    };
    logs.append_batch(
        CONSUMER_OFFSETS_TOPIC,
        offset_topic_partition,
        &StoredBatch::from_records(&[record]),
    )
}

pub(super) fn append_group_state(
    logs: &RecordLog,
    record_offset: i64,
    group_state: GroupStateRecord<'_>,
) -> Result<()> {
    let record = BrokerRecord {
        offset: record_offset,
        timestamp_ms: group_state.now_ms,
        producer_id: -1,
        producer_epoch: -1,
        sequence: record_offset as i32,
        key: Some(Bytes::from(group_metadata::encode_key(
            group_state.group_id,
        ))),
        value: Some(Bytes::from(group_metadata::encode_value(group_state.group))),
        headers_json: b"[]".to_vec(),
        partition_leader_epoch: 0,
        transactional: false,
        control: false,
    };
    logs.append_batch(
        CONSUMER_OFFSETS_TOPIC,
        group_state.offset_topic_partition,
        &StoredBatch::from_records(&[record]),
    )
}

pub(super) fn partition_for_group_id(group_id: &str) -> i32 {
    internal_hash::partition_for_key(group_id, DEFAULT_CONSUMER_OFFSETS_PARTITIONS)
}

fn apply_record(
    offsets: &mut BTreeMap<String, i64>,
    groups: &mut BTreeMap<String, GroupState>,
    pending_transactional_offsets: &mut PendingTransactionalOffsets,
    offset_topic_partition: i32,
    record: BrokerRecord,
) -> Result<()> {
    if record.transactional {
        return apply_transactional_record(
            offsets,
            pending_transactional_offsets,
            offset_topic_partition,
            record,
        );
    }
    let Some(key) = record.key else {
        return Ok(());
    };
    let Some(key) = decode_record_key(&key)? else {
        return Ok(());
    };
    match key {
        ConsumerOffsetsKey::OffsetCommit(offset_key) => {
            if let Some(value) = record.value {
                if let Some(next_offset) = decode_offset_value(&value)? {
                    offsets.insert(offset_key, next_offset);
                }
            } else {
                offsets.remove(&offset_key);
            }
        }
        ConsumerOffsetsKey::GroupMetadata(group_id) => {
            if let Some(value) = record.value {
                if let Some(group) = group_metadata::decode_value(&value)? {
                    groups.insert(group_id, group);
                }
            } else {
                groups.remove(&group_id);
            }
        }
    }
    Ok(())
}

fn apply_transactional_record(
    offsets: &mut BTreeMap<String, i64>,
    pending_transactional_offsets: &mut PendingTransactionalOffsets,
    offset_topic_partition: i32,
    record: BrokerRecord,
) -> Result<()> {
    if record.control {
        let committed = parse_transaction_marker_key(record.key.as_deref().unwrap_or_default());
        let pending = pending_transactional_offsets
            .remove(&(record.producer_id, offset_topic_partition))
            .unwrap_or_default();
        if committed == Some(true) {
            for commit in pending {
                offsets.insert(
                    serialize_offset_key(&commit.group_id, &commit.topic, commit.partition),
                    commit.next_offset,
                );
            }
        }
        return Ok(());
    }
    let Some(pending) = decode_pending_transactional_offset_commit(&record)? else {
        return Ok(());
    };
    let key = (record.producer_id, pending.offset_topic_partition);
    let Some(producer_records) = pending_transactional_offsets.get_mut(&key) else {
        pending_transactional_offsets.insert(key, vec![pending]);
        return Ok(());
    };
    if let Some(existing) = producer_records.iter_mut().find(|existing| {
        existing.group_id == pending.group_id
            && existing.topic == pending.topic
            && existing.partition == pending.partition
    }) {
        *existing = pending;
    } else {
        producer_records.push(pending);
    }
    Ok(())
}

fn decode_pending_transactional_offset_commit(
    record: &BrokerRecord,
) -> Result<Option<TransactionalOffsetCommit>> {
    let Some(key) = record.key.as_deref() else {
        return Ok(None);
    };
    let Some(ConsumerOffsetsKey::OffsetCommit(offset_key)) = decode_record_key(key)? else {
        return Ok(None);
    };
    let Some(value) = record.value.as_deref() else {
        return Ok(None);
    };
    let Some(next_offset) = decode_offset_value(value)? else {
        return Ok(None);
    };
    let Some((group_id, topic, partition)) = deserialize_offset_key(&offset_key) else {
        return Ok(None);
    };
    Ok(Some(TransactionalOffsetCommit {
        producer_id: record.producer_id,
        producer_epoch: record.producer_epoch,
        offset_topic_partition: partition_for_group_id(&group_id),
        group_id,
        topic,
        partition,
        next_offset,
    }))
}

fn decode_record_key(bytes: &[u8]) -> Result<Option<ConsumerOffsetsKey>> {
    let mut bytes = bytes;
    if bytes.remaining() < 2 {
        return Ok(None);
    }
    match bytes.get_i16() {
        OFFSET_COMMIT_KEY_VERSION => decode_offset_key_payload(&mut bytes),
        group_metadata::KEY_VERSION => group_metadata::decode_key_payload(&mut bytes)
            .map(|key| key.map(ConsumerOffsetsKey::GroupMetadata)),
        _ => Ok(None),
    }
}

fn encode_offset_key(group_id: &str, topic: &str, partition: i32) -> Vec<u8> {
    let mut bytes = BytesMut::new();
    bytes.put_i16(OFFSET_COMMIT_KEY_VERSION);
    put_string(&mut bytes, group_id);
    put_string(&mut bytes, topic);
    bytes.put_i32(partition);
    bytes.to_vec()
}

fn decode_offset_key_payload(bytes: &mut &[u8]) -> Result<Option<ConsumerOffsetsKey>> {
    let Some(group_id) = get_string(bytes)? else {
        return Ok(None);
    };
    let Some(topic) = get_string(bytes)? else {
        return Ok(None);
    };
    if bytes.remaining() < 4 {
        return Ok(None);
    }
    Ok(Some(ConsumerOffsetsKey::OffsetCommit(
        serialize_offset_key(&group_id, &topic, bytes.get_i32()),
    )))
}

fn serialize_offset_key(group_id: &str, topic: &str, partition: i32) -> String {
    format!("{group_id}:{topic}:{partition}")
}

fn deserialize_offset_key(value: &str) -> Option<(String, String, i32)> {
    let mut parts = value.splitn(3, ':');
    let group_id = parts.next()?.to_string();
    let topic = parts.next()?.to_string();
    let partition = parts.next()?.parse().ok()?;
    Some((group_id, topic, partition))
}

fn encode_offset_value(next_offset: i64, now_ms: i64) -> Vec<u8> {
    let mut bytes = BytesMut::new();
    bytes.put_i16(OFFSET_COMMIT_VALUE_VERSION);
    bytes.put_i64(next_offset);
    put_string(&mut bytes, "");
    bytes.put_i64(now_ms);
    bytes.put_i64(NO_EXPIRATION_TIMESTAMP);
    bytes.to_vec()
}

fn decode_offset_value(bytes: &[u8]) -> Result<Option<i64>> {
    let mut bytes = bytes;
    if bytes.remaining() < 2 {
        return Ok(None);
    }
    if bytes.get_i16() != OFFSET_COMMIT_VALUE_VERSION || bytes.remaining() < 8 {
        return Ok(None);
    }
    Ok(Some(bytes.get_i64()))
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

pub(super) fn put_string(bytes: &mut BytesMut, value: &str) {
    bytes.put_i16(value.len() as i16);
    bytes.put_slice(value.as_bytes());
}

pub(super) fn get_string(bytes: &mut &[u8]) -> Result<Option<String>> {
    if bytes.remaining() < 2 {
        return Ok(None);
    }
    let len = bytes.get_i16();
    if len < 0 {
        return Ok(None);
    }
    read_utf8(bytes, len as usize)
}

pub(super) fn put_nullable_string(bytes: &mut BytesMut, value: Option<&str>) {
    if let Some(value) = value {
        put_string(bytes, value);
    } else {
        bytes.put_i16(-1);
    }
}

pub(super) fn get_nullable_string(bytes: &mut &[u8]) -> Result<Option<String>> {
    if bytes.remaining() < 2 {
        return Ok(None);
    }
    let len = bytes.get_i16();
    if len < 0 {
        return Ok(None);
    }
    read_utf8(bytes, len as usize)
}

pub(super) fn put_bytes(bytes: &mut BytesMut, value: &[u8]) {
    bytes.put_i32(value.len() as i32);
    bytes.put_slice(value);
}

pub(super) fn get_bytes(bytes: &mut &[u8]) -> Result<Option<Vec<u8>>> {
    if bytes.remaining() < 4 {
        return Ok(None);
    }
    let len = bytes.get_i32();
    if len < 0 {
        return Ok(None);
    }
    let len = len as usize;
    if bytes.remaining() < len {
        return Ok(None);
    }
    let value = bytes[..len].to_vec();
    bytes.advance(len);
    Ok(Some(value))
}

fn read_utf8(bytes: &mut &[u8], len: usize) -> Result<Option<String>> {
    if bytes.remaining() < len {
        return Ok(None);
    }
    let value = std::str::from_utf8(&bytes[..len])
        .map_err(|err| StoreError::Protocol(err.to_string()))?
        .to_string();
    bytes.advance(len);
    Ok(Some(value))
}
