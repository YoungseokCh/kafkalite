use std::collections::BTreeMap;

use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::store::{BrokerRecord, Result, StoreError};

use super::log::{RecordLog, StoredBatch};

const CONSUMER_OFFSETS_TOPIC: &str = "__consumer_offsets";
const CONSUMER_OFFSETS_PARTITION: i32 = 0;
const OFFSET_COMMIT_KEY_VERSION: i16 = 1;
const OFFSET_COMMIT_VALUE_VERSION: i16 = 1;
const NO_EXPIRATION_TIMESTAMP: i64 = -1;

pub(super) struct OffsetCommitRecord<'a> {
    pub group_id: &'a str,
    pub topic: &'a str,
    pub partition: i32,
    pub next_offset: i64,
    pub now_ms: i64,
}

pub(super) fn replay(logs: &RecordLog) -> Result<(BTreeMap<String, i64>, i64)> {
    logs.recover_partition(CONSUMER_OFFSETS_TOPIC, CONSUMER_OFFSETS_PARTITION)?;
    let records = logs.read_all_records(CONSUMER_OFFSETS_TOPIC, CONSUMER_OFFSETS_PARTITION)?;
    let next_record_offset = records.last().map(|record| record.offset + 1).unwrap_or(0);
    let mut offsets = BTreeMap::new();
    for record in records {
        apply_record(&mut offsets, record)?;
    }
    Ok((offsets, next_record_offset))
}

pub(super) fn append_commit(
    logs: &RecordLog,
    record_offset: i64,
    commit: OffsetCommitRecord<'_>,
) -> Result<()> {
    let record = BrokerRecord {
        offset: record_offset,
        timestamp_ms: commit.now_ms,
        producer_id: -1,
        producer_epoch: -1,
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
    };
    logs.append_batch(
        CONSUMER_OFFSETS_TOPIC,
        CONSUMER_OFFSETS_PARTITION,
        &StoredBatch::from_records(&[record]),
    )
}

fn apply_record(offsets: &mut BTreeMap<String, i64>, record: BrokerRecord) -> Result<()> {
    let Some(key) = record.key else {
        return Ok(());
    };
    let Some(offset_key) = decode_offset_key(&key)? else {
        return Ok(());
    };
    if let Some(value) = record.value {
        if let Some(next_offset) = decode_offset_value(&value)? {
            offsets.insert(offset_key, next_offset);
        }
    } else {
        offsets.remove(&offset_key);
    }
    Ok(())
}

fn encode_offset_key(group_id: &str, topic: &str, partition: i32) -> Vec<u8> {
    let mut bytes = BytesMut::new();
    bytes.put_i16(OFFSET_COMMIT_KEY_VERSION);
    put_string(&mut bytes, group_id);
    put_string(&mut bytes, topic);
    bytes.put_i32(partition);
    bytes.to_vec()
}

fn decode_offset_key(bytes: &[u8]) -> Result<Option<String>> {
    let mut bytes = bytes;
    if bytes.remaining() < 2 {
        return Ok(None);
    }
    if bytes.get_i16() != OFFSET_COMMIT_KEY_VERSION {
        return Ok(None);
    }
    let Some(group_id) = get_string(&mut bytes)? else {
        return Ok(None);
    };
    let Some(topic) = get_string(&mut bytes)? else {
        return Ok(None);
    };
    if bytes.remaining() < 4 {
        return Ok(None);
    }
    Ok(Some(serialize_offset_key(
        &group_id,
        &topic,
        bytes.get_i32(),
    )))
}

fn serialize_offset_key(group_id: &str, topic: &str, partition: i32) -> String {
    format!("{group_id}:{topic}:{partition}")
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
    let value = std::str::from_utf8(&bytes[..len])
        .map_err(|err| StoreError::Protocol(err.to_string()))?
        .to_string();
    bytes.advance(len);
    Ok(Some(value))
}
