use std::collections::BTreeMap;

use bytes::{Buf, Bytes};

use crate::store::{Result, StoreError};

use super::log::RecordLog;
use super::state::{PartitionState, TopicState};

const CLUSTER_METADATA_TOPIC: &str = "__cluster_metadata";
const CLUSTER_METADATA_PARTITION: i32 = 0;
const API_MESSAGE_FRAME_VERSION: u32 = 1;
const TOPIC_RECORD_API_KEY: u32 = 2;
const PARTITION_RECORD_API_KEY: u32 = 3;
const TOPIC_RECORD_VERSION: u32 = 0;
const PARTITION_RECORD_MIN_VERSION: u32 = 0;
const PARTITION_RECORD_MAX_VERSION: u32 = 2;
const UUID_BYTES: usize = 16;

type TopicId = [u8; UUID_BYTES];

pub(super) fn recover_topic_states(logs: &RecordLog) -> Result<BTreeMap<String, TopicState>> {
    logs.recover_internal_partition(CLUSTER_METADATA_TOPIC, CLUSTER_METADATA_PARTITION)?;
    let records = logs.read_all_records(CLUSTER_METADATA_TOPIC, CLUSTER_METADATA_PARTITION)?;
    let mut topics = BTreeMap::new();
    let mut topic_ids = BTreeMap::new();
    for record in records {
        let Some(value) = record.value else {
            continue;
        };
        apply_metadata_record(&mut topics, &mut topic_ids, &value)?;
    }
    Ok(to_topic_states(topics))
}

fn apply_metadata_record(
    topics: &mut BTreeMap<String, BTreeMap<i32, i32>>,
    topic_ids: &mut BTreeMap<TopicId, String>,
    value: &[u8],
) -> Result<()> {
    let mut bytes = Bytes::copy_from_slice(value);
    let Some(frame_version) = get_unsigned_varint(&mut bytes) else {
        return Ok(());
    };
    if frame_version != API_MESSAGE_FRAME_VERSION {
        return Ok(());
    }
    let Some(api_key) = get_unsigned_varint(&mut bytes) else {
        return Ok(());
    };
    let Some(version) = get_unsigned_varint(&mut bytes) else {
        return Ok(());
    };
    match api_key {
        TOPIC_RECORD_API_KEY if version == TOPIC_RECORD_VERSION => {
            if let Some((topic_id, name)) = decode_topic_record(&mut bytes)? {
                topic_ids.insert(topic_id, name.clone());
                topics.entry(name).or_default();
            }
        }
        PARTITION_RECORD_API_KEY
            if (PARTITION_RECORD_MIN_VERSION..=PARTITION_RECORD_MAX_VERSION).contains(&version) =>
        {
            if let Some((topic_id, partition, leader_epoch)) =
                decode_partition_record(&mut bytes, version)?
            {
                if let Some(topic_name) = topic_ids.get(&topic_id) {
                    topics
                        .entry(topic_name.clone())
                        .or_default()
                        .insert(partition, leader_epoch);
                }
            }
        }
        _ => {}
    }
    Ok(())
}

fn decode_topic_record(bytes: &mut Bytes) -> Result<Option<(TopicId, String)>> {
    let Some(name) = get_compact_string(bytes)? else {
        return Ok(None);
    };
    let Some(topic_id) = get_uuid(bytes) else {
        return Ok(None);
    };
    skip_tagged_fields(bytes)?;
    Ok(Some((topic_id, name)))
}

fn decode_partition_record(bytes: &mut Bytes, version: u32) -> Result<Option<(TopicId, i32, i32)>> {
    if bytes.remaining() < 4 {
        return Ok(None);
    }
    let partition_id = bytes.get_i32();
    let Some(topic_id) = get_uuid(bytes) else {
        return Ok(None);
    };
    skip_compact_i32_array(bytes)?;
    skip_compact_i32_array(bytes)?;
    skip_compact_i32_array(bytes)?;
    skip_compact_i32_array(bytes)?;
    if bytes.remaining() < 12 {
        return Ok(None);
    }
    bytes.advance(4);
    let leader_epoch = bytes.get_i32();
    bytes.advance(4);
    if version >= 1 {
        skip_compact_uuid_array(bytes)?;
    }
    skip_tagged_fields(bytes)?;
    Ok(Some((topic_id, partition_id, leader_epoch)))
}

fn get_uuid(bytes: &mut Bytes) -> Option<TopicId> {
    if bytes.remaining() < UUID_BYTES {
        return None;
    }
    let mut value = [0_u8; UUID_BYTES];
    bytes.copy_to_slice(&mut value);
    Some(value)
}

fn get_compact_string(bytes: &mut Bytes) -> Result<Option<String>> {
    let Some(len) = get_unsigned_varint(bytes) else {
        return Ok(None);
    };
    if len == 0 {
        return Ok(None);
    }
    let len = (len - 1) as usize;
    if bytes.remaining() < len {
        return Ok(None);
    }
    let value = std::str::from_utf8(&bytes[..len])
        .map_err(|err| StoreError::Protocol(err.to_string()))?
        .to_string();
    bytes.advance(len);
    Ok(Some(value))
}

fn skip_compact_i32_array(bytes: &mut Bytes) -> Result<()> {
    let Some(len) = get_unsigned_varint(bytes) else {
        return Ok(());
    };
    if len == 0 {
        return Ok(());
    }
    skip_bytes(bytes, (len - 1) as usize * 4)
}

fn skip_compact_uuid_array(bytes: &mut Bytes) -> Result<()> {
    let Some(len) = get_unsigned_varint(bytes) else {
        return Ok(());
    };
    if len == 0 {
        return Ok(());
    }
    skip_bytes(bytes, (len - 1) as usize * UUID_BYTES)
}

fn skip_tagged_fields(bytes: &mut Bytes) -> Result<()> {
    let Some(count) = get_unsigned_varint(bytes) else {
        return Ok(());
    };
    for _ in 0..count {
        let Some(_) = get_unsigned_varint(bytes) else {
            return Ok(());
        };
        let Some(size) = get_unsigned_varint(bytes) else {
            return Ok(());
        };
        skip_bytes(bytes, size as usize)?;
    }
    Ok(())
}

fn skip_bytes(bytes: &mut Bytes, len: usize) -> Result<()> {
    if bytes.remaining() < len {
        return Ok(());
    }
    bytes.advance(len);
    Ok(())
}

fn get_unsigned_varint(bytes: &mut Bytes) -> Option<u32> {
    let mut value = 0_u32;
    for shift in (0..35).step_by(7) {
        if !bytes.has_remaining() {
            return None;
        }
        let byte = bytes.get_u8();
        value |= u32::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Some(value);
        }
    }
    None
}

pub(super) fn to_topic_states(
    topics: BTreeMap<String, BTreeMap<i32, i32>>,
) -> BTreeMap<String, TopicState> {
    topics
        .into_iter()
        .map(|(name, partitions)| {
            let partitions = partitions
                .into_iter()
                .map(|(partition, leader_epoch)| {
                    let mut state = PartitionState::new(0);
                    state.current_leader_epoch = leader_epoch;
                    (partition, state)
                })
                .collect();
            (
                name.clone(),
                TopicState {
                    name,
                    partitions,
                    created_at_unix_ms: 0,
                    updated_at_unix_ms: 0,
                },
            )
        })
        .collect()
}

#[cfg(test)]
#[path = "cluster_metadata_tests.rs"]
mod tests;
