use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TopicState {
    pub name: String,
    pub partitions: BTreeMap<i32, PartitionState>,
    pub created_at_unix_ms: i64,
    pub updated_at_unix_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartitionState {
    pub next_offset: i64,
    pub log_start_offset: i64,
    pub active_segment_base_offset: i64,
}

impl PartitionState {
    pub fn new(_now_ms: i64) -> Self {
        Self {
            next_offset: 0,
            log_start_offset: 0,
            active_segment_base_offset: 0,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProducerState {
    pub next_producer_id: i64,
    pub sequences: BTreeMap<String, ProducerSequenceState>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProducerSequenceState {
    pub producer_epoch: i16,
    pub first_sequence: i32,
    pub last_sequence: i32,
    pub base_offset: i64,
    pub last_offset: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GroupState {
    pub generation_id: i32,
    pub protocol_type: String,
    pub protocol_name: String,
    pub leader_member_id: Option<String>,
    #[serde(default)]
    pub assignments_ready: bool,
    #[serde(default)]
    pub assignments_failed: bool,
    pub members: BTreeMap<String, GroupMemberState>,
    pub updated_at_unix_ms: i64,
}

impl GroupState {
    pub fn new(protocol_type: &str, protocol_name: &str, now_ms: i64) -> Self {
        Self {
            generation_id: 0,
            protocol_type: protocol_type.to_string(),
            protocol_name: protocol_name.to_string(),
            leader_member_id: None,
            assignments_ready: false,
            assignments_failed: false,
            members: BTreeMap::new(),
            updated_at_unix_ms: now_ms,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GroupMemberState {
    pub member_id: String,
    pub generation_id: i32,
    pub protocol_type: String,
    pub protocol_name: String,
    pub subscription_metadata: Vec<u8>,
    pub assignment: Vec<u8>,
    pub session_timeout_ms: i32,
    pub rebalance_timeout_ms: i32,
    pub last_heartbeat_unix_ms: i64,
    pub updated_at_unix_ms: i64,
}

#[derive(Debug, Clone, Default)]
pub struct SnapshotSet {
    pub topics: BTreeMap<String, TopicState>,
    pub producers: ProducerState,
    pub groups: BTreeMap<String, GroupState>,
    pub offsets: BTreeMap<String, i64>,
}

impl SnapshotSet {
    pub fn load() -> Self {
        Self {
            topics: BTreeMap::new(),
            producers: ProducerState {
                next_producer_id: 1,
                sequences: BTreeMap::new(),
            },
            groups: BTreeMap::new(),
            offsets: BTreeMap::new(),
        }
    }
}

#[cfg(test)]
mod state_tests;
