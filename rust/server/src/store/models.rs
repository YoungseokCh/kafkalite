use bytes::Bytes;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionMetadata {
    pub partition: i32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicMetadata {
    pub name: String,
    pub partitions: Vec<PartitionMetadata>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrokerRecord {
    pub offset: i64,
    pub timestamp_ms: i64,
    pub producer_id: i64,
    pub producer_epoch: i16,
    pub sequence: i32,
    pub partition_leader_epoch: i32,
    pub transactional: bool,
    pub control: bool,
    pub key: Option<Bytes>,
    pub value: Option<Bytes>,
    pub headers_json: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchResult {
    pub high_watermark: i64,
    pub records: Vec<BrokerRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicaFetchResult {
    pub high_watermark: i64,
    pub log_end_offset: i64,
    pub records: Vec<BrokerRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicaApplyResult {
    pub high_watermark: i64,
    pub log_end_offset: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListOffsetResult {
    pub offset: i64,
    pub timestamp_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProducerSession {
    pub producer_id: i64,
    pub producer_epoch: i16,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupMember {
    pub member_id: String,
    pub metadata: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupJoinResult {
    pub generation_id: i32,
    pub protocol_name: String,
    pub leader: String,
    pub member_id: String,
    pub members: Vec<GroupMember>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SyncGroupResult {
    pub protocol_name: String,
    pub assignment: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PendingOffsetCommit {
    pub group_id: String,
    pub member_id: String,
    pub generation_id: i32,
    pub topic: String,
    pub partition: i32,
    pub next_offset: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TransactionSessionState {
    pub producer_id: i64,
    pub producer_epoch: i16,
    pub transaction_timeout_ms: i32,
    pub last_updated_ms: i64,
    #[serde(default = "default_transaction_start_timestamp_ms")]
    pub transaction_start_timestamp_ms: i64,
    #[serde(default)]
    pub fenced: bool,
    pub status: TransactionStatus,
    pub partitions: Vec<(String, i32)>,
    pub pending_offset_commits: Vec<PendingOffsetCommit>,
}

fn default_transaction_start_timestamp_ms() -> i64 {
    -1
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TransactionStatus {
    Empty,
    Ongoing,
    PrepareCommit,
    PrepareAbort,
    CompleteCommit,
    CompleteAbort,
}
