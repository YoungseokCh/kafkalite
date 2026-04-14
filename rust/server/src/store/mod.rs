mod error;
mod file;
mod models;

pub use error::{Result, StoreError};
pub use file::{FileStore, StorageSummary, TopicPartitionSummary, TopicSummary};
pub use models::{
    BrokerRecord, FetchResult, GroupJoinResult, GroupMember, ListOffsetResult, PartitionMetadata,
    ProducerSession, ReplicaApplyResult, ReplicaFetchResult, SyncGroupResult, TopicMetadata,
};

pub const DEFAULT_PARTITION: i32 = 0;

#[derive(Debug, Clone, Copy)]
pub struct GroupJoinRequest<'a> {
    pub group_id: &'a str,
    pub member_id: Option<&'a str>,
    pub protocol_type: &'a str,
    pub protocol_name: &'a str,
    pub metadata: &'a [u8],
    pub session_timeout_ms: i32,
    pub rebalance_timeout_ms: i32,
    pub now_ms: i64,
}

#[derive(Debug, Clone, Copy)]
pub struct OffsetCommitRequest<'a> {
    pub group_id: &'a str,
    pub member_id: &'a str,
    pub generation_id: i32,
    pub topic: &'a str,
    pub partition: i32,
    pub next_offset: i64,
    pub now_ms: i64,
}

pub trait Storage: Send + Sync {
    fn topic_metadata(&self, topics: Option<&[String]>, now_ms: i64) -> Result<Vec<TopicMetadata>>;
    fn ensure_topic(&self, topic: &str, partition_count: i32, now_ms: i64) -> Result<()>;
    fn init_producer(&self, now_ms: i64) -> Result<ProducerSession>;
    fn append_records(
        &self,
        topic: &str,
        partition: i32,
        records: &[BrokerRecord],
        now_ms: i64,
    ) -> Result<(i64, i64)>;
    fn fetch_records(
        &self,
        topic: &str,
        partition: i32,
        start_offset: i64,
        limit: usize,
    ) -> Result<FetchResult>;
    fn replica_fetch_records(
        &self,
        topic: &str,
        partition: i32,
        start_offset: i64,
        limit: usize,
    ) -> Result<ReplicaFetchResult>;
    fn apply_replica_records(
        &self,
        topic: &str,
        partition: i32,
        records: &[BrokerRecord],
        leader_high_watermark: i64,
        now_ms: i64,
    ) -> Result<ReplicaApplyResult>;
    fn list_offsets(
        &self,
        topic: &str,
        partition: i32,
    ) -> Result<(ListOffsetResult, ListOffsetResult)>;
    fn join_group(&self, request: GroupJoinRequest<'_>) -> Result<GroupJoinResult>;
    fn sync_group(
        &self,
        group_id: &str,
        member_id: &str,
        generation_id: i32,
        protocol_name: &str,
        assignments: &[(String, Vec<u8>)],
        now_ms: i64,
    ) -> Result<SyncGroupResult>;
    fn heartbeat(
        &self,
        group_id: &str,
        member_id: &str,
        generation_id: i32,
        now_ms: i64,
    ) -> Result<()>;
    fn leave_group(&self, group_id: &str, member_id: &str, now_ms: i64) -> Result<()>;
    fn commit_offset(&self, request: OffsetCommitRequest<'_>) -> Result<()>;
    fn fetch_offset(&self, group_id: &str, topic: &str, partition: i32) -> Result<Option<i64>>;
}
