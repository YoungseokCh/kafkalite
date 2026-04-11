mod error;
mod file;
mod models;

pub use error::{Result, StoreError};
pub use file::FileStore;
pub use models::{
    BrokerRecord, FetchResult, GroupJoinResult, GroupMember, ListOffsetResult, ProducerSession,
    SyncGroupResult, TopicMetadata,
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

pub trait Storage: Send + Sync {
    fn topic_metadata(&self, topics: Option<&[String]>, now_ms: i64) -> Result<Vec<TopicMetadata>>;
    fn ensure_topic(&self, topic: &str, now_ms: i64) -> Result<()>;
    fn init_producer(&self, now_ms: i64) -> Result<ProducerSession>;
    fn append_records(
        &self,
        topic: &str,
        records: &[BrokerRecord],
        now_ms: i64,
    ) -> Result<(i64, i64)>;
    fn fetch_records(&self, topic: &str, start_offset: i64, limit: usize) -> Result<FetchResult>;
    fn list_offsets(&self, topic: &str) -> Result<(ListOffsetResult, ListOffsetResult)>;
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
    fn commit_offset(
        &self,
        group_id: &str,
        member_id: &str,
        generation_id: i32,
        topic: &str,
        next_offset: i64,
        now_ms: i64,
    ) -> Result<()>;
    fn fetch_offset(&self, group_id: &str, topic: &str) -> Result<Option<i64>>;
}
