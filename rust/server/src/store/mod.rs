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

pub trait Storage: Send + Sync {
    fn topic_metadata(&self, topics: Option<&[String]>, now_ms: i64) -> Result<Vec<TopicMetadata>>;
    fn ensure_topic(&self, topic: &str, now_ms: i64) -> Result<()>;
    fn init_producer(&self, now_ms: i64) -> Result<ProducerSession>;
    fn append_records(&self, topic: &str, records: &[BrokerRecord], now_ms: i64) -> Result<(i64, i64)>;
    fn fetch_records(&self, topic: &str, start_offset: i64, limit: usize) -> Result<FetchResult>;
    fn list_offsets(&self, topic: &str) -> Result<(ListOffsetResult, ListOffsetResult)>;
    fn join_group(
        &self,
        group_id: &str,
        member_id: Option<&str>,
        protocol_type: &str,
        protocol_name: &str,
        metadata: &[u8],
        session_timeout_ms: i32,
        rebalance_timeout_ms: i32,
        now_ms: i64,
    ) -> Result<GroupJoinResult>;
    fn sync_group(
        &self,
        group_id: &str,
        member_id: &str,
        generation_id: i32,
        protocol_name: &str,
        assignments: &[(String, Vec<u8>)],
        now_ms: i64,
    ) -> Result<SyncGroupResult>;
    fn heartbeat(&self, group_id: &str, member_id: &str, generation_id: i32, now_ms: i64) -> Result<()>;
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
