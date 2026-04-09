use bytes::Bytes;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicMetadata {
    pub name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BrokerRecord {
    pub offset: i64,
    pub timestamp_ms: i64,
    pub producer_id: i64,
    pub producer_epoch: i16,
    pub sequence: i32,
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
