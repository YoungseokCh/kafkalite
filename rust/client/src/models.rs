use serde::{Deserialize, Serialize};

pub const DEFAULT_PARTITION: i32 = 0;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Header {
    pub key: String,
    pub value: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NewMessage {
    pub topic: String,
    pub key: Option<String>,
    pub payload_json: Vec<u8>,
    pub headers: Vec<Header>,
    pub published_at_unix_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredMessage {
    pub topic: String,
    pub partition: i32,
    pub offset: u64,
    pub key: Option<String>,
    pub payload_json: Vec<u8>,
    pub headers: Vec<Header>,
    pub published_at_unix_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppendAck {
    pub topic: String,
    pub partition: i32,
    pub offset: u64,
    pub published_at_unix_ms: i64,
}
