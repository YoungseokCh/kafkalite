use thiserror::Error;

#[derive(Debug, Error)]
pub enum StoreError {
    #[error("sqlite error: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("serialization error: {0}")]
    SerdeJson(#[from] serde_json::Error),
    #[error("protocol error: {0}")]
    Protocol(String),
    #[error("unknown group member '{member_id}' in '{group_id}'")]
    UnknownMember { group_id: String, member_id: String },
    #[error("stale generation: expected {expected}, got {actual}")]
    StaleGeneration { expected: i32, actual: i32 },
    #[error(
        "invalid producer sequence for producer {producer_id}: expected {expected}, got {actual}"
    )]
    InvalidProducerSequence {
        producer_id: i64,
        expected: i32,
        actual: i32,
    },
}

pub type Result<T> = std::result::Result<T, StoreError>;
