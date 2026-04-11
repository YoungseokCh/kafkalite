use thiserror::Error;

#[derive(Debug, Error)]
pub enum StoreError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
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
    #[error(
        "stale producer epoch for producer {producer_id}: expected at least {expected}, got {actual}"
    )]
    StaleProducerEpoch {
        producer_id: i64,
        expected: i16,
        actual: i16,
    },
    #[error("unknown producer id {producer_id}")]
    UnknownProducerId { producer_id: i64 },
    #[error("unknown topic '{topic}' or partition {partition}")]
    UnknownTopicOrPartition { topic: String, partition: i32 },
}

pub type Result<T> = std::result::Result<T, StoreError>;
