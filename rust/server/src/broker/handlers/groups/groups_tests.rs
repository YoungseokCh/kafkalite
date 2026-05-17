use std::sync::Arc;

use bytes::Bytes;
use kafka_protocol::messages::{BrokerId, TopicName};
use kafka_protocol::protocol::StrBytes;
use tempfile::tempdir;

use super::*;
use crate::config::Config;
use crate::store::FileStore;

mod coordinator_tests;
mod lifecycle_tests;
mod offset_tests;

fn test_broker() -> KafkaBroker {
    let dir = tempdir().unwrap().keep();
    let config = Config::single_node(dir.join("data"), 9092, 1);
    let store = Arc::new(FileStore::open(&config.storage.data_dir).unwrap());
    KafkaBroker::new(config, store).unwrap()
}
