#![doc = "Server-first Kafka-compatible broker crate with `kafkalite` and `store_tool` binaries."]
#![doc = ""]
#![doc = "Library lifecycle:"]
#![doc = "- construct a broker with `KafkaBroker::new`"]
#![doc = "- launch it with `KafkaBroker::start`"]
#![doc = "- manage the running instance through `BrokerHandle`"]

pub mod broker;
pub mod cluster;
pub mod config;
pub mod protocol;
pub mod store;

pub use broker::{BrokerHandle, KafkaBroker};
pub use config::Config;
pub use store::FileStore;
