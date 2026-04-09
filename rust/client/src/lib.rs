mod conversions;
mod models;

pub mod proto {
    tonic::include_proto!("kafkalite");
}

pub use models::{AppendAck, Header, NewMessage, StoredMessage, DEFAULT_PARTITION};
