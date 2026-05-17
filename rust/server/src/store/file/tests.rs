use bytes::{Bytes, BytesMut};
use kafka_protocol::messages::{ConsumerProtocolAssignment, ConsumerProtocolSubscription};
use kafka_protocol::protocol::{Decodable, Encodable, StrBytes};
use std::io::Write;
use tempfile::tempdir;

use super::*;
use crate::store::{BrokerRecord, GroupJoinRequest, OffsetCommitRequest, Storage, StoreError};

mod group;
mod producer;
mod replica;
mod storage;

fn encode_subscription(topics: &[&str]) -> Vec<u8> {
    let subscription = ConsumerProtocolSubscription::default().with_topics(
        topics
            .iter()
            .map(|topic| StrBytes::from((*topic).to_string()))
            .collect(),
    );
    let mut bytes = BytesMut::new();
    subscription.encode(&mut bytes, 3).unwrap();
    bytes.to_vec()
}

fn decode_assignment_topics(bytes: &[u8]) -> Vec<String> {
    let mut payload = Bytes::copy_from_slice(bytes);
    let assignment = ConsumerProtocolAssignment::decode(&mut payload, 3).unwrap();
    assignment
        .assigned_partitions
        .into_iter()
        .map(|partition| partition.topic.to_string())
        .collect()
}

fn decode_assignment_partitions(bytes: &[u8], topic: &str) -> Vec<i32> {
    let mut payload = Bytes::copy_from_slice(bytes);
    let assignment = ConsumerProtocolAssignment::decode(&mut payload, 3).unwrap();
    assignment
        .assigned_partitions
        .into_iter()
        .find(|partition| partition.topic.to_string() == topic)
        .map(|partition| partition.partitions)
        .unwrap_or_default()
}

fn commit_request<'a>(
    group_id: &'a str,
    member_id: &'a str,
    generation_id: i32,
    topic: &'a str,
    partition: i32,
    next_offset: i64,
    now_ms: i64,
) -> OffsetCommitRequest<'a> {
    OffsetCommitRequest {
        group_id,
        member_id,
        generation_id,
        topic,
        partition,
        next_offset,
        now_ms,
    }
}
