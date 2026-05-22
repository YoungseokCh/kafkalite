use bytes::{Bytes, BytesMut};
use kafka_protocol::messages::{ConsumerProtocolAssignment, ConsumerProtocolSubscription};
use kafka_protocol::protocol::{Decodable, Encodable, StrBytes};
use std::collections::BTreeMap;
use std::fs;
use std::io::Write;
use std::path::Path;
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

#[derive(Debug, Clone, PartialEq, Eq)]
struct ManifestEntry {
    file_type: ManifestFileType,
    size: u64,
    content_hash: u64,
    bytes: Vec<u8>,
    readonly: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ManifestFileType {
    Directory,
    File,
    Symlink,
}

fn filesystem_manifest(root: &Path) -> BTreeMap<String, ManifestEntry> {
    let mut entries = BTreeMap::new();
    collect_manifest_entries(root, root, &mut entries);
    entries
}

fn collect_manifest_entries(
    root: &Path,
    current: &Path,
    entries: &mut BTreeMap<String, ManifestEntry>,
) {
    let mut children = fs::read_dir(current)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .collect::<Vec<_>>();
    children.sort();
    for path in children {
        let metadata = fs::symlink_metadata(&path).unwrap();
        let file_type = manifest_file_type(&metadata.file_type());
        let bytes = manifest_bytes(&path, file_type);
        entries.insert(
            relative_manifest_path(root, &path),
            ManifestEntry {
                file_type,
                size: metadata.len(),
                content_hash: fnv1a64(&bytes),
                bytes,
                readonly: metadata.permissions().readonly(),
            },
        );
        if file_type == ManifestFileType::Directory {
            collect_manifest_entries(root, &path, entries);
        }
    }
}

fn manifest_file_type(file_type: &fs::FileType) -> ManifestFileType {
    if file_type.is_dir() {
        ManifestFileType::Directory
    } else if file_type.is_symlink() {
        ManifestFileType::Symlink
    } else {
        ManifestFileType::File
    }
}

fn manifest_bytes(path: &Path, file_type: ManifestFileType) -> Vec<u8> {
    match file_type {
        ManifestFileType::Directory => Vec::new(),
        ManifestFileType::File => fs::read(path).unwrap(),
        ManifestFileType::Symlink => fs::read_link(path)
            .unwrap()
            .to_string_lossy()
            .as_bytes()
            .to_vec(),
    }
}

fn relative_manifest_path(root: &Path, path: &Path) -> String {
    path.strip_prefix(root)
        .unwrap()
        .to_string_lossy()
        .to_string()
}

fn fnv1a64(bytes: &[u8]) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}
