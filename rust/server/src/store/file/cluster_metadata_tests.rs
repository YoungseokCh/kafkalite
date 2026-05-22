use super::*;
use bytes::{BufMut, Bytes};
use tempfile::tempdir;

use crate::store::file::log::StoredBatch;
use crate::store::{BrokerRecord, FileStore, Storage};

#[test]
fn recovers_topic_metadata_from_cluster_metadata_partition() {
    let dir = tempdir().unwrap();
    let logs = RecordLog::open(dir.path()).unwrap();
    let topic_id = [7_u8; UUID_BYTES];
    let records = vec![
        metadata_record(0, topic_record("kraft-topic", topic_id)),
        metadata_record(1, partition_record(0, topic_id)),
        metadata_record(2, partition_record(1, topic_id)),
    ];
    logs.append_batch(
        CLUSTER_METADATA_TOPIC,
        CLUSTER_METADATA_PARTITION,
        &StoredBatch::from_records(&records),
    )
    .unwrap();

    let store = FileStore::open(dir.path()).unwrap();
    let metadata = store
        .topic_metadata(Some(&["kraft-topic".to_string()]), 0)
        .unwrap();

    assert_eq!(metadata.len(), 1);
    assert_eq!(metadata[0].name, "kraft-topic");
    assert_eq!(metadata[0].partitions.len(), 2);
    assert_eq!(metadata[0].partitions[0].partition, 0);
    assert_eq!(metadata[0].partitions[1].partition, 1);
}

fn metadata_record(offset: i64, value: Vec<u8>) -> BrokerRecord {
    BrokerRecord {
        offset,
        timestamp_ms: 0,
        producer_id: -1,
        producer_epoch: -1,
        sequence: offset as i32,
        key: None,
        value: Some(Bytes::from(value)),
        headers_json: b"[]".to_vec(),
    }
}

fn topic_record(name: &str, topic_id: TopicId) -> Vec<u8> {
    let mut bytes = Vec::new();
    put_unsigned_varint(&mut bytes, API_MESSAGE_FRAME_VERSION);
    put_unsigned_varint(&mut bytes, TOPIC_RECORD_API_KEY);
    put_unsigned_varint(&mut bytes, TOPIC_RECORD_VERSION);
    put_compact_string(&mut bytes, name);
    bytes.put_slice(&topic_id);
    put_unsigned_varint(&mut bytes, 0);
    bytes
}

fn partition_record(partition: i32, topic_id: TopicId) -> Vec<u8> {
    let mut bytes = Vec::new();
    put_unsigned_varint(&mut bytes, API_MESSAGE_FRAME_VERSION);
    put_unsigned_varint(&mut bytes, PARTITION_RECORD_API_KEY);
    put_unsigned_varint(&mut bytes, 0);
    bytes.put_i32(partition);
    bytes.put_slice(&topic_id);
    put_i32_array(&mut bytes, &[0]);
    put_i32_array(&mut bytes, &[0]);
    put_i32_array(&mut bytes, &[]);
    put_i32_array(&mut bytes, &[]);
    bytes.put_i32(0);
    bytes.put_i32(0);
    bytes.put_i32(0);
    put_unsigned_varint(&mut bytes, 0);
    bytes
}

fn put_compact_string(bytes: &mut Vec<u8>, value: &str) {
    put_unsigned_varint(bytes, value.len() as u32 + 1);
    bytes.put_slice(value.as_bytes());
}

fn put_i32_array(bytes: &mut Vec<u8>, values: &[i32]) {
    put_unsigned_varint(bytes, values.len() as u32 + 1);
    for value in values {
        bytes.put_i32(*value);
    }
}

fn put_unsigned_varint(bytes: &mut Vec<u8>, mut value: u32) {
    while value >= 0x80 {
        bytes.put_u8((value as u8 & 0x7f) | 0x80);
        value >>= 7;
    }
    bytes.put_u8(value as u8);
}
