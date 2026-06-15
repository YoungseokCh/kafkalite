use anyhow::Result;
use bytes::{Bytes, BytesMut};
use kafka_protocol::indexmap::IndexMap;
use kafka_protocol::protocol::StrBytes;
use kafka_protocol::records::{
    Compression, Record, RecordBatchEncoder, RecordEncodeOptions, TimestampType,
};

use crate::store::BrokerRecord;

pub(super) fn to_broker_record(record: Record) -> BrokerRecord {
    BrokerRecord {
        offset: record.offset,
        timestamp_ms: record.timestamp,
        producer_id: record.producer_id,
        producer_epoch: record.producer_epoch,
        sequence: record.sequence,
        partition_leader_epoch: record.partition_leader_epoch,
        transactional: record.transactional,
        control: record.control,
        key: record.key,
        value: record.value,
        headers_json: serde_json::to_vec(
            &record
                .headers
                .iter()
                .map(|(key, value)| (key.to_string(), value.clone().map(|bytes| bytes.to_vec())))
                .collect::<Vec<_>>(),
        )
        .unwrap_or_else(|_| b"[]".to_vec()),
    }
}

pub(super) fn encode_records(
    records: &[BrokerRecord],
    partition_leader_epoch: i32,
) -> Result<Bytes> {
    let kafka_records = records
        .iter()
        .enumerate()
        .map(|(index, record)| Record {
            transactional: record.transactional,
            control: record.control,
            partition_leader_epoch,
            producer_id: record.producer_id,
            producer_epoch: record.producer_epoch,
            timestamp_type: TimestampType::Creation,
            offset: record.offset.max(index as i64),
            sequence: record.sequence,
            timestamp: record.timestamp_ms,
            key: record.key.clone(),
            value: record.value.clone(),
            headers: decode_headers(&record.headers_json),
        })
        .collect::<Vec<_>>();
    let mut encoded = BytesMut::new();
    RecordBatchEncoder::encode(
        &mut encoded,
        &kafka_records,
        &RecordEncodeOptions {
            version: 2,
            compression: Compression::None,
        },
    )?;
    Ok(encoded.freeze())
}

fn decode_headers(headers_json: &[u8]) -> IndexMap<StrBytes, Option<Bytes>> {
    serde_json::from_slice::<Vec<(String, Option<Vec<u8>>)>>(headers_json)
        .unwrap_or_default()
        .into_iter()
        .map(|(key, value)| (StrBytes::from(key), value.map(Bytes::from)))
        .collect()
}
