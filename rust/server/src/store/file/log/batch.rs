use bytes::{Bytes, BytesMut};
use kafka_protocol::indexmap::IndexMap;
use kafka_protocol::protocol::StrBytes;
use kafka_protocol::records::{
    Compression, Record, RecordBatchDecoder, RecordBatchEncoder, RecordEncodeOptions, TimestampType,
};
use serde::{Deserialize, Serialize};

use crate::store::{BrokerRecord, Result, StoreError};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(in crate::store::file) struct StoredBatch {
    pub base_offset: i64,
    pub last_offset: i64,
    pub max_timestamp_ms: i64,
    pub records: Vec<BrokerRecord>,
}

impl StoredBatch {
    pub fn from_records(records: &[BrokerRecord]) -> Self {
        let base_offset = records.first().map(|record| record.offset).unwrap_or(0);
        let last_offset = records
            .last()
            .map(|record| record.offset)
            .unwrap_or(base_offset);
        let max_timestamp_ms = records
            .iter()
            .map(|record| record.timestamp_ms)
            .max()
            .unwrap_or(0);
        Self {
            base_offset,
            last_offset,
            max_timestamp_ms,
            records: records.to_vec(),
        }
    }

    pub fn encode_binary(&self) -> Result<Vec<u8>> {
        let kafka_records = self.records.iter().map(to_kafka_record).collect::<Vec<_>>();
        let mut encoded = BytesMut::new();
        RecordBatchEncoder::encode(
            &mut encoded,
            &kafka_records,
            &RecordEncodeOptions {
                version: 2,
                compression: Compression::None,
            },
        )
        .map_err(|err| StoreError::Protocol(err.to_string()))?;
        Ok(encoded.to_vec())
    }

    pub fn decode_binary(payload: &[u8]) -> Result<Self> {
        let records = Self::decode_batches(payload)?
            .into_iter()
            .flat_map(|batch| batch.records)
            .collect::<Vec<_>>();
        Ok(Self::from_records(&records))
    }

    pub fn decode_batches(payload: &[u8]) -> Result<Vec<Self>> {
        let mut bytes = Bytes::copy_from_slice(payload);
        let batches = RecordBatchDecoder::decode_all(&mut bytes)
            .map_err(|err| StoreError::Protocol(err.to_string()))?;
        Ok(batches
            .into_iter()
            .map(|batch| {
                let records = batch
                    .records
                    .into_iter()
                    .map(to_broker_record)
                    .collect::<Vec<_>>();
                Self::from_records(&records)
            })
            .collect())
    }
}

fn to_kafka_record(record: &BrokerRecord) -> Record {
    Record {
        transactional: record.transactional,
        control: record.control,
        partition_leader_epoch: record.partition_leader_epoch,
        producer_id: record.producer_id,
        producer_epoch: record.producer_epoch,
        timestamp_type: TimestampType::Creation,
        offset: record.offset,
        sequence: kafka_sequence(record),
        timestamp: record.timestamp_ms,
        key: record.key.clone(),
        value: record.value.clone(),
        headers: decode_headers(&record.headers_json),
    }
}

fn kafka_sequence(record: &BrokerRecord) -> i32 {
    if record.producer_id < 0 {
        -1
    } else {
        record.sequence
    }
}

fn to_broker_record(record: Record) -> BrokerRecord {
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

fn decode_headers(headers_json: &[u8]) -> IndexMap<StrBytes, Option<Bytes>> {
    serde_json::from_slice::<Vec<(String, Option<Vec<u8>>)>>(headers_json)
        .unwrap_or_default()
        .into_iter()
        .map(|(key, value)| (StrBytes::from(key), value.map(Bytes::from)))
        .collect()
}
