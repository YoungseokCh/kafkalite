use std::io::Read;

use serde::{Deserialize, Serialize};

use crate::store::{BrokerRecord, Result, StoreError};

const BATCH_MAGIC: &[u8; 4] = b"KFLG";

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
        let mut out = Vec::new();
        out.extend_from_slice(BATCH_MAGIC);
        out.extend_from_slice(&self.base_offset.to_le_bytes());
        out.extend_from_slice(&self.last_offset.to_le_bytes());
        out.extend_from_slice(&self.max_timestamp_ms.to_le_bytes());
        out.extend_from_slice(&(self.records.len() as u32).to_le_bytes());
        for record in &self.records {
            out.extend_from_slice(&record.offset.to_le_bytes());
            out.extend_from_slice(&record.timestamp_ms.to_le_bytes());
            out.extend_from_slice(&record.producer_id.to_le_bytes());
            out.extend_from_slice(&record.producer_epoch.to_le_bytes());
            out.extend_from_slice(&record.sequence.to_le_bytes());
            write_bytes(&mut out, record.key.as_ref().map(|value| value.as_ref()));
            write_bytes(&mut out, record.value.as_ref().map(|value| value.as_ref()));
            write_bytes(&mut out, Some(record.headers_json.as_slice()));
        }
        Ok(out)
    }

    pub fn decode_binary(payload: &[u8]) -> Result<Self> {
        let mut cursor = std::io::Cursor::new(payload);
        let mut magic = [0_u8; 4];
        cursor.read_exact(&mut magic)?;
        if &magic != BATCH_MAGIC {
            return Err(StoreError::Protocol("invalid batch magic".to_string()));
        }
        let base_offset = read_i64(&mut cursor)?;
        let last_offset = read_i64(&mut cursor)?;
        let max_timestamp_ms = read_i64(&mut cursor)?;
        let mut count_bytes = [0_u8; 4];
        cursor.read_exact(&mut count_bytes)?;
        let count = u32::from_le_bytes(count_bytes);
        let mut records = Vec::with_capacity(count as usize);
        for _ in 0..count {
            let offset = read_i64(&mut cursor)?;
            let timestamp_ms = read_i64(&mut cursor)?;
            let producer_id = read_i64(&mut cursor)?;
            let producer_epoch = read_i16(&mut cursor)?;
            let sequence = read_i32(&mut cursor)?;
            let key = read_bytes(&mut cursor)?.map(bytes::Bytes::from);
            let value = read_bytes(&mut cursor)?.map(bytes::Bytes::from);
            let headers_json = read_bytes(&mut cursor)?.unwrap_or_default();
            records.push(BrokerRecord {
                offset,
                timestamp_ms,
                producer_id,
                producer_epoch,
                sequence,
                key,
                value,
                headers_json,
            });
        }
        Ok(Self {
            base_offset,
            last_offset,
            max_timestamp_ms,
            records,
        })
    }
}

pub(super) fn write_bytes(out: &mut Vec<u8>, bytes: Option<&[u8]>) {
    match bytes {
        Some(bytes) => {
            out.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
            out.extend_from_slice(bytes);
        }
        None => out.extend_from_slice(&u32::MAX.to_le_bytes()),
    }
}

pub(super) fn read_bytes(reader: &mut std::io::Cursor<&[u8]>) -> Result<Option<Vec<u8>>> {
    let mut len = [0_u8; 4];
    reader.read_exact(&mut len)?;
    let len = u32::from_le_bytes(len);
    if len == u32::MAX {
        return Ok(None);
    }
    let mut bytes = vec![0_u8; len as usize];
    reader.read_exact(&mut bytes)?;
    Ok(Some(bytes))
}

fn read_i64(reader: &mut std::io::Cursor<&[u8]>) -> Result<i64> {
    let mut bytes = [0_u8; 8];
    reader.read_exact(&mut bytes)?;
    Ok(i64::from_le_bytes(bytes))
}

fn read_i32(reader: &mut std::io::Cursor<&[u8]>) -> Result<i32> {
    let mut bytes = [0_u8; 4];
    reader.read_exact(&mut bytes)?;
    Ok(i32::from_le_bytes(bytes))
}

fn read_i16(reader: &mut std::io::Cursor<&[u8]>) -> Result<i16> {
    let mut bytes = [0_u8; 2];
    reader.read_exact(&mut bytes)?;
    Ok(i16::from_le_bytes(bytes))
}
