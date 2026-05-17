use std::fs::File;
use std::io::{Read, Write};

use crate::store::Result;

use super::super::policy::DEFAULT_POLICY;
use super::StoredBatch;

#[derive(Debug, Clone, Copy)]
pub(super) struct IndexEntry {
    pub base_offset: i64,
    pub position: u64,
    pub length: u32,
    pub last_offset: i64,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct TimeIndexEntry {
    pub max_timestamp_ms: i64,
    pub base_offset: i64,
    pub position: u64,
}

pub(super) fn write_index_entry(writer: &mut File, entry: &IndexEntry) -> Result<()> {
    writer.write_all(&entry.base_offset.to_le_bytes())?;
    writer.write_all(&entry.position.to_le_bytes())?;
    writer.write_all(&entry.length.to_le_bytes())?;
    writer.write_all(&entry.last_offset.to_le_bytes())?;
    Ok(())
}

pub(super) fn should_index_batch(batch: &StoredBatch) -> bool {
    batch.base_offset == 0 || batch.base_offset % DEFAULT_POLICY.index_stride == 0
}

pub(super) fn read_index_entry(reader: &mut File) -> Result<Option<IndexEntry>> {
    let mut base_offset = [0_u8; 8];
    if reader.read_exact(&mut base_offset).is_err() {
        return Ok(None);
    }
    let mut position = [0_u8; 8];
    let mut length = [0_u8; 4];
    let mut last_offset = [0_u8; 8];
    reader.read_exact(&mut position)?;
    reader.read_exact(&mut length)?;
    reader.read_exact(&mut last_offset)?;
    Ok(Some(IndexEntry {
        base_offset: i64::from_le_bytes(base_offset),
        position: u64::from_le_bytes(position),
        length: u32::from_le_bytes(length),
        last_offset: i64::from_le_bytes(last_offset),
    }))
}

pub(super) fn write_time_index_entry(writer: &mut File, entry: &TimeIndexEntry) -> Result<()> {
    writer.write_all(&entry.max_timestamp_ms.to_le_bytes())?;
    writer.write_all(&entry.base_offset.to_le_bytes())?;
    writer.write_all(&entry.position.to_le_bytes())?;
    Ok(())
}
