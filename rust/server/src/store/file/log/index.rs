use std::fs::File;
use std::io::Write;

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

pub(super) fn write_time_index_entry(writer: &mut File, entry: &TimeIndexEntry) -> Result<()> {
    writer.write_all(&entry.max_timestamp_ms.to_le_bytes())?;
    writer.write_all(&entry.base_offset.to_le_bytes())?;
    writer.write_all(&entry.position.to_le_bytes())?;
    Ok(())
}
