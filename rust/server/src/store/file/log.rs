use std::fs::{self, File, OpenOptions};
use std::io::{BufReader, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use crate::store::{BrokerRecord, Result};

mod batch;
mod index;
mod recovery;

use super::policy::DEFAULT_POLICY;
pub(super) use batch::StoredBatch;
use index::{IndexEntry, TimeIndexEntry, read_index_entry, should_index_batch};
use index::{write_index_entry, write_time_index_entry};

#[derive(Debug)]
pub struct RecordLog {
    root: PathBuf,
    append_count: std::sync::atomic::AtomicU64,
}

impl RecordLog {
    pub fn open(root: &Path) -> Result<Self> {
        fs::create_dir_all(root.join("topics"))?;
        fs::create_dir_all(root.join("broker"))?;
        let log = Self {
            root: root.to_path_buf(),
            append_count: std::sync::atomic::AtomicU64::new(0),
        };
        log.recover()?;
        Ok(log)
    }

    pub fn ensure_topic(&self, topic: &str, partition_count: i32) -> Result<()> {
        for partition in 0..partition_count.max(0) {
            self.ensure_partition(topic, partition)?;
        }
        Ok(())
    }

    pub fn ensure_partition(&self, topic: &str, partition: i32) -> Result<()> {
        fs::create_dir_all(self.partition_dir(topic, partition))?;
        if !self.segment_path(topic, partition).exists() {
            File::create(self.segment_path(topic, partition))?;
        }
        if !self.index_path(topic, partition).exists() {
            File::create(self.index_path(topic, partition))?;
        }
        if !self.time_index_path(topic, partition).exists() {
            File::create(self.time_index_path(topic, partition))?;
        }
        Ok(())
    }

    pub fn append_batch(&self, topic: &str, partition: i32, batch: &StoredBatch) -> Result<()> {
        self.ensure_partition(topic, partition)?;
        let mut segment = OpenOptions::new()
            .append(true)
            .read(true)
            .open(self.segment_path(topic, partition))?;
        let position = segment.seek(SeekFrom::End(0))?;
        let payload = batch.encode_binary()?;
        segment.write_all(&(payload.len() as u32).to_le_bytes())?;
        segment.write_all(&payload)?;
        let append_number = self
            .append_count
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            + 1;
        if append_number.is_multiple_of(DEFAULT_POLICY.log_sync_interval) {
            segment.sync_data()?;
        }

        if should_index_batch(batch) {
            let mut index = OpenOptions::new()
                .append(true)
                .open(self.index_path(topic, partition))?;
            write_index_entry(
                &mut index,
                &IndexEntry {
                    base_offset: batch.base_offset,
                    position,
                    length: payload.len() as u32,
                    last_offset: batch.last_offset,
                },
            )?;

            let mut time_index = OpenOptions::new()
                .append(true)
                .open(self.time_index_path(topic, partition))?;
            write_time_index_entry(
                &mut time_index,
                &TimeIndexEntry {
                    max_timestamp_ms: batch.max_timestamp_ms,
                    base_offset: batch.base_offset,
                    position,
                },
            )?;
        }
        Ok(())
    }

    pub fn read_records(
        &self,
        topic: &str,
        partition: i32,
        start_offset: i64,
        limit: usize,
    ) -> Result<Vec<BrokerRecord>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        if !self.segment_path(topic, partition).exists() {
            return Ok(Vec::new());
        }
        let start_position = self.lookup_position(topic, partition, start_offset)?;
        let mut file = File::open(self.segment_path(topic, partition))?;
        file.seek(SeekFrom::Start(start_position))?;
        let mut reader = BufReader::new(file);
        let mut records = Vec::new();
        loop {
            let mut len = [0_u8; 4];
            if reader.read_exact(&mut len).is_err() {
                break;
            }
            let mut payload = vec![0_u8; u32::from_le_bytes(len) as usize];
            reader.read_exact(&mut payload)?;
            let batch = StoredBatch::decode_binary(&payload)?;
            for record in batch.records {
                if record.offset >= start_offset {
                    records.push(record);
                }
                if records.len() >= limit {
                    return Ok(records);
                }
            }
        }
        Ok(records)
    }

    pub fn read_records_for_client(
        &self,
        topic: &str,
        partition: i32,
        start_offset: i64,
        limit: usize,
    ) -> Result<Vec<BrokerRecord>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        if !self.segment_path(topic, partition).exists() {
            return Ok(Vec::new());
        }
        let start_position = self.lookup_position(topic, partition, start_offset)?;
        let mut file = File::open(self.segment_path(topic, partition))?;
        file.seek(SeekFrom::Start(start_position))?;
        let mut reader = BufReader::new(file);
        let mut records = Vec::new();
        let mut visible_count = 0_usize;
        loop {
            let mut len = [0_u8; 4];
            if reader.read_exact(&mut len).is_err() {
                break;
            }
            let mut payload = vec![0_u8; u32::from_le_bytes(len) as usize];
            reader.read_exact(&mut payload)?;
            let batch = StoredBatch::decode_binary(&payload)?;
            if batch.last_offset < start_offset {
                continue;
            }
            visible_count += batch
                .records
                .iter()
                .filter(|record| record.offset >= start_offset)
                .count();
            records.extend(batch.records);
            if visible_count >= limit {
                return Ok(records);
            }
        }
        Ok(records)
    }

    pub fn earliest_offset(&self, topic: &str, partition: i32) -> Result<Option<(i64, i64)>> {
        let records = self.read_records(topic, partition, 0, 1)?;
        Ok(records
            .into_iter()
            .next()
            .map(|record| (record.offset, record.timestamp_ms)))
    }

    fn lookup_position(&self, topic: &str, partition: i32, start_offset: i64) -> Result<u64> {
        let mut candidate = 0_u64;
        for entry in self.read_index_entries(topic, partition)? {
            if entry.base_offset <= start_offset {
                candidate = entry.position;
            } else {
                break;
            }
        }
        Ok(candidate)
    }

    fn read_index_entries(&self, topic: &str, partition: i32) -> Result<Vec<IndexEntry>> {
        if !self.index_path(topic, partition).exists() {
            return Ok(Vec::new());
        }
        let mut reader = File::open(self.index_path(topic, partition))?;
        let mut entries = Vec::new();
        while let Some(entry) = read_index_entry(&mut reader)? {
            entries.push(entry);
        }
        Ok(entries)
    }

    fn topic_dir(&self, topic: &str) -> PathBuf {
        self.root.join("topics").join(topic)
    }

    pub(super) fn partition_ids(&self, topic: &str) -> Result<Vec<i32>> {
        let partitions_dir = self.topic_dir(topic).join("partitions");
        if !partitions_dir.exists() {
            return Ok(Vec::new());
        }
        let mut ids = fs::read_dir(partitions_dir)?
            .filter_map(|entry| {
                entry.ok().and_then(|entry| {
                    entry
                        .file_type()
                        .ok()
                        .filter(|kind| kind.is_dir())
                        .and_then(|_| entry.file_name().to_string_lossy().parse::<i32>().ok())
                })
            })
            .collect::<Vec<_>>();
        ids.sort_unstable();
        Ok(ids)
    }

    fn partition_dir(&self, topic: &str, partition: i32) -> PathBuf {
        self.topic_dir(topic)
            .join("partitions")
            .join(partition.to_string())
    }

    fn segment_path(&self, topic: &str, partition: i32) -> PathBuf {
        self.partition_dir(topic, partition)
            .join("00000000000000000000.log")
    }

    fn index_path(&self, topic: &str, partition: i32) -> PathBuf {
        self.partition_dir(topic, partition)
            .join("00000000000000000000.index")
    }

    fn time_index_path(&self, topic: &str, partition: i32) -> PathBuf {
        self.partition_dir(topic, partition)
            .join("00000000000000000000.timeindex")
    }
}

#[cfg(test)]
mod log_tests;
