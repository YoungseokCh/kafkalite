use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use crate::store::{BrokerRecord, Result};

mod batch;
mod index;
mod recovery;

use super::policy::DEFAULT_POLICY;
pub(super) use batch::StoredBatch;
use index::{IndexEntry, TimeIndexEntry, should_index_batch};
use index::{write_index_entry, write_time_index_entry};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(super) struct StorageBytes {
    pub log_bytes: u64,
    pub index_bytes: u64,
    pub timeindex_bytes: u64,
    pub total_bytes: u64,
}

impl StorageBytes {
    fn add(&mut self, other: StorageBytes) {
        self.log_bytes += other.log_bytes;
        self.index_bytes += other.index_bytes;
        self.timeindex_bytes += other.timeindex_bytes;
        self.total_bytes += other.total_bytes;
    }
}

#[derive(Debug)]
pub struct RecordLog {
    root: PathBuf,
    append_count: std::sync::atomic::AtomicU64,
    append_lock: std::sync::Mutex<()>,
}

impl RecordLog {
    pub fn open(root: &Path) -> Result<Self> {
        let log = Self {
            root: root.to_path_buf(),
            append_count: std::sync::atomic::AtomicU64::new(0),
            append_lock: std::sync::Mutex::new(()),
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
        let _append_guard = self.append_lock.lock().expect("record log mutex poisoned");
        self.ensure_partition(topic, partition)?;
        let mut segment = OpenOptions::new()
            .append(true)
            .read(true)
            .open(self.segment_path(topic, partition))?;
        let position = segment.seek(SeekFrom::End(0))?;
        let payload = batch.encode_binary()?;
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
        Ok(self
            .read_all_records(topic, partition)?
            .into_iter()
            .filter(|record| record.offset >= start_offset)
            .take(limit)
            .collect())
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
        let mut records = Vec::new();
        let mut visible_count = 0_usize;
        for batch in self.read_all_batches(topic, partition)? {
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

    pub(super) fn recover_internal_partition(&self, topic: &str, partition: i32) -> Result<()> {
        self.recover_partition(topic, partition)
    }

    pub(super) fn read_all_batches(&self, topic: &str, partition: i32) -> Result<Vec<StoredBatch>> {
        if !self.segment_path(topic, partition).exists() {
            return Ok(Vec::new());
        }
        let mut payload = Vec::new();
        File::open(self.segment_path(topic, partition))?.read_to_end(&mut payload)?;
        if payload.is_empty() {
            return Ok(Vec::new());
        }
        StoredBatch::decode_batches(&payload)
    }

    pub(super) fn read_all_records(
        &self,
        topic: &str,
        partition: i32,
    ) -> Result<Vec<BrokerRecord>> {
        Ok(self
            .read_all_batches(topic, partition)?
            .into_iter()
            .flat_map(|batch| batch.records)
            .collect())
    }

    pub(super) fn storage_bytes(&self) -> Result<StorageBytes> {
        let mut bytes = StorageBytes::default();
        for (topic, partition) in self.discover_user_partitions()? {
            bytes.add(walk_partition_storage(
                &self.partition_dir(&topic, partition),
            )?);
        }
        Ok(bytes)
    }

    pub(super) fn partition_ids(&self, topic: &str) -> Result<Vec<i32>> {
        let mut ids = self
            .discover_user_partitions()?
            .into_iter()
            .filter_map(|(candidate, partition)| (candidate == topic).then_some(partition))
            .collect::<Vec<_>>();
        ids.sort_unstable();
        Ok(ids)
    }

    pub(super) fn internal_topic_partitions(&self, topic: &str) -> Result<Vec<i32>> {
        let mut partitions = Vec::new();
        if !self.root.exists() {
            return Ok(partitions);
        }
        for entry in fs::read_dir(&self.root)? {
            let entry = entry?;
            if !entry.file_type()?.is_dir() {
                continue;
            }
            let name = entry.file_name().to_string_lossy().to_string();
            let Some((candidate, partition)) = parse_partition_dir(&name) else {
                continue;
            };
            if candidate == topic {
                partitions.push(partition);
            }
        }
        partitions.sort_unstable();
        Ok(partitions)
    }

    pub(super) fn partition_dir(&self, topic: &str, partition: i32) -> PathBuf {
        self.root.join(format!("{topic}-{partition}"))
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

    pub(super) fn discover_user_partitions(&self) -> Result<Vec<(String, i32)>> {
        let mut partitions = Vec::new();
        if !self.root.exists() {
            return Ok(partitions);
        }
        for entry in fs::read_dir(&self.root)? {
            let entry = entry?;
            if !entry.file_type()?.is_dir() {
                continue;
            }
            let name = entry.file_name().to_string_lossy().to_string();
            if is_internal_dir(&name) {
                continue;
            }
            let Some((topic, partition)) = parse_partition_dir(&name) else {
                continue;
            };
            partitions.push((topic.to_string(), partition));
        }
        partitions.sort();
        Ok(partitions)
    }
}

fn is_internal_dir(name: &str) -> bool {
    name.starts_with("__")
}

fn parse_partition_dir(name: &str) -> Option<(&str, i32)> {
    let (topic, partition) = name.rsplit_once('-')?;
    if topic.is_empty() {
        return None;
    }
    let partition = partition.parse::<i32>().ok()?;
    (partition >= 0).then_some((topic, partition))
}

fn walk_partition_storage(root: &Path) -> Result<StorageBytes> {
    let mut bytes = StorageBytes::default();
    if !root.exists() {
        return Ok(bytes);
    }
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        let path = entry.path();
        if entry.file_type()?.is_dir() {
            bytes.add(walk_partition_storage(&path)?);
        } else {
            let size = entry.metadata()?.len();
            bytes.total_bytes += size;
            match path.extension().and_then(|ext| ext.to_str()) {
                Some("log") => bytes.log_bytes += size,
                Some("index") => bytes.index_bytes += size,
                Some("timeindex") => bytes.timeindex_bytes += size,
                _ => {}
            }
        }
    }
    Ok(bytes)
}

#[cfg(test)]
mod log_tests;
