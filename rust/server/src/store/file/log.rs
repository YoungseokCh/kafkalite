mod batch;
mod index;
mod recovery;

use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use crate::store::{BrokerRecord, Result};

use super::internal_topics::is_internal_topic_name;
use super::policy::{DEFAULT_POLICY, FileStorePolicy};
pub(super) use batch::StoredBatch;
use index::{
    IndexEntry, TimeIndexEntry, append_kafka_index_entries, last_time_index_entry, lookup_offset,
    lookup_timestamp, should_index_batch,
};
use index::{write_index_entry, write_time_index_entry};

const SEGMENT_SUFFIX_LEN: usize = 4;
const LOG_EXT: &str = "log";
const INDEX_EXT: &str = "index";
const TIMEINDEX_EXT: &str = "timeindex";

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

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct SegmentPaths {
    pub base_offset: i64,
    pub log: PathBuf,
    pub index: PathBuf,
    pub timeindex: PathBuf,
}

#[derive(Debug)]
pub struct RecordLog {
    root: PathBuf,
    policy: FileStorePolicy,
    append_count: std::sync::atomic::AtomicU64,
    append_lock: std::sync::Mutex<()>,
}

impl RecordLog {
    #[allow(dead_code)]
    pub fn open(root: &Path) -> Result<Self> {
        Self::open_with_policy(root, DEFAULT_POLICY)
    }

    pub fn open_with_policy(root: &Path, policy: FileStorePolicy) -> Result<Self> {
        let log = Self {
            root: root.to_path_buf(),
            policy,
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
        if self.segment_paths(topic, partition)?.is_empty() {
            self.ensure_segment_files(topic, partition, 0)?;
        }
        Ok(())
    }

    pub fn append_batch(
        &self,
        topic: &str,
        partition: i32,
        batch: &StoredBatch,
        _now_ms: i64,
    ) -> Result<()> {
        let _append_guard = self.append_lock.lock().expect("record log mutex poisoned");
        self.ensure_partition(topic, partition)?;
        let payload = batch.encode_binary()?;
        let active =
            self.writable_segment(topic, partition, batch.base_offset, payload.len() as u64)?;
        let mut segment = OpenOptions::new()
            .append(true)
            .read(true)
            .open(&active.log)?;
        let position = segment.seek(SeekFrom::End(0))?;
        segment.write_all(&payload)?;
        let append_number = self
            .append_count
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            + 1;
        if append_number.is_multiple_of(self.policy.log_sync_interval) {
            segment.sync_data()?;
        }

        if self.segment_has_native_indexes(&active)? && (position == 0 || should_index_batch(batch))
        {
            let mut index = OpenOptions::new().append(true).open(&active.index)?;
            write_index_entry(
                &mut index,
                &IndexEntry {
                    base_offset: batch.base_offset,
                    position,
                    length: payload.len() as u32,
                    last_offset: batch.last_offset,
                },
            )?;

            let mut time_index = OpenOptions::new().append(true).open(&active.timeindex)?;
            write_time_index_entry(
                &mut time_index,
                &TimeIndexEntry {
                    max_timestamp_ms: batch.max_timestamp_ms,
                    offset: batch.base_offset,
                },
            )?;
        }
        append_kafka_index_entries(&active, batch, position)?;
        self.enforce_retention(topic, partition)?;
        Ok(())
    }

    pub(super) fn update_root_checkpoints(
        &self,
        topic: &str,
        partition: i32,
        log_end_offset: i64,
    ) -> Result<()> {
        self.update_checkpoint_file(
            "recovery-point-offset-checkpoint",
            topic,
            partition,
            log_end_offset,
        )?;
        self.update_checkpoint_file(
            "replication-offset-checkpoint",
            topic,
            partition,
            log_end_offset,
        )?;
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
        let mut records = Vec::new();
        for segment in self.segments_from_offset(topic, partition, start_offset)? {
            let start_position = self.start_position_for_offset(&segment, start_offset)?;
            let mut reached_limit = false;
            self.scan_batches_from_position(&segment, start_position, |batch| {
                if batch.last_offset < start_offset {
                    return Ok(true);
                }
                for record in batch.records {
                    if record.offset < start_offset {
                        continue;
                    }
                    records.push(record);
                    if records.len() >= limit {
                        reached_limit = true;
                        return Ok(false);
                    }
                }
                Ok(true)
            })?;
            if reached_limit {
                return Ok(records);
            }
        }
        Ok(records)
    }

    pub fn read_records_for_client(
        &self,
        topic: &str,
        partition: i32,
        start_offset: i64,
        max_bytes: usize,
    ) -> Result<Vec<BrokerRecord>> {
        if max_bytes == 0 {
            return Ok(Vec::new());
        }
        let mut records = Vec::new();
        let mut fetched_bytes = 0_usize;
        for segment in self.segments_from_offset(topic, partition, start_offset)? {
            let start_position = self.start_position_for_offset(&segment, start_offset)?;
            let mut finished = false;
            self.scan_batches_from_position(&segment, start_position, |batch| {
                if batch.last_offset < start_offset {
                    return Ok(true);
                }
                let batch_bytes = batch.encode_binary()?.len();
                let visible_batch_records = batch
                    .records
                    .into_iter()
                    .filter(|record| record.offset >= start_offset)
                    .collect::<Vec<_>>();
                if visible_batch_records.is_empty() {
                    return Ok(true);
                }
                let is_first_batch = records.is_empty();
                if !is_first_batch && fetched_bytes + batch_bytes > max_bytes {
                    finished = true;
                    return Ok(false);
                }
                fetched_bytes += batch_bytes;
                records.extend(visible_batch_records);
                if fetched_bytes >= max_bytes {
                    finished = true;
                    return Ok(false);
                }
                Ok(true)
            })?;
            if finished {
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

    pub fn offset_for_timestamp(
        &self,
        topic: &str,
        partition: i32,
        target_timestamp_ms: i64,
    ) -> Result<Option<(i64, i64)>> {
        for segment in self.segment_paths(topic, partition)? {
            let Some(segment_max_timestamp) = self.segment_max_timestamp(&segment)? else {
                continue;
            };
            if segment_max_timestamp < target_timestamp_ms {
                continue;
            }
            let start_position =
                self.start_position_for_timestamp(&segment, target_timestamp_ms)?;
            let mut found = None;
            self.scan_batches_from_position(&segment, start_position, |batch| {
                if batch.max_timestamp_ms < target_timestamp_ms {
                    return Ok(true);
                }
                if let Some(record) = batch
                    .records
                    .into_iter()
                    .find(|record| record.timestamp_ms >= target_timestamp_ms)
                {
                    found = Some((record.offset, record.timestamp_ms));
                    return Ok(false);
                }
                Ok(true)
            })?;
            if found.is_some() {
                return Ok(found);
            }
        }
        Ok(None)
    }

    pub(super) fn recover_internal_partition(&self, topic: &str, partition: i32) -> Result<()> {
        self.recover_partition(topic, partition)
    }

    pub(super) fn read_all_batches(&self, topic: &str, partition: i32) -> Result<Vec<StoredBatch>> {
        let mut batches = Vec::new();
        for segment in self.segment_paths(topic, partition)? {
            self.scan_batches_from_position(&segment, 0, |batch| {
                batches.push(batch);
                Ok(true)
            })?;
        }
        Ok(batches)
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

    fn update_checkpoint_file(
        &self,
        filename: &str,
        topic: &str,
        partition: i32,
        offset: i64,
    ) -> Result<()> {
        let path = self.root.join(filename);
        if !path.exists() {
            return Ok(());
        }
        let contents = fs::read_to_string(&path)?;
        let mut lines = contents.lines();
        let version = lines.next().unwrap_or("0");
        let _count = lines.next().unwrap_or("0");
        let mut entries = lines
            .map(parse_checkpoint_entry)
            .collect::<Result<Vec<_>>>()?;
        let mut found = false;
        for entry in &mut entries {
            if entry.0 == topic && entry.1 == partition {
                entry.2 = offset;
                found = true;
                break;
            }
        }
        if !found {
            entries.push((topic.to_string(), partition, offset));
        }

        let mut updated = format!("{version}\n{}\n", entries.len());
        for (topic, partition, offset) in entries {
            updated.push_str(&format!("{topic} {partition} {offset}\n"));
        }
        fs::write(path, updated)?;
        Ok(())
    }

    pub(super) fn active_segment_base_offset(&self, topic: &str, partition: i32) -> Result<i64> {
        Ok(self
            .segment_paths(topic, partition)?
            .last()
            .map(|segment| segment.base_offset)
            .unwrap_or(0))
    }

    pub(super) fn log_start_offset(&self, topic: &str, partition: i32) -> Result<i64> {
        Ok(self
            .earliest_offset(topic, partition)?
            .map(|(offset, _)| offset)
            .unwrap_or(0))
    }

    pub(super) fn segment_paths(&self, topic: &str, partition: i32) -> Result<Vec<SegmentPaths>> {
        let root = self.partition_dir(topic, partition);
        let mut segments = Vec::new();
        if !root.exists() {
            return Ok(segments);
        }
        for entry in fs::read_dir(&root)? {
            let entry = entry?;
            if !entry.file_type()?.is_file() {
                continue;
            }
            let path = entry.path();
            if path.extension().and_then(|ext| ext.to_str()) != Some(LOG_EXT) {
                continue;
            }
            let Some(base_offset) = parse_segment_base_offset(&path) else {
                continue;
            };
            segments.push(self.segment_paths_for_base(topic, partition, base_offset));
        }
        segments.sort_by_key(|segment| segment.base_offset);
        Ok(segments)
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
            if is_internal_topic_name(&name) {
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

    fn ensure_segment_files(
        &self,
        topic: &str,
        partition: i32,
        base_offset: i64,
    ) -> Result<SegmentPaths> {
        let paths = self.segment_paths_for_base(topic, partition, base_offset);
        for path in [&paths.log, &paths.index, &paths.timeindex] {
            if !path.exists() {
                File::create(path)?;
            }
        }
        Ok(paths)
    }

    fn segment_paths_for_base(
        &self,
        topic: &str,
        partition: i32,
        base_offset: i64,
    ) -> SegmentPaths {
        let prefix = format!("{base_offset:020}");
        let partition_dir = self.partition_dir(topic, partition);
        SegmentPaths {
            base_offset,
            log: partition_dir.join(format!("{prefix}.{LOG_EXT}")),
            index: partition_dir.join(format!("{prefix}.{INDEX_EXT}")),
            timeindex: partition_dir.join(format!("{prefix}.{TIMEINDEX_EXT}")),
        }
    }

    fn writable_segment(
        &self,
        topic: &str,
        partition: i32,
        batch_base_offset: i64,
        payload_len: u64,
    ) -> Result<SegmentPaths> {
        let segments = self.segment_paths(topic, partition)?;
        let Some(active) = segments.last() else {
            return self.ensure_segment_files(topic, partition, 0);
        };
        let current_len = fs::metadata(&active.log)?.len();
        if current_len > 0
            && (current_len + payload_len > self.policy.segment_bytes
                || self.should_roll_segment_by_time(active)?)
        {
            return self.ensure_segment_files(topic, partition, batch_base_offset);
        }
        Ok(active.clone())
    }

    fn enforce_retention(&self, topic: &str, partition: i32) -> Result<()> {
        if is_internal_topic_name(topic) {
            return Ok(());
        }
        let mut segments = self.segment_paths(topic, partition)?;
        if segments.len() <= 1 {
            return Ok(());
        }

        if let Some(retention_ms) = self.policy.retention_ms {
            while segments.len() > 1 {
                let Some(age_ms) = self.segment_age_ms(&segments[0])? else {
                    break;
                };
                if age_ms < retention_ms {
                    break;
                }
                let removed = segments.remove(0);
                self.remove_segment_files(&removed)?;
            }
        }

        let Some(retention_bytes) = self.policy.retention_bytes else {
            return Ok(());
        };
        if segments.len() <= 1 {
            return Ok(());
        }

        let mut total_log_bytes = segments.iter().try_fold(0_u64, |sum, segment| {
            Ok::<u64, crate::store::StoreError>(sum + fs::metadata(&segment.log)?.len())
        })?;
        while total_log_bytes > retention_bytes && segments.len() > 1 {
            let removed = segments.remove(0);
            total_log_bytes = total_log_bytes.saturating_sub(fs::metadata(&removed.log)?.len());
            self.remove_segment_files(&removed)?;
        }
        Ok(())
    }

    fn should_roll_segment_by_time(&self, segment: &SegmentPaths) -> Result<bool> {
        let Some(age_ms) = self.segment_age_ms(segment)? else {
            return Ok(false);
        };
        Ok(age_ms >= self.policy.segment_ms)
    }

    fn segments_from_offset(
        &self,
        topic: &str,
        partition: i32,
        start_offset: i64,
    ) -> Result<Vec<SegmentPaths>> {
        let segments = self.segment_paths(topic, partition)?;
        if segments.is_empty() {
            return Ok(Vec::new());
        }
        let start_index = segments
            .iter()
            .rposition(|segment| segment.base_offset <= start_offset)
            .unwrap_or(0);
        Ok(segments.into_iter().skip(start_index).collect())
    }

    fn start_position_for_offset(&self, segment: &SegmentPaths, start_offset: i64) -> Result<u64> {
        Ok(lookup_offset(segment, start_offset)?
            .map(|entry| entry.position)
            .unwrap_or(0))
    }

    fn start_position_for_timestamp(
        &self,
        segment: &SegmentPaths,
        target_timestamp_ms: i64,
    ) -> Result<u64> {
        let Some(entry) = lookup_timestamp(segment, target_timestamp_ms)? else {
            return Ok(0);
        };
        self.start_position_for_offset(segment, entry.offset)
    }

    fn segment_max_timestamp(&self, segment: &SegmentPaths) -> Result<Option<i64>> {
        Ok(last_time_index_entry(segment)?.map(|entry| entry.max_timestamp_ms))
    }

    fn segment_age_ms(&self, segment: &SegmentPaths) -> Result<Option<u64>> {
        if !segment.log.exists() {
            return Ok(None);
        }
        let modified = fs::metadata(&segment.log)?.modified()?;
        let elapsed = modified.elapsed().unwrap_or_default();
        Ok(Some(elapsed.as_millis().min(u128::from(u64::MAX)) as u64))
    }

    pub(super) fn segment_has_native_indexes(&self, segment: &SegmentPaths) -> Result<bool> {
        Ok(self.segment_has_native_offset_index(segment)?
            && self.segment_has_native_time_index(segment)?)
    }

    fn segment_has_native_offset_index(&self, segment: &SegmentPaths) -> Result<bool> {
        Ok(segment.index.exists() && fs::metadata(&segment.index)?.len() % 28 == 0)
    }

    fn segment_has_native_time_index(&self, segment: &SegmentPaths) -> Result<bool> {
        Ok(segment.timeindex.exists() && fs::metadata(&segment.timeindex)?.len() % 16 == 0)
    }

    fn read_batches_from_position(
        &self,
        segment: &SegmentPaths,
        position: u64,
    ) -> Result<Vec<StoredBatch>> {
        let mut batches = Vec::new();
        self.scan_batches_from_position(segment, position, |batch| {
            batches.push(batch);
            Ok(true)
        })?;
        Ok(batches)
    }

    fn scan_batches_from_position<F>(
        &self,
        segment: &SegmentPaths,
        position: u64,
        mut visit: F,
    ) -> Result<()>
    where
        F: FnMut(StoredBatch) -> Result<bool>,
    {
        if !segment.log.exists() {
            return Ok(());
        }
        let mut file = File::open(&segment.log)?;
        let file_len = file.metadata()?.len();
        if position >= file_len {
            return Ok(());
        }
        file.seek(SeekFrom::Start(position))?;
        while let Some(batch) = read_next_batch(&mut file)? {
            if !visit(batch)? {
                break;
            }
        }
        Ok(())
    }
}

fn parse_checkpoint_entry(line: &str) -> Result<(String, i32, i64)> {
    let mut parts = line.split_whitespace();
    let topic = parts.next().ok_or_else(|| {
        crate::store::StoreError::Protocol(format!("invalid checkpoint line `{line}`"))
    })?;
    let partition = parts
        .next()
        .ok_or_else(|| {
            crate::store::StoreError::Protocol(format!("invalid checkpoint line `{line}`"))
        })?
        .parse::<i32>()
        .map_err(|_| {
            crate::store::StoreError::Protocol(format!("invalid checkpoint line `{line}`"))
        })?;
    let offset = parts
        .next()
        .ok_or_else(|| {
            crate::store::StoreError::Protocol(format!("invalid checkpoint line `{line}`"))
        })?
        .parse::<i64>()
        .map_err(|_| {
            crate::store::StoreError::Protocol(format!("invalid checkpoint line `{line}`"))
        })?;
    if parts.next().is_some() {
        return Err(crate::store::StoreError::Protocol(format!(
            "invalid checkpoint line `{line}`"
        )));
    }
    Ok((topic.to_string(), partition, offset))
}

fn read_next_batch(file: &mut File) -> Result<Option<StoredBatch>> {
    let mut header = [0_u8; 12];
    match file.read_exact(&mut header) {
        Ok(()) => {}
        Err(err) if err.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(err) => return Err(err.into()),
    }
    let batch_len = i32::from_be_bytes(header[8..12].try_into().expect("slice size"));
    if batch_len < 0 {
        return Ok(None);
    }
    let payload_len = 12 + batch_len as usize;
    let mut payload = vec![0_u8; payload_len];
    payload[..12].copy_from_slice(&header);
    file.read_exact(&mut payload[12..])?;
    Ok(Some(StoredBatch::decode_binary(&payload)?))
}

fn parse_partition_dir(name: &str) -> Option<(&str, i32)> {
    let (topic, partition) = name.rsplit_once('-')?;
    if topic.is_empty() {
        return None;
    }
    let partition = partition.parse::<i32>().ok()?;
    (partition >= 0).then_some((topic, partition))
}

fn parse_segment_base_offset(path: &Path) -> Option<i64> {
    let stem = path.file_stem()?.to_str()?;
    if stem.len() < SEGMENT_SUFFIX_LEN {
        return None;
    }
    stem.parse::<i64>().ok()
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
                Some(LOG_EXT) => bytes.log_bytes += size,
                Some(INDEX_EXT) => bytes.index_bytes += size,
                Some(TIMEINDEX_EXT) => bytes.timeindex_bytes += size,
                _ => {}
            }
        }
    }
    Ok(bytes)
}

#[cfg(test)]
mod log_tests;
