use std::fs::{self, File, OpenOptions};
use std::io::{BufReader, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::store::{BrokerRecord, Result, StoreError};

use super::policy::DEFAULT_POLICY;
use super::state::{PartitionState, TopicState};

const BATCH_MAGIC: &[u8; 4] = b"KFLG";

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

    pub fn ensure_topic(&self, topic: &str) -> Result<()> {
        fs::create_dir_all(self.partition_dir(topic))?;
        if !self.segment_path(topic).exists() {
            File::create(self.segment_path(topic))?;
        }
        if !self.index_path(topic).exists() {
            File::create(self.index_path(topic))?;
        }
        if !self.time_index_path(topic).exists() {
            File::create(self.time_index_path(topic))?;
        }
        Ok(())
    }

    pub fn append_batch(&self, topic: &str, batch: &StoredBatch) -> Result<()> {
        self.ensure_topic(topic)?;
        let mut segment = OpenOptions::new()
            .append(true)
            .read(true)
            .open(self.segment_path(topic))?;
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
                .open(self.index_path(topic))?;
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
                .open(self.time_index_path(topic))?;
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
        start_offset: i64,
        limit: usize,
    ) -> Result<Vec<BrokerRecord>> {
        if !self.segment_path(topic).exists() {
            return Ok(Vec::new());
        }
        let start_position = self.lookup_position(topic, start_offset)?;
        let mut file = File::open(self.segment_path(topic))?;
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

    pub fn earliest_offset(&self, topic: &str) -> Result<Option<(i64, i64)>> {
        let records = self.read_records(topic, 0, 1)?;
        Ok(records
            .into_iter()
            .next()
            .map(|record| (record.offset, record.timestamp_ms)))
    }

    fn lookup_position(&self, topic: &str, start_offset: i64) -> Result<u64> {
        let mut candidate = 0_u64;
        for entry in self.read_index_entries(topic)? {
            if entry.base_offset <= start_offset {
                candidate = entry.position;
            } else {
                break;
            }
        }
        Ok(candidate)
    }

    fn read_index_entries(&self, topic: &str) -> Result<Vec<IndexEntry>> {
        if !self.index_path(topic).exists() {
            return Ok(Vec::new());
        }
        let mut reader = File::open(self.index_path(topic))?;
        let mut entries = Vec::new();
        while let Some(entry) = read_index_entry(&mut reader)? {
            entries.push(entry);
        }
        Ok(entries)
    }

    fn recover(&self) -> Result<()> {
        for entry in fs::read_dir(self.root.join("topics"))? {
            let entry = entry?;
            if entry.file_type()?.is_dir() {
                self.recover_topic(&entry.file_name().to_string_lossy())?;
            }
        }
        Ok(())
    }

    fn recover_topic(&self, topic: &str) -> Result<()> {
        let segment_path = self.segment_path(topic);
        if !segment_path.exists() {
            return Ok(());
        }
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&segment_path)?;
        let file_len = file.metadata()?.len();
        let mut safe_len = 0_u64;
        while safe_len < file_len {
            file.seek(SeekFrom::Start(safe_len))?;
            let mut len = [0_u8; 4];
            if file.read_exact(&mut len).is_err() {
                break;
            }
            let payload_len = u32::from_le_bytes(len) as u64;
            let mut payload = vec![0_u8; payload_len as usize];
            if file.read_exact(&mut payload).is_err()
                || StoredBatch::decode_binary(&payload).is_err()
            {
                break;
            }
            safe_len += 4 + payload_len;
        }
        if safe_len < file_len {
            file.set_len(safe_len)?;
            file.sync_all()?;
            self.rebuild_indexes_for_topic(topic)?;
        }
        Ok(())
    }

    pub fn recover_topic_states(
        &self,
        previous: &std::collections::BTreeMap<String, TopicState>,
    ) -> Result<std::collections::BTreeMap<String, TopicState>> {
        let mut topics = std::collections::BTreeMap::new();
        for entry in fs::read_dir(self.root.join("topics"))? {
            let entry = entry?;
            if !entry.file_type()?.is_dir() {
                continue;
            }
            let topic_name = entry.file_name().to_string_lossy().to_string();
            let mut topic = previous.get(&topic_name).cloned().unwrap_or(TopicState {
                name: topic_name.clone(),
                partitions: std::collections::BTreeMap::new(),
                created_at_unix_ms: 0,
                updated_at_unix_ms: 0,
            });
            topic.name = topic_name.clone();
            topic
                .partitions
                .insert(0, self.recover_partition_state(&topic_name)?);
            topics.insert(topic_name, topic);
        }
        Ok(topics)
    }

    pub fn rebuild_indexes_for_topic(&self, topic: &str) -> Result<()> {
        if !self.segment_path(topic).exists() {
            return Ok(());
        }
        let mut index = File::create(self.index_path(topic))?;
        let mut time_index = File::create(self.time_index_path(topic))?;
        let mut reader = BufReader::new(File::open(self.segment_path(topic))?);
        let mut position = 0_u64;
        loop {
            let mut len = [0_u8; 4];
            if reader.read_exact(&mut len).is_err() {
                break;
            }
            let payload_len = u32::from_le_bytes(len) as usize;
            let mut payload = vec![0_u8; payload_len];
            reader.read_exact(&mut payload)?;
            let batch = StoredBatch::decode_binary(&payload)?;
            write_index_entry(
                &mut index,
                &IndexEntry {
                    base_offset: batch.base_offset,
                    position,
                    length: payload_len as u32,
                    last_offset: batch.last_offset,
                },
            )?;
            write_time_index_entry(
                &mut time_index,
                &TimeIndexEntry {
                    max_timestamp_ms: batch.max_timestamp_ms,
                    base_offset: batch.base_offset,
                    position,
                },
            )?;
            position += 4 + payload_len as u64;
        }
        index.sync_all()?;
        time_index.sync_all()?;
        Ok(())
    }

    fn topic_dir(&self, topic: &str) -> PathBuf {
        self.root.join("topics").join(topic)
    }

    fn recover_partition_state(&self, topic: &str) -> Result<PartitionState> {
        if !self.segment_path(topic).exists() {
            return Ok(PartitionState::new(0));
        }
        let mut reader = BufReader::new(File::open(self.segment_path(topic))?);
        let mut next_offset = 0;
        loop {
            let mut len = [0_u8; 4];
            if reader.read_exact(&mut len).is_err() {
                break;
            }
            let payload_len = u32::from_le_bytes(len) as usize;
            let mut payload = vec![0_u8; payload_len];
            reader.read_exact(&mut payload)?;
            let batch = StoredBatch::decode_binary(&payload)?;
            next_offset = batch.last_offset + 1;
        }
        Ok(PartitionState {
            next_offset,
            log_start_offset: 0,
            active_segment_base_offset: 0,
        })
    }

    fn partition_dir(&self, topic: &str) -> PathBuf {
        self.topic_dir(topic)
            .join("partitions")
            .join(crate::store::DEFAULT_PARTITION.to_string())
    }

    fn segment_path(&self, topic: &str) -> PathBuf {
        self.partition_dir(topic).join("00000000000000000000.log")
    }

    fn index_path(&self, topic: &str) -> PathBuf {
        self.partition_dir(topic).join("00000000000000000000.index")
    }

    fn time_index_path(&self, topic: &str) -> PathBuf {
        self.partition_dir(topic)
            .join("00000000000000000000.timeindex")
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredBatch {
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

#[derive(Debug, Clone, Copy)]
struct IndexEntry {
    base_offset: i64,
    position: u64,
    length: u32,
    last_offset: i64,
}

#[derive(Debug, Clone, Copy)]
struct TimeIndexEntry {
    max_timestamp_ms: i64,
    base_offset: i64,
    position: u64,
}

fn write_index_entry(writer: &mut File, entry: &IndexEntry) -> Result<()> {
    writer.write_all(&entry.base_offset.to_le_bytes())?;
    writer.write_all(&entry.position.to_le_bytes())?;
    writer.write_all(&entry.length.to_le_bytes())?;
    writer.write_all(&entry.last_offset.to_le_bytes())?;
    Ok(())
}

fn should_index_batch(batch: &StoredBatch) -> bool {
    batch.base_offset == 0 || batch.base_offset % DEFAULT_POLICY.index_stride == 0
}

fn read_index_entry(reader: &mut File) -> Result<Option<IndexEntry>> {
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

fn write_time_index_entry(writer: &mut File, entry: &TimeIndexEntry) -> Result<()> {
    writer.write_all(&entry.max_timestamp_ms.to_le_bytes())?;
    writer.write_all(&entry.base_offset.to_le_bytes())?;
    writer.write_all(&entry.position.to_le_bytes())?;
    Ok(())
}

fn write_bytes(out: &mut Vec<u8>, bytes: Option<&[u8]>) {
    match bytes {
        Some(bytes) => {
            out.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
            out.extend_from_slice(bytes);
        }
        None => out.extend_from_slice(&u32::MAX.to_le_bytes()),
    }
}

fn read_bytes(reader: &mut std::io::Cursor<&[u8]>) -> Result<Option<Vec<u8>>> {
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
