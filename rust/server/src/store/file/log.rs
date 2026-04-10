use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::store::{BrokerRecord, Result};

#[derive(Debug)]
pub struct RecordLog {
    root: PathBuf,
}

impl RecordLog {
    pub fn open(root: &Path) -> Result<Self> {
        fs::create_dir_all(root.join("topics"))?;
        fs::create_dir_all(root.join("broker"))?;
        let log = Self {
            root: root.to_path_buf(),
        };
        log.recover()?;
        Ok(log)
    }

    pub fn ensure_topic(&self, topic: &str) -> Result<()> {
        fs::create_dir_all(self.topic_dir(topic))?;
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
        let payload = serde_json::to_vec(batch)?;
        segment.write_all(&(payload.len() as u32).to_le_bytes())?;
        segment.write_all(&payload)?;
        segment.sync_data()?;

        let mut index = OpenOptions::new()
            .append(true)
            .open(self.index_path(topic))?;
        serde_json::to_writer(
            &mut index,
            &IndexEntry {
                base_offset: batch.base_offset,
                position,
                length: payload.len() as u32,
                last_offset: batch.last_offset,
            },
        )?;
        index.write_all(b"\n")?;

        let mut time_index = OpenOptions::new()
            .append(true)
            .open(self.time_index_path(topic))?;
        serde_json::to_writer(
            &mut time_index,
            &TimeIndexEntry {
                max_timestamp_ms: batch.max_timestamp_ms,
                base_offset: batch.base_offset,
                position,
            },
        )?;
        time_index.write_all(b"\n")?;
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
            let batch: StoredBatch = serde_json::from_slice(&payload)?;
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
        let reader = BufReader::new(File::open(self.index_path(topic))?);
        let mut entries = Vec::new();
        for line in reader.lines() {
            let line = line?;
            if line.is_empty() {
                continue;
            }
            entries.push(serde_json::from_str::<IndexEntry>(&line)?);
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
                || serde_json::from_slice::<StoredBatch>(&payload).is_err()
            {
                break;
            }
            safe_len += 4 + payload_len;
        }
        if safe_len < file_len {
            file.set_len(safe_len)?;
            file.sync_all()?;
            self.rebuild_indexes(topic)?;
        }
        Ok(())
    }

    fn rebuild_indexes(&self, topic: &str) -> Result<()> {
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
            let batch: StoredBatch = serde_json::from_slice(&payload)?;
            serde_json::to_writer(
                &mut index,
                &IndexEntry {
                    base_offset: batch.base_offset,
                    position,
                    length: payload_len as u32,
                    last_offset: batch.last_offset,
                },
            )?;
            index.write_all(b"\n")?;
            serde_json::to_writer(
                &mut time_index,
                &TimeIndexEntry {
                    max_timestamp_ms: batch.max_timestamp_ms,
                    base_offset: batch.base_offset,
                    position,
                },
            )?;
            time_index.write_all(b"\n")?;
            position += 4 + payload_len as u64;
        }
        index.sync_all()?;
        time_index.sync_all()?;
        Ok(())
    }

    fn topic_dir(&self, topic: &str) -> PathBuf {
        self.root.join("topics").join(topic)
    }

    fn segment_path(&self, topic: &str) -> PathBuf {
        self.topic_dir(topic).join("00000000000000000000.log")
    }

    fn index_path(&self, topic: &str) -> PathBuf {
        self.topic_dir(topic).join("00000000000000000000.index")
    }

    fn time_index_path(&self, topic: &str) -> PathBuf {
        self.topic_dir(topic).join("00000000000000000000.timeindex")
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
}

#[derive(Debug, Serialize, Deserialize)]
struct IndexEntry {
    base_offset: i64,
    position: u64,
    length: u32,
    last_offset: i64,
}

#[derive(Debug, Serialize)]
struct TimeIndexEntry {
    max_timestamp_ms: i64,
    base_offset: i64,
    position: u64,
}
