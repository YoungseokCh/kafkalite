use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{BufReader, Read, Seek, SeekFrom, Write};

use crate::store::Result;

use super::index::{IndexEntry, TimeIndexEntry, write_index_entry, write_time_index_entry};
use super::{RecordLog, StoredBatch};
use crate::store::file::state::{PartitionState, TopicState};

impl RecordLog {
    pub(super) fn recover(&self) -> Result<()> {
        for entry in fs::read_dir(self.root.join("topics"))? {
            let entry = entry?;
            if entry.file_type()?.is_dir() {
                self.recover_topic(&entry.file_name().to_string_lossy())?;
            }
        }
        Ok(())
    }

    fn recover_topic(&self, topic: &str) -> Result<()> {
        for partition in self.partition_ids(topic)? {
            self.recover_partition(topic, partition)?;
        }
        Ok(())
    }

    pub(super) fn recover_partition(&self, topic: &str, partition: i32) -> Result<()> {
        let segment_path = self.segment_path(topic, partition);
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
        }
        drop(file);
        self.rebuild_indexes_for_partition(topic, partition)?;
        Ok(())
    }

    pub fn recover_topic_states(
        &self,
        previous: &BTreeMap<String, TopicState>,
    ) -> Result<BTreeMap<String, TopicState>> {
        let mut topics = BTreeMap::new();
        for entry in fs::read_dir(self.root.join("topics"))? {
            let entry = entry?;
            if !entry.file_type()?.is_dir() {
                continue;
            }
            let topic_name = entry.file_name().to_string_lossy().to_string();
            let mut topic = previous.get(&topic_name).cloned().unwrap_or(TopicState {
                name: topic_name.clone(),
                partitions: BTreeMap::new(),
                created_at_unix_ms: 0,
                updated_at_unix_ms: 0,
            });
            topic.name = topic_name.clone();
            let partition_ids = self.partition_ids(&topic_name)?;
            for partition in partition_ids {
                topic.partitions.insert(
                    partition,
                    self.recover_partition_state(&topic_name, partition)?,
                );
            }
            topics.insert(topic_name, topic);
        }
        Ok(topics)
    }

    pub fn rebuild_indexes_for_topic(&self, topic: &str) -> Result<()> {
        for partition in self.partition_ids(topic)? {
            self.rebuild_indexes_for_partition(topic, partition)?;
        }
        Ok(())
    }

    pub fn truncate_to_offset(&self, topic: &str, partition: i32, next_offset: i64) -> Result<()> {
        if !self.segment_path(topic, partition).exists() {
            return Ok(());
        }
        let mut reader = BufReader::new(File::open(self.segment_path(topic, partition))?);
        let mut rewritten = Vec::new();
        loop {
            let mut len = [0_u8; 4];
            if reader.read_exact(&mut len).is_err() {
                break;
            }
            let payload_len = u32::from_le_bytes(len) as usize;
            let mut payload = vec![0_u8; payload_len];
            reader.read_exact(&mut payload)?;
            let mut batch = StoredBatch::decode_binary(&payload)?;
            if batch.base_offset >= next_offset {
                break;
            }
            if batch.last_offset >= next_offset {
                batch.records.retain(|record| record.offset < next_offset);
                if batch.records.is_empty() {
                    break;
                }
                batch.base_offset = batch
                    .records
                    .first()
                    .map(|record| record.offset)
                    .unwrap_or(0);
                batch.last_offset = batch
                    .records
                    .last()
                    .map(|record| record.offset)
                    .unwrap_or(batch.base_offset);
                batch.max_timestamp_ms = batch
                    .records
                    .iter()
                    .map(|record| record.timestamp_ms)
                    .max()
                    .unwrap_or(0);
            }
            let encoded = batch.encode_binary()?;
            rewritten.extend_from_slice(&(encoded.len() as u32).to_le_bytes());
            rewritten.extend_from_slice(&encoded);
            if batch.last_offset + 1 >= next_offset {
                break;
            }
        }
        let mut file = OpenOptions::new()
            .write(true)
            .open(self.segment_path(topic, partition))?;
        file.set_len(0)?;
        file.write_all(&rewritten)?;
        file.sync_all()?;
        self.rebuild_indexes_for_partition(topic, partition)
    }

    pub(super) fn rebuild_indexes_for_partition(&self, topic: &str, partition: i32) -> Result<()> {
        if !self.segment_path(topic, partition).exists() {
            return Ok(());
        }
        let mut index = File::create(self.index_path(topic, partition))?;
        let mut time_index = File::create(self.time_index_path(topic, partition))?;
        let mut reader = BufReader::new(File::open(self.segment_path(topic, partition))?);
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

    pub(super) fn recover_partition_state(
        &self,
        topic: &str,
        partition: i32,
    ) -> Result<PartitionState> {
        if !self.segment_path(topic, partition).exists() {
            return Ok(PartitionState::new(0));
        }
        let mut reader = BufReader::new(File::open(self.segment_path(topic, partition))?);
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
}
