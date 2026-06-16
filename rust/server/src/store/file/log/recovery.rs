use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};

use crate::store::Result;

use super::index::{
    IndexEntry, TimeIndexEntry, should_index_batch, write_index_entry, write_time_index_entry,
};
use super::{RecordLog, SegmentPaths, StoredBatch};
use crate::store::file::state::{PartitionState, TopicState};

impl RecordLog {
    pub(super) fn recover(&self) -> Result<()> {
        for (topic, partition) in self.discover_user_partitions()? {
            self.recover_partition(&topic, partition)?;
        }
        Ok(())
    }

    pub(super) fn recover_partition(&self, topic: &str, partition: i32) -> Result<()> {
        for segment in self.segment_paths(topic, partition)? {
            self.recover_segment(&segment)?;
        }
        Ok(())
    }

    pub fn recover_topic_states(
        &self,
        previous: &BTreeMap<String, TopicState>,
    ) -> Result<BTreeMap<String, TopicState>> {
        let mut topics = previous.clone();
        for (topic_name, partition) in self.discover_user_partitions()? {
            let topic = topics.entry(topic_name.clone()).or_insert_with(|| {
                previous.get(&topic_name).cloned().unwrap_or(TopicState {
                    name: topic_name.clone(),
                    partitions: BTreeMap::new(),
                    created_at_unix_ms: 0,
                    updated_at_unix_ms: 0,
                })
            });
            topic.name = topic_name.clone();
            let mut recovered = self.recover_partition_state(&topic_name, partition)?;
            if let Some(existing) = previous
                .get(&topic_name)
                .and_then(|topic| topic.partitions.get(&partition))
            {
                recovered.current_leader_epoch = existing.current_leader_epoch;
            }
            topic.partitions.insert(partition, recovered);
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
        let segments = self.segment_paths(topic, partition)?;
        if segments.is_empty() {
            return Ok(());
        }
        let mut retained = Vec::new();
        for mut batch in self.read_all_batches(topic, partition)? {
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
            retained.push(batch);
        }
        self.rewrite_partition(topic, partition, &retained)
    }

    pub(super) fn rebuild_indexes_for_partition(&self, topic: &str, partition: i32) -> Result<()> {
        for segment in self.segment_paths(topic, partition)? {
            self.rebuild_indexes_for_segment(&segment)?;
        }
        Ok(())
    }

    pub(super) fn recover_partition_state(
        &self,
        topic: &str,
        partition: i32,
    ) -> Result<PartitionState> {
        let segments = self.segment_paths(topic, partition)?;
        if segments.is_empty() {
            return Ok(PartitionState::new(0));
        }
        let mut next_offset = 0;
        for batch in self.read_all_batches(topic, partition)? {
            next_offset = batch.last_offset + 1;
        }
        let log_start_offset = self
            .earliest_offset(topic, partition)?
            .map(|(offset, _)| offset)
            .unwrap_or(0);
        Ok(PartitionState {
            next_offset,
            log_start_offset,
            active_segment_base_offset: segments
                .last()
                .map(|segment| segment.base_offset)
                .unwrap_or(0),
            current_leader_epoch: self.recover_partition_leader_epoch(topic, partition)?,
        })
    }

    fn recover_partition_leader_epoch(&self, topic: &str, partition: i32) -> Result<i32> {
        let path = self
            .partition_dir(topic, partition)
            .join("leader-epoch-checkpoint");
        if !path.exists() {
            return Ok(0);
        }
        let content = fs::read_to_string(path)?;
        let lines = content
            .lines()
            .map(str::trim)
            .filter(|line| !line.is_empty())
            .collect::<Vec<_>>();
        let Some(last_entry) = lines.iter().skip(2).last().copied() else {
            return Ok(0);
        };
        Ok(last_entry
            .split_whitespace()
            .next()
            .and_then(|value| value.parse::<i32>().ok())
            .unwrap_or(0))
    }

    fn recover_segment(&self, segment: &SegmentPaths) -> Result<()> {
        if !segment.log.exists() {
            return Ok(());
        }
        let mut file = File::open(&segment.log)?;
        let file_len = file.metadata()?.len();
        let mut safe_len = 0_u64;
        while safe_len + 12 <= file_len {
            file.seek(SeekFrom::Start(safe_len))?;
            let mut header = [0_u8; 12];
            if file.read_exact(&mut header).is_err() {
                break;
            }
            let batch_len = i32::from_be_bytes(header[8..12].try_into().expect("slice size"));
            if batch_len < 0 {
                break;
            }
            let payload_len = 12 + batch_len as u64;
            if safe_len + payload_len > file_len {
                break;
            }
            file.seek(SeekFrom::Start(safe_len))?;
            let mut payload = vec![0_u8; payload_len as usize];
            if file.read_exact(&mut payload).is_err()
                || StoredBatch::decode_binary(&payload).is_err()
            {
                break;
            }
            safe_len += payload_len;
        }
        let truncated = safe_len < file_len;
        if truncated {
            drop(file);
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .open(&segment.log)?;
            file.set_len(safe_len)?;
            file.sync_all()?;
        } else {
            drop(file);
        }
        if truncated {
            self.rebuild_indexes_for_segment(segment)?;
        }
        if fs::metadata(&segment.log)?.len() == 0 && segment.base_offset != 0 {
            self.remove_segment_files(segment)?;
        }
        Ok(())
    }

    fn rebuild_indexes_for_segment(&self, segment: &SegmentPaths) -> Result<()> {
        if !segment.log.exists() {
            return Ok(());
        }
        let mut index = File::create(&segment.index)?;
        let mut time_index = File::create(&segment.timeindex)?;
        let mut position = 0_u64;
        for batch in self.read_batches_from_position(segment, 0)? {
            let payload_len = batch.encode_binary()?.len();
            if position == 0 || should_index_batch(&batch) {
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
                        offset: batch.base_offset,
                    },
                )?;
            }
            position += payload_len as u64;
        }
        index.sync_all()?;
        time_index.sync_all()?;
        Ok(())
    }

    fn rewrite_partition(
        &self,
        topic: &str,
        partition: i32,
        batches: &[StoredBatch],
    ) -> Result<()> {
        self.ensure_partition(topic, partition)?;
        for segment in self.segment_paths(topic, partition)? {
            self.remove_segment_files(&segment)?;
        }
        if batches.is_empty() {
            self.ensure_segment_files(topic, partition, 0)?;
            return Ok(());
        }
        let mut current_segment: Option<SegmentPaths> = None;
        let mut current_len = 0_u64;
        for batch in batches {
            let payload = batch.encode_binary()?;
            let payload_len = payload.len() as u64;
            let should_roll = current_segment.is_some()
                && current_len > 0
                && current_len + payload_len > self.policy.segment_bytes;
            if current_segment.is_none() || should_roll {
                current_segment =
                    Some(self.ensure_segment_files(topic, partition, batch.base_offset)?);
                current_len = 0;
            }
            let segment = current_segment.as_ref().expect("segment initialized");
            let mut log = OpenOptions::new().append(true).open(&segment.log)?;
            let position = log.seek(SeekFrom::End(0))?;
            log.write_all(&payload)?;
            if position == 0 || should_index_batch(batch) {
                let mut index = OpenOptions::new().append(true).open(&segment.index)?;
                write_index_entry(
                    &mut index,
                    &IndexEntry {
                        base_offset: batch.base_offset,
                        position,
                        length: payload_len as u32,
                        last_offset: batch.last_offset,
                    },
                )?;
                let mut time_index = OpenOptions::new().append(true).open(&segment.timeindex)?;
                write_time_index_entry(
                    &mut time_index,
                    &TimeIndexEntry {
                        max_timestamp_ms: batch.max_timestamp_ms,
                        offset: batch.base_offset,
                    },
                )?;
            }
            current_len += payload_len;
        }
        Ok(())
    }

    pub(super) fn remove_segment_files(&self, segment: &SegmentPaths) -> Result<()> {
        for path in [&segment.log, &segment.index, &segment.timeindex] {
            if path.exists() {
                fs::remove_file(path)?;
            }
        }
        Ok(())
    }
}
