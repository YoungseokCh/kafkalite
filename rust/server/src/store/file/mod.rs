mod control_plane;
mod data_plane;
mod log;
mod policy;
mod state;
mod topic_catalog;

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use super::{
    BrokerRecord, FetchResult, GroupJoinRequest, GroupJoinResult, ListOffsetResult,
    OffsetCommitRequest, ProducerSession, Result, Storage, SyncGroupResult, TopicMetadata,
};
use control_plane::{ControlPlaneState, SyncGroupStateRequest};
use data_plane::{AppendDecision, DataPlaneState};
use log::{RecordLog, StoredBatch};
#[allow(unused_imports)]
pub use policy::FileStorePolicy;
use state::{SnapshotSet, StateJournal};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicPartitionSummary {
    pub partition: i32,
    pub next_offset: i64,
    pub log_start_offset: i64,
    pub active_segment_base_offset: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicSummary {
    pub name: String,
    pub partition_count: usize,
    pub partitions: Vec<TopicPartitionSummary>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageSummary {
    pub topic_count: usize,
    pub group_count: usize,
    pub committed_offset_count: usize,
    pub total_bytes: u64,
    pub log_bytes: u64,
    pub index_bytes: u64,
    pub timeindex_bytes: u64,
    pub state_bytes: u64,
}

pub struct FileStore {
    root: PathBuf,
    logs: RecordLog,
    data: Mutex<DataPlaneState>,
    control: Mutex<ControlPlaneState>,
}

impl FileStore {
    pub fn open(root: impl AsRef<Path>) -> Result<Self> {
        let root = root.as_ref().to_path_buf();
        let logs = RecordLog::open(&root)?;
        let journal = StateJournal::open(&root)?;
        let mut snapshots = SnapshotSet::load(&root)?;
        journal.replay(&mut snapshots)?;
        snapshots.topics = logs.recover_topic_states(&snapshots.topics)?;
        let recovered = snapshots
            .topics
            .iter()
            .map(|(topic, state)| {
                (
                    topic.clone(),
                    state.partitions.keys().copied().collect::<Vec<_>>(),
                )
            })
            .collect::<Vec<_>>();
        let mut data = DataPlaneState::new(snapshots.topics, snapshots.producers, journal.clone());
        for (topic, partitions) in recovered {
            data.ensure_known_partitions(&topic, &partitions, 0);
        }
        Ok(Self {
            root,
            logs,
            data: Mutex::new(data),
            control: Mutex::new(ControlPlaneState::new(
                snapshots.groups,
                snapshots.offsets,
                journal,
            )),
        })
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn describe_topic(&self, topic: &str) -> Option<TopicSummary> {
        let data = self.data.lock().expect("file store mutex poisoned");
        data.describe_topic(topic)
    }

    pub fn describe_storage(&self) -> Result<StorageSummary> {
        let (log_bytes, index_bytes, timeindex_bytes, total_bytes) =
            walk_storage(&self.root.join("topics"))?;
        let (_, _, _, state_bytes) = walk_storage(&self.root.join("state"))?;
        let data = self.data.lock().expect("file store mutex poisoned");
        let control = self.control.lock().expect("file store mutex poisoned");
        Ok(StorageSummary {
            topic_count: data.topic_count(),
            group_count: control.group_count(),
            committed_offset_count: control.committed_offset_count(),
            total_bytes: total_bytes + state_bytes,
            log_bytes,
            index_bytes,
            timeindex_bytes,
            state_bytes,
        })
    }

    pub fn rebuild_indexes(&self, topic: &str) -> Result<()> {
        self.logs.rebuild_indexes_for_topic(topic)
    }
}

fn walk_storage(root: &Path) -> Result<(u64, u64, u64, u64)> {
    let mut log_bytes = 0;
    let mut index_bytes = 0;
    let mut timeindex_bytes = 0;
    let mut total = 0;
    if !root.exists() {
        return Ok((0, 0, 0, 0));
    }
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        let path = entry.path();
        if entry.file_type()?.is_dir() {
            let (l, i, t, tt) = walk_storage(&path)?;
            log_bytes += l;
            index_bytes += i;
            timeindex_bytes += t;
            total += tt;
        } else {
            let size = entry.metadata()?.len();
            total += size;
            match path.extension().and_then(|ext| ext.to_str()) {
                Some("log") => log_bytes += size,
                Some("index") => index_bytes += size,
                Some("timeindex") => timeindex_bytes += size,
                _ => {}
            }
        }
    }
    Ok((log_bytes, index_bytes, timeindex_bytes, total))
}

impl Storage for FileStore {
    fn topic_metadata(
        &self,
        topics: Option<&[String]>,
        _now_ms: i64,
    ) -> Result<Vec<TopicMetadata>> {
        let data = self.data.lock().expect("file store mutex poisoned");
        Ok(data.topic_metadata(topics))
    }

    fn ensure_topic(&self, topic: &str, partition_count: i32, now_ms: i64) -> Result<()> {
        self.logs.ensure_topic(topic, partition_count)?;
        let mut data = self.data.lock().expect("file store mutex poisoned");
        data.ensure_topic(topic, partition_count, now_ms)
    }

    fn init_producer(&self, now_ms: i64) -> Result<ProducerSession> {
        let mut data = self.data.lock().expect("file store mutex poisoned");
        data.init_producer(now_ms)
    }

    fn append_records(
        &self,
        topic: &str,
        partition: i32,
        records: &[BrokerRecord],
        now_ms: i64,
    ) -> Result<(i64, i64)> {
        let decision = {
            let mut data = self.data.lock().expect("file store mutex poisoned");
            match data.prepare_append(topic, partition, records, now_ms) {
                Ok(decision) => decision,
                Err(crate::store::StoreError::UnknownTopicOrPartition { .. }) if partition == 0 => {
                    drop(data);
                    self.ensure_topic(topic, 1, now_ms)?;
                    let mut data = self.data.lock().expect("file store mutex poisoned");
                    data.prepare_append(topic, partition, records, now_ms)?
                }
                Err(err) => return Err(err),
            }
        };
        match decision {
            AppendDecision::Duplicate {
                base_offset,
                last_offset,
            } => Ok((base_offset, last_offset)),
            AppendDecision::Append(prepared) => {
                self.logs.ensure_partition(topic, partition)?;
                self.logs.append_batch(
                    topic,
                    partition,
                    &StoredBatch::from_records(&prepared.records),
                )?;
                let result = (prepared.base_offset, prepared.last_offset);
                let mut data = self.data.lock().expect("file store mutex poisoned");
                data.finish_append(&prepared, now_ms)?;
                Ok(result)
            }
        }
    }

    fn fetch_records(
        &self,
        topic: &str,
        partition: i32,
        start_offset: i64,
        limit: usize,
    ) -> Result<FetchResult> {
        let high_watermark = self
            .data
            .lock()
            .expect("file store mutex poisoned")
            .high_watermark(topic, partition)?;
        let records = self
            .logs
            .read_records(topic, partition, start_offset, limit)?;
        Ok(FetchResult {
            high_watermark,
            records,
        })
    }

    fn list_offsets(
        &self,
        topic: &str,
        partition: i32,
    ) -> Result<(ListOffsetResult, ListOffsetResult)> {
        let earliest = self
            .logs
            .earliest_offset(topic, partition)?
            .unwrap_or((0, 0));
        let latest = self
            .data
            .lock()
            .expect("file store mutex poisoned")
            .latest_offset(topic, partition)?;
        Ok((
            ListOffsetResult {
                offset: earliest.0,
                timestamp_ms: earliest.1,
            },
            ListOffsetResult {
                offset: latest,
                timestamp_ms: 0,
            },
        ))
    }

    fn join_group(&self, request: GroupJoinRequest<'_>) -> Result<GroupJoinResult> {
        let mut control = self.control.lock().expect("file store mutex poisoned");
        control.join_group(request)
    }

    fn sync_group(
        &self,
        group_id: &str,
        member_id: &str,
        generation_id: i32,
        protocol_name: &str,
        assignments: &[(String, Vec<u8>)],
        now_ms: i64,
    ) -> Result<SyncGroupResult> {
        let topics = {
            let data = self.data.lock().expect("file store mutex poisoned");
            data.topic_metadata(None)
        };
        let mut control = self.control.lock().expect("file store mutex poisoned");
        control.sync_group(SyncGroupStateRequest {
            group_id,
            member_id,
            generation_id,
            protocol_name,
            assignments,
            topics: &topics,
            now_ms,
        })
    }

    fn heartbeat(
        &self,
        group_id: &str,
        member_id: &str,
        generation_id: i32,
        now_ms: i64,
    ) -> Result<()> {
        let mut control = self.control.lock().expect("file store mutex poisoned");
        control.heartbeat(group_id, member_id, generation_id, now_ms)
    }

    fn leave_group(&self, group_id: &str, member_id: &str, now_ms: i64) -> Result<()> {
        let mut control = self.control.lock().expect("file store mutex poisoned");
        control.leave_group(group_id, member_id, now_ms)
    }

    fn commit_offset(&self, request: OffsetCommitRequest<'_>) -> Result<()> {
        let known_partition = self
            .data
            .lock()
            .expect("file store mutex poisoned")
            .has_partition(request.topic, request.partition);
        if !known_partition {
            return Err(crate::store::StoreError::UnknownTopicOrPartition {
                topic: request.topic.to_string(),
                partition: request.partition,
            });
        }
        let mut control = self.control.lock().expect("file store mutex poisoned");
        control.commit_offset(request)
    }

    fn fetch_offset(&self, group_id: &str, topic: &str, partition: i32) -> Result<Option<i64>> {
        let known_partition = self
            .data
            .lock()
            .expect("file store mutex poisoned")
            .has_partition(topic, partition);
        if !known_partition {
            return Err(crate::store::StoreError::UnknownTopicOrPartition {
                topic: topic.to_string(),
                partition,
            });
        }
        let control = self.control.lock().expect("file store mutex poisoned");
        Ok(control.fetch_offset(group_id, topic, partition))
    }
}

#[cfg(test)]
mod tests;
#[cfg(test)]
mod tests_d1;
