mod control_plane;
mod data_plane;
mod log;
mod policy;
mod replica_prepare;
mod state;
mod storage_impl;
mod storage_offsets;
mod topic_catalog;

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use super::Result;
use control_plane::ControlPlaneState;
use data_plane::DataPlaneState;
use log::RecordLog;
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

#[cfg(test)]
mod tests;
#[cfg(test)]
mod tests_d1;
