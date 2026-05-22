mod cluster_metadata;
mod consumer_offsets;
mod control_plane;
mod data_plane;
mod internal_topics;
mod log;
mod policy;
mod replica_prepare;
mod state;
mod storage_impl;
mod storage_offsets;
mod topic_catalog;

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

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
    logs: Arc<RecordLog>,
    data: Mutex<DataPlaneState>,
    control: Mutex<ControlPlaneState>,
}

impl FileStore {
    pub fn open(root: impl AsRef<Path>) -> Result<Self> {
        let root = root.as_ref().to_path_buf();
        let logs = Arc::new(RecordLog::open(&root)?);
        let journal = StateJournal::new();
        let mut snapshots = SnapshotSet::load();
        journal.replay(&mut snapshots)?;
        let replayed_control = consumer_offsets::replay(&logs)?;
        snapshots.offsets = replayed_control.offsets;
        snapshots.groups = replayed_control.groups;
        snapshots.topics = cluster_metadata::recover_topic_states(&logs)?;
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
            logs: logs.clone(),
            data: Mutex::new(data),
            control: Mutex::new(ControlPlaneState::new(
                snapshots.groups,
                snapshots.offsets,
                logs.clone(),
                replayed_control.next_record_offsets,
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
        let data_bytes = self.logs.storage_bytes()?;
        let data = self.data.lock().expect("file store mutex poisoned");
        let control = self.control.lock().expect("file store mutex poisoned");
        Ok(StorageSummary {
            topic_count: data.topic_count(),
            group_count: control.group_count(),
            committed_offset_count: control.committed_offset_count(),
            total_bytes: data_bytes.total_bytes,
            log_bytes: data_bytes.log_bytes,
            index_bytes: data_bytes.index_bytes,
            timeindex_bytes: data_bytes.timeindex_bytes,
            state_bytes: 0,
        })
    }

    pub fn rebuild_indexes(&self, topic: &str) -> Result<()> {
        self.logs.rebuild_indexes_for_topic(topic)
    }
}

#[cfg(test)]
fn root_directories(root: &Path) -> Vec<String> {
    let mut names = std::fs::read_dir(root)
        .unwrap()
        .filter_map(|entry| {
            let entry = entry.unwrap();
            entry
                .file_type()
                .unwrap()
                .is_dir()
                .then(|| entry.file_name().to_string_lossy().to_string())
        })
        .collect::<Vec<_>>();
    names.sort();
    names
}

#[cfg(test)]
mod tests;
#[cfg(test)]
mod tests_d1;
