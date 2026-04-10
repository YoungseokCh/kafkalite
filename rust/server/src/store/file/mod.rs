mod control_plane;
mod data_plane;
mod log;
mod state;

use std::path::{Path, PathBuf};
use std::sync::Mutex;

use super::{
    BrokerRecord, FetchResult, GroupJoinResult, ListOffsetResult, ProducerSession, Result, Storage,
    SyncGroupResult, TopicMetadata,
};
use control_plane::ControlPlaneState;
use data_plane::{AppendDecision, DataPlaneState};
use log::{RecordLog, StoredBatch};
use state::{SnapshotSet, StateJournal};

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
        Ok(Self {
            root,
            logs,
            data: Mutex::new(DataPlaneState::new(
                snapshots.topics,
                snapshots.producers,
                journal.clone(),
            )),
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

    fn ensure_topic(&self, topic: &str, now_ms: i64) -> Result<()> {
        self.logs.ensure_topic(topic)?;
        let mut data = self.data.lock().expect("file store mutex poisoned");
        data.ensure_topic(topic, now_ms)
    }

    fn init_producer(&self, now_ms: i64) -> Result<ProducerSession> {
        let mut data = self.data.lock().expect("file store mutex poisoned");
        data.init_producer(now_ms)
    }

    fn append_records(
        &self,
        topic: &str,
        records: &[BrokerRecord],
        now_ms: i64,
    ) -> Result<(i64, i64)> {
        self.logs.ensure_topic(topic)?;
        let decision = {
            let mut data = self.data.lock().expect("file store mutex poisoned");
            data.prepare_append(topic, records, now_ms)?
        };
        match decision {
            AppendDecision::Duplicate {
                base_offset,
                last_offset,
            } => Ok((base_offset, last_offset)),
            AppendDecision::Append(prepared) => {
                self.logs
                    .append_batch(topic, &StoredBatch::from_records(&prepared.records))?;
                let result = (prepared.base_offset, prepared.last_offset);
                let mut data = self.data.lock().expect("file store mutex poisoned");
                data.finish_append(&prepared, now_ms)?;
                Ok(result)
            }
        }
    }

    fn fetch_records(&self, topic: &str, start_offset: i64, limit: usize) -> Result<FetchResult> {
        let high_watermark = self
            .data
            .lock()
            .expect("file store mutex poisoned")
            .high_watermark(topic);
        let records = self.logs.read_records(topic, start_offset, limit)?;
        Ok(FetchResult {
            high_watermark,
            records,
        })
    }

    fn list_offsets(&self, topic: &str) -> Result<(ListOffsetResult, ListOffsetResult)> {
        let earliest = self.logs.earliest_offset(topic)?.unwrap_or((0, 0));
        let latest = self
            .data
            .lock()
            .expect("file store mutex poisoned")
            .latest_offset(topic);
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

    fn join_group(
        &self,
        group_id: &str,
        member_id: Option<&str>,
        protocol_type: &str,
        protocol_name: &str,
        metadata: &[u8],
        session_timeout_ms: i32,
        rebalance_timeout_ms: i32,
        now_ms: i64,
    ) -> Result<GroupJoinResult> {
        let mut control = self.control.lock().expect("file store mutex poisoned");
        control.join_group(
            group_id,
            member_id,
            protocol_type,
            protocol_name,
            metadata,
            session_timeout_ms,
            rebalance_timeout_ms,
            now_ms,
        )
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
        let mut control = self.control.lock().expect("file store mutex poisoned");
        control.sync_group(
            group_id,
            member_id,
            generation_id,
            protocol_name,
            assignments,
            now_ms,
        )
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

    fn commit_offset(
        &self,
        group_id: &str,
        member_id: &str,
        generation_id: i32,
        topic: &str,
        next_offset: i64,
        now_ms: i64,
    ) -> Result<()> {
        let mut control = self.control.lock().expect("file store mutex poisoned");
        control.commit_offset(
            group_id,
            member_id,
            generation_id,
            topic,
            next_offset,
            now_ms,
        )
    }

    fn fetch_offset(&self, group_id: &str, topic: &str) -> Result<Option<i64>> {
        let control = self.control.lock().expect("file store mutex poisoned");
        Ok(control.fetch_offset(group_id, topic))
    }
}

#[cfg(test)]
mod tests;
