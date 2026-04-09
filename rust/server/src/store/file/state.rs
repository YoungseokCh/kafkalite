use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::store::Result;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TopicState {
    pub name: String,
    pub next_offset: i64,
    pub log_start_offset: i64,
    pub active_segment_base_offset: i64,
    pub created_at_unix_ms: i64,
    pub updated_at_unix_ms: i64,
}

impl TopicState {
    pub fn new(name: &str, now_ms: i64) -> Self {
        Self {
            name: name.to_string(),
            next_offset: 0,
            log_start_offset: 0,
            active_segment_base_offset: 0,
            created_at_unix_ms: now_ms,
            updated_at_unix_ms: now_ms,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProducerState {
    pub next_producer_id: i64,
    pub sequences: BTreeMap<String, ProducerSequenceState>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProducerSequenceState {
    pub producer_epoch: i16,
    pub first_sequence: i32,
    pub last_sequence: i32,
    pub base_offset: i64,
    pub last_offset: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GroupState {
    pub generation_id: i32,
    pub protocol_type: String,
    pub protocol_name: String,
    pub leader_member_id: Option<String>,
    pub members: BTreeMap<String, GroupMemberState>,
    pub updated_at_unix_ms: i64,
}

impl GroupState {
    pub fn new(protocol_type: &str, protocol_name: &str, now_ms: i64) -> Self {
        Self {
            generation_id: 0,
            protocol_type: protocol_type.to_string(),
            protocol_name: protocol_name.to_string(),
            leader_member_id: None,
            members: BTreeMap::new(),
            updated_at_unix_ms: now_ms,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GroupMemberState {
    pub member_id: String,
    pub generation_id: i32,
    pub protocol_type: String,
    pub protocol_name: String,
    pub subscription_metadata: Vec<u8>,
    pub assignment: Vec<u8>,
    pub session_timeout_ms: i32,
    pub rebalance_timeout_ms: i32,
    pub last_heartbeat_unix_ms: i64,
    pub updated_at_unix_ms: i64,
}

#[derive(Debug, Clone, Default)]
pub struct SnapshotSet {
    pub topics: BTreeMap<String, TopicState>,
    pub producers: ProducerState,
    pub groups: BTreeMap<String, GroupState>,
    pub offsets: BTreeMap<String, i64>,
}

impl SnapshotSet {
    pub fn load(root: &Path) -> Result<Self> {
        Ok(Self {
            topics: read_json(root.join("state/topics.snapshot"))?.unwrap_or_default(),
            producers: read_json(root.join("state/producers.snapshot"))?.unwrap_or(ProducerState {
                next_producer_id: 1,
                sequences: BTreeMap::new(),
            }),
            groups: read_json(root.join("state/groups.snapshot"))?.unwrap_or_default(),
            offsets: read_json(root.join("state/offsets.snapshot"))?.unwrap_or_default(),
        })
    }

    pub fn write(
        root: &Path,
        topics: &BTreeMap<String, TopicState>,
        producers: &ProducerState,
        groups: &BTreeMap<String, GroupState>,
        offsets: &BTreeMap<String, i64>,
    ) -> Result<()> {
        fs::create_dir_all(root.join("state"))?;
        write_json(root.join("state/topics.snapshot"), topics)?;
        write_json(root.join("state/producers.snapshot"), producers)?;
        write_json(root.join("state/groups.snapshot"), groups)?;
        write_json(root.join("state/offsets.snapshot"), offsets)?;
        write_json(
            root.join("state/checkpoints.json"),
            &serde_json::json!({"journal_entries": 0}),
        )
    }
}

pub struct StateJournal {
    path: PathBuf,
}

impl StateJournal {
    pub fn open(root: &Path) -> Result<Self> {
        fs::create_dir_all(root.join("state"))?;
        let path = root.join("state/state.journal");
        if !path.exists() {
            File::create(&path)?;
        }
        Ok(Self { path })
    }

    pub fn replay(&self, snapshots: &mut SnapshotSet) -> Result<()> {
        let file = File::open(&self.path)?;
        for line in BufReader::new(file).lines() {
            let line = line?;
            if line.is_empty() {
                continue;
            }
            match serde_json::from_str::<JournalEntry>(&line)? {
                JournalEntry::Topics(topics) => snapshots.topics = topics,
                JournalEntry::Producers(producers) => snapshots.producers = producers,
                JournalEntry::Groups(groups) => snapshots.groups = groups,
                JournalEntry::Offsets(offsets) => snapshots.offsets = offsets,
            }
        }
        Ok(())
    }

    pub fn append_topics(
        &mut self,
        root: &Path,
        topics: &BTreeMap<String, TopicState>,
    ) -> Result<()> {
        self.append(root, JournalEntry::Topics(topics.clone()))
    }

    pub fn append_producer_state(
        &mut self,
        root: &Path,
        producers: &ProducerState,
        _now_ms: i64,
    ) -> Result<()> {
        self.append(root, JournalEntry::Producers(producers.clone()))
    }

    pub fn append_groups(
        &mut self,
        root: &Path,
        groups: &BTreeMap<String, GroupState>,
    ) -> Result<()> {
        self.append(root, JournalEntry::Groups(groups.clone()))
    }

    pub fn append_offsets(&mut self, root: &Path, offsets: &BTreeMap<String, i64>) -> Result<()> {
        self.append(root, JournalEntry::Offsets(offsets.clone()))
    }

    pub fn clear(&mut self, root: &Path) -> Result<()> {
        File::create(&self.path)?.sync_all()?;
        write_json(
            root.join("state/checkpoints.json"),
            &serde_json::json!({"journal_entries": 0}),
        )
    }

    fn append(&mut self, root: &Path, entry: JournalEntry) -> Result<()> {
        let mut file = OpenOptions::new().append(true).open(&self.path)?;
        serde_json::to_writer(&mut file, &entry)?;
        file.write_all(b"\n")?;
        file.sync_all()?;
        write_json(
            root.join("state/checkpoints.json"),
            &serde_json::json!({"journal_entries": 1}),
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum JournalEntry {
    Topics(BTreeMap<String, TopicState>),
    Producers(ProducerState),
    Groups(BTreeMap<String, GroupState>),
    Offsets(BTreeMap<String, i64>),
}

fn read_json<T: for<'de> Deserialize<'de>>(path: PathBuf) -> Result<Option<T>> {
    if !path.exists() {
        return Ok(None);
    }
    Ok(Some(serde_json::from_reader(File::open(path)?)?))
}

fn write_json<T: Serialize>(path: PathBuf, value: &T) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let tmp = path.with_extension("tmp");
    let mut file = File::create(&tmp)?;
    serde_json::to_writer_pretty(&mut file, value)?;
    file.write_all(b"\n")?;
    file.sync_all()?;
    fs::rename(tmp, path)?;
    Ok(())
}
