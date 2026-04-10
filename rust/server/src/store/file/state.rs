use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{BufReader, Read, Write};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::store::{Result, StoreError};

const JOURNAL_MAGIC: &[u8; 4] = b"KFSJ";

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
}

#[derive(Debug, Clone)]
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
        let mut reader = BufReader::new(File::open(&self.path)?);
        loop {
            let Some(entry) = read_journal_entry(&mut reader)? else {
                break;
            };
            match entry {
                JournalEntry::Topics(topics) => snapshots.topics = topics,
                JournalEntry::Producers(producers) => snapshots.producers = producers,
                JournalEntry::Groups(groups) => snapshots.groups = groups,
                JournalEntry::Offsets(offsets) => snapshots.offsets = offsets,
            }
        }
        Ok(())
    }

    pub fn append_topics(&self, topics: &BTreeMap<String, TopicState>) -> Result<()> {
        self.append(JournalEntry::Topics(topics.clone()), false)
    }

    pub fn append_producer_state(&self, producers: &ProducerState, _now_ms: i64) -> Result<()> {
        self.append(JournalEntry::Producers(producers.clone()), false)
    }

    pub fn append_groups(&self, groups: &BTreeMap<String, GroupState>) -> Result<()> {
        self.append(JournalEntry::Groups(groups.clone()), false)
    }

    pub fn append_offsets(&self, offsets: &BTreeMap<String, i64>) -> Result<()> {
        self.append(JournalEntry::Offsets(offsets.clone()), true)
    }

    fn append(&self, entry: JournalEntry, sync: bool) -> Result<()> {
        let mut file = OpenOptions::new().append(true).open(&self.path)?;
        write_journal_entry(&mut file, &entry)?;
        if sync {
            file.sync_all()?;
        }
        Ok(())
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

fn write_journal_entry(writer: &mut File, entry: &JournalEntry) -> Result<()> {
    let payload = serde_json::to_vec(entry)?;
    writer.write_all(JOURNAL_MAGIC)?;
    writer.write_all(&(payload.len() as u32).to_le_bytes())?;
    writer.write_all(&payload)?;
    Ok(())
}

fn read_journal_entry(reader: &mut BufReader<File>) -> Result<Option<JournalEntry>> {
    let mut magic = [0_u8; 4];
    if reader.read_exact(&mut magic).is_err() {
        return Ok(None);
    }
    if &magic != JOURNAL_MAGIC {
        return Err(StoreError::Protocol("invalid journal magic".to_string()));
    }
    let mut len = [0_u8; 4];
    reader.read_exact(&mut len)?;
    let len = u32::from_le_bytes(len) as usize;
    let mut payload = vec![0_u8; len];
    reader.read_exact(&mut payload)?;
    Ok(Some(serde_json::from_slice(&payload)?))
}
