mod log;
mod state;

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use bytes::Bytes;

use super::{
    BrokerRecord, FetchResult, GroupJoinResult, GroupMember, ListOffsetResult, ProducerSession,
    Result, Storage, StoreError, SyncGroupResult, TopicMetadata,
};
use log::{RecordLog, StoredBatch};
use state::{
    GroupMemberState, GroupState, ProducerSequenceState, ProducerState, SnapshotSet, StateJournal,
    TopicState,
};

pub struct FileStore {
    root: PathBuf,
    inner: Mutex<FileStoreInner>,
}

struct FileStoreInner {
    topics: BTreeMap<String, TopicState>,
    producers: ProducerState,
    groups: BTreeMap<String, GroupState>,
    offsets: BTreeMap<String, i64>,
    logs: RecordLog,
    journal: StateJournal,
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
            inner: Mutex::new(FileStoreInner {
                topics: snapshots.topics,
                producers: snapshots.producers,
                groups: snapshots.groups,
                offsets: snapshots.offsets,
                logs,
                journal,
            }),
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
        let inner = self.inner.lock().expect("file store mutex poisoned");
        if let Some(requested) = topics {
            return Ok(requested
                .iter()
                .filter(|topic| inner.topics.contains_key(*topic))
                .map(|topic| TopicMetadata {
                    name: topic.clone(),
                })
                .collect());
        }
        Ok(inner
            .topics
            .keys()
            .cloned()
            .map(|name| TopicMetadata { name })
            .collect())
    }

    fn ensure_topic(&self, topic: &str, now_ms: i64) -> Result<()> {
        let mut inner = self.inner.lock().expect("file store mutex poisoned");
        ensure_topic_state(&mut inner, topic, now_ms)?;
        persist_all(&mut inner, &self.root)
    }

    fn init_producer(&self, now_ms: i64) -> Result<ProducerSession> {
        let mut inner = self.inner.lock().expect("file store mutex poisoned");
        let session = ProducerSession {
            producer_id: inner.producers.next_producer_id,
            producer_epoch: 0,
        };
        inner.producers.next_producer_id += 1;
        let producers = inner.producers.clone();
        inner
            .journal
            .append_producer_state(&self.root, &producers, now_ms)?;
        SnapshotSet::write(
            &self.root,
            &inner.topics,
            &inner.producers,
            &inner.groups,
            &inner.offsets,
        )?;
        inner.journal.clear(&self.root)?;
        Ok(session)
    }

    fn append_records(
        &self,
        topic: &str,
        records: &[BrokerRecord],
        now_ms: i64,
    ) -> Result<(i64, i64)> {
        let mut inner = self.inner.lock().expect("file store mutex poisoned");
        let batch_info = ProducerBatchInfo::from_records(records);
        if let Some(batch) = batch_info.as_ref() {
            validate_producer_state(&inner.producers, topic, batch)?;
            if let Some(duplicate) = duplicate_append_result(&inner.producers, topic, batch) {
                return Ok(duplicate);
            }
        }
        let base_offset = ensure_topic_state(&mut inner, topic, now_ms)?.next_offset;
        let mut appended = Vec::new();
        for (index, record) in records.iter().enumerate() {
            let offset = base_offset + index as i64;
            appended.push(BrokerRecord {
                offset,
                timestamp_ms: record.timestamp_ms,
                producer_id: record.producer_id,
                producer_epoch: record.producer_epoch,
                sequence: record.sequence,
                key: record.key.clone(),
                value: record.value.clone(),
                headers_json: record.headers_json.clone(),
            });
        }
        let last_offset = appended
            .last()
            .map(|record| record.offset)
            .unwrap_or(base_offset);
        inner
            .logs
            .append_batch(topic, &StoredBatch::from_records(&appended))?;
        let topic_state = inner.topics.get_mut(topic).expect("topic inserted");
        topic_state.next_offset = last_offset + 1;
        topic_state.updated_at_unix_ms = now_ms;
        for record in &appended {
            inner.producers.sequences.insert(
                producer_key(record.producer_id, topic),
                ProducerSequenceState {
                    producer_epoch: record.producer_epoch,
                    first_sequence: appended
                        .first()
                        .map(|value| value.sequence)
                        .unwrap_or(record.sequence),
                    last_sequence: record.sequence,
                    base_offset,
                    last_offset: record.offset,
                },
            );
        }
        persist_all(&mut inner, &self.root)?;
        Ok((base_offset, last_offset))
    }

    fn fetch_records(&self, topic: &str, start_offset: i64, limit: usize) -> Result<FetchResult> {
        let inner = self.inner.lock().expect("file store mutex poisoned");
        let records = inner.logs.read_records(topic, start_offset, limit)?;
        let high_watermark = inner
            .topics
            .get(topic)
            .map(|topic| topic.next_offset)
            .unwrap_or(0);
        Ok(FetchResult {
            high_watermark,
            records,
        })
    }

    fn list_offsets(&self, topic: &str) -> Result<(ListOffsetResult, ListOffsetResult)> {
        let inner = self.inner.lock().expect("file store mutex poisoned");
        let earliest = inner.logs.earliest_offset(topic)?.unwrap_or((0, 0));
        let latest = inner
            .topics
            .get(topic)
            .map(|topic| topic.next_offset)
            .unwrap_or(0);
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
        let mut inner = self.inner.lock().expect("file store mutex poisoned");
        let member_id = member_id
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned)
            .unwrap_or_else(|| format!("{group_id}-member-{now_ms}"));
        let (generation_id, protocol_name_result, leader, members) = {
            let group = inner
                .groups
                .entry(group_id.to_string())
                .or_insert_with(|| GroupState::new(protocol_type, protocol_name, now_ms));
            let pruned = prune_expired_members(group, now_ms);
            let changed = upsert_group_member(
                group,
                &member_id,
                protocol_type,
                protocol_name,
                metadata,
                session_timeout_ms,
                rebalance_timeout_ms,
                now_ms,
            );
            if pruned || changed || group.generation_id == 0 {
                group.generation_id += 1;
                group.protocol_type = protocol_type.to_string();
                group.protocol_name = protocol_name.to_string();
                for member in group.members.values_mut() {
                    member.generation_id = group.generation_id;
                    member.assignment = Vec::new();
                }
            }
            group.leader_member_id = group.members.keys().next().cloned();
            (
                group.generation_id,
                group.protocol_name.clone(),
                group
                    .leader_member_id
                    .clone()
                    .unwrap_or_else(|| member_id.clone()),
                group
                    .members
                    .values()
                    .map(|member| GroupMember {
                        member_id: member.member_id.clone(),
                        metadata: member.subscription_metadata.clone(),
                    })
                    .collect::<Vec<_>>(),
            )
        };
        persist_all(&mut inner, &self.root)?;
        Ok(GroupJoinResult {
            generation_id,
            protocol_name: protocol_name_result,
            leader,
            member_id,
            members,
        })
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
        let mut inner = self.inner.lock().expect("file store mutex poisoned");
        let group = inner
            .groups
            .get_mut(group_id)
            .ok_or_else(|| StoreError::StaleGeneration {
                expected: 0,
                actual: generation_id,
            })?;
        let _ = prune_expired_members(group, now_ms);
        if !group.members.contains_key(member_id) {
            return Err(StoreError::UnknownMember {
                group_id: group_id.to_string(),
                member_id: member_id.to_string(),
            });
        }
        if generation_id < group.generation_id {
            return Err(StoreError::UnknownMember {
                group_id: group_id.to_string(),
                member_id: member_id.to_string(),
            });
        }
        ensure_generation(group, generation_id)?;
        if !assignments.is_empty() {
            for (assigned_member, assignment) in assignments {
                if let Some(member) = group.members.get_mut(assigned_member) {
                    member.assignment = assignment.clone();
                    member.updated_at_unix_ms = now_ms;
                }
            }
        } else {
            maybe_build_assignments(group)?;
        }
        let assignment = group
            .members
            .get(member_id)
            .map(|member| member.assignment.clone())
            .unwrap_or_default();
        persist_all(&mut inner, &self.root)?;
        Ok(SyncGroupResult {
            protocol_name: protocol_name.to_string(),
            assignment,
        })
    }

    fn heartbeat(
        &self,
        group_id: &str,
        member_id: &str,
        generation_id: i32,
        now_ms: i64,
    ) -> Result<()> {
        let mut inner = self.inner.lock().expect("file store mutex poisoned");
        let group = inner
            .groups
            .get_mut(group_id)
            .ok_or_else(|| StoreError::UnknownMember {
                group_id: group_id.to_string(),
                member_id: member_id.to_string(),
            })?;
        if !group.members.contains_key(member_id) {
            return Err(StoreError::UnknownMember {
                group_id: group_id.to_string(),
                member_id: member_id.to_string(),
            });
        }
        ensure_generation(group, generation_id)?;
        let member = group
            .members
            .get_mut(member_id)
            .expect("member checked above");
        member.last_heartbeat_unix_ms = now_ms;
        member.updated_at_unix_ms = now_ms;
        persist_all(&mut inner, &self.root)
    }

    fn leave_group(&self, group_id: &str, member_id: &str, now_ms: i64) -> Result<()> {
        let mut inner = self.inner.lock().expect("file store mutex poisoned");
        if let Some(group) = inner.groups.get_mut(group_id) {
            if group.members.remove(member_id).is_some() {
                group.generation_id += 1;
                group.leader_member_id = group.members.keys().next().cloned();
                group.updated_at_unix_ms = now_ms;
            }
        }
        persist_all(&mut inner, &self.root)
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
        let mut inner = self.inner.lock().expect("file store mutex poisoned");
        let group = inner
            .groups
            .get_mut(group_id)
            .ok_or_else(|| StoreError::UnknownMember {
                group_id: group_id.to_string(),
                member_id: member_id.to_string(),
            })?;
        if !group.members.contains_key(member_id) {
            return Err(StoreError::UnknownMember {
                group_id: group_id.to_string(),
                member_id: member_id.to_string(),
            });
        }
        if generation_id > group.generation_id {
            return Err(StoreError::StaleGeneration {
                expected: group.generation_id,
                actual: generation_id,
            });
        }
        if let Some(member) = group.members.get_mut(member_id) {
            member.updated_at_unix_ms = now_ms;
        }
        inner
            .offsets
            .insert(offset_key(group_id, topic), next_offset);
        persist_all(&mut inner, &self.root)
    }

    fn fetch_offset(&self, group_id: &str, topic: &str) -> Result<Option<i64>> {
        let inner = self.inner.lock().expect("file store mutex poisoned");
        Ok(inner.offsets.get(&offset_key(group_id, topic)).copied())
    }
}

fn ensure_topic_state<'a>(
    inner: &'a mut FileStoreInner,
    topic: &str,
    now_ms: i64,
) -> Result<&'a mut TopicState> {
    if !inner.topics.contains_key(topic) {
        inner.logs.ensure_topic(topic)?;
        inner
            .topics
            .insert(topic.to_string(), TopicState::new(topic, now_ms));
    }
    Ok(inner.topics.get_mut(topic).expect("topic inserted"))
}

fn persist_all(inner: &mut FileStoreInner, root: &Path) -> Result<()> {
    inner.journal.append_topics(root, &inner.topics)?;
    inner.journal.append_producer_state(
        root,
        &inner.producers,
        chrono::Utc::now().timestamp_millis(),
    )?;
    inner.journal.append_groups(root, &inner.groups)?;
    inner.journal.append_offsets(root, &inner.offsets)?;
    SnapshotSet::write(
        root,
        &inner.topics,
        &inner.producers,
        &inner.groups,
        &inner.offsets,
    )?;
    inner.journal.clear(root)
}

fn validate_producer_state(
    producers: &ProducerState,
    topic: &str,
    batch: &ProducerBatchInfo,
) -> Result<()> {
    if batch.producer_id < 0 {
        return Ok(());
    }
    if batch.producer_id >= producers.next_producer_id {
        return Err(StoreError::UnknownProducerId {
            producer_id: batch.producer_id,
        });
    }
    if let Some(sequence) = producers
        .sequences
        .get(&producer_key(batch.producer_id, topic))
    {
        if batch.producer_epoch < sequence.producer_epoch {
            return Err(StoreError::StaleProducerEpoch {
                producer_id: batch.producer_id,
                expected: sequence.producer_epoch,
                actual: batch.producer_epoch,
            });
        }
        if batch.producer_epoch == sequence.producer_epoch {
            if batch.first_sequence == sequence.first_sequence
                && batch.last_sequence == sequence.last_sequence
            {
                return Ok(());
            }
            let expected = sequence.last_sequence + 1;
            if batch.first_sequence != expected {
                return Err(StoreError::InvalidProducerSequence {
                    producer_id: batch.producer_id,
                    expected,
                    actual: batch.first_sequence,
                });
            }
        }
    }
    Ok(())
}

fn duplicate_append_result(
    producers: &ProducerState,
    topic: &str,
    batch: &ProducerBatchInfo,
) -> Option<(i64, i64)> {
    let state = producers
        .sequences
        .get(&producer_key(batch.producer_id, topic))?;
    if batch.producer_epoch == state.producer_epoch
        && batch.first_sequence == state.first_sequence
        && batch.last_sequence == state.last_sequence
    {
        Some((state.base_offset, state.last_offset))
    } else {
        None
    }
}

fn prune_expired_members(group: &mut GroupState, now_ms: i64) -> bool {
    let before = group.members.len();
    group.members.retain(|_, member| {
        now_ms - member.last_heartbeat_unix_ms <= i64::from(member.session_timeout_ms)
    });
    before != group.members.len()
}

fn upsert_group_member(
    group: &mut GroupState,
    member_id: &str,
    protocol_type: &str,
    protocol_name: &str,
    metadata: &[u8],
    session_timeout_ms: i32,
    rebalance_timeout_ms: i32,
    now_ms: i64,
) -> bool {
    let next = GroupMemberState {
        member_id: member_id.to_string(),
        generation_id: group.generation_id,
        protocol_type: protocol_type.to_string(),
        protocol_name: protocol_name.to_string(),
        subscription_metadata: metadata.to_vec(),
        assignment: Vec::new(),
        session_timeout_ms,
        rebalance_timeout_ms,
        last_heartbeat_unix_ms: now_ms,
        updated_at_unix_ms: now_ms,
    };
    match group.members.insert(member_id.to_string(), next.clone()) {
        None => true,
        Some(previous) => {
            previous.protocol_type != next.protocol_type
                || previous.protocol_name != next.protocol_name
                || previous.subscription_metadata != next.subscription_metadata
                || previous.session_timeout_ms != next.session_timeout_ms
                || previous.rebalance_timeout_ms != next.rebalance_timeout_ms
        }
    }
}

fn ensure_generation(group: &GroupState, generation_id: i32) -> Result<()> {
    if group.generation_id != generation_id {
        return Err(StoreError::StaleGeneration {
            expected: group.generation_id,
            actual: generation_id,
        });
    }
    Ok(())
}

fn maybe_build_assignments(group: &mut GroupState) -> Result<()> {
    let subscriptions = group
        .members
        .values()
        .filter_map(|member| {
            parse_topics(&member.subscription_metadata)
                .ok()
                .map(|topics| (member.member_id.clone(), topics))
        })
        .collect::<Vec<_>>();
    let mut topic_subscribers: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for (member_id, topics) in subscriptions {
        for topic in topics.into_iter().collect::<BTreeSet<_>>() {
            topic_subscribers
                .entry(topic)
                .or_default()
                .push(member_id.clone());
        }
    }
    let mut assignments: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for (index, (topic, mut subscribers)) in topic_subscribers.into_iter().enumerate() {
        subscribers.sort();
        let member_id = subscribers[index % subscribers.len()].clone();
        assignments.entry(member_id).or_default().push(topic);
    }
    for member in group.members.values_mut() {
        member.assignment = encode_assignment(
            assignments
                .remove(&member.member_id)
                .unwrap_or_default()
                .as_slice(),
        )?;
    }
    Ok(())
}

fn parse_topics(bytes: &[u8]) -> anyhow::Result<Vec<String>> {
    use kafka_protocol::messages::ConsumerProtocolSubscription;
    use kafka_protocol::protocol::Decodable;

    let mut payload = Bytes::copy_from_slice(bytes);
    let subscription = ConsumerProtocolSubscription::decode(&mut payload, 3)?;
    Ok(subscription
        .topics
        .into_iter()
        .map(|topic| topic.to_string())
        .collect())
}

fn encode_assignment(topics: &[String]) -> Result<Vec<u8>> {
    use bytes::BytesMut;
    use kafka_protocol::messages::consumer_protocol_assignment::TopicPartition;
    use kafka_protocol::messages::{ConsumerProtocolAssignment, TopicName};
    use kafka_protocol::protocol::{Encodable, StrBytes};

    let partitions = topics
        .iter()
        .map(|topic| {
            TopicPartition::default()
                .with_topic(TopicName(StrBytes::from(topic.clone())))
                .with_partitions(vec![super::DEFAULT_PARTITION])
        })
        .collect();
    let assignment = ConsumerProtocolAssignment::default().with_assigned_partitions(partitions);
    let mut bytes = BytesMut::new();
    assignment
        .encode(&mut bytes, 3)
        .map_err(|err| StoreError::Protocol(err.to_string()))?;
    Ok(bytes.to_vec())
}

fn producer_key(producer_id: i64, topic: &str) -> String {
    format!("{producer_id}:{topic}")
}

struct ProducerBatchInfo {
    producer_id: i64,
    producer_epoch: i16,
    first_sequence: i32,
    last_sequence: i32,
}

impl ProducerBatchInfo {
    fn from_records(records: &[BrokerRecord]) -> Option<Self> {
        let first = records.first()?;
        let last = records.last()?;
        Some(Self {
            producer_id: first.producer_id,
            producer_epoch: first.producer_epoch,
            first_sequence: first.sequence,
            last_sequence: last.sequence,
        })
    }
}

fn offset_key(group_id: &str, topic: &str) -> String {
    format!("{group_id}:{topic}:0")
}

#[cfg(test)]
mod tests;
