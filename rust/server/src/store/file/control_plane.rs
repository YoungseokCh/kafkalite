use std::collections::BTreeMap;
use std::sync::Arc;

use crate::store::{
    GroupJoinRequest, GroupJoinResult, GroupMember, OffsetCommitRequest, Result, StoreError,
    SyncGroupResult, TopicMetadata, TransactionalOffsetCommit, TransactionalOffsetCommitRequest,
};

use self::assignment::{
    ensure_assignment_ready, ensure_complete_assignments, maybe_build_assignments,
};
use self::membership::{
    MemberRegistration, ensure_generation, prune_expired_members, upsert_group_member,
};
use self::offset_key::OffsetKey;
use super::log::RecordLog;
use super::state::{GroupMemberState, GroupState};

mod assignment;
mod membership;
mod offset_key;
mod persistence;

type PendingTransactionalOffsets = BTreeMap<(i64, i32), Vec<TransactionalOffsetCommit>>;

pub struct ControlPlaneState {
    groups: BTreeMap<String, GroupState>,
    offsets: BTreeMap<OffsetKey, i64>,
    pending_transactional_offsets: PendingTransactionalOffsets,
    logs: Arc<RecordLog>,
    next_consumer_offsets_records: BTreeMap<i32, i64>,
}

pub struct SyncGroupStateRequest<'a> {
    pub group_id: &'a str,
    pub member_id: &'a str,
    pub generation_id: i32,
    pub protocol_name: &'a str,
    pub assignments: &'a [(String, Vec<u8>)],
    pub topics: &'a [TopicMetadata],
    pub now_ms: i64,
}

impl ControlPlaneState {
    pub fn new(
        groups: BTreeMap<String, GroupState>,
        offsets: BTreeMap<String, i64>,
        pending_transactional_offsets: PendingTransactionalOffsets,
        logs: Arc<RecordLog>,
        next_consumer_offsets_records: BTreeMap<i32, i64>,
    ) -> Self {
        Self {
            groups,
            offsets: offsets
                .into_iter()
                .map(|(key, value)| (OffsetKey::from_serialized(&key), value))
                .collect(),
            pending_transactional_offsets,
            logs,
            next_consumer_offsets_records,
        }
    }

    pub fn join_group(&mut self, request: GroupJoinRequest<'_>) -> Result<GroupJoinResult> {
        let member_id = request
            .member_id
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned)
            .unwrap_or_else(|| format!("{}-member-{}", request.group_id, request.now_ms));
        let (generation_id, protocol_name_result, leader, members, group_snapshot) = {
            let group = self
                .groups
                .entry(request.group_id.to_string())
                .or_insert_with(|| {
                    GroupState::new(request.protocol_type, request.protocol_name, request.now_ms)
                });
            let pruned = prune_expired_members(group, request.now_ms);
            let changed =
                upsert_group_member(group, &member_id, MemberRegistration::from_request(request));
            if pruned || changed || group.generation_id == 0 {
                group.generation_id += 1;
                group.protocol_type = request.protocol_type.to_string();
                group.protocol_name = request.protocol_name.to_string();
                for member in group.members.values_mut() {
                    member.generation_id = group.generation_id;
                    member.assignment = Vec::new();
                }
                group.assignments_ready = false;
                group.assignments_failed = false;
            }
            group.updated_at_unix_ms = request.now_ms;
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
                group.clone(),
            )
        };
        self.persist_group_state_snapshot(request.group_id, group_snapshot, request.now_ms)?;
        Ok(GroupJoinResult {
            generation_id,
            protocol_name: protocol_name_result,
            leader,
            member_id,
            members,
        })
    }

    pub fn sync_group(&mut self, request: SyncGroupStateRequest<'_>) -> Result<SyncGroupResult> {
        let (assignment, group_snapshot) = {
            let group =
                self.groups
                    .get_mut(request.group_id)
                    .ok_or(StoreError::StaleGeneration {
                        expected: 0,
                        actual: request.generation_id,
                    })?;
            let _ = prune_expired_members(group, request.now_ms);
            if !group.members.contains_key(request.member_id) {
                return Err(StoreError::UnknownMember {
                    group_id: request.group_id.to_string(),
                    member_id: request.member_id.to_string(),
                });
            }
            if request.generation_id < group.generation_id {
                return Err(StoreError::UnknownMember {
                    group_id: request.group_id.to_string(),
                    member_id: request.member_id.to_string(),
                });
            }
            ensure_generation(group, request.generation_id)?;
            if !request.assignments.is_empty() {
                if let Err(err) =
                    ensure_complete_assignments(group, request.group_id, request.assignments)
                {
                    group.assignments_ready = false;
                    group.assignments_failed = true;
                    return Err(err);
                }
                for (assigned_member, assignment) in request.assignments {
                    if let Some(member) = group.members.get_mut(assigned_member) {
                        member.assignment = assignment.clone();
                        member.updated_at_unix_ms = request.now_ms;
                    }
                }
                group.assignments_ready = true;
                group.assignments_failed = false;
            } else if group.leader_member_id.as_deref() == Some(request.member_id) {
                maybe_build_assignments(group, request.topics)?;
                group.assignments_ready = true;
                group.assignments_failed = false;
            } else {
                if group.assignments_failed {
                    return Err(StoreError::UnknownMember {
                        group_id: request.group_id.to_string(),
                        member_id: request.member_id.to_string(),
                    });
                }
                if !group.assignments_ready {
                    maybe_build_assignments(group, request.topics)?;
                    group.assignments_ready = true;
                    group.assignments_failed = false;
                }
                ensure_assignment_ready(group, request.group_id, request.member_id)?;
            }
            let assignment = group
                .members
                .get(request.member_id)
                .map(|member| member.assignment.clone())
                .ok_or_else(|| StoreError::UnknownMember {
                    group_id: request.group_id.to_string(),
                    member_id: request.member_id.to_string(),
                })?;
            group.updated_at_unix_ms = request.now_ms;
            (assignment, group.clone())
        };
        self.persist_group_state_snapshot(request.group_id, group_snapshot, request.now_ms)?;
        Ok(SyncGroupResult {
            protocol_name: request.protocol_name.to_string(),
            assignment,
        })
    }

    pub fn heartbeat(
        &mut self,
        group_id: &str,
        member_id: &str,
        generation_id: i32,
        now_ms: i64,
    ) -> Result<()> {
        let group = self
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
        Ok(())
    }

    pub fn leave_group(&mut self, group_id: &str, member_id: &str, now_ms: i64) -> Result<()> {
        let mut group_snapshot = None;
        if let Some(group) = self.groups.get_mut(group_id) {
            if group.members.remove(member_id).is_some() {
                group.generation_id += 1;
                group.leader_member_id = group.members.keys().next().cloned();
                for member in group.members.values_mut() {
                    member.generation_id = group.generation_id;
                    member.assignment = Vec::new();
                    member.updated_at_unix_ms = now_ms;
                }
                group.assignments_ready = false;
                group.assignments_failed = false;
                group.updated_at_unix_ms = now_ms;
                group_snapshot = Some(group.clone());
            }
        }
        if let Some(group) = group_snapshot {
            self.persist_group_state_snapshot(group_id, group, now_ms)?;
        }
        Ok(())
    }

    pub fn commit_offset(&mut self, request: OffsetCommitRequest<'_>) -> Result<()> {
        self.validate_offset_commit(request)?;
        let offset_key = OffsetKey::new(request.group_id, request.topic, request.partition);
        self.persist_offset_commit(request, offset_key)
    }

    pub fn validate_offset_commit(&mut self, request: OffsetCommitRequest<'_>) -> Result<()> {
        let group =
            self.groups
                .get_mut(request.group_id)
                .ok_or_else(|| StoreError::UnknownMember {
                    group_id: request.group_id.to_string(),
                    member_id: request.member_id.to_string(),
                })?;
        if !group.members.contains_key(request.member_id) {
            return Err(StoreError::UnknownMember {
                group_id: request.group_id.to_string(),
                member_id: request.member_id.to_string(),
            });
        }
        if request.generation_id > group.generation_id {
            return Err(StoreError::StaleGeneration {
                expected: group.generation_id,
                actual: request.generation_id,
            });
        }
        if let Some(member) = group.members.get_mut(request.member_id) {
            member.updated_at_unix_ms = request.now_ms;
        }
        Ok(())
    }

    pub fn fetch_offset(&self, group_id: &str, topic: &str, partition: i32) -> Option<i64> {
        self.offsets
            .get(&OffsetKey::new(group_id, topic, partition))
            .copied()
    }

    pub fn stage_transactional_offset_commit(
        &mut self,
        request: TransactionalOffsetCommitRequest<'_>,
    ) -> Result<()> {
        let offset_topic_partition =
            super::consumer_offsets::partition_for_group_id(request.group_id);
        let record_offset = self.next_record_offset(offset_topic_partition);
        super::consumer_offsets::append_commit(
            &self.logs,
            record_offset,
            super::consumer_offsets::OffsetCommitRecord {
                producer_id: request.producer_id,
                producer_epoch: request.producer_epoch,
                group_id: request.group_id,
                offset_topic_partition,
                topic: request.topic,
                partition: request.partition,
                next_offset: request.next_offset,
                now_ms: request.now_ms,
            },
        )?;
        self.advance_record_offset(offset_topic_partition, record_offset);
        let commit = TransactionalOffsetCommit {
            producer_id: request.producer_id,
            producer_epoch: request.producer_epoch,
            offset_topic_partition,
            group_id: request.group_id.to_string(),
            topic: request.topic.to_string(),
            partition: request.partition,
            next_offset: request.next_offset,
        };
        let entry = self
            .pending_transactional_offsets
            .entry((request.producer_id, offset_topic_partition))
            .or_default();
        if let Some(existing) = entry.iter_mut().find(|existing| {
            existing.group_id == commit.group_id
                && existing.topic == commit.topic
                && existing.partition == commit.partition
        }) {
            *existing = commit;
        } else {
            entry.push(commit);
        }
        Ok(())
    }

    pub fn complete_transactional_offset_commits(
        &mut self,
        producer_id: i64,
        producer_epoch: i16,
        committed: bool,
        now_ms: i64,
    ) -> Result<()> {
        let pending = self
            .pending_transactional_offsets
            .iter()
            .filter(|((pending_producer_id, _), _)| *pending_producer_id == producer_id)
            .flat_map(|(_, commits)| commits.iter().cloned())
            .collect::<Vec<_>>();
        if pending.is_empty() {
            return Ok(());
        }
        if pending
            .iter()
            .any(|commit| commit.producer_epoch != producer_epoch)
        {
            return Err(StoreError::StaleProducerEpoch {
                producer_id,
                expected: pending
                    .iter()
                    .map(|commit| commit.producer_epoch)
                    .max()
                    .unwrap_or(producer_epoch),
                actual: producer_epoch,
            });
        }
        let mut partitions = pending
            .iter()
            .map(|commit| commit.offset_topic_partition)
            .collect::<Vec<_>>();
        partitions.sort_unstable();
        partitions.dedup();
        for &offset_topic_partition in &partitions {
            let record_offset = self.next_record_offset(offset_topic_partition);
            super::consumer_offsets::append_transaction_marker(
                &self.logs,
                record_offset,
                offset_topic_partition,
                producer_id,
                producer_epoch,
                committed,
                now_ms,
            )?;
            self.advance_record_offset(offset_topic_partition, record_offset);
        }
        for offset_topic_partition in partitions {
            self.pending_transactional_offsets
                .remove(&(producer_id, offset_topic_partition));
        }
        if committed {
            for commit in pending {
                self.offsets.insert(
                    OffsetKey::new(&commit.group_id, &commit.topic, commit.partition),
                    commit.next_offset,
                );
            }
        }
        Ok(())
    }

    pub fn transactional_offset_commits(&self, producer_id: i64) -> Vec<TransactionalOffsetCommit> {
        self.pending_transactional_offsets
            .iter()
            .filter(|((pending_producer_id, _), _)| *pending_producer_id == producer_id)
            .flat_map(|(_, commits)| commits.iter().cloned())
            .collect()
    }

    #[cfg(test)]
    pub fn debug_group_state(&self, group_id: &str) -> Option<GroupState> {
        self.groups.get(group_id).cloned()
    }

    pub fn group_count(&self) -> usize {
        self.groups.len()
    }

    pub fn committed_offset_count(&self) -> usize {
        self.offsets.len()
    }
}
