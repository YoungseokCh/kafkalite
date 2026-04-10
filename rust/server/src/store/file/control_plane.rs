use std::collections::{BTreeMap, BTreeSet};

use bytes::Bytes;

use crate::store::{GroupJoinResult, GroupMember, Result, StoreError, SyncGroupResult};

use super::state::{GroupMemberState, GroupState, StateJournal};

pub struct ControlPlaneState {
    groups: BTreeMap<String, GroupState>,
    offsets: BTreeMap<String, i64>,
    journal: StateJournal,
}

impl ControlPlaneState {
    pub fn new(
        groups: BTreeMap<String, GroupState>,
        offsets: BTreeMap<String, i64>,
        journal: StateJournal,
    ) -> Self {
        Self {
            groups,
            offsets,
            journal,
        }
    }

    pub fn join_group(
        &mut self,
        group_id: &str,
        member_id: Option<&str>,
        protocol_type: &str,
        protocol_name: &str,
        metadata: &[u8],
        session_timeout_ms: i32,
        rebalance_timeout_ms: i32,
        now_ms: i64,
    ) -> Result<GroupJoinResult> {
        let member_id = member_id
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned)
            .unwrap_or_else(|| format!("{group_id}-member-{now_ms}"));
        let (generation_id, protocol_name_result, leader, members) = {
            let group = self
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
        self.persist_groups()?;
        Ok(GroupJoinResult {
            generation_id,
            protocol_name: protocol_name_result,
            leader,
            member_id,
            members,
        })
    }

    pub fn sync_group(
        &mut self,
        group_id: &str,
        member_id: &str,
        generation_id: i32,
        protocol_name: &str,
        assignments: &[(String, Vec<u8>)],
        now_ms: i64,
    ) -> Result<SyncGroupResult> {
        let group = self
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
        self.persist_groups()?;
        Ok(SyncGroupResult {
            protocol_name: protocol_name.to_string(),
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
        self.persist_groups()
    }

    pub fn leave_group(&mut self, group_id: &str, member_id: &str, now_ms: i64) -> Result<()> {
        if let Some(group) = self.groups.get_mut(group_id) {
            if group.members.remove(member_id).is_some() {
                group.generation_id += 1;
                group.leader_member_id = group.members.keys().next().cloned();
                group.updated_at_unix_ms = now_ms;
            }
        }
        self.persist_groups()
    }

    pub fn commit_offset(
        &mut self,
        group_id: &str,
        member_id: &str,
        generation_id: i32,
        topic: &str,
        next_offset: i64,
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
        if generation_id > group.generation_id {
            return Err(StoreError::StaleGeneration {
                expected: group.generation_id,
                actual: generation_id,
            });
        }
        if let Some(member) = group.members.get_mut(member_id) {
            member.updated_at_unix_ms = now_ms;
        }
        self.offsets
            .insert(offset_key(group_id, topic), next_offset);
        self.persist_groups()?;
        self.persist_offsets()
    }

    pub fn fetch_offset(&self, group_id: &str, topic: &str) -> Option<i64> {
        self.offsets.get(&offset_key(group_id, topic)).copied()
    }

    fn persist_groups(&self) -> Result<()> {
        self.journal.append_groups(&self.groups)
    }

    fn persist_offsets(&self) -> Result<()> {
        self.journal.append_offsets(&self.offsets)
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
                .with_partitions(vec![super::super::DEFAULT_PARTITION])
        })
        .collect();
    let assignment = ConsumerProtocolAssignment::default().with_assigned_partitions(partitions);
    let mut bytes = BytesMut::new();
    assignment
        .encode(&mut bytes, 3)
        .map_err(|err| StoreError::Protocol(err.to_string()))?;
    Ok(bytes.to_vec())
}

fn offset_key(group_id: &str, topic: &str) -> String {
    format!("{group_id}:{topic}:0")
}
