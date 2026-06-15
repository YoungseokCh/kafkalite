use std::collections::{BTreeMap, BTreeSet};

use bytes::Bytes;

use crate::store::{Result, StoreError, TopicMetadata};

use super::GroupState;

pub(super) fn maybe_build_assignments(
    group: &mut GroupState,
    topics: &[TopicMetadata],
) -> Result<()> {
    let subscriptions = group
        .members
        .values()
        .filter_map(|member| {
            parse_topics(&member.subscription_metadata)
                .ok()
                .map(|topics| (member.member_id.clone(), topics))
        })
        .collect::<Vec<_>>();
    let topic_partitions = topics
        .iter()
        .map(|topic| {
            (
                topic.name.clone(),
                topic
                    .partitions
                    .iter()
                    .map(|partition| partition.partition)
                    .collect::<Vec<_>>(),
            )
        })
        .collect::<BTreeMap<_, _>>();
    let mut topic_subscribers: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for (member_id, topics) in subscriptions {
        for topic in topics.into_iter().collect::<BTreeSet<_>>() {
            topic_subscribers
                .entry(topic)
                .or_default()
                .push(member_id.clone());
        }
    }
    let mut assignments: BTreeMap<String, Vec<(String, i32)>> = BTreeMap::new();
    for (topic, mut subscribers) in topic_subscribers {
        if let Some(partitions) = topic_partitions.get(&topic) {
            subscribers.sort();
            let base = partitions.len() / subscribers.len();
            let remainder = partitions.len() % subscribers.len();
            let mut start = 0_usize;
            for (index, subscriber) in subscribers.iter().enumerate() {
                let size = base + usize::from(index < remainder);
                let end = start + size;
                for partition in &partitions[start..end] {
                    assignments
                        .entry(subscriber.clone())
                        .or_default()
                        .push((topic.clone(), *partition));
                }
                start = end;
            }
        }
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

pub(super) fn ensure_complete_assignments(
    group: &GroupState,
    group_id: &str,
    assignments: &[(String, Vec<u8>)],
) -> Result<()> {
    let mut assigned_members = BTreeSet::new();
    for (member_id, assignment) in assignments {
        if assignment.is_empty() || !group.members.contains_key(member_id) {
            return Err(StoreError::UnknownMember {
                group_id: group_id.to_string(),
                member_id: member_id.clone(),
            });
        }
        assigned_members.insert(member_id.as_str());
    }

    if group
        .members
        .keys()
        .any(|member_id| !assigned_members.contains(member_id.as_str()))
    {
        return Err(StoreError::UnknownMember {
            group_id: group_id.to_string(),
            member_id: "incomplete-assignment".to_string(),
        });
    }

    Ok(())
}

pub(super) fn ensure_assignment_ready(
    group: &GroupState,
    group_id: &str,
    member_id: &str,
) -> Result<()> {
    let assignment_ready = group
        .members
        .get(member_id)
        .map(|member| !member.assignment.is_empty())
        .unwrap_or(false);
    if assignment_ready {
        return Ok(());
    }
    Err(StoreError::UnknownMember {
        group_id: group_id.to_string(),
        member_id: member_id.to_string(),
    })
}

fn parse_topics(bytes: &[u8]) -> anyhow::Result<Vec<String>> {
    let subscription = decode_subscription(bytes)?;
    Ok(subscription
        .topics
        .into_iter()
        .map(|topic| topic.to_string())
        .collect())
}

fn encode_assignment(assignments: &[(String, i32)]) -> Result<Vec<u8>> {
    use bytes::BytesMut;
    use kafka_protocol::messages::consumer_protocol_assignment::TopicPartition;
    use kafka_protocol::messages::{ConsumerProtocolAssignment, TopicName};
    use kafka_protocol::protocol::{Encodable, StrBytes};

    const VERSION: i16 = 3;
    let mut by_topic: BTreeMap<String, Vec<i32>> = BTreeMap::new();
    for (topic, partition) in assignments {
        by_topic.entry(topic.clone()).or_default().push(*partition);
    }
    let partitions = by_topic
        .into_iter()
        .map(|(topic, mut partitions)| {
            partitions.sort_unstable();
            TopicPartition::default()
                .with_topic(TopicName(StrBytes::from(topic)))
                .with_partitions(partitions)
        })
        .collect();
    let assignment = ConsumerProtocolAssignment::default().with_assigned_partitions(partitions);
    let mut bytes = BytesMut::new();
    bytes.extend_from_slice(&VERSION.to_be_bytes());
    assignment
        .encode(&mut bytes, VERSION)
        .map_err(|err| StoreError::Protocol(err.to_string()))?;
    Ok(bytes.to_vec())
}

fn decode_subscription(
    bytes: &[u8],
) -> anyhow::Result<kafka_protocol::messages::ConsumerProtocolSubscription> {
    use bytes::Buf;
    use kafka_protocol::messages::ConsumerProtocolSubscription;
    use kafka_protocol::protocol::Decodable;

    let mut prefixed = Bytes::copy_from_slice(bytes);
    if prefixed.remaining() >= 2 {
        let version = prefixed.get_i16();
        if (0..=3).contains(&version) {
            if let Ok(subscription) = ConsumerProtocolSubscription::decode(&mut prefixed, version) {
                return Ok(subscription);
            }
        }
    }

    let mut legacy = Bytes::copy_from_slice(bytes);
    ConsumerProtocolSubscription::decode(&mut legacy, 3)
}
