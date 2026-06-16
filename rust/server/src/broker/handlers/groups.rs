use anyhow::Result;
use bytes::Bytes;
use kafka_protocol::messages::find_coordinator_response::Coordinator;
use kafka_protocol::messages::join_group_response::JoinGroupResponseMember;
use kafka_protocol::messages::offset_commit_response::{
    OffsetCommitResponsePartition, OffsetCommitResponseTopic,
};
use kafka_protocol::messages::offset_fetch_response::{
    OffsetFetchResponseGroup, OffsetFetchResponsePartition, OffsetFetchResponsePartitions,
    OffsetFetchResponseTopic, OffsetFetchResponseTopics,
};
use kafka_protocol::messages::txn_offset_commit_response::{
    TxnOffsetCommitResponsePartition, TxnOffsetCommitResponseTopic,
};
use kafka_protocol::messages::{
    BrokerId, FindCoordinatorRequest, FindCoordinatorResponse, HeartbeatRequest, HeartbeatResponse,
    JoinGroupRequest, JoinGroupResponse, LeaveGroupRequest, LeaveGroupResponse,
    OffsetCommitRequest, OffsetCommitResponse, OffsetFetchRequest, OffsetFetchResponse,
    SyncGroupRequest, SyncGroupResponse, TopicName, TxnOffsetCommitRequest,
    TxnOffsetCommitResponse,
};
use kafka_protocol::protocol::StrBytes;

use super::super::KafkaBroker;
use super::super::server::StagedOffsetCommit;
use super::error_codes::{
    INVALID_PRODUCER_EPOCH, INVALID_PRODUCER_ID_MAPPING, UNKNOWN_TOPIC_OR_PARTITION,
};
use crate::store::TransactionStatus;
use crate::store::{
    GroupJoinRequest, OffsetCommitRequest as StoreOffsetCommitRequest, StoreError,
    TransactionalOffsetCommitRequest as StoreTransactionalOffsetCommitRequest,
};

pub fn handle_find_coordinator(
    broker: &KafkaBroker,
    request: FindCoordinatorRequest,
    api_version: i16,
) -> FindCoordinatorResponse {
    let response = FindCoordinatorResponse::default().with_throttle_time_ms(0);
    let node_id = BrokerId(broker.config().broker.broker_id);
    let host = StrBytes::from(broker.config().broker.advertised_host.clone());
    let port = i32::from(broker.config().broker.advertised_port);

    if api_version >= 4 {
        let keys = if request.coordinator_keys.is_empty() {
            vec![request.key]
        } else {
            request.coordinator_keys
        };
        let coordinators = keys
            .into_iter()
            .map(|key| {
                Coordinator::default()
                    .with_key(key)
                    .with_node_id(node_id)
                    .with_host(host.clone())
                    .with_port(port)
                    .with_error_code(0)
                    .with_error_message(None)
            })
            .collect();
        return response.with_coordinators(coordinators);
    }

    response
        .with_error_code(0)
        .with_node_id(node_id)
        .with_host(host)
        .with_port(port)
}

pub async fn handle_join_group(
    broker: &KafkaBroker,
    request: JoinGroupRequest,
) -> Result<JoinGroupResponse> {
    let selected = request
        .protocols
        .iter()
        .find(|protocol| protocol.name.to_string() == "range")
        .or_else(|| request.protocols.first())
        .map(|protocol| {
            (
                protocol.name.to_string(),
                protocol.metadata.clone().to_vec(),
            )
        })
        .unwrap_or_else(|| ("range".to_string(), Vec::new()));
    let result = broker.store().join_group(GroupJoinRequest {
        group_id: request.group_id.as_ref(),
        member_id: Some(request.member_id.as_ref()),
        protocol_type: request.protocol_type.as_ref(),
        protocol_name: &selected.0,
        metadata: &selected.1,
        session_timeout_ms: request.session_timeout_ms,
        rebalance_timeout_ms: request.rebalance_timeout_ms,
        now_ms: chrono::Utc::now().timestamp_millis(),
    })?;
    let members = result
        .members
        .into_iter()
        .map(|member| {
            JoinGroupResponseMember::default()
                .with_member_id(StrBytes::from(member.member_id))
                .with_metadata(Bytes::from(member.metadata))
        })
        .collect();

    Ok(JoinGroupResponse::default()
        .with_throttle_time_ms(0)
        .with_error_code(0)
        .with_generation_id(result.generation_id)
        .with_protocol_name(Some(StrBytes::from(result.protocol_name)))
        .with_leader(StrBytes::from(result.leader))
        .with_member_id(StrBytes::from(result.member_id))
        .with_members(members))
}

pub async fn handle_sync_group(
    broker: &KafkaBroker,
    request: SyncGroupRequest,
) -> Result<SyncGroupResponse> {
    let assignments = request
        .assignments
        .into_iter()
        .map(|assignment| {
            (
                assignment.member_id.to_string(),
                assignment.assignment.to_vec(),
            )
        })
        .collect::<Vec<_>>();
    let protocol_name = request
        .protocol_name
        .as_ref()
        .map(|value| value.as_ref())
        .unwrap_or("range");
    let response = match broker.store().sync_group(
        request.group_id.as_ref(),
        request.member_id.as_ref(),
        request.generation_id,
        protocol_name,
        &assignments,
        chrono::Utc::now().timestamp_millis(),
    ) {
        Ok(result) => SyncGroupResponse::default()
            .with_throttle_time_ms(0)
            .with_error_code(0)
            .with_protocol_name(Some(StrBytes::from(result.protocol_name)))
            .with_assignment(Bytes::from(result.assignment)),
        Err(StoreError::UnknownMember { .. }) => SyncGroupResponse::default()
            .with_throttle_time_ms(0)
            .with_error_code(25)
            .with_protocol_name(Some(StrBytes::from(protocol_name.to_string())))
            .with_assignment(Bytes::new()),
        Err(StoreError::StaleGeneration { .. }) => SyncGroupResponse::default()
            .with_throttle_time_ms(0)
            .with_error_code(22)
            .with_protocol_name(Some(StrBytes::from(protocol_name.to_string())))
            .with_assignment(Bytes::new()),
        Err(err) => return Err(err.into()),
    };
    Ok(response)
}

pub async fn handle_heartbeat(
    broker: &KafkaBroker,
    request: HeartbeatRequest,
) -> HeartbeatResponse {
    let error_code = match broker.store().heartbeat(
        request.group_id.as_ref(),
        request.member_id.as_ref(),
        request.generation_id,
        chrono::Utc::now().timestamp_millis(),
    ) {
        Ok(()) => 0,
        Err(StoreError::UnknownMember { .. }) => 25,
        Err(StoreError::StaleGeneration { .. }) => 22,
        Err(_) => -1,
    };
    HeartbeatResponse::default()
        .with_throttle_time_ms(0)
        .with_error_code(error_code)
}

pub async fn handle_leave_group(
    broker: &KafkaBroker,
    request: LeaveGroupRequest,
) -> LeaveGroupResponse {
    let members = if request.members.is_empty() {
        vec![request.member_id.to_string()]
    } else {
        request
            .members
            .into_iter()
            .map(|member| member.member_id.to_string())
            .collect()
    };
    for member in members {
        let _ = broker.store().leave_group(
            request.group_id.as_ref(),
            &member,
            chrono::Utc::now().timestamp_millis(),
        );
    }
    LeaveGroupResponse::default()
        .with_throttle_time_ms(0)
        .with_error_code(0)
}

pub async fn handle_offset_commit(
    broker: &KafkaBroker,
    request: OffsetCommitRequest,
) -> Result<OffsetCommitResponse> {
    let now = chrono::Utc::now().timestamp_millis();
    let topics = commit_offsets(
        broker,
        request.group_id.as_ref(),
        request.member_id.as_ref(),
        request.generation_id_or_member_epoch,
        request.topics.into_iter().map(|topic| {
            (
                topic.name.to_string(),
                topic
                    .partitions
                    .into_iter()
                    .map(|partition| (partition.partition_index, partition.committed_offset))
                    .collect::<Vec<_>>(),
            )
        }),
        now,
    )?;
    Ok(OffsetCommitResponse::default()
        .with_throttle_time_ms(0)
        .with_topics(topics))
}

pub async fn handle_txn_offset_commit(
    broker: &KafkaBroker,
    request: TxnOffsetCommitRequest,
) -> Result<TxnOffsetCommitResponse> {
    let now = chrono::Utc::now().timestamp_millis();
    broker.expire_timed_out_transactions(now)?;
    let validated_session = validated_transaction_session(
        broker,
        request.transactional_id.as_ref(),
        request.producer_id.0,
        request.producer_epoch,
    );
    let consumer_offsets_partition =
        crate::store::consumer_offsets_partition_for_group_id(request.group_id.as_ref());
    let session_error = validated_session
        .as_ref()
        .and_then(|session| {
            if !matches!(
                session.status,
                TransactionStatus::Empty | TransactionStatus::Ongoing
            ) {
                Some(crate::broker::handlers::error_codes::INVALID_TXN_STATE)
            } else {
                (!broker.transaction_contains_partition(
                    request.transactional_id.as_ref(),
                    "__consumer_offsets",
                    consumer_offsets_partition,
                ))
                .then_some(crate::broker::handlers::error_codes::INVALID_TXN_STATE)
            }
        })
        .or_else(|| {
            if validated_session.is_none() {
                transaction_session_error(
                    broker,
                    request.transactional_id.as_ref(),
                    request.producer_id.0,
                    request.producer_epoch,
                )
            } else {
                None
            }
        });
    let topics = if let Some(error_code) = session_error {
        request
            .topics
            .into_iter()
            .map(|topic| {
                let partitions = topic
                    .partitions
                    .into_iter()
                    .map(|partition| {
                        TxnOffsetCommitResponsePartition::default()
                            .with_partition_index(partition.partition_index)
                            .with_error_code(error_code)
                    })
                    .collect();
                TxnOffsetCommitResponseTopic::default()
                    .with_name(TopicName(StrBytes::from(topic.name.to_string())))
                    .with_partitions(partitions)
            })
            .collect()
    } else {
        stage_txn_offset_commits(broker, &request, now)?
    };
    Ok(TxnOffsetCommitResponse::default()
        .with_throttle_time_ms(0)
        .with_topics(topics))
}

fn stage_txn_offset_commits(
    broker: &KafkaBroker,
    request: &TxnOffsetCommitRequest,
    now_ms: i64,
) -> Result<Vec<TxnOffsetCommitResponseTopic>> {
    let mut topics = Vec::new();
    for topic in &request.topics {
        let topic_name = topic.name.to_string();
        let mut partitions = Vec::new();
        for partition in &topic.partitions {
            let store_request = StoreOffsetCommitRequest {
                group_id: request.group_id.as_ref(),
                member_id: request.member_id.as_ref(),
                generation_id: request.generation_id,
                topic: &topic_name,
                partition: partition.partition_index,
                next_offset: partition.committed_offset,
                now_ms,
            };
            let error_code = match broker.store().validate_offset_commit(store_request) {
                Ok(()) => {
                    let staged_commit = StagedOffsetCommit {
                        group_id: request.group_id.to_string(),
                        topic: topic_name.clone(),
                        partition: partition.partition_index,
                        next_offset: partition.committed_offset,
                    };
                    broker.stage_transaction_offset_commit(
                        request.transactional_id.as_ref(),
                        staged_commit.clone(),
                        now_ms,
                    )?;
                    if let Err(err) = broker.store().stage_transactional_offset_commit(
                        StoreTransactionalOffsetCommitRequest {
                            producer_id: request.producer_id.0,
                            producer_epoch: request.producer_epoch,
                            group_id: request.group_id.as_ref(),
                            topic: &topic_name,
                            partition: partition.partition_index,
                            next_offset: partition.committed_offset,
                            now_ms,
                        },
                    ) {
                        broker.remove_transaction_offset_commit(
                            request.transactional_id.as_ref(),
                            &staged_commit,
                            now_ms,
                        )?;
                        return Err(err.into());
                    }
                    0
                }
                Err(StoreError::UnknownTopicOrPartition { .. }) => UNKNOWN_TOPIC_OR_PARTITION,
                Err(StoreError::UnknownMember { .. }) => 25,
                Err(StoreError::StaleGeneration { .. }) => 22,
                Err(err) => return Err(err.into()),
            };
            partitions.push(
                TxnOffsetCommitResponsePartition::default()
                    .with_partition_index(partition.partition_index)
                    .with_error_code(error_code),
            );
        }
        topics.push(
            TxnOffsetCommitResponseTopic::default()
                .with_name(TopicName(StrBytes::from(topic_name)))
                .with_partitions(partitions),
        );
    }
    Ok(topics)
}

pub async fn handle_offset_fetch(
    broker: &KafkaBroker,
    request: OffsetFetchRequest,
    api_version: i16,
) -> Result<OffsetFetchResponse> {
    let mut topics = Vec::new();
    let mut group_topics = Vec::new();
    if let Some(request_topics) = request.topics {
        for topic in request_topics {
            let mut partitions = Vec::new();
            let mut group_partitions = Vec::new();
            for partition in topic.partition_indexes {
                let (offset, error_code) = match broker.store().fetch_offset(
                    request.group_id.as_ref(),
                    topic.name.as_ref(),
                    partition,
                ) {
                    Ok(offset) => (offset.unwrap_or(-1), 0),
                    Err(StoreError::UnknownTopicOrPartition { .. }) => {
                        (-1, UNKNOWN_TOPIC_OR_PARTITION)
                    }
                    Err(err) => return Err(err.into()),
                };
                partitions.push(
                    OffsetFetchResponsePartition::default()
                        .with_partition_index(partition)
                        .with_committed_offset(offset)
                        .with_metadata(None)
                        .with_error_code(error_code),
                );
                group_partitions.push(
                    OffsetFetchResponsePartitions::default()
                        .with_partition_index(partition)
                        .with_committed_offset(offset)
                        .with_committed_leader_epoch(-1)
                        .with_metadata(None)
                        .with_error_code(error_code),
                );
            }
            topics.push(
                OffsetFetchResponseTopic::default()
                    .with_name(TopicName(StrBytes::from(topic.name.to_string())))
                    .with_partitions(partitions),
            );
            group_topics.push(
                OffsetFetchResponseTopics::default()
                    .with_name(TopicName(StrBytes::from(topic.name.to_string())))
                    .with_partitions(group_partitions),
            );
        }
    }
    let mut response = OffsetFetchResponse::default().with_throttle_time_ms(0);
    if api_version >= 8 {
        response = response.with_groups(vec![
            OffsetFetchResponseGroup::default()
                .with_group_id(request.group_id)
                .with_topics(group_topics)
                .with_error_code(0),
        ]);
    } else {
        response = response.with_topics(topics).with_error_code(0);
    }
    Ok(response)
}

fn commit_offsets(
    broker: &KafkaBroker,
    group_id: &str,
    member_id: &str,
    generation_id: i32,
    topics: impl IntoIterator<Item = (String, Vec<(i32, i64)>)>,
    now_ms: i64,
) -> Result<Vec<OffsetCommitResponseTopic>> {
    let mut responses = Vec::new();
    for (topic_name, partitions) in topics {
        let mut partition_responses = Vec::new();
        for (partition_index, committed_offset) in partitions {
            let error_code = match broker.store().commit_offset(StoreOffsetCommitRequest {
                group_id,
                member_id,
                generation_id,
                topic: &topic_name,
                partition: partition_index,
                next_offset: committed_offset,
                now_ms,
            }) {
                Ok(()) => 0,
                Err(StoreError::UnknownTopicOrPartition { .. }) => UNKNOWN_TOPIC_OR_PARTITION,
                Err(StoreError::UnknownMember { .. }) => 25,
                Err(StoreError::StaleGeneration { .. }) => 22,
                Err(err) => return Err(err.into()),
            };
            partition_responses.push(
                OffsetCommitResponsePartition::default()
                    .with_partition_index(partition_index)
                    .with_error_code(error_code),
            );
        }
        responses.push(
            OffsetCommitResponseTopic::default()
                .with_name(TopicName(StrBytes::from(topic_name)))
                .with_partitions(partition_responses),
        );
    }
    Ok(responses)
}

fn transaction_session_error(
    broker: &KafkaBroker,
    transactional_id: &str,
    producer_id: i64,
    producer_epoch: i16,
) -> Option<i16> {
    let Some(session) = broker.transaction_session(transactional_id) else {
        return Some(INVALID_PRODUCER_ID_MAPPING);
    };
    if session.producer_id != producer_id {
        return Some(INVALID_PRODUCER_ID_MAPPING);
    }
    if session.producer_epoch != producer_epoch {
        return Some(INVALID_PRODUCER_EPOCH);
    }
    None
}

fn validated_transaction_session(
    broker: &KafkaBroker,
    transactional_id: &str,
    producer_id: i64,
    producer_epoch: i16,
) -> Option<crate::broker::server::TransactionSession> {
    let session = broker.transaction_session(transactional_id)?;
    if session.producer_id != producer_id || session.producer_epoch != producer_epoch {
        return None;
    }
    Some(session)
}

#[cfg(test)]
mod groups_tests;
