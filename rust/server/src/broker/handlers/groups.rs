use anyhow::Result;
use bytes::Bytes;
use kafka_protocol::messages::find_coordinator_response::Coordinator;
use kafka_protocol::messages::join_group_response::JoinGroupResponseMember;
use kafka_protocol::messages::offset_commit_response::{
    OffsetCommitResponsePartition, OffsetCommitResponseTopic,
};
use kafka_protocol::messages::offset_fetch_response::{
    OffsetFetchResponsePartition, OffsetFetchResponseTopic,
};
use kafka_protocol::messages::{
    BrokerId, FindCoordinatorRequest, FindCoordinatorResponse, HeartbeatRequest, HeartbeatResponse,
    JoinGroupRequest, JoinGroupResponse, LeaveGroupRequest, LeaveGroupResponse,
    OffsetCommitRequest, OffsetCommitResponse, OffsetFetchRequest, OffsetFetchResponse,
    SyncGroupRequest, SyncGroupResponse, TopicName,
};
use kafka_protocol::protocol::StrBytes;

use super::super::KafkaBroker;
use crate::store::StoreError;

const UNKNOWN_TOPIC_OR_PARTITION: i16 = 3;

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

pub async fn handle_join_group(broker: &KafkaBroker, request: JoinGroupRequest) -> Result<JoinGroupResponse> {
    let selected = request
        .protocols
        .first()
        .map(|protocol| (protocol.name.to_string(), protocol.metadata.clone().to_vec()))
        .unwrap_or_else(|| ("range".to_string(), Vec::new()));
    let result = broker.store().join_group(
        request.group_id.as_ref(),
        Some(request.member_id.as_ref()),
        request.protocol_type.as_ref(),
        &selected.0,
        &selected.1,
        request.session_timeout_ms,
        request.rebalance_timeout_ms,
        chrono::Utc::now().timestamp_millis(),
    )?;
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

pub async fn handle_sync_group(broker: &KafkaBroker, request: SyncGroupRequest) -> Result<SyncGroupResponse> {
    let assignments = request
        .assignments
        .into_iter()
        .map(|assignment| (assignment.member_id.to_string(), assignment.assignment.to_vec()))
        .collect::<Vec<_>>();
    let result = broker.store().sync_group(
        request.group_id.as_ref(),
        request.member_id.as_ref(),
        request.generation_id,
        request.protocol_name.as_ref().map(|value| value.as_ref()).unwrap_or("range"),
        &assignments,
        chrono::Utc::now().timestamp_millis(),
    )?;
    Ok(SyncGroupResponse::default()
        .with_throttle_time_ms(0)
        .with_error_code(0)
        .with_protocol_name(Some(StrBytes::from(result.protocol_name)))
        .with_assignment(Bytes::from(result.assignment)))
}

pub async fn handle_heartbeat(broker: &KafkaBroker, request: HeartbeatRequest) -> HeartbeatResponse {
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

pub async fn handle_leave_group(broker: &KafkaBroker, request: LeaveGroupRequest) -> LeaveGroupResponse {
    let members = if request.members.is_empty() {
        vec![request.member_id.to_string()]
    } else {
        request.members.into_iter().map(|member| member.member_id.to_string()).collect()
    };
    for member in members {
        let _ = broker
            .store()
            .leave_group(request.group_id.as_ref(), &member, chrono::Utc::now().timestamp_millis());
    }
    LeaveGroupResponse::default()
        .with_throttle_time_ms(0)
        .with_error_code(0)
}

pub async fn handle_offset_commit(
    broker: &KafkaBroker,
    request: OffsetCommitRequest,
) -> Result<OffsetCommitResponse> {
    let mut topics = Vec::new();
    let now = chrono::Utc::now().timestamp_millis();
    for topic in request.topics {
        let mut partitions = Vec::new();
        for partition in topic.partitions {
            let error_code = if partition.partition_index != 0 {
                UNKNOWN_TOPIC_OR_PARTITION
            } else {
                match broker.store().commit_offset(
                    request.group_id.as_ref(),
                    request.member_id.as_ref(),
                    request.generation_id_or_member_epoch,
                    topic.name.as_ref(),
                    partition.committed_offset,
                    now,
                ) {
                    Ok(()) => 0,
                    Err(StoreError::UnknownMember { .. }) => 25,
                    Err(StoreError::StaleGeneration { .. }) => 22,
                    Err(err) => return Err(err.into()),
                }
            };
            partitions.push(
                OffsetCommitResponsePartition::default()
                    .with_partition_index(partition.partition_index)
                    .with_error_code(error_code),
            );
        }
        topics.push(
            OffsetCommitResponseTopic::default()
                .with_name(TopicName(StrBytes::from(topic.name.to_string())))
                .with_partitions(partitions),
        );
    }
    Ok(OffsetCommitResponse::default()
        .with_throttle_time_ms(0)
        .with_topics(topics))
}

pub async fn handle_offset_fetch(
    broker: &KafkaBroker,
    request: OffsetFetchRequest,
) -> Result<OffsetFetchResponse> {
    let mut topics = Vec::new();
    if let Some(request_topics) = request.topics {
        for topic in request_topics {
            let mut partitions = Vec::new();
            for partition in topic.partition_indexes {
                let (offset, error_code) = if partition == 0 {
                    (
                        broker
                            .store()
                            .fetch_offset(request.group_id.as_ref(), topic.name.as_ref())?
                            .unwrap_or(-1),
                        0,
                    )
                } else {
                    (-1, UNKNOWN_TOPIC_OR_PARTITION)
                };
                partitions.push(
                    OffsetFetchResponsePartition::default()
                        .with_partition_index(partition)
                        .with_committed_offset(offset)
                        .with_metadata(None)
                        .with_error_code(error_code),
                );
            }
            topics.push(
                OffsetFetchResponseTopic::default()
                    .with_name(TopicName(StrBytes::from(topic.name.to_string())))
                    .with_partitions(partitions),
            );
        }
    }
    Ok(OffsetFetchResponse::default()
        .with_throttle_time_ms(0)
        .with_topics(topics)
        .with_error_code(0))
}
