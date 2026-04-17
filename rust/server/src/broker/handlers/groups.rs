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
use crate::store::{GroupJoinRequest, OffsetCommitRequest as StoreOffsetCommitRequest, StoreError};

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

pub async fn handle_join_group(
    broker: &KafkaBroker,
    request: JoinGroupRequest,
) -> Result<JoinGroupResponse> {
    let selected = request
        .protocols
        .first()
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
    let mut topics = Vec::new();
    let now = chrono::Utc::now().timestamp_millis();
    for topic in request.topics {
        let mut partitions = Vec::new();
        for partition in topic.partitions {
            let error_code = match broker.store().commit_offset(StoreOffsetCommitRequest {
                group_id: request.group_id.as_ref(),
                member_id: request.member_id.as_ref(),
                generation_id: request.generation_id_or_member_epoch,
                topic: topic.name.as_ref(),
                partition: partition.partition_index,
                next_offset: partition.committed_offset,
                now_ms: now,
            }) {
                Ok(()) => 0,
                Err(StoreError::UnknownTopicOrPartition { .. }) => UNKNOWN_TOPIC_OR_PARTITION,
                Err(StoreError::UnknownMember { .. }) => 25,
                Err(StoreError::StaleGeneration { .. }) => 22,
                Err(err) => return Err(err.into()),
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use kafka_protocol::messages::join_group_request::JoinGroupRequestProtocol;
    use kafka_protocol::messages::leave_group_request::MemberIdentity;
    use kafka_protocol::messages::offset_commit_request::{
        OffsetCommitRequestPartition, OffsetCommitRequestTopic,
    };
    use kafka_protocol::messages::offset_fetch_request::OffsetFetchRequestTopic;
    use kafka_protocol::messages::{
        FindCoordinatorRequest, GroupId, JoinGroupRequest, LeaveGroupRequest, OffsetCommitRequest,
        OffsetFetchRequest, SyncGroupRequest,
    };
    use tempfile::tempdir;

    use crate::config::Config;
    use crate::store::FileStore;

    use super::*;

    #[tokio::test]
    async fn stale_sync_group_returns_unknown_member_instead_of_erroring_connection() {
        let broker = test_broker();
        let joined = handle_join_group(
            &broker,
            JoinGroupRequest::default()
                .with_group_id(GroupId(StrBytes::from("group-a".to_string())))
                .with_member_id(StrBytes::from("member-a".to_string()))
                .with_protocol_type(StrBytes::from("consumer".to_string()))
                .with_session_timeout_ms(5_000)
                .with_rebalance_timeout_ms(5_000)
                .with_protocols(vec![
                    JoinGroupRequestProtocol::default()
                        .with_name(StrBytes::from("range".to_string()))
                        .with_metadata(Bytes::new()),
                ]),
        )
        .await
        .unwrap();

        let response = handle_sync_group(
            &broker,
            SyncGroupRequest::default()
                .with_group_id(GroupId(StrBytes::from("group-a".to_string())))
                .with_member_id(StrBytes::from("member-a".to_string()))
                .with_generation_id(joined.generation_id - 1)
                .with_protocol_name(Some(StrBytes::from("range".to_string()))),
        )
        .await
        .unwrap();

        assert_eq!(response.error_code, 25);
    }

    #[tokio::test]
    async fn stale_offset_commit_from_current_member_is_accepted() {
        let broker = test_broker();
        broker.store().ensure_topic("topic-a", 1, 0).unwrap();
        let joined = handle_join_group(
            &broker,
            JoinGroupRequest::default()
                .with_group_id(GroupId(StrBytes::from("group-b".to_string())))
                .with_member_id(StrBytes::from("member-a".to_string()))
                .with_protocol_type(StrBytes::from("consumer".to_string()))
                .with_session_timeout_ms(5_000)
                .with_rebalance_timeout_ms(5_000)
                .with_protocols(vec![
                    JoinGroupRequestProtocol::default()
                        .with_name(StrBytes::from("range".to_string()))
                        .with_metadata(Bytes::new()),
                ]),
        )
        .await
        .unwrap();

        let current = handle_offset_commit(
            &broker,
            OffsetCommitRequest::default()
                .with_group_id(GroupId(StrBytes::from("group-b".to_string())))
                .with_member_id(StrBytes::from("member-a".to_string()))
                .with_generation_id_or_member_epoch(joined.generation_id)
                .with_topics(vec![
                    OffsetCommitRequestTopic::default()
                        .with_name(TopicName(StrBytes::from("topic-a".to_string())))
                        .with_partitions(vec![
                            OffsetCommitRequestPartition::default()
                                .with_partition_index(0)
                                .with_committed_offset(1),
                        ]),
                ]),
        )
        .await
        .unwrap();
        assert_eq!(current.topics[0].partitions[0].error_code, 0);

        let response = handle_offset_commit(
            &broker,
            OffsetCommitRequest::default()
                .with_group_id(GroupId(StrBytes::from("group-b".to_string())))
                .with_member_id(StrBytes::from("member-a".to_string()))
                .with_generation_id_or_member_epoch(joined.generation_id - 1)
                .with_topics(vec![
                    OffsetCommitRequestTopic::default()
                        .with_name(TopicName(StrBytes::from("topic-a".to_string())))
                        .with_partitions(vec![
                            OffsetCommitRequestPartition::default()
                                .with_partition_index(0)
                                .with_committed_offset(1),
                        ]),
                ]),
        )
        .await
        .unwrap();

        assert_eq!(response.topics[0].partitions[0].error_code, 0);
    }

    #[test]
    fn find_coordinator_v4_falls_back_to_single_key() {
        let broker = test_broker();

        let response = handle_find_coordinator(
            &broker,
            FindCoordinatorRequest::default().with_key(StrBytes::from("group-a".to_string())),
            4,
        );

        assert_eq!(response.coordinators.len(), 1);
        assert_eq!(response.coordinators[0].key.to_string(), "group-a");
    }

    #[test]
    fn find_coordinator_v4_uses_explicit_keys_and_v3_uses_legacy_fields() {
        let broker = test_broker();

        let v4 = handle_find_coordinator(
            &broker,
            FindCoordinatorRequest::default()
                .with_key(StrBytes::from("ignored".to_string()))
                .with_coordinator_keys(vec![
                    StrBytes::from("group-a".to_string()),
                    StrBytes::from("group-b".to_string()),
                ]),
            4,
        );
        assert_eq!(v4.coordinators.len(), 2);
        assert_eq!(v4.coordinators[0].key.to_string(), "group-a");
        assert_eq!(v4.coordinators[1].key.to_string(), "group-b");

        let v3 = handle_find_coordinator(
            &broker,
            FindCoordinatorRequest::default()
                .with_key(StrBytes::from("group-c".to_string()))
                .with_coordinator_keys(vec![StrBytes::from("group-d".to_string())]),
            3,
        );
        assert_eq!(v3.error_code, 0);
        assert_eq!(v3.node_id, BrokerId(1));
        assert_eq!(v3.host.to_string(), "127.0.0.1");
        assert_eq!(v3.port, 9092);
        assert!(v3.coordinators.is_empty());
    }

    #[tokio::test]
    async fn join_group_without_protocols_defaults_to_range() {
        let broker = test_broker();

        let response = handle_join_group(
            &broker,
            JoinGroupRequest::default()
                .with_group_id(GroupId(StrBytes::from("group-defaults".to_string())))
                .with_member_id(StrBytes::from("member-a".to_string()))
                .with_protocol_type(StrBytes::from("consumer".to_string()))
                .with_session_timeout_ms(5_000)
                .with_rebalance_timeout_ms(5_000),
        )
        .await
        .unwrap();

        assert_eq!(response.error_code, 0);
        assert_eq!(
            response.protocol_name.as_ref().unwrap().to_string(),
            "range"
        );
        assert_eq!(response.member_id.to_string(), "member-a");
        assert_eq!(response.members.len(), 1);
        assert!(response.members[0].metadata.is_empty());
    }

    #[tokio::test]
    async fn sync_group_missing_group_defaults_protocol_name_and_returns_stale_generation() {
        let broker = test_broker();

        let response = handle_sync_group(
            &broker,
            SyncGroupRequest::default()
                .with_group_id(GroupId(StrBytes::from("missing".to_string())))
                .with_member_id(StrBytes::from("member-a".to_string()))
                .with_generation_id(1),
        )
        .await
        .unwrap();

        assert_eq!(response.error_code, 22);
        assert_eq!(
            response.protocol_name.as_ref().unwrap().to_string(),
            "range"
        );
        assert!(response.assignment.is_empty());
    }

    #[tokio::test]
    async fn leave_group_uses_explicit_members_when_present() {
        let broker = test_broker();

        for member_id in ["member-a", "member-b"] {
            let _ = handle_join_group(
                &broker,
                JoinGroupRequest::default()
                    .with_group_id(GroupId(StrBytes::from("group-c".to_string())))
                    .with_member_id(StrBytes::from(member_id.to_string()))
                    .with_protocol_type(StrBytes::from("consumer".to_string()))
                    .with_session_timeout_ms(5_000)
                    .with_rebalance_timeout_ms(5_000)
                    .with_protocols(vec![
                        JoinGroupRequestProtocol::default()
                            .with_name(StrBytes::from("range".to_string()))
                            .with_metadata(Bytes::new()),
                    ]),
            )
            .await
            .unwrap();
        }

        let response = handle_leave_group(
            &broker,
            LeaveGroupRequest::default()
                .with_group_id(GroupId(StrBytes::from("group-c".to_string())))
                .with_member_id(StrBytes::from("member-a".to_string()))
                .with_members(vec![
                    MemberIdentity::default()
                        .with_member_id(StrBytes::from("member-a".to_string())),
                    MemberIdentity::default()
                        .with_member_id(StrBytes::from("member-b".to_string())),
                ]),
        )
        .await;

        assert_eq!(response.error_code, 0);
        let remaining = handle_sync_group(
            &broker,
            SyncGroupRequest::default()
                .with_group_id(GroupId(StrBytes::from("group-c".to_string())))
                .with_member_id(StrBytes::from("member-a".to_string()))
                .with_generation_id(1)
                .with_protocol_name(Some(StrBytes::from("range".to_string()))),
        )
        .await
        .unwrap();
        assert_eq!(remaining.error_code, 25);
    }

    #[tokio::test]
    async fn leave_group_without_members_falls_back_to_request_member_id() {
        let broker = test_broker();

        let joined = handle_join_group(
            &broker,
            JoinGroupRequest::default()
                .with_group_id(GroupId(StrBytes::from("group-fallback-leave".to_string())))
                .with_member_id(StrBytes::from("member-a".to_string()))
                .with_protocol_type(StrBytes::from("consumer".to_string()))
                .with_session_timeout_ms(5_000)
                .with_rebalance_timeout_ms(5_000)
                .with_protocols(vec![
                    JoinGroupRequestProtocol::default()
                        .with_name(StrBytes::from("range".to_string()))
                        .with_metadata(Bytes::new()),
                ]),
        )
        .await
        .unwrap();

        let response = handle_leave_group(
            &broker,
            LeaveGroupRequest::default()
                .with_group_id(GroupId(StrBytes::from("group-fallback-leave".to_string())))
                .with_member_id(StrBytes::from("member-a".to_string())),
        )
        .await;

        assert_eq!(response.error_code, 0);
        let remaining = handle_sync_group(
            &broker,
            SyncGroupRequest::default()
                .with_group_id(GroupId(StrBytes::from("group-fallback-leave".to_string())))
                .with_member_id(StrBytes::from("member-a".to_string()))
                .with_generation_id(joined.generation_id)
                .with_protocol_name(Some(StrBytes::from("range".to_string()))),
        )
        .await
        .unwrap();
        assert_eq!(remaining.error_code, 25);
    }

    #[tokio::test]
    async fn offset_fetch_without_topics_returns_empty_topics() {
        let broker = test_broker();

        let response = handle_offset_fetch(
            &broker,
            OffsetFetchRequest::default()
                .with_group_id(GroupId(StrBytes::from("group-d".to_string())))
                .with_topics(None),
        )
        .await
        .unwrap();

        assert!(response.topics.is_empty());
        assert_eq!(response.error_code, 0);
    }

    #[tokio::test]
    async fn offset_fetch_reports_unknown_topic_or_partition() {
        let broker = test_broker();

        let response = handle_offset_fetch(
            &broker,
            OffsetFetchRequest::default()
                .with_group_id(GroupId(StrBytes::from("group-e".to_string())))
                .with_topics(Some(vec![
                    OffsetFetchRequestTopic::default()
                        .with_name(TopicName(StrBytes::from("missing".to_string())))
                        .with_partition_indexes(vec![0]),
                ])),
        )
        .await
        .unwrap();

        assert_eq!(response.topics[0].partitions[0].committed_offset, -1);
        assert_eq!(
            response.topics[0].partitions[0].error_code,
            UNKNOWN_TOPIC_OR_PARTITION
        );
    }

    fn test_broker() -> KafkaBroker {
        let dir = tempdir().unwrap().keep();
        let config = Config::single_node(dir.join("data"), 9092, 1);
        let store = Arc::new(FileStore::open(&config.storage.data_dir).unwrap());
        KafkaBroker::new(config, store).unwrap()
    }
}
