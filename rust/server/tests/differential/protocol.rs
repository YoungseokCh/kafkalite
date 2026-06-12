use bytes::{Bytes, BytesMut};
use kafka_protocol::messages::consumer_protocol_assignment::TopicPartition as AssignmentTopicPartition;
use kafka_protocol::messages::join_group_request::JoinGroupRequestProtocol;
use kafka_protocol::messages::leave_group_request::MemberIdentity;
use kafka_protocol::messages::offset_commit_request::{
    OffsetCommitRequestPartition, OffsetCommitRequestTopic,
};
use kafka_protocol::messages::offset_fetch_request::OffsetFetchRequestTopic;
use kafka_protocol::messages::sync_group_request::SyncGroupRequestAssignment;
use kafka_protocol::messages::{
    ApiKey, ConsumerProtocolAssignment, ConsumerProtocolSubscription, FetchRequest, FetchResponse,
    GroupId, HeartbeatRequest, HeartbeatResponse, JoinGroupRequest, JoinGroupResponse,
    LeaveGroupRequest, LeaveGroupResponse, OffsetCommitRequest, OffsetCommitResponse,
    OffsetFetchRequest, OffsetFetchResponse, RequestHeader, ResponseHeader, SyncGroupRequest,
    SyncGroupResponse, TopicName,
};
use kafka_protocol::protocol::{Decodable, Encodable, StrBytes};
use std::time::Duration;

use kafkalite_server::protocol;

pub(super) fn join_group(
    bootstrap: &str,
    group_id: &str,
    member_id: Option<&str>,
    topic: &str,
    user_data: &[u8],
) -> JoinGroupResponse {
    join_group_with_timeout(bootstrap, group_id, member_id, topic, user_data, 5_000)
}

pub(super) fn complete_join_group(
    bootstrap: &str,
    group_id: &str,
    topic: &str,
    user_data: &[u8],
) -> JoinGroupResponse {
    const MEMBER_ID_REQUIRED: i16 = 79;

    let joined = join_group(bootstrap, group_id, None, topic, user_data);
    if joined.error_code == MEMBER_ID_REQUIRED {
        return join_group(
            bootstrap,
            group_id,
            Some(joined.member_id.as_ref()),
            topic,
            user_data,
        );
    }
    joined
}

pub(super) fn join_group_with_timeout(
    bootstrap: &str,
    group_id: &str,
    member_id: Option<&str>,
    topic: &str,
    user_data: &[u8],
    timeout_ms: i32,
) -> JoinGroupResponse {
    send_request::<JoinGroupRequest, JoinGroupResponse>(
        bootstrap,
        ApiKey::JoinGroup,
        protocol::JOIN_GROUP_VERSION,
        JoinGroupRequest::default()
            .with_group_id(GroupId(StrBytes::from(group_id.to_string())))
            .with_session_timeout_ms(timeout_ms)
            .with_rebalance_timeout_ms(timeout_ms)
            .with_member_id(StrBytes::from(member_id.unwrap_or("").to_string()))
            .with_protocol_type(StrBytes::from("consumer".to_string()))
            .with_protocols(vec![
                JoinGroupRequestProtocol::default()
                    .with_name(StrBytes::from("range".to_string()))
                    .with_metadata(Bytes::from(encode_subscription(topic, user_data))),
            ]),
    )
}

pub(super) fn heartbeat(
    bootstrap: &str,
    group_id: &str,
    generation_id: i32,
    member_id: &str,
) -> HeartbeatResponse {
    send_request::<HeartbeatRequest, HeartbeatResponse>(
        bootstrap,
        ApiKey::Heartbeat,
        protocol::HEARTBEAT_VERSION,
        HeartbeatRequest::default()
            .with_group_id(GroupId(StrBytes::from(group_id.to_string())))
            .with_generation_id(generation_id)
            .with_member_id(StrBytes::from(member_id.to_string())),
    )
}

pub(super) fn leave_group(bootstrap: &str, group_id: &str, member_id: &str) -> LeaveGroupResponse {
    send_request::<LeaveGroupRequest, LeaveGroupResponse>(
        bootstrap,
        ApiKey::LeaveGroup,
        protocol::LEAVE_GROUP_VERSION,
        LeaveGroupRequest::default()
            .with_group_id(GroupId(StrBytes::from(group_id.to_string())))
            .with_members(vec![
                MemberIdentity::default()
                    .with_member_id(StrBytes::from(member_id.to_string()))
                    .with_group_instance_id(None),
            ]),
    )
}

pub(super) fn sync_group(
    bootstrap: &str,
    group_id: &str,
    generation_id: i32,
    member_id: &str,
    leader_member_id: &str,
    assignments: &[(&str, Vec<u8>)],
) -> SyncGroupResponse {
    send_request::<SyncGroupRequest, SyncGroupResponse>(
        bootstrap,
        ApiKey::SyncGroup,
        protocol::SYNC_GROUP_VERSION,
        SyncGroupRequest::default()
            .with_group_id(GroupId(StrBytes::from(group_id.to_string())))
            .with_generation_id(generation_id)
            .with_member_id(StrBytes::from(member_id.to_string()))
            .with_protocol_type(Some(StrBytes::from("consumer".to_string())))
            .with_protocol_name(Some(StrBytes::from("range".to_string())))
            .with_assignments(if member_id == leader_member_id {
                assignments
                    .iter()
                    .map(|(member, assignment)| {
                        SyncGroupRequestAssignment::default()
                            .with_member_id(StrBytes::from((*member).to_string()))
                            .with_assignment(Bytes::from(assignment.clone()))
                    })
                    .collect()
            } else {
                vec![]
            }),
    )
}

pub(super) fn offset_commit(
    bootstrap: &str,
    group_id: &str,
    generation_id: i32,
    member_id: &str,
    topic: &str,
    partition: i32,
    next_offset: i64,
) -> OffsetCommitResponse {
    send_request::<OffsetCommitRequest, OffsetCommitResponse>(
        bootstrap,
        ApiKey::OffsetCommit,
        protocol::OFFSET_COMMIT_VERSION,
        OffsetCommitRequest::default()
            .with_group_id(GroupId(StrBytes::from(group_id.to_string())))
            .with_generation_id_or_member_epoch(generation_id)
            .with_member_id(StrBytes::from(member_id.to_string()))
            .with_group_instance_id(None)
            .with_topics(vec![
                OffsetCommitRequestTopic::default()
                    .with_name(TopicName(StrBytes::from(topic.to_string())))
                    .with_partitions(vec![
                        OffsetCommitRequestPartition::default()
                            .with_partition_index(partition)
                            .with_committed_offset(next_offset),
                    ]),
            ]),
    )
}

pub(super) fn offset_fetch(
    bootstrap: &str,
    group_id: &str,
    topic: &str,
    partitions: &[i32],
) -> OffsetFetchResponse {
    send_request::<OffsetFetchRequest, OffsetFetchResponse>(
        bootstrap,
        ApiKey::OffsetFetch,
        protocol::OFFSET_FETCH_VERSION,
        OffsetFetchRequest::default()
            .with_group_id(GroupId(StrBytes::from(group_id.to_string())))
            .with_topics(Some(vec![
                OffsetFetchRequestTopic::default()
                    .with_name(TopicName(StrBytes::from(topic.to_string())))
                    .with_partition_indexes(partitions.to_vec()),
            ])),
    )
}

pub(super) fn fetch(bootstrap: &str, request: FetchRequest) -> FetchResponse {
    send_request::<FetchRequest, FetchResponse>(
        bootstrap,
        ApiKey::Fetch,
        protocol::FETCH_VERSION,
        request,
    )
}

pub(super) fn send_request<TReq: Encodable, TResp: Decodable>(
    bootstrap: &str,
    api_key: ApiKey,
    api_version: i16,
    request: TReq,
) -> TResp {
    use std::io::{Read, Write};

    let mut stream = std::net::TcpStream::connect(bootstrap).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    stream
        .set_write_timeout(Some(Duration::from_secs(10)))
        .unwrap();

    let mut payload = BytesMut::new();
    RequestHeader::default()
        .with_request_api_key(api_key as i16)
        .with_request_api_version(api_version)
        .with_correlation_id(1)
        .with_client_id(Some(StrBytes::from("differential".to_string())))
        .encode(&mut payload, api_key.request_header_version(api_version))
        .unwrap();
    request.encode(&mut payload, api_version).unwrap();

    stream
        .write_all(&(payload.len() as i32).to_be_bytes())
        .unwrap();
    stream.write_all(payload.as_ref()).unwrap();

    let mut size = [0_u8; 4];
    stream.read_exact(&mut size).unwrap();
    let size = i32::from_be_bytes(size) as usize;
    let mut body = vec![0_u8; size];
    stream.read_exact(&mut body).unwrap();
    let mut bytes = Bytes::from(body);
    let _ =
        ResponseHeader::decode(&mut bytes, api_key.response_header_version(api_version)).unwrap();
    TResp::decode(&mut bytes, api_version).unwrap()
}

pub(super) fn encode_subscription(topic: &str, user_data: &[u8]) -> Vec<u8> {
    let subscription = ConsumerProtocolSubscription::default()
        .with_topics(vec![StrBytes::from(topic.to_string())])
        .with_user_data(Some(Bytes::copy_from_slice(user_data)));
    let mut bytes = BytesMut::new();
    subscription.encode(&mut bytes, 3).unwrap();
    bytes.to_vec()
}

pub(super) fn encode_assignment(topic: &str) -> Vec<u8> {
    encode_assignment_partitions(topic, &[0])
}

pub(super) fn encode_assignment_partitions(topic: &str, partitions: &[i32]) -> Vec<u8> {
    let assignment = ConsumerProtocolAssignment::default().with_assigned_partitions(vec![
        AssignmentTopicPartition::default()
            .with_topic(TopicName(StrBytes::from(topic.to_string())))
            .with_partitions(partitions.to_vec()),
    ]);
    let mut bytes = BytesMut::new();
    assignment.encode(&mut bytes, 3).unwrap();
    bytes.to_vec()
}

pub(super) fn encode_empty_assignment() -> Vec<u8> {
    let assignment = ConsumerProtocolAssignment::default().with_assigned_partitions(vec![]);
    let mut bytes = BytesMut::new();
    assignment.encode(&mut bytes, 3).unwrap();
    bytes.to_vec()
}

pub(super) fn decode_assignment(bytes: &[u8]) -> bool {
    let mut payload = Bytes::copy_from_slice(bytes);
    ConsumerProtocolAssignment::decode(&mut payload, 3).is_ok()
}
