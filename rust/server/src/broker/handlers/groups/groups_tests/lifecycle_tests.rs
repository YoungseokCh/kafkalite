use kafka_protocol::messages::join_group_request::JoinGroupRequestProtocol;
use kafka_protocol::messages::leave_group_request::MemberIdentity;
use kafka_protocol::messages::{GroupId, JoinGroupRequest, LeaveGroupRequest, SyncGroupRequest};

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
                MemberIdentity::default().with_member_id(StrBytes::from("member-a".to_string())),
                MemberIdentity::default().with_member_id(StrBytes::from("member-b".to_string())),
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
