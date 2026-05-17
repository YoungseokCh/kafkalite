use kafka_protocol::messages::join_group_request::JoinGroupRequestProtocol;
use kafka_protocol::messages::offset_commit_request::{
    OffsetCommitRequestPartition, OffsetCommitRequestTopic,
};
use kafka_protocol::messages::offset_fetch_request::OffsetFetchRequestTopic;
use kafka_protocol::messages::{
    GroupId, JoinGroupRequest, OffsetCommitRequest, OffsetFetchRequest,
};

use super::*;

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
