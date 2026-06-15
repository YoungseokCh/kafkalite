use super::*;

#[tokio::test]
async fn produce_is_rejected_when_local_broker_is_not_leader() {
    let broker = test_broker();
    broker.store().ensure_topic("remote.topic", 1, 0).unwrap();
    let metadata = broker
        .store()
        .topic_metadata(Some(&["remote.topic".to_string()]), 0)
        .unwrap();
    broker.sync_topic_metadata(&metadata).unwrap();
    let initial_offset = broker.cluster().metadata_image().metadata_offset;
    broker
        .cluster()
        .handle_append_metadata(crate::cluster::AppendMetadataRequest {
            term: 1,
            leader_id: 9,
            prev_metadata_offset: initial_offset,
            records: vec![crate::cluster::MetadataRecord::UpsertTopic(
                crate::cluster::TopicMetadataImage {
                    name: "remote.topic".to_string(),
                    partitions: vec![crate::cluster::PartitionMetadataImage {
                        partition: 0,
                        leader_id: 9,
                        leader_epoch: 1,
                        high_watermark: 0,
                        replicas: vec![9],
                        isr: vec![9],
                        replica_progress: vec![],
                        reassignment: None,
                    }],
                },
            )],
        })
        .unwrap();

    let response = handle_produce(&broker, produce_request("remote.topic", -1, -1, 0))
        .await
        .unwrap();

    assert_eq!(
        response.responses[0].partition_responses[0].error_code,
        NOT_LEADER_OR_FOLLOWER
    );
}

#[tokio::test]
async fn fetch_and_list_offsets_are_rejected_when_local_broker_is_not_leader() {
    let broker = test_broker();
    broker.store().ensure_topic("remote.fetch", 1, 0).unwrap();
    let metadata = broker
        .store()
        .topic_metadata(Some(&["remote.fetch".to_string()]), 0)
        .unwrap();
    broker.sync_topic_metadata(&metadata).unwrap();
    let initial_offset = broker.cluster().metadata_image().metadata_offset;
    broker
        .cluster()
        .handle_append_metadata(crate::cluster::AppendMetadataRequest {
            term: 1,
            leader_id: 9,
            prev_metadata_offset: initial_offset,
            records: vec![crate::cluster::MetadataRecord::UpsertTopic(
                crate::cluster::TopicMetadataImage {
                    name: "remote.fetch".to_string(),
                    partitions: vec![crate::cluster::PartitionMetadataImage {
                        partition: 0,
                        leader_id: 9,
                        leader_epoch: 1,
                        high_watermark: 0,
                        replicas: vec![9],
                        isr: vec![9],
                        replica_progress: vec![],
                        reassignment: None,
                    }],
                },
            )],
        })
        .unwrap();

    let fetch = handle_fetch(
        &broker,
        FetchRequest::default().with_topics(vec![
            kafka_protocol::messages::fetch_request::FetchTopic::default()
                .with_topic(TopicName(StrBytes::from("remote.fetch".to_string())))
                .with_partitions(vec![
                    kafka_protocol::messages::fetch_request::FetchPartition::default()
                        .with_partition(0)
                        .with_fetch_offset(0)
                        .with_partition_max_bytes(1024),
                ]),
        ]),
    )
    .await
    .unwrap();
    assert_eq!(
        fetch.responses[0].partitions[0].error_code,
        NOT_LEADER_OR_FOLLOWER
    );

    let offsets = handle_list_offsets(
        &broker,
        ListOffsetsRequest::default().with_topics(vec![
            kafka_protocol::messages::list_offsets_request::ListOffsetsTopic::default()
                .with_name(TopicName(StrBytes::from("remote.fetch".to_string())))
                .with_partitions(vec![
                    kafka_protocol::messages::list_offsets_request::ListOffsetsPartition::default()
                        .with_partition_index(0)
                        .with_timestamp(-1),
                ]),
        ]),
        4,
    )
    .await
    .unwrap();
    assert_eq!(
        offsets.topics[0].partitions[0].error_code,
        NOT_LEADER_OR_FOLLOWER
    );
}

#[tokio::test]
async fn list_offsets_sets_leader_epoch_by_api_version() {
    let broker = test_broker();
    let _ = handle_produce(&broker, produce_request("offsets.topic", -1, -1, 0))
        .await
        .unwrap();
    let initial_offset = broker.cluster().metadata_image().metadata_offset;
    broker
        .cluster()
        .handle_append_metadata(crate::cluster::AppendMetadataRequest {
            term: 2,
            leader_id: 1,
            prev_metadata_offset: initial_offset,
            records: vec![crate::cluster::MetadataRecord::UpdatePartitionLeader {
                topic_name: "offsets.topic".to_string(),
                partition_index: 0,
                leader_id: 1,
                leader_epoch: 2,
            }],
        })
        .unwrap();

    let request = ListOffsetsRequest::default().with_topics(vec![
        kafka_protocol::messages::list_offsets_request::ListOffsetsTopic::default()
            .with_name(TopicName(StrBytes::from("offsets.topic".to_string())))
            .with_partitions(vec![
                kafka_protocol::messages::list_offsets_request::ListOffsetsPartition::default()
                    .with_partition_index(0)
                    .with_timestamp(-1),
            ]),
    ]);

    let v3 = handle_list_offsets(&broker, request.clone(), 3)
        .await
        .unwrap();
    let v4 = handle_list_offsets(&broker, request, 4).await.unwrap();

    assert_eq!(v3.topics[0].partitions[0].error_code, 0);
    assert_eq!(v3.topics[0].partitions[0].leader_epoch, -1);
    assert_eq!(v4.topics[0].partitions[0].error_code, 0);
    assert_eq!(v4.topics[0].partitions[0].leader_epoch, 2);
}

#[tokio::test]
async fn list_offsets_timestamp_queries_return_matching_offset_or_minus_one() {
    let broker = test_broker();
    broker
        .store()
        .append_records(
            "offsets.time",
            0,
            &[
                BrokerRecord {
                    offset: 0,
                    timestamp_ms: 100,
                    producer_id: -1,
                    producer_epoch: -1,
                    sequence: 0,
                    key: None,
                    value: Some(Bytes::from_static(b"v0")),
                    headers_json: b"[]".to_vec(),
                    partition_leader_epoch: 0,
                    transactional: false,
                    control: false,
                },
                BrokerRecord {
                    offset: 1,
                    timestamp_ms: 200,
                    producer_id: -1,
                    producer_epoch: -1,
                    sequence: 1,
                    key: None,
                    value: Some(Bytes::from_static(b"v1")),
                    headers_json: b"[]".to_vec(),
                    partition_leader_epoch: 0,
                    transactional: false,
                    control: false,
                },
            ],
            200,
        )
        .unwrap();

    let request = |timestamp| {
        ListOffsetsRequest::default().with_topics(vec![
            kafka_protocol::messages::list_offsets_request::ListOffsetsTopic::default()
                .with_name(TopicName(StrBytes::from("offsets.time".to_string())))
                .with_partitions(vec![
                    kafka_protocol::messages::list_offsets_request::ListOffsetsPartition::default()
                        .with_partition_index(0)
                        .with_timestamp(timestamp),
                ]),
        ])
    };

    let found = handle_list_offsets(&broker, request(150), 4).await.unwrap();
    let missing = handle_list_offsets(&broker, request(250), 4).await.unwrap();

    assert_eq!(found.topics[0].partitions[0].error_code, 0);
    assert_eq!(found.topics[0].partitions[0].offset, 1);
    assert_eq!(found.topics[0].partitions[0].timestamp, 200);
    assert_eq!(missing.topics[0].partitions[0].error_code, 0);
    assert_eq!(missing.topics[0].partitions[0].offset, -1);
    assert_eq!(missing.topics[0].partitions[0].timestamp, -1);
}

#[tokio::test]
async fn fetch_reencodes_records_with_current_partition_leader_epoch() {
    let broker = test_broker();
    let _ = handle_produce(&broker, produce_request("fetch.epoch", -1, -1, 0))
        .await
        .unwrap();
    let initial_offset = broker.cluster().metadata_image().metadata_offset;
    broker
        .cluster()
        .handle_append_metadata(crate::cluster::AppendMetadataRequest {
            term: 3,
            leader_id: 1,
            prev_metadata_offset: initial_offset,
            records: vec![crate::cluster::MetadataRecord::UpdatePartitionLeader {
                topic_name: "fetch.epoch".to_string(),
                partition_index: 0,
                leader_id: 1,
                leader_epoch: 3,
            }],
        })
        .unwrap();

    let fetch = handle_fetch(
        &broker,
        FetchRequest::default().with_topics(vec![
            kafka_protocol::messages::fetch_request::FetchTopic::default()
                .with_topic(TopicName(StrBytes::from("fetch.epoch".to_string())))
                .with_partitions(vec![
                    kafka_protocol::messages::fetch_request::FetchPartition::default()
                        .with_partition(0)
                        .with_fetch_offset(0)
                        .with_partition_max_bytes(1024),
                ]),
        ]),
    )
    .await
    .unwrap();

    let mut payload = fetch.responses[0].partitions[0].records.clone().unwrap();
    let decoded = kafka_protocol::records::RecordBatchDecoder::decode_all(&mut payload).unwrap();
    assert_eq!(decoded[0].records[0].partition_leader_epoch, 3);
}
