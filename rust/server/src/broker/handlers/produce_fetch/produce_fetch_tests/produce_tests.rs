use super::*;

#[tokio::test]
async fn duplicate_retry_returns_same_base_offset() {
    let broker = test_broker();
    let session = broker.store().init_producer(0).unwrap();
    let first = handle_produce(
        &broker,
        produce_request(
            "retry.topic",
            session.producer_id,
            session.producer_epoch,
            0,
        ),
    )
    .await
    .unwrap();
    let duplicate = handle_produce(
        &broker,
        produce_request(
            "retry.topic",
            session.producer_id,
            session.producer_epoch,
            0,
        ),
    )
    .await
    .unwrap();

    let first_partition = &first.responses[0].partition_responses[0];
    let duplicate_partition = &duplicate.responses[0].partition_responses[0];
    assert_eq!(first_partition.error_code, 0);
    assert_eq!(duplicate_partition.error_code, 0);
    assert_eq!(first_partition.base_offset, duplicate_partition.base_offset);
    let fetched = broker
        .store()
        .fetch_records("retry.topic", 0, 0, 10)
        .unwrap();
    assert_eq!(fetched.records.len(), 1);
}

#[tokio::test]
async fn stale_epoch_maps_to_invalid_producer_epoch() {
    let broker = test_broker();
    let session = broker.store().init_producer(0).unwrap();
    let _ = handle_produce(
        &broker,
        produce_request(
            "epoch.topic",
            session.producer_id,
            session.producer_epoch + 1,
            0,
        ),
    )
    .await
    .unwrap();
    let stale = handle_produce(
        &broker,
        produce_request(
            "epoch.topic",
            session.producer_id,
            session.producer_epoch,
            1,
        ),
    )
    .await
    .unwrap();

    let partition = &stale.responses[0].partition_responses[0];
    assert_eq!(partition.error_code, INVALID_PRODUCER_EPOCH);
    assert_eq!(partition.base_offset, -1);
}

#[tokio::test]
async fn produce_returns_unknown_when_metadata_is_local_but_store_partition_missing() {
    let broker = test_broker();
    let initial_offset = broker.cluster().metadata_image().metadata_offset;
    broker
        .cluster()
        .handle_append_metadata(crate::cluster::AppendMetadataRequest {
            term: 1,
            leader_id: 1,
            prev_metadata_offset: initial_offset,
            records: vec![crate::cluster::MetadataRecord::UpsertTopic(
                crate::cluster::TopicMetadataImage {
                    name: "missing-local.topic".to_string(),
                    partitions: vec![crate::cluster::PartitionMetadataImage {
                        partition: 1,
                        leader_id: 1,
                        leader_epoch: 1,
                        high_watermark: 0,
                        replicas: vec![1],
                        isr: vec![1],
                        replica_progress: vec![],
                        reassignment: None,
                    }],
                },
            )],
        })
        .unwrap();

    let response = handle_produce(
        &broker,
        produce_request_for_partition("missing-local.topic", 1, -1, -1, 0),
    )
    .await
    .unwrap();

    assert_eq!(
        response.responses[0].partition_responses[0].error_code,
        UNKNOWN_TOPIC_OR_PARTITION
    );
}
