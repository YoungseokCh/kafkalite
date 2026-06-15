use super::*;
use kafka_protocol::messages::write_txn_markers_request::{
    WritableTxnMarker, WritableTxnMarkerTopic,
};

#[tokio::test]
async fn write_txn_markers_commit_makes_transaction_visible_to_read_committed_fetch() {
    let broker = test_broker();
    let session = broker.store().init_producer().unwrap();
    let _ = handle_produce(
        &broker,
        produce_request(
            "txn.commit.topic",
            session.producer_id,
            session.producer_epoch,
            0,
        ),
    )
    .await
    .unwrap();

    let response = handle_write_txn_markers(
        &broker,
        kafka_protocol::messages::WriteTxnMarkersRequest::default().with_markers(vec![
            WritableTxnMarker::default()
                .with_producer_id(kafka_protocol::messages::ProducerId(session.producer_id))
                .with_producer_epoch(session.producer_epoch)
                .with_transaction_result(true)
                .with_coordinator_epoch(7)
                .with_topics(vec![
                    WritableTxnMarkerTopic::default()
                        .with_name(TopicName(StrBytes::from("txn.commit.topic".to_string())))
                        .with_partition_indexes(vec![0]),
                ]),
        ]),
    )
    .await
    .unwrap();

    assert_eq!(response.markers.len(), 1);
    assert_eq!(response.markers[0].topics[0].partitions[0].error_code, 0);

    let fetch = handle_fetch(
        &broker,
        FetchRequest::default()
            .with_isolation_level(1)
            .with_topics(vec![
                kafka_protocol::messages::fetch_request::FetchTopic::default()
                    .with_topic(TopicName(StrBytes::from("txn.commit.topic".to_string())))
                    .with_partitions(vec![
                        kafka_protocol::messages::fetch_request::FetchPartition::default()
                            .with_partition(0)
                            .with_fetch_offset(0)
                            .with_partition_max_bytes(4096),
                    ]),
            ]),
    )
    .await
    .unwrap();

    let partition = &fetch.responses[0].partitions[0];
    assert_eq!(partition.last_stable_offset, 2);
    let mut payload = partition.records.clone().expect("records");
    let decoded = kafka_protocol::records::RecordBatchDecoder::decode_all(&mut payload).unwrap();
    assert_eq!(decoded.len(), 1);
    assert_eq!(decoded[0].records.len(), 1);
    assert_eq!(decoded[0].records[0].value.as_deref(), Some(&b"value"[..]));
}

#[tokio::test]
async fn write_txn_markers_abort_reports_aborted_transaction_to_read_committed_fetch() {
    let broker = test_broker();
    broker
        .store()
        .ensure_topic("txn.abort.topic", 1, 0)
        .unwrap();
    let session = crate::broker::handlers::bootstrap::handle_init_producer_id(
        &broker,
        kafka_protocol::messages::InitProducerIdRequest::default().with_transactional_id(Some(
            kafka_protocol::messages::TransactionalId(StrBytes::from("txn-abort".to_string())),
        )),
    )
    .await
    .unwrap();
    let add = handle_add_partitions_to_txn(
        &broker,
        kafka_protocol::messages::AddPartitionsToTxnRequest::default().with_transactions(vec![
            kafka_protocol::messages::add_partitions_to_txn_request::AddPartitionsToTxnTransaction::default()
                .with_transactional_id(kafka_protocol::messages::TransactionalId(StrBytes::from(
                    "txn-abort".to_string(),
                )))
                .with_producer_id(session.producer_id)
                .with_producer_epoch(session.producer_epoch)
                .with_topics(vec![
                    kafka_protocol::messages::add_partitions_to_txn_request::AddPartitionsToTxnTopic::default()
                        .with_name(TopicName(StrBytes::from("txn.abort.topic".to_string())))
                        .with_partitions(vec![0]),
                ]),
        ]),
        5,
    )
    .await
    .unwrap();
    assert_eq!(
        add.results_by_transaction[0].topic_results[0].results_by_partition[0].partition_error_code,
        0
    );
    let request = transactional_produce_request(
        "txn.abort.topic",
        session.producer_id.0,
        session.producer_epoch,
        0,
    );
    let _ = handle_produce(&broker, request).await.unwrap();

    let response = handle_write_txn_markers(
        &broker,
        kafka_protocol::messages::WriteTxnMarkersRequest::default().with_markers(vec![
            WritableTxnMarker::default()
                .with_producer_id(session.producer_id)
                .with_producer_epoch(session.producer_epoch)
                .with_transaction_result(false)
                .with_coordinator_epoch(9)
                .with_topics(vec![
                    WritableTxnMarkerTopic::default()
                        .with_name(TopicName(StrBytes::from("txn.abort.topic".to_string())))
                        .with_partition_indexes(vec![0]),
                ]),
        ]),
    )
    .await
    .unwrap();

    assert_eq!(response.markers[0].topics[0].partitions[0].error_code, 0);

    let fetch = handle_fetch(
        &broker,
        FetchRequest::default()
            .with_isolation_level(1)
            .with_topics(vec![
                kafka_protocol::messages::fetch_request::FetchTopic::default()
                    .with_topic(TopicName(StrBytes::from("txn.abort.topic".to_string())))
                    .with_partitions(vec![
                        kafka_protocol::messages::fetch_request::FetchPartition::default()
                            .with_partition(0)
                            .with_fetch_offset(0)
                            .with_partition_max_bytes(4096),
                    ]),
            ]),
    )
    .await
    .unwrap();

    let partition = &fetch.responses[0].partitions[0];
    assert!(partition.records.as_ref().expect("records").is_empty());
    let aborted = partition
        .aborted_transactions
        .as_ref()
        .expect("aborted transactions");
    assert_eq!(aborted.len(), 1);
    assert_eq!(aborted[0].producer_id.0, session.producer_id);
    assert_eq!(aborted[0].first_offset, 0);
}

#[tokio::test]
async fn duplicate_write_txn_marker_retry_is_idempotent() {
    let broker = test_broker();
    let session = broker.store().init_producer().unwrap();
    let _ = handle_produce(
        &broker,
        produce_request(
            "txn.retry.topic",
            session.producer_id,
            session.producer_epoch,
            0,
        ),
    )
    .await
    .unwrap();

    let request = kafka_protocol::messages::WriteTxnMarkersRequest::default().with_markers(vec![
        WritableTxnMarker::default()
            .with_producer_id(kafka_protocol::messages::ProducerId(session.producer_id))
            .with_producer_epoch(session.producer_epoch)
            .with_transaction_result(true)
            .with_coordinator_epoch(11)
            .with_topics(vec![
                WritableTxnMarkerTopic::default()
                    .with_name(TopicName(StrBytes::from("txn.retry.topic".to_string())))
                    .with_partition_indexes(vec![0]),
            ]),
    ]);

    let first = handle_write_txn_markers(&broker, request.clone())
        .await
        .unwrap();
    let second = handle_write_txn_markers(&broker, request).await.unwrap();

    assert_eq!(first.markers[0].topics[0].partitions[0].error_code, 0);
    assert_eq!(second.markers[0].topics[0].partitions[0].error_code, 0);

    let fetched = broker
        .store()
        .fetch_records("txn.retry.topic", 0, 0, 10)
        .unwrap();
    assert_eq!(fetched.records.len(), 2);
    assert_eq!(fetched.records[0].offset, 0);
    assert_eq!(fetched.records[1].offset, 1);
    assert!(fetched.records[1].control);
}

#[tokio::test]
async fn write_txn_markers_stale_epoch_maps_to_invalid_producer_epoch() {
    let broker = test_broker();
    let session = broker.store().init_producer().unwrap();
    let _ = handle_produce(
        &broker,
        produce_request(
            "txn.epoch.topic",
            session.producer_id,
            session.producer_epoch + 1,
            0,
        ),
    )
    .await
    .unwrap();

    let response = handle_write_txn_markers(
        &broker,
        kafka_protocol::messages::WriteTxnMarkersRequest::default().with_markers(vec![
            WritableTxnMarker::default()
                .with_producer_id(kafka_protocol::messages::ProducerId(session.producer_id))
                .with_producer_epoch(session.producer_epoch)
                .with_transaction_result(true)
                .with_coordinator_epoch(5)
                .with_topics(vec![
                    WritableTxnMarkerTopic::default()
                        .with_name(TopicName(StrBytes::from("txn.epoch.topic".to_string())))
                        .with_partition_indexes(vec![0]),
                ]),
        ]),
    )
    .await
    .unwrap();

    assert_eq!(
        response.markers[0].topics[0].partitions[0].error_code,
        crate::broker::handlers::error_codes::INVALID_PRODUCER_EPOCH
    );
}
