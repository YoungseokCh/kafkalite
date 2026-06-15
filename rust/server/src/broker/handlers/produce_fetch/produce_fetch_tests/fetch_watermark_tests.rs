use super::*;
use kafka_protocol::messages::produce_request::{PartitionProduceData, TopicProduceData};
use kafka_protocol::records::{
    Compression, Record, RecordBatchEncoder, RecordEncodeOptions, TimestampType,
};

#[tokio::test]
async fn fetch_uses_metadata_high_watermark_when_replication_progress_exists() {
    let broker = test_broker();
    broker.store().ensure_topic("hw.topic", 1, 0).unwrap();
    let metadata = broker
        .store()
        .topic_metadata(Some(&["hw.topic".to_string()]), 0)
        .unwrap();
    broker.sync_topic_metadata(&metadata).unwrap();

    let _ = handle_produce(&broker, produce_request("hw.topic", -1, -1, 0))
        .await
        .unwrap();

    broker
        .cluster()
        .handle_update_partition_replication(crate::cluster::UpdatePartitionReplicationRequest {
            topic_name: "hw.topic".to_string(),
            partition_index: 0,
            replicas: vec![1, 2],
            isr: vec![1, 2],
            leader_epoch: 1,
        })
        .unwrap();
    broker
        .cluster()
        .handle_update_replica_progress(crate::cluster::UpdateReplicaProgressRequest {
            topic_name: "hw.topic".to_string(),
            partition_index: 0,
            leader_epoch: 1,
            broker_id: 1,
            log_end_offset: 1,
            last_caught_up_ms: 100,
        })
        .unwrap();
    broker
        .cluster()
        .handle_update_replica_progress(crate::cluster::UpdateReplicaProgressRequest {
            topic_name: "hw.topic".to_string(),
            partition_index: 0,
            leader_epoch: 1,
            broker_id: 2,
            log_end_offset: 0,
            last_caught_up_ms: 100,
        })
        .unwrap();

    let fetch = handle_fetch(
        &broker,
        FetchRequest::default().with_topics(vec![
            kafka_protocol::messages::fetch_request::FetchTopic::default()
                .with_topic(TopicName(StrBytes::from("hw.topic".to_string())))
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

    assert_eq!(fetch.responses[0].partitions[0].high_watermark, 0);
    assert_eq!(fetch.responses[0].partitions[0].last_stable_offset, 0);
}

#[tokio::test]
async fn empty_fetch_uses_metadata_high_watermark_when_replication_progress_exists() {
    let broker = test_broker();
    broker.store().ensure_topic("hw.empty.topic", 1, 0).unwrap();
    let metadata = broker
        .store()
        .topic_metadata(Some(&["hw.empty.topic".to_string()]), 0)
        .unwrap();
    broker.sync_topic_metadata(&metadata).unwrap();

    let _ = handle_produce(&broker, produce_request("hw.empty.topic", -1, -1, 0))
        .await
        .unwrap();

    broker
        .cluster()
        .handle_update_partition_replication(crate::cluster::UpdatePartitionReplicationRequest {
            topic_name: "hw.empty.topic".to_string(),
            partition_index: 0,
            replicas: vec![1, 2],
            isr: vec![1, 2],
            leader_epoch: 1,
        })
        .unwrap();
    broker
        .cluster()
        .handle_update_replica_progress(crate::cluster::UpdateReplicaProgressRequest {
            topic_name: "hw.empty.topic".to_string(),
            partition_index: 0,
            leader_epoch: 1,
            broker_id: 1,
            log_end_offset: 1,
            last_caught_up_ms: 100,
        })
        .unwrap();
    broker
        .cluster()
        .handle_update_replica_progress(crate::cluster::UpdateReplicaProgressRequest {
            topic_name: "hw.empty.topic".to_string(),
            partition_index: 0,
            leader_epoch: 1,
            broker_id: 2,
            log_end_offset: 0,
            last_caught_up_ms: 100,
        })
        .unwrap();

    let fetch = handle_fetch(
        &broker,
        FetchRequest::default().with_topics(vec![
            kafka_protocol::messages::fetch_request::FetchTopic::default()
                .with_topic(TopicName(StrBytes::from("hw.empty.topic".to_string())))
                .with_partitions(vec![
                    kafka_protocol::messages::fetch_request::FetchPartition::default()
                        .with_partition(0)
                        .with_fetch_offset(1)
                        .with_partition_max_bytes(1024),
                ]),
        ]),
    )
    .await
    .unwrap();

    let partition = &fetch.responses[0].partitions[0];
    assert_eq!(partition.high_watermark, 0);
    assert_eq!(partition.last_stable_offset, 0);
    assert!(partition.records.as_ref().unwrap().is_empty());
}

#[tokio::test]
async fn read_committed_hides_open_transactional_records_and_never_exposes_control_marker() {
    let broker = test_broker();
    broker.store().ensure_topic("tx.topic", 1, 0).unwrap();
    let session = crate::broker::handlers::bootstrap::handle_init_producer_id(
        &broker,
        kafka_protocol::messages::InitProducerIdRequest::default().with_transactional_id(Some(
            kafka_protocol::messages::TransactionalId(StrBytes::from("tx-watermark".to_string())),
        )),
    )
    .await
    .unwrap();
    let add = handle_add_partitions_to_txn(
        &broker,
        kafka_protocol::messages::AddPartitionsToTxnRequest::default().with_transactions(vec![
            kafka_protocol::messages::add_partitions_to_txn_request::AddPartitionsToTxnTransaction::default()
                .with_transactional_id(kafka_protocol::messages::TransactionalId(StrBytes::from(
                    "tx-watermark".to_string(),
                )))
                .with_producer_id(session.producer_id)
                .with_producer_epoch(session.producer_epoch)
                .with_topics(vec![
                    kafka_protocol::messages::add_partitions_to_txn_request::AddPartitionsToTxnTopic::default()
                        .with_name(TopicName(StrBytes::from("tx.topic".to_string())))
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

    let mut transactional_payload = BytesMut::new();
    RecordBatchEncoder::encode(
        &mut transactional_payload,
        &vec![
            Record {
                transactional: true,
                control: false,
                partition_leader_epoch: 0,
                producer_id: session.producer_id.0,
                producer_epoch: session.producer_epoch,
                timestamp_type: TimestampType::Creation,
                offset: 0,
                sequence: 0,
                timestamp: 100,
                key: None,
                value: Some(Bytes::from_static(b"tx-0")),
                headers: Default::default(),
            },
            Record {
                transactional: true,
                control: false,
                partition_leader_epoch: 0,
                producer_id: session.producer_id.0,
                producer_epoch: session.producer_epoch,
                timestamp_type: TimestampType::Creation,
                offset: 1,
                sequence: 1,
                timestamp: 101,
                key: None,
                value: Some(Bytes::from_static(b"tx-1")),
                headers: Default::default(),
            },
        ],
        &RecordEncodeOptions {
            version: 2,
            compression: Compression::None,
        },
    )
    .unwrap();

    handle_produce(
        &broker,
        ProduceRequest::default()
            .with_acks(1)
            .with_timeout_ms(5_000)
            .with_topic_data(vec![
                TopicProduceData::default()
                    .with_name(TopicName(StrBytes::from("tx.topic".to_string())))
                    .with_partition_data(vec![
                        PartitionProduceData::default()
                            .with_index(0)
                            .with_records(Some(transactional_payload.freeze())),
                    ]),
            ]),
    )
    .await
    .unwrap();

    let read_committed = |offset| {
        FetchRequest::default()
            .with_isolation_level(1)
            .with_topics(vec![
                kafka_protocol::messages::fetch_request::FetchTopic::default()
                    .with_topic(TopicName(StrBytes::from("tx.topic".to_string())))
                    .with_partitions(vec![
                        kafka_protocol::messages::fetch_request::FetchPartition::default()
                            .with_partition(0)
                            .with_fetch_offset(offset)
                            .with_partition_max_bytes(4096),
                    ]),
            ])
    };

    let before_commit = handle_fetch(&broker, read_committed(0)).await.unwrap();
    let before_partition = &before_commit.responses[0].partitions[0];
    assert_eq!(before_partition.high_watermark, 2);
    assert_eq!(before_partition.last_stable_offset, 0);
    assert!(before_partition.records.as_ref().unwrap().is_empty());

    let marker = handle_write_txn_markers(
        &broker,
        kafka_protocol::messages::WriteTxnMarkersRequest::default().with_markers(vec![
            kafka_protocol::messages::write_txn_markers_request::WritableTxnMarker::default()
                .with_producer_id(session.producer_id)
                .with_producer_epoch(session.producer_epoch)
                .with_transaction_result(true)
                .with_coordinator_epoch(0)
                .with_topics(vec![
                    kafka_protocol::messages::write_txn_markers_request::WritableTxnMarkerTopic::default()
                        .with_name(TopicName(StrBytes::from("tx.topic".to_string())))
                        .with_partition_indexes(vec![0]),
                ]),
        ]),
    )
    .await
    .unwrap();
    assert_eq!(marker.markers[0].topics[0].partitions[0].error_code, 0);

    let control_fetch = handle_fetch(
        &broker,
        FetchRequest::default().with_topics(vec![
            kafka_protocol::messages::fetch_request::FetchTopic::default()
                .with_topic(TopicName(StrBytes::from("tx.topic".to_string())))
                .with_partitions(vec![
                    kafka_protocol::messages::fetch_request::FetchPartition::default()
                        .with_partition(0)
                        .with_fetch_offset(2)
                        .with_partition_max_bytes(4096),
                ]),
        ]),
    )
    .await
    .unwrap();
    assert!(
        control_fetch.responses[0].partitions[0]
            .records
            .as_ref()
            .unwrap()
            .is_empty()
    );
}
