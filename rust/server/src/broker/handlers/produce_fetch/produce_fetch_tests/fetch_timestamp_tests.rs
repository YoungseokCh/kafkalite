use super::*;
use kafka_protocol::records::RecordBatchDecoder;

#[tokio::test]
async fn fetch_round_trips_timestamps_in_order() {
    let broker = test_broker();
    let records = vec![
        Record {
            transactional: false,
            control: false,
            partition_leader_epoch: 0,
            producer_id: -1,
            producer_epoch: -1,
            timestamp_type: TimestampType::Creation,
            offset: 0,
            sequence: -1,
            timestamp: 100,
            key: None,
            value: Some(Bytes::from_static(b"first")),
            headers: Default::default(),
        },
        Record {
            transactional: false,
            control: false,
            partition_leader_epoch: 0,
            producer_id: -1,
            producer_epoch: -1,
            timestamp_type: TimestampType::Creation,
            offset: 1,
            sequence: -1,
            timestamp: 101,
            key: None,
            value: Some(Bytes::from_static(b"second")),
            headers: Default::default(),
        },
    ];
    let mut encoded = BytesMut::new();
    RecordBatchEncoder::encode(
        &mut encoded,
        &records,
        &RecordEncodeOptions {
            version: 2,
            compression: Compression::None,
        },
    )
    .unwrap();
    let request = ProduceRequest::default()
        .with_acks(1)
        .with_timeout_ms(5_000)
        .with_topic_data(vec![
            TopicProduceData::default()
                .with_name(TopicName(StrBytes::from("timestamps.topic".to_string())))
                .with_partition_data(vec![
                    PartitionProduceData::default()
                        .with_index(0)
                        .with_records(Some(encoded.freeze())),
                ]),
        ]);
    let _ = handle_produce(&broker, request).await.unwrap();

    let fetch = handle_fetch(
        &broker,
        FetchRequest::default().with_topics(vec![
            kafka_protocol::messages::fetch_request::FetchTopic::default()
                .with_topic(TopicName(StrBytes::from("timestamps.topic".to_string())))
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
    let decoded = RecordBatchDecoder::decode_all(&mut payload).unwrap();
    let records = decoded
        .into_iter()
        .flat_map(|batch| batch.records)
        .collect::<Vec<_>>();

    assert_eq!(records.len(), 2);
    assert_eq!(records[0].timestamp, 100);
    assert_eq!(records[1].timestamp, 101);
    assert!(records[1].timestamp >= records[0].timestamp);
}
