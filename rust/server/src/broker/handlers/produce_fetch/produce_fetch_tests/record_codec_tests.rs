use super::*;
use kafka_protocol::records::RecordBatchDecoder;

#[test]
fn encode_records_round_trips_nonzero_offsets() {
    let encoded = encode_records(
        &[
            BrokerRecord {
                offset: 1,
                timestamp_ms: 100,
                producer_id: -1,
                producer_epoch: -1,
                sequence: -1,
                key: Some(Bytes::from_static(b"key-1")),
                value: Some(Bytes::from_static(b"value-1")),
                headers_json: vec![],
                partition_leader_epoch: 0,
                transactional: false,
                control: false,
            },
            BrokerRecord {
                offset: 2,
                timestamp_ms: 101,
                producer_id: -1,
                producer_epoch: -1,
                sequence: -1,
                key: Some(Bytes::from_static(b"key-2")),
                value: Some(Bytes::from_static(b"value-2")),
                headers_json: vec![],
                partition_leader_epoch: 0,
                transactional: false,
                control: false,
            },
        ],
        7,
    )
    .unwrap();

    let mut bytes = encoded.clone();
    let decoded = RecordBatchDecoder::decode_all(&mut bytes).unwrap();
    assert_eq!(decoded[0].records[0].partition_leader_epoch, 7);
    let records = decoded
        .into_iter()
        .flat_map(|batch| batch.records)
        .collect::<Vec<_>>();

    assert_eq!(
        records
            .iter()
            .map(|record| record.offset)
            .collect::<Vec<_>>(),
        vec![1, 2]
    );
    assert_eq!(
        records
            .iter()
            .map(|record| record.value.clone().unwrap())
            .collect::<Vec<_>>(),
        vec![
            Bytes::from_static(b"value-1"),
            Bytes::from_static(b"value-2")
        ]
    );
}

#[test]
fn encode_records_round_trips_headers() {
    let encoded = encode_records(
        &[BrokerRecord {
            offset: 0,
            timestamp_ms: 100,
            producer_id: -1,
            producer_epoch: -1,
            sequence: -1,
            key: Some(Bytes::from_static(b"key")),
            value: Some(Bytes::from_static(b"value")),
            headers_json: serde_json::to_vec(&vec![
                ("trace-id".to_string(), Some(b"abc123".to_vec())),
                ("empty".to_string(), None),
            ])
            .unwrap(),
            partition_leader_epoch: 0,
            transactional: false,
            control: false,
        }],
        0,
    )
    .unwrap();

    let mut bytes = encoded.clone();
    let decoded = RecordBatchDecoder::decode_all(&mut bytes).unwrap();
    let record = decoded
        .into_iter()
        .flat_map(|batch| batch.records)
        .next()
        .unwrap();

    assert_eq!(
        record
            .headers
            .get(&StrBytes::from("trace-id".to_string()))
            .cloned(),
        Some(Some(Bytes::from_static(b"abc123")))
    );
    assert_eq!(
        record
            .headers
            .get(&StrBytes::from("empty".to_string()))
            .cloned(),
        Some(None)
    );
}
