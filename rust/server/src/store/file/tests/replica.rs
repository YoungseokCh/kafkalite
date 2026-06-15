use super::*;

#[test]
fn replica_fetch_and_apply_preserve_offsets_and_clamp_high_watermark() {
    let leader_dir = tempdir().unwrap();
    let follower_dir = tempdir().unwrap();
    let leader = FileStore::open(leader_dir.path()).unwrap();
    let follower = FileStore::open(follower_dir.path()).unwrap();
    let producer = leader.init_producer().unwrap();
    let records = vec![
        BrokerRecord {
            offset: 0,
            timestamp_ms: 10,
            producer_id: producer.producer_id,
            producer_epoch: producer.producer_epoch,
            sequence: 0,
            key: Some(Bytes::from_static(b"key")),
            value: Some(Bytes::from_static(b"one")),
            headers_json: b"[]".to_vec(),
            partition_leader_epoch: 0,
            transactional: false,
            control: false,
        },
        BrokerRecord {
            offset: 0,
            timestamp_ms: 20,
            producer_id: producer.producer_id,
            producer_epoch: producer.producer_epoch,
            sequence: 1,
            key: Some(Bytes::from_static(b"key")),
            value: Some(Bytes::from_static(b"two")),
            headers_json: b"[]".to_vec(),
            partition_leader_epoch: 0,
            transactional: false,
            control: false,
        },
    ];
    leader
        .append_records("replica.events", 0, &records, 20)
        .unwrap();
    follower.ensure_topic("replica.events", 1, 10).unwrap();

    let fetched = leader
        .replica_fetch_records("replica.events", 0, 0, 10)
        .unwrap();
    let applied = follower
        .apply_replica_records(
            "replica.events",
            0,
            &fetched.records[..1],
            fetched.high_watermark,
            30,
        )
        .unwrap();
    let follower_fetch = follower.fetch_records("replica.events", 0, 0, 10).unwrap();

    assert_eq!(fetched.log_end_offset, 2);
    assert_eq!(applied.log_end_offset, 1);
    assert_eq!(applied.high_watermark, 1);
    assert_eq!(follower_fetch.high_watermark, 1);
    assert_eq!(follower_fetch.records.len(), 1);
    assert_eq!(follower_fetch.records[0].offset, 0);
    assert_eq!(
        follower_fetch.records[0].value.as_deref(),
        Some(&b"one"[..])
    );
}

#[test]
fn replica_apply_rejects_offset_mismatches() {
    let dir = tempdir().unwrap();
    let store = FileStore::open(dir.path()).unwrap();
    store.ensure_topic("replica.events", 1, 10).unwrap();

    let result = store.apply_replica_records(
        "replica.events",
        0,
        &[BrokerRecord {
            offset: 1,
            timestamp_ms: 10,
            producer_id: -1,
            producer_epoch: -1,
            sequence: -1,
            key: None,
            value: Some(Bytes::from_static(b"value")),
            headers_json: b"[]".to_vec(),
            partition_leader_epoch: 0,
            transactional: false,
            control: false,
        }],
        1,
        20,
    );

    assert!(matches!(
        result,
        Err(StoreError::ReplicaOffsetMismatch {
            expected: 0,
            actual: 1,
        })
    ));
}

#[test]
fn replica_append_rejects_misaligned_or_non_contiguous_offsets() {
    let dir = tempdir().unwrap();
    let store = FileStore::open(dir.path()).unwrap();
    store.ensure_topic("replica.topic", 1, 0).unwrap();
    let producer = store.init_producer().unwrap();
    let seed = vec![BrokerRecord {
        offset: 0,
        timestamp_ms: 1,
        producer_id: producer.producer_id,
        producer_epoch: producer.producer_epoch,
        sequence: 0,
        key: Some(Bytes::from_static(b"seed")),
        value: Some(Bytes::from_static(b"seed")),
        headers_json: b"[]".to_vec(),
        partition_leader_epoch: 0,
        transactional: false,
        control: false,
    }];
    store.append_records("replica.topic", 0, &seed, 1).unwrap();

    let misaligned = vec![BrokerRecord {
        offset: 5,
        timestamp_ms: 2,
        producer_id: -1,
        producer_epoch: -1,
        sequence: 0,
        key: Some(Bytes::from_static(b"m")),
        value: Some(Bytes::from_static(b"m")),
        headers_json: b"[]".to_vec(),
        partition_leader_epoch: 0,
        transactional: false,
        control: false,
    }];
    let misaligned_err = store.append_replica_records("replica.topic", 0, &misaligned, 2);
    assert!(matches!(
        misaligned_err,
        Err(StoreError::ReplicaOffsetMismatch {
            expected: 1,
            actual: 5,
        })
    ));

    let non_contiguous = vec![
        BrokerRecord {
            offset: 1,
            timestamp_ms: 3,
            producer_id: -1,
            producer_epoch: -1,
            sequence: 0,
            key: Some(Bytes::from_static(b"a")),
            value: Some(Bytes::from_static(b"a")),
            headers_json: b"[]".to_vec(),
            partition_leader_epoch: 0,
            transactional: false,
            control: false,
        },
        BrokerRecord {
            offset: 3,
            timestamp_ms: 4,
            producer_id: -1,
            producer_epoch: -1,
            sequence: 1,
            key: Some(Bytes::from_static(b"b")),
            value: Some(Bytes::from_static(b"b")),
            headers_json: b"[]".to_vec(),
            partition_leader_epoch: 0,
            transactional: false,
            control: false,
        },
    ];
    let non_contiguous_err = store
        .append_replica_records("replica.topic", 0, &non_contiguous, 3)
        .unwrap_err()
        .to_string();
    assert!(non_contiguous_err.contains("must be contiguous"));
}

#[test]
fn replica_append_skips_stale_offsets_and_returns_current_high_watermark() {
    let dir = tempdir().unwrap();
    let store = FileStore::open(dir.path()).unwrap();
    store.ensure_topic("replica-skip.topic", 1, 0).unwrap();
    let producer = store.init_producer().unwrap();
    let seed = vec![BrokerRecord {
        offset: 0,
        timestamp_ms: 1,
        producer_id: producer.producer_id,
        producer_epoch: producer.producer_epoch,
        sequence: 0,
        key: Some(Bytes::from_static(b"seed")),
        value: Some(Bytes::from_static(b"seed")),
        headers_json: b"[]".to_vec(),
        partition_leader_epoch: 0,
        transactional: false,
        control: false,
    }];
    store
        .append_records("replica-skip.topic", 0, &seed, 1)
        .unwrap();

    let stale = vec![BrokerRecord {
        offset: 0,
        timestamp_ms: 2,
        producer_id: -1,
        producer_epoch: -1,
        sequence: 0,
        key: Some(Bytes::from_static(b"stale")),
        value: Some(Bytes::from_static(b"stale")),
        headers_json: b"[]".to_vec(),
        partition_leader_epoch: 0,
        transactional: false,
        control: false,
    }];
    let latest = store
        .append_replica_records("replica-skip.topic", 0, &stale, 2)
        .unwrap();

    assert_eq!(latest, 1);
}

#[test]
fn replica_append_with_empty_batch_is_a_noop() {
    let dir = tempdir().unwrap();
    let store = FileStore::open(dir.path()).unwrap();
    store.ensure_topic("replica-empty.topic", 1, 0).unwrap();
    let producer = store.init_producer().unwrap();
    let seed = vec![BrokerRecord {
        offset: 0,
        timestamp_ms: 1,
        producer_id: producer.producer_id,
        producer_epoch: producer.producer_epoch,
        sequence: 0,
        key: Some(Bytes::from_static(b"seed")),
        value: Some(Bytes::from_static(b"seed")),
        headers_json: b"[]".to_vec(),
        partition_leader_epoch: 0,
        transactional: false,
        control: false,
    }];
    store
        .append_records("replica-empty.topic", 0, &seed, 1)
        .unwrap();

    let latest = store
        .append_replica_records("replica-empty.topic", 0, &[], 2)
        .unwrap();

    assert_eq!(latest, 1);
    assert_eq!(
        store
            .fetch_records("replica-empty.topic", 0, 0, 10)
            .unwrap()
            .records
            .len(),
        1
    );
}
