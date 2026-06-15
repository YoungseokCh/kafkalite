use super::*;

#[tokio::test]
async fn tcp_broker_transport_serves_replica_fetch_apply_and_runtime_dispatch() {
    let dir = tempdir().unwrap();
    let mut config = Config::single_node(dir.path().join("data"), 19092, 1);
    config.cluster.node_id = 1;
    config.cluster.process_roles = vec![ProcessRole::Broker, ProcessRole::Controller];
    let runtime = ClusterRuntime::from_config(&config).unwrap();
    runtime
        .sync_local_topics(
            &[crate::store::TopicMetadata {
                name: "broker.topic".to_string(),
                partitions: vec![crate::store::PartitionMetadata { partition: 0 }],
            }],
            1,
        )
        .unwrap();
    let store = Arc::new(FileStore::open(dir.path().join("broker-store")).unwrap());
    store.ensure_topic("broker.topic", 1, 0).unwrap();
    store.ensure_topic("store-only.topic", 1, 0).unwrap();
    store
        .append_replica_records(
            "store-only.topic",
            0,
            &[crate::store::BrokerRecord {
                offset: 0,
                timestamp_ms: 100,
                producer_id: -1,
                producer_epoch: -1,
                sequence: 0,
                key: Some(bytes::Bytes::from_static(b"k")),
                value: Some(bytes::Bytes::from_static(b"v")),
                headers_json: vec![],
                partition_leader_epoch: 0,
                transactional: false,
                control: false,
            }],
            100,
        )
        .unwrap();

    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(TcpClusterRpcTransport::serve_broker_forever(
        listener,
        runtime.clone(),
        store.clone(),
        Arc::new(crate::broker::fetch_signals::FetchSignals::default()),
    ));
    let transport = TcpClusterRpcTransport;
    let target = ClusterRpcTarget {
        node_id: 1,
        host: addr.ip().to_string(),
        port: addr.port(),
    };

    let applied = transport
        .apply_replica_records_to(
            &target,
            ApplyReplicaRecordsRequest {
                topic_name: "broker.topic".to_string(),
                partition_index: 0,
                records: vec![crate::store::BrokerRecord {
                    offset: 0,
                    timestamp_ms: 101,
                    producer_id: -1,
                    producer_epoch: -1,
                    sequence: 0,
                    key: Some(bytes::Bytes::from_static(b"k1")),
                    value: Some(bytes::Bytes::from_static(b"v1")),
                    headers_json: vec![],
                    partition_leader_epoch: 0,
                    transactional: false,
                    control: false,
                }],
                now_ms: 101,
            },
        )
        .await
        .unwrap();
    assert!(applied.accepted);
    assert_eq!(applied.next_offset, 1);

    let fetched = transport
        .send_to(
            &target,
            ClusterRpcRequest::ReplicaFetch(ReplicaFetchRequest {
                topic_name: "broker.topic".to_string(),
                partition_index: 0,
                start_offset: 0,
                max_records: 10,
            }),
        )
        .await
        .unwrap();
    let ClusterRpcResponse::ReplicaFetch(fetched) = fetched else {
        panic!("unexpected response variant");
    };
    assert!(fetched.found);
    assert_eq!(fetched.leader_id, 1);
    assert_eq!(fetched.leader_log_end_offset, 1);
    assert_eq!(fetched.records.len(), 1);

    let fallback_fetch = transport
        .send_to(
            &target,
            ClusterRpcRequest::ReplicaFetch(ReplicaFetchRequest {
                topic_name: "store-only.topic".to_string(),
                partition_index: 0,
                start_offset: 0,
                max_records: 10,
            }),
        )
        .await
        .unwrap();
    let ClusterRpcResponse::ReplicaFetch(fallback_fetch) = fallback_fetch else {
        panic!("unexpected response variant");
    };
    assert!(fallback_fetch.found);
    assert_eq!(fallback_fetch.leader_id, -1);
    assert_eq!(fallback_fetch.high_watermark, 0);

    let missing = transport
        .send_to(
            &target,
            ClusterRpcRequest::ReplicaFetch(ReplicaFetchRequest {
                topic_name: "missing.topic".to_string(),
                partition_index: 0,
                start_offset: 0,
                max_records: 10,
            }),
        )
        .await
        .unwrap();
    let ClusterRpcResponse::ReplicaFetch(missing) = missing else {
        panic!("unexpected response variant");
    };
    assert!(!missing.found);

    let vote = transport
        .send_to(
            &target,
            ClusterRpcRequest::Vote(VoteRequest {
                term: 2,
                candidate_id: 1,
                last_metadata_offset: runtime.metadata_image().metadata_offset,
            }),
        )
        .await
        .unwrap();
    assert!(matches!(vote, ClusterRpcResponse::Vote(_)));

    server.abort();
    let _ = server.await;
}
