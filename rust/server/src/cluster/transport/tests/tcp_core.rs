use super::*;

#[tokio::test]
async fn tcp_transport_round_trips_cluster_rpc() {
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        TcpClusterRpcTransport::serve_once(&listener, |request| match request {
            ClusterRpcRequest::Vote(request) => Ok(ClusterRpcResponse::Vote(VoteResponse {
                term: request.term,
                vote_granted: true,
            })),
            other => panic!("unexpected request {other:?}"),
        })
        .await
        .unwrap();
    });

    let transport = TcpClusterRpcTransport;
    let response = transport
        .send_to(
            &ClusterRpcTarget {
                node_id: 1,
                host: addr.ip().to_string(),
                port: addr.port(),
            },
            ClusterRpcRequest::Vote(VoteRequest {
                term: 7,
                candidate_id: 1,
                last_metadata_offset: 3,
            }),
        )
        .await
        .unwrap();

    let ClusterRpcResponse::Vote(response) = response else {
        panic!("unexpected response variant")
    };
    assert_eq!(response.term, 7);
    assert!(response.vote_granted);
    server.await.unwrap();
}

#[tokio::test]
async fn tcp_transport_can_dispatch_to_runtime() {
    let dir = tempdir().unwrap();
    let mut config = Config::single_node(dir.path().join("data"), 19092, 1);
    config.cluster.node_id = 4;
    config.cluster.process_roles = vec![ProcessRole::Controller];
    let runtime = ClusterRuntime::from_config(&config).unwrap();
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server_runtime = runtime.clone();
    let server = tokio::spawn(async move {
        TcpClusterRpcTransport::serve_runtime_once(&listener, server_runtime)
            .await
            .unwrap();
    });

    let transport = TcpClusterRpcTransport;
    let response = transport
        .send_to(
            &ClusterRpcTarget {
                node_id: 4,
                host: addr.ip().to_string(),
                port: addr.port(),
            },
            ClusterRpcRequest::AppendMetadata(AppendMetadataRequest {
                term: 1,
                leader_id: 4,
                prev_metadata_offset: runtime.metadata_image().metadata_offset,
                records: vec![crate::cluster::MetadataRecord::SetController { controller_id: 4 }],
            }),
        )
        .await
        .unwrap();

    let ClusterRpcResponse::AppendMetadata(response) = response else {
        panic!("unexpected response variant")
    };
    assert!(response.accepted);
    server.await.unwrap();
}

#[tokio::test]
async fn tcp_transport_routes_partition_leader_update_to_controller_runtime() {
    let dir = tempdir().unwrap();
    let mut config = Config::single_node(dir.path().join("data"), 19092, 1);
    config.cluster.node_id = 2;
    config.cluster.process_roles = vec![ProcessRole::Controller];
    let runtime = ClusterRuntime::from_config(&config).unwrap();
    runtime
        .sync_local_topics(
            &[crate::store::TopicMetadata {
                name: "tcp.route.topic".to_string(),
                partitions: vec![crate::store::PartitionMetadata { partition: 0 }],
            }],
            2,
        )
        .unwrap();
    let _ = runtime
        .handle_append_metadata(AppendMetadataRequest {
            term: 1,
            leader_id: 2,
            prev_metadata_offset: runtime.metadata_image().metadata_offset,
            records: vec![crate::cluster::MetadataRecord::SetController { controller_id: 2 }],
        })
        .unwrap();

    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server_runtime = runtime.clone();
    let server = tokio::spawn(async move {
        TcpClusterRpcTransport::serve_runtime_once(&listener, server_runtime)
            .await
            .unwrap();
    });

    let transport = TcpClusterRpcTransport;
    let response = transport
        .update_partition_leader_to(
            &ClusterRpcTarget {
                node_id: 2,
                host: addr.ip().to_string(),
                port: addr.port(),
            },
            UpdatePartitionLeaderRequest {
                topic_name: "tcp.route.topic".to_string(),
                partition_index: 0,
                leader_id: 2,
                leader_epoch: 1,
            },
        )
        .await
        .unwrap();

    assert!(response.accepted);
    assert_eq!(
        runtime
            .metadata_image()
            .partition_leader_id("tcp.route.topic", 0),
        Some(2)
    );
    server.await.unwrap();
}

#[tokio::test]
async fn tcp_transport_round_trips_register_broker_and_heartbeat() {
    let dir = tempdir().unwrap();
    let mut config = Config::single_node(dir.path().join("data"), 19092, 1);
    config.cluster.node_id = 4;
    config.cluster.process_roles = vec![ProcessRole::Broker, ProcessRole::Controller];
    let runtime = ClusterRuntime::from_config(&config).unwrap();
    let _ = runtime
        .handle_append_metadata(AppendMetadataRequest {
            term: 1,
            leader_id: 4,
            prev_metadata_offset: runtime.metadata_image().metadata_offset,
            records: vec![crate::cluster::MetadataRecord::SetController { controller_id: 4 }],
        })
        .unwrap();

    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server_runtime = runtime.clone();
    let server = tokio::spawn(async move {
        TcpClusterRpcTransport::serve_runtime_once(&listener, server_runtime)
            .await
            .unwrap();
    });

    let transport = TcpClusterRpcTransport;
    let registration = transport
        .register_broker_to(
            &ClusterRpcTarget {
                node_id: 4,
                host: addr.ip().to_string(),
                port: addr.port(),
            },
            RegisterBrokerRequest {
                node_id: 9,
                advertised_host: "broker-9.local".to_string(),
                advertised_port: 39092,
            },
        )
        .await
        .unwrap();
    assert!(registration.accepted);
    server.await.unwrap();

    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server_runtime = runtime.clone();
    let server = tokio::spawn(async move {
        TcpClusterRpcTransport::serve_runtime_once(&listener, server_runtime)
            .await
            .unwrap();
    });

    let heartbeat = transport
        .broker_heartbeat_to(
            &ClusterRpcTarget {
                node_id: 4,
                host: addr.ip().to_string(),
                port: addr.port(),
            },
            BrokerHeartbeatRequest {
                node_id: 9,
                broker_epoch: registration.broker_epoch,
                timestamp_ms: 123,
            },
        )
        .await
        .unwrap();
    assert!(heartbeat.accepted);
    server.await.unwrap();
}

#[tokio::test]
async fn tcp_transport_forever_server_handles_multiple_requests() {
    let dir = tempdir().unwrap();
    let mut config = Config::single_node(dir.path().join("data"), 19092, 1);
    config.cluster.node_id = 4;
    config.cluster.process_roles = vec![ProcessRole::Controller];
    let runtime = ClusterRuntime::from_config(&config).unwrap();
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server_runtime = runtime.clone();
    let server = tokio::spawn(async move {
        TcpClusterRpcTransport::serve_runtime_forever(listener, server_runtime)
            .await
            .unwrap();
    });

    let transport = TcpClusterRpcTransport;
    for term in [1_i64, 2_i64] {
        let response = transport
            .send_to(
                &ClusterRpcTarget {
                    node_id: 4,
                    host: addr.ip().to_string(),
                    port: addr.port(),
                },
                ClusterRpcRequest::Vote(VoteRequest {
                    term,
                    candidate_id: 4,
                    last_metadata_offset: runtime.metadata_image().metadata_offset,
                }),
            )
            .await
            .unwrap();
        let ClusterRpcResponse::Vote(response) = response else {
            panic!("unexpected response variant")
        };
        assert_eq!(response.term, term);
    }

    server.abort();
    let _ = server.await;
}
