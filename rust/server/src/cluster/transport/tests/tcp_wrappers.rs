use super::*;

#[tokio::test]
async fn tcp_transport_typed_wrappers_reject_unexpected_response_variants() {
    async fn expect_unexpected_response<T>(fut: impl std::future::Future<Output = Result<T>>) {
        let err = match fut.await {
            Ok(_) => panic!("expected unexpected response error"),
            Err(err) => err.to_string(),
        };
        assert!(err.contains("unexpected RPC response"));
    }

    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        for _ in 0..6 {
            TcpClusterRpcTransport::serve_once(&listener, |_| {
                Ok(ClusterRpcResponse::Vote(VoteResponse {
                    term: 1,
                    vote_granted: true,
                }))
            })
            .await
            .unwrap();
        }
    });
    let target = ClusterRpcTarget {
        node_id: 4,
        host: addr.ip().to_string(),
        port: addr.port(),
    };
    let transport = TcpClusterRpcTransport;

    expect_unexpected_response(transport.register_broker_to(
        &target,
        RegisterBrokerRequest {
            node_id: 9,
            advertised_host: "broker-9.local".to_string(),
            advertised_port: 39092,
        },
    ))
    .await;
    expect_unexpected_response(transport.broker_heartbeat_to(
        &target,
        BrokerHeartbeatRequest {
            node_id: 9,
            broker_epoch: 1,
            timestamp_ms: 123,
        },
    ))
    .await;
    expect_unexpected_response(transport.update_partition_replication_to(
        &target,
        UpdatePartitionReplicationRequest {
            topic_name: "missing.topic".to_string(),
            partition_index: 0,
            replicas: vec![4],
            isr: vec![4],
            leader_epoch: 1,
        },
    ))
    .await;
    expect_unexpected_response(transport.begin_partition_reassignment_to(
        &target,
        BeginPartitionReassignmentRequest {
            topic_name: "missing.topic".to_string(),
            partition_index: 0,
            target_replicas: vec![4],
        },
    ))
    .await;
    expect_unexpected_response(transport.update_replica_progress_to(
        &target,
        UpdateReplicaProgressRequest {
            topic_name: "missing.topic".to_string(),
            partition_index: 0,
            leader_epoch: 1,
            broker_id: 4,
            log_end_offset: 0,
            last_caught_up_ms: 1,
        },
    ))
    .await;
    expect_unexpected_response(transport.apply_replica_records_to(
        &target,
        ApplyReplicaRecordsRequest {
            topic_name: "missing.topic".to_string(),
            partition_index: 0,
            records: Vec::new(),
            now_ms: 1,
        },
    ))
    .await;

    server.abort();
    let _ = server.await;
}

#[tokio::test]
async fn tcp_transport_typed_wrappers_accept_expected_response_variants() {
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        for _ in 0..4 {
            TcpClusterRpcTransport::serve_once(&listener, |request| match request {
                ClusterRpcRequest::UpdatePartitionReplication(_) => {
                    Ok(ClusterRpcResponse::UpdatePartitionReplication(
                        UpdatePartitionReplicationResponse {
                            accepted: true,
                            metadata_offset: 11,
                        },
                    ))
                }
                ClusterRpcRequest::BeginPartitionReassignment(_) => Ok(
                    ClusterRpcResponse::BeginPartitionReassignment(PartitionReassignmentResponse {
                        accepted: true,
                        metadata_offset: 12,
                    }),
                ),
                ClusterRpcRequest::UpdateReplicaProgress(_) => Ok(
                    ClusterRpcResponse::UpdateReplicaProgress(UpdateReplicaProgressResponse {
                        accepted: true,
                        metadata_offset: 13,
                        high_watermark: 13,
                    }),
                ),
                ClusterRpcRequest::ApplyReplicaRecords(_) => Ok(
                    ClusterRpcResponse::ApplyReplicaRecords(ApplyReplicaRecordsResponse {
                        accepted: true,
                        next_offset: 14,
                    }),
                ),
                other => panic!("unexpected request {other:?}"),
            })
            .await
            .unwrap();
        }
    });
    let target = ClusterRpcTarget {
        node_id: 4,
        host: addr.ip().to_string(),
        port: addr.port(),
    };
    let transport = TcpClusterRpcTransport;

    let replication = transport
        .update_partition_replication_to(
            &target,
            UpdatePartitionReplicationRequest {
                topic_name: "typed-success.topic".to_string(),
                partition_index: 0,
                replicas: vec![1, 2],
                isr: vec![1],
                leader_epoch: 1,
            },
        )
        .await
        .unwrap();
    assert!(replication.accepted);
    assert_eq!(replication.metadata_offset, 11);

    let reassignment = transport
        .begin_partition_reassignment_to(
            &target,
            BeginPartitionReassignmentRequest {
                topic_name: "typed-success.topic".to_string(),
                partition_index: 0,
                target_replicas: vec![2, 3],
            },
        )
        .await
        .unwrap();
    assert!(reassignment.accepted);
    assert_eq!(reassignment.metadata_offset, 12);

    let progress = transport
        .update_replica_progress_to(
            &target,
            UpdateReplicaProgressRequest {
                topic_name: "typed-success.topic".to_string(),
                partition_index: 0,
                leader_epoch: 1,
                broker_id: 2,
                log_end_offset: 9,
                last_caught_up_ms: 100,
            },
        )
        .await
        .unwrap();
    assert!(progress.accepted);
    assert_eq!(progress.high_watermark, 13);

    let applied = transport
        .apply_replica_records_to(
            &target,
            ApplyReplicaRecordsRequest {
                topic_name: "typed-success.topic".to_string(),
                partition_index: 0,
                records: Vec::new(),
                now_ms: 100,
            },
        )
        .await
        .unwrap();
    assert!(applied.accepted);
    assert_eq!(applied.next_offset, 14);

    server.abort();
    let _ = server.await;
}

#[tokio::test]
async fn tcp_transport_update_partition_leader_wrapper_rejects_unexpected_response() {
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        TcpClusterRpcTransport::serve_once(&listener, |_| {
            Ok(ClusterRpcResponse::Vote(VoteResponse {
                term: 1,
                vote_granted: true,
            }))
        })
        .await
        .unwrap();
    });
    let transport = TcpClusterRpcTransport;

    let err = transport
        .update_partition_leader_to(
            &ClusterRpcTarget {
                node_id: 4,
                host: addr.ip().to_string(),
                port: addr.port(),
            },
            UpdatePartitionLeaderRequest {
                topic_name: "typed-error.topic".to_string(),
                partition_index: 0,
                leader_id: 4,
                leader_epoch: 1,
            },
        )
        .await
        .unwrap_err()
        .to_string();

    assert!(err.contains("unexpected RPC response"));
    server.await.unwrap();
}
