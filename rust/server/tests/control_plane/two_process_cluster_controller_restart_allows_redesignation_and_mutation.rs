use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn two_process_cluster_controller_restart_allows_redesignation_and_mutation() {
    if std::env::var("CARGO_BIN_EXE_kafkalite").is_err() {
        return;
    }
    let tempdir = tempdir().unwrap();
    let broker1 = free_port();
    let broker2 = free_port();
    let controller1 = free_port();
    let controller2 = free_port();
    let quorum = format!("1@127.0.0.1:{controller1},2@127.0.0.1:{controller2}");

    let mut node1 = spawn_cluster_process(tempdir.path(), 1, broker1, controller1, &quorum);
    let mut node2 = spawn_cluster_process(tempdir.path(), 2, broker2, controller2, &quorum);
    wait_until_broker_ready(&node1.bootstrap, Duration::from_secs(10)).unwrap();
    wait_until_broker_ready(&node2.bootstrap, Duration::from_secs(10)).unwrap();

    let transport = TcpClusterRpcTransport;
    for target in [&node1.controller_target, &node2.controller_target] {
        let response = transport
            .send_to(
                target,
                ClusterRpcRequest::AppendMetadata(AppendMetadataRequest {
                    term: 1,
                    leader_id: 2,
                    prev_metadata_offset: -1,
                    records: vec![kafkalite_server::cluster::MetadataRecord::SetController {
                        controller_id: 2,
                    }],
                }),
            )
            .await
            .unwrap();
        assert!(matches!(response, ClusterRpcResponse::AppendMetadata(_)));
    }

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &node2.bootstrap)
        .create()
        .unwrap();
    producer
        .send(
            FutureRecord::to("two.process.restart.topic")
                .payload("hello")
                .key("k"),
            Duration::from_secs(3),
        )
        .await
        .unwrap();

    let _ = node2.child.kill();
    let _ = node2.child.wait();
    node2 = spawn_cluster_process(tempdir.path(), 2, broker2, controller2, &quorum);
    wait_until_broker_ready(&node2.bootstrap, Duration::from_secs(10)).unwrap();

    for target in [&node1.controller_target, &node2.controller_target] {
        let response = transport
            .send_to(
                target,
                ClusterRpcRequest::AppendMetadata(AppendMetadataRequest {
                    term: 2,
                    leader_id: 2,
                    prev_metadata_offset: -1,
                    records: vec![kafkalite_server::cluster::MetadataRecord::SetController {
                        controller_id: 2,
                    }],
                }),
            )
            .await
            .unwrap();
        assert!(matches!(response, ClusterRpcResponse::AppendMetadata(_)));
    }

    let update = transport
        .update_partition_leader_to(
            &node2.controller_target,
            UpdatePartitionLeaderRequest {
                topic_name: "two.process.restart.topic".to_string(),
                partition_index: 0,
                leader_id: 2,
                leader_epoch: 2,
            },
        )
        .await
        .unwrap();
    assert!(update.accepted);
    let registration = transport
        .register_broker_to(
            &node2.controller_target,
            RegisterBrokerRequest {
                node_id: 9,
                advertised_host: "127.0.0.1".to_string(),
                advertised_port: 39092,
            },
        )
        .await
        .unwrap();
    assert!(registration.accepted);
    let heartbeat = transport
        .broker_heartbeat_to(
            &node2.controller_target,
            BrokerHeartbeatRequest {
                node_id: 9,
                broker_epoch: registration.broker_epoch,
                timestamp_ms: 123,
            },
        )
        .await
        .unwrap();
    assert!(heartbeat.accepted);
    let stale_heartbeat = transport
        .broker_heartbeat_to(
            &node2.controller_target,
            BrokerHeartbeatRequest {
                node_id: 9,
                broker_epoch: registration.broker_epoch - 1,
                timestamp_ms: 124,
            },
        )
        .await
        .unwrap();
    assert!(!stale_heartbeat.accepted);
    let stale_heartbeat_again = transport
        .broker_heartbeat_to(
            &node2.controller_target,
            BrokerHeartbeatRequest {
                node_id: 9,
                broker_epoch: registration.broker_epoch - 1,
                timestamp_ms: 125,
            },
        )
        .await
        .unwrap();
    assert!(!stale_heartbeat_again.accepted);
    assert_eq!(stale_heartbeat_again.leader_id, Some(2));
    assert_eq!(stale_heartbeat_again.controller_epoch, 3);
    let stale = transport
        .send_to(
            &node2.controller_target,
            ClusterRpcRequest::AppendMetadata(AppendMetadataRequest {
                term: 2,
                leader_id: 1,
                prev_metadata_offset: update.metadata_offset,
                records: vec![kafkalite_server::cluster::MetadataRecord::SetController {
                    controller_id: 1,
                }],
            }),
        )
        .await
        .unwrap();
    let ClusterRpcResponse::AppendMetadata(stale) = stale else {
        panic!("unexpected response variant")
    };
    assert!(!stale.accepted);

    let stale_again = transport
        .send_to(
            &node2.controller_target,
            ClusterRpcRequest::AppendMetadata(AppendMetadataRequest {
                term: 2,
                leader_id: 1,
                prev_metadata_offset: update.metadata_offset,
                records: vec![kafkalite_server::cluster::MetadataRecord::SetController {
                    controller_id: 1,
                }],
            }),
        )
        .await
        .unwrap();
    let ClusterRpcResponse::AppendMetadata(stale_again) = stale_again else {
        panic!("unexpected response variant")
    };
    assert!(!stale_again.accepted);

    let state = transport
        .send_to(
            &node2.controller_target,
            ClusterRpcRequest::GetPartitionState(GetPartitionStateRequest {
                topic_name: "two.process.restart.topic".to_string(),
                partition_index: 0,
            }),
        )
        .await
        .unwrap();
    let ClusterRpcResponse::GetPartitionState(state) = state else {
        panic!("unexpected response variant")
    };
    assert!(state.found);
    assert_eq!(state.leader_epoch, 2);

    let _ = node1.child.kill();
    let _ = node1.child.wait();
    let _ = node2.child.kill();
    let _ = node2.child.wait();
}
