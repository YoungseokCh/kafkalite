use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn two_process_cluster_rejects_broker_control_on_non_controller_node() {
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

    let rejected = transport
        .register_broker_to(
            &node1.controller_target,
            RegisterBrokerRequest {
                node_id: 9,
                advertised_host: "127.0.0.1".to_string(),
                advertised_port: 39092,
            },
        )
        .await
        .unwrap();
    assert!(!rejected.accepted);
    assert_eq!(rejected.leader_id, Some(2));
    assert_eq!(rejected.controller_epoch, 1);
    let rejected_again = transport
        .register_broker_to(
            &node1.controller_target,
            RegisterBrokerRequest {
                node_id: 9,
                advertised_host: "127.0.0.1".to_string(),
                advertised_port: 39092,
            },
        )
        .await
        .unwrap();
    assert!(!rejected_again.accepted);
    assert_eq!(rejected_again.leader_id, Some(2));
    assert_eq!(rejected_again.controller_epoch, 1);

    let accepted = transport
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
    assert!(accepted.accepted);

    let heartbeat_rejected = transport
        .broker_heartbeat_to(
            &node1.controller_target,
            BrokerHeartbeatRequest {
                node_id: 9,
                broker_epoch: accepted.broker_epoch,
                timestamp_ms: 123,
            },
        )
        .await
        .unwrap();
    assert!(!heartbeat_rejected.accepted);
    assert_eq!(heartbeat_rejected.leader_id, Some(2));
    assert_eq!(heartbeat_rejected.controller_epoch, 1);
    let heartbeat_rejected_again = transport
        .broker_heartbeat_to(
            &node1.controller_target,
            BrokerHeartbeatRequest {
                node_id: 9,
                broker_epoch: accepted.broker_epoch,
                timestamp_ms: 124,
            },
        )
        .await
        .unwrap();
    assert!(!heartbeat_rejected_again.accepted);
    assert_eq!(heartbeat_rejected_again.leader_id, Some(2));
    assert_eq!(heartbeat_rejected_again.controller_epoch, 1);

    let heartbeat_accepted = transport
        .broker_heartbeat_to(
            &node2.controller_target,
            BrokerHeartbeatRequest {
                node_id: 9,
                broker_epoch: accepted.broker_epoch,
                timestamp_ms: 123,
            },
        )
        .await
        .unwrap();
    assert!(heartbeat_accepted.accepted);

    let _ = node1.child.kill();
    let _ = node1.child.wait();
    let _ = node2.child.kill();
    let _ = node2.child.wait();
}
