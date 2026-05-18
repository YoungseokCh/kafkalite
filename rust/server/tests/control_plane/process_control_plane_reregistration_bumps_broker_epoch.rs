use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn process_control_plane_reregistration_bumps_broker_epoch() {
    if std::env::var("CARGO_BIN_EXE_kafkalite").is_err() {
        return;
    }
    let tempdir = tempdir().unwrap();
    let broker_port = free_port();
    let controller_port = free_port();
    let config_path = tempdir.path().join("server.properties");
    fs::write(
        &config_path,
        format!(
            concat!(
                "process.roles=broker,controller\n",
                "node.id=1\n",
                "listeners=PLAINTEXT://127.0.0.1:{broker},CONTROLLER://127.0.0.1:{controller}\n",
                "advertised.listeners=PLAINTEXT://127.0.0.1:{broker}\n",
                "controller.listener.names=CONTROLLER\n",
                "controller.quorum.voters=1@127.0.0.1:{controller}\n",
                "cluster.id=test-cluster\n",
                "log.dirs={data}\n",
                "num.partitions=1\n"
            ),
            broker = broker_port,
            controller = controller_port,
            data = tempdir.path().join("data").display(),
        ),
    )
    .unwrap();

    let mut child = spawn_broker(&config_path);
    wait_until_broker_ready(&format!("127.0.0.1:{broker_port}"), Duration::from_secs(10)).unwrap();

    let transport = TcpClusterRpcTransport;
    let first = transport
        .register_broker_to(
            &ClusterRpcTarget {
                node_id: 1,
                host: "127.0.0.1".to_string(),
                port: controller_port,
            },
            RegisterBrokerRequest {
                node_id: 9,
                advertised_host: "127.0.0.1".to_string(),
                advertised_port: 39092,
            },
        )
        .await
        .unwrap();
    let second = transport
        .register_broker_to(
            &ClusterRpcTarget {
                node_id: 1,
                host: "127.0.0.1".to_string(),
                port: controller_port,
            },
            RegisterBrokerRequest {
                node_id: 9,
                advertised_host: "127.0.0.1".to_string(),
                advertised_port: 39092,
            },
        )
        .await
        .unwrap();

    assert!(first.accepted);
    assert!(second.accepted);
    assert_eq!(first.leader_id, second.leader_id);
    assert_eq!(first.controller_epoch, second.controller_epoch);
    assert!(second.broker_epoch > first.broker_epoch);
    let third = transport
        .register_broker_to(
            &ClusterRpcTarget {
                node_id: 1,
                host: "127.0.0.1".to_string(),
                port: controller_port,
            },
            RegisterBrokerRequest {
                node_id: 9,
                advertised_host: "127.0.0.1".to_string(),
                advertised_port: 39092,
            },
        )
        .await
        .unwrap();
    assert!(third.accepted);
    assert_eq!(third.leader_id, second.leader_id);
    assert_eq!(third.controller_epoch, second.controller_epoch);
    assert!(third.broker_epoch > second.broker_epoch);

    let heartbeat = transport
        .broker_heartbeat_to(
            &ClusterRpcTarget {
                node_id: 1,
                host: "127.0.0.1".to_string(),
                port: controller_port,
            },
            BrokerHeartbeatRequest {
                node_id: 9,
                broker_epoch: third.broker_epoch,
                timestamp_ms: 123,
            },
        )
        .await
        .unwrap();
    assert!(heartbeat.accepted);

    let stale_heartbeat = transport
        .broker_heartbeat_to(
            &ClusterRpcTarget {
                node_id: 1,
                host: "127.0.0.1".to_string(),
                port: controller_port,
            },
            BrokerHeartbeatRequest {
                node_id: 9,
                broker_epoch: first.broker_epoch,
                timestamp_ms: 124,
            },
        )
        .await
        .unwrap();
    assert!(!stale_heartbeat.accepted);

    let stale_heartbeat_again = transport
        .broker_heartbeat_to(
            &ClusterRpcTarget {
                node_id: 1,
                host: "127.0.0.1".to_string(),
                port: controller_port,
            },
            BrokerHeartbeatRequest {
                node_id: 9,
                broker_epoch: first.broker_epoch,
                timestamp_ms: 125,
            },
        )
        .await
        .unwrap();
    assert!(!stale_heartbeat_again.accepted);

    let latest_heartbeat_again = transport
        .broker_heartbeat_to(
            &ClusterRpcTarget {
                node_id: 1,
                host: "127.0.0.1".to_string(),
                port: controller_port,
            },
            BrokerHeartbeatRequest {
                node_id: 9,
                broker_epoch: third.broker_epoch,
                timestamp_ms: 125,
            },
        )
        .await
        .unwrap();
    assert!(latest_heartbeat_again.accepted);
    assert_eq!(latest_heartbeat_again.leader_id, Some(1));
    assert_eq!(
        latest_heartbeat_again.controller_epoch,
        third.controller_epoch
    );

    let _ = child.kill();
    let _ = child.wait();
}
