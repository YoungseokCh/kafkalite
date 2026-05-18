use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn process_control_plane_accepts_register_broker_and_heartbeat() {
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
    let registration = transport
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
    assert!(registration.accepted);
    assert_eq!(registration.leader_id, Some(1));

    let heartbeat = transport
        .broker_heartbeat_to(
            &ClusterRpcTarget {
                node_id: 1,
                host: "127.0.0.1".to_string(),
                port: controller_port,
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
    assert_eq!(heartbeat.leader_id, Some(1));
    assert_eq!(heartbeat.controller_epoch, registration.controller_epoch);
    let heartbeat_again = transport
        .broker_heartbeat_to(
            &ClusterRpcTarget {
                node_id: 1,
                host: "127.0.0.1".to_string(),
                port: controller_port,
            },
            BrokerHeartbeatRequest {
                node_id: 9,
                broker_epoch: registration.broker_epoch,
                timestamp_ms: 124,
            },
        )
        .await
        .unwrap();
    assert!(heartbeat_again.accepted);
    assert_eq!(heartbeat_again.leader_id, Some(1));
    assert_eq!(
        heartbeat_again.controller_epoch,
        registration.controller_epoch
    );
    let wrong_node = transport
        .broker_heartbeat_to(
            &ClusterRpcTarget {
                node_id: 1,
                host: "127.0.0.1".to_string(),
                port: controller_port,
            },
            BrokerHeartbeatRequest {
                node_id: 99,
                broker_epoch: registration.broker_epoch,
                timestamp_ms: 123,
            },
        )
        .await
        .unwrap();
    assert!(!wrong_node.accepted);
    assert_eq!(wrong_node.leader_id, Some(1));
    assert_eq!(wrong_node.controller_epoch, registration.controller_epoch);

    let wrong_node_again = transport
        .broker_heartbeat_to(
            &ClusterRpcTarget {
                node_id: 1,
                host: "127.0.0.1".to_string(),
                port: controller_port,
            },
            BrokerHeartbeatRequest {
                node_id: 99,
                broker_epoch: registration.broker_epoch,
                timestamp_ms: 124,
            },
        )
        .await
        .unwrap();
    assert!(!wrong_node_again.accepted);
    assert_eq!(wrong_node_again.leader_id, Some(1));
    assert_eq!(
        wrong_node_again.controller_epoch,
        registration.controller_epoch
    );

    let _ = child.kill();
    let _ = child.wait();
}
