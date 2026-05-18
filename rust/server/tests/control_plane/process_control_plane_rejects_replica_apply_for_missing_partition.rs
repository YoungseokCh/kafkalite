use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn process_control_plane_rejects_replica_apply_for_missing_partition() {
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
    let err = transport
        .apply_replica_records_to(
            &ClusterRpcTarget {
                node_id: 1,
                host: "127.0.0.1".to_string(),
                port: controller_port,
            },
            ApplyReplicaRecordsRequest {
                topic_name: "missing.apply.topic".to_string(),
                partition_index: 0,
                records: vec![],
                now_ms: 123,
            },
        )
        .await
        .unwrap_err()
        .to_string();

    assert!(err.contains("UnknownTopicOrPartition") || err.contains("unknown topic or partition"));

    let repeated_err = transport
        .apply_replica_records_to(
            &ClusterRpcTarget {
                node_id: 1,
                host: "127.0.0.1".to_string(),
                port: controller_port,
            },
            ApplyReplicaRecordsRequest {
                topic_name: "missing.apply.topic".to_string(),
                partition_index: 0,
                records: vec![],
                now_ms: 124,
            },
        )
        .await
        .unwrap_err()
        .to_string();

    assert!(
        repeated_err.contains("UnknownTopicOrPartition")
            || repeated_err.contains("unknown topic or partition")
    );

    let _ = child.kill();
    let _ = child.wait();
}
