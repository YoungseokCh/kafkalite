use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn process_control_plane_replica_fetch_reports_missing_partition_for_existing_topic() {
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

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", format!("127.0.0.1:{broker_port}"))
        .create()
        .unwrap();
    producer
        .send(
            FutureRecord::to("existing.fetch.partition.miss")
                .payload("hello")
                .key("k"),
            Duration::from_secs(3),
        )
        .await
        .unwrap();

    let transport = TcpClusterRpcTransport;
    let response = transport
        .send_to(
            &ClusterRpcTarget {
                node_id: 1,
                host: "127.0.0.1".to_string(),
                port: controller_port,
            },
            ClusterRpcRequest::ReplicaFetch(ReplicaFetchRequest {
                topic_name: "existing.fetch.partition.miss".to_string(),
                partition_index: 1,
                start_offset: 0,
                max_records: 10,
            }),
        )
        .await
        .unwrap();
    let ClusterRpcResponse::ReplicaFetch(response) = response else {
        panic!("unexpected response variant")
    };
    assert!(!response.found);
    assert_eq!(response.leader_id, -1);
    assert_eq!(response.leader_epoch, -1);
    assert_eq!(response.high_watermark, -1);
    assert_eq!(response.leader_log_end_offset, -1);
    assert!(response.records.is_empty());

    let repeated = transport
        .send_to(
            &ClusterRpcTarget {
                node_id: 1,
                host: "127.0.0.1".to_string(),
                port: controller_port,
            },
            ClusterRpcRequest::ReplicaFetch(ReplicaFetchRequest {
                topic_name: "existing.fetch.partition.miss".to_string(),
                partition_index: 1,
                start_offset: 0,
                max_records: 10,
            }),
        )
        .await
        .unwrap();
    let ClusterRpcResponse::ReplicaFetch(repeated) = repeated else {
        panic!("unexpected response variant")
    };
    assert!(!repeated.found);
    assert_eq!(repeated.leader_id, -1);
    assert_eq!(repeated.leader_epoch, -1);
    assert_eq!(repeated.high_watermark, -1);
    assert_eq!(repeated.leader_log_end_offset, -1);
    assert!(repeated.records.is_empty());

    let _ = child.kill();
    let _ = child.wait();
}
