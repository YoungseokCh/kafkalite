use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn process_control_plane_accepts_empty_replica_apply_as_noop() {
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
            FutureRecord::to("process.apply.noop.topic")
                .payload("hello")
                .key("k"),
            Duration::from_secs(3),
        )
        .await
        .unwrap();

    let transport = TcpClusterRpcTransport;
    let response = transport
        .apply_replica_records_to(
            &ClusterRpcTarget {
                node_id: 1,
                host: "127.0.0.1".to_string(),
                port: controller_port,
            },
            ApplyReplicaRecordsRequest {
                topic_name: "process.apply.noop.topic".to_string(),
                partition_index: 0,
                records: vec![],
                now_ms: 123,
            },
        )
        .await
        .unwrap();

    assert!(response.accepted);
    assert_eq!(response.next_offset, 1);

    let repeated = transport
        .apply_replica_records_to(
            &ClusterRpcTarget {
                node_id: 1,
                host: "127.0.0.1".to_string(),
                port: controller_port,
            },
            ApplyReplicaRecordsRequest {
                topic_name: "process.apply.noop.topic".to_string(),
                partition_index: 0,
                records: vec![],
                now_ms: 124,
            },
        )
        .await
        .unwrap();

    assert!(repeated.accepted);
    assert_eq!(repeated.next_offset, 1);

    let fetched = transport
        .send_to(
            &ClusterRpcTarget {
                node_id: 1,
                host: "127.0.0.1".to_string(),
                port: controller_port,
            },
            ClusterRpcRequest::ReplicaFetch(ReplicaFetchRequest {
                topic_name: "process.apply.noop.topic".to_string(),
                partition_index: 0,
                start_offset: 0,
                max_records: 10,
            }),
        )
        .await
        .unwrap();
    let ClusterRpcResponse::ReplicaFetch(fetched) = fetched else {
        panic!("unexpected response variant")
    };
    assert!(fetched.found);
    assert_eq!(fetched.leader_log_end_offset, 1);
    assert_eq!(fetched.records.len(), 1);
    assert_eq!(fetched.records[0].offset, 0);

    let _ = child.kill();
    let _ = child.wait();
}
