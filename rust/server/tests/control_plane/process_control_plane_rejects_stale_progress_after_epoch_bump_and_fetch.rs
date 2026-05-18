use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn process_control_plane_rejects_stale_progress_after_epoch_bump_and_fetch() {
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
            FutureRecord::to("process.fetch.stale.epoch.topic")
                .payload("hello")
                .key("k"),
            Duration::from_secs(3),
        )
        .await
        .unwrap();

    let transport = TcpClusterRpcTransport;
    let _ = transport
        .update_partition_leader_to(
            &ClusterRpcTarget {
                node_id: 1,
                host: "127.0.0.1".to_string(),
                port: controller_port,
            },
            UpdatePartitionLeaderRequest {
                topic_name: "process.fetch.stale.epoch.topic".to_string(),
                partition_index: 0,
                leader_id: 1,
                leader_epoch: 2,
            },
        )
        .await
        .unwrap();
    let fetched = transport
        .send_to(
            &ClusterRpcTarget {
                node_id: 1,
                host: "127.0.0.1".to_string(),
                port: controller_port,
            },
            ClusterRpcRequest::ReplicaFetch(ReplicaFetchRequest {
                topic_name: "process.fetch.stale.epoch.topic".to_string(),
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
    assert_eq!(fetched.leader_epoch, 2);

    let rejected = transport
        .update_replica_progress_to(
            &ClusterRpcTarget {
                node_id: 1,
                host: "127.0.0.1".to_string(),
                port: controller_port,
            },
            UpdateReplicaProgressRequest {
                topic_name: "process.fetch.stale.epoch.topic".to_string(),
                partition_index: 0,
                leader_epoch: 1,
                broker_id: 1,
                log_end_offset: 1,
                last_caught_up_ms: 123,
            },
        )
        .await
        .unwrap();
    assert!(!rejected.accepted);

    let repeated = transport
        .update_replica_progress_to(
            &ClusterRpcTarget {
                node_id: 1,
                host: "127.0.0.1".to_string(),
                port: controller_port,
            },
            UpdateReplicaProgressRequest {
                topic_name: "process.fetch.stale.epoch.topic".to_string(),
                partition_index: 0,
                leader_epoch: 1,
                broker_id: 1,
                log_end_offset: 1,
                last_caught_up_ms: 124,
            },
        )
        .await
        .unwrap();
    assert!(!repeated.accepted);

    let _ = child.kill();
    let _ = child.wait();
}
