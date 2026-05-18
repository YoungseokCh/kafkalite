use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn process_control_plane_completes_valid_reassignment_lifecycle() {
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
            FutureRecord::to("process.reassign.valid.topic")
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
                topic_name: "process.reassign.valid.topic".to_string(),
                partition_index: 0,
                leader_id: 1,
                leader_epoch: 1,
            },
        )
        .await
        .unwrap();
    let _ = transport
        .update_partition_replication_to(
            &ClusterRpcTarget {
                node_id: 1,
                host: "127.0.0.1".to_string(),
                port: controller_port,
            },
            UpdatePartitionReplicationRequest {
                topic_name: "process.reassign.valid.topic".to_string(),
                partition_index: 0,
                replicas: vec![1],
                isr: vec![1],
                leader_epoch: 1,
            },
        )
        .await
        .unwrap();
    let _ = transport
        .begin_partition_reassignment_to(
            &ClusterRpcTarget {
                node_id: 1,
                host: "127.0.0.1".to_string(),
                port: controller_port,
            },
            BeginPartitionReassignmentRequest {
                topic_name: "process.reassign.valid.topic".to_string(),
                partition_index: 0,
                target_replicas: vec![2, 3],
            },
        )
        .await
        .unwrap();
    let _ = transport
        .update_replica_progress_to(
            &ClusterRpcTarget {
                node_id: 1,
                host: "127.0.0.1".to_string(),
                port: controller_port,
            },
            UpdateReplicaProgressRequest {
                topic_name: "process.reassign.valid.topic".to_string(),
                partition_index: 0,
                leader_epoch: 1,
                broker_id: 2,
                log_end_offset: 1,
                last_caught_up_ms: 123,
            },
        )
        .await
        .unwrap();
    let _ = transport
        .update_replica_progress_to(
            &ClusterRpcTarget {
                node_id: 1,
                host: "127.0.0.1".to_string(),
                port: controller_port,
            },
            UpdateReplicaProgressRequest {
                topic_name: "process.reassign.valid.topic".to_string(),
                partition_index: 0,
                leader_epoch: 1,
                broker_id: 3,
                log_end_offset: 1,
                last_caught_up_ms: 123,
            },
        )
        .await
        .unwrap();

    for step in [
        kafkalite_server::cluster::ReassignmentStep::ExpandingIsr,
        kafkalite_server::cluster::ReassignmentStep::LeaderSwitch,
        kafkalite_server::cluster::ReassignmentStep::Shrinking,
        kafkalite_server::cluster::ReassignmentStep::Complete,
    ] {
        let response = transport
            .send_to(
                &ClusterRpcTarget {
                    node_id: 1,
                    host: "127.0.0.1".to_string(),
                    port: controller_port,
                },
                ClusterRpcRequest::AdvancePartitionReassignment(
                    AdvancePartitionReassignmentRequest {
                        topic_name: "process.reassign.valid.topic".to_string(),
                        partition_index: 0,
                        step,
                    },
                ),
            )
            .await
            .unwrap();
        let ClusterRpcResponse::AdvancePartitionReassignment(response) = response else {
            panic!("unexpected response variant")
        };
        assert!(response.accepted);
    }

    let state = transport
        .send_to(
            &ClusterRpcTarget {
                node_id: 1,
                host: "127.0.0.1".to_string(),
                port: controller_port,
            },
            ClusterRpcRequest::GetPartitionState(GetPartitionStateRequest {
                topic_name: "process.reassign.valid.topic".to_string(),
                partition_index: 0,
            }),
        )
        .await
        .unwrap();
    let ClusterRpcResponse::GetPartitionState(state) = state else {
        panic!("unexpected response variant")
    };
    assert!(state.found);
    assert_eq!(state.leader_id, 2);
    assert_eq!(state.leader_epoch, 2);

    let repeated_state = transport
        .send_to(
            &ClusterRpcTarget {
                node_id: 1,
                host: "127.0.0.1".to_string(),
                port: controller_port,
            },
            ClusterRpcRequest::GetPartitionState(GetPartitionStateRequest {
                topic_name: "process.reassign.valid.topic".to_string(),
                partition_index: 0,
            }),
        )
        .await
        .unwrap();
    let ClusterRpcResponse::GetPartitionState(repeated_state) = repeated_state else {
        panic!("unexpected response variant")
    };
    assert!(repeated_state.found);
    assert_eq!(repeated_state.leader_id, state.leader_id);
    assert_eq!(repeated_state.leader_epoch, state.leader_epoch);
    assert_eq!(repeated_state.high_watermark, state.high_watermark);
    assert_eq!(
        repeated_state.leader_log_end_offset,
        state.leader_log_end_offset
    );

    let fetch = transport
        .send_to(
            &ClusterRpcTarget {
                node_id: 1,
                host: "127.0.0.1".to_string(),
                port: controller_port,
            },
            ClusterRpcRequest::ReplicaFetch(ReplicaFetchRequest {
                topic_name: "process.reassign.valid.topic".to_string(),
                partition_index: 0,
                start_offset: 0,
                max_records: 10,
            }),
        )
        .await
        .unwrap();
    let ClusterRpcResponse::ReplicaFetch(fetch) = fetch else {
        panic!("unexpected response variant")
    };
    assert_eq!(fetch.leader_epoch, 2);
    assert_eq!(fetch.leader_id, 2);
    assert_eq!(fetch.high_watermark, 0);
    assert_eq!(fetch.records.len(), 1);

    let repeated_fetch = transport
        .send_to(
            &ClusterRpcTarget {
                node_id: 1,
                host: "127.0.0.1".to_string(),
                port: controller_port,
            },
            ClusterRpcRequest::ReplicaFetch(ReplicaFetchRequest {
                topic_name: "process.reassign.valid.topic".to_string(),
                partition_index: 0,
                start_offset: 0,
                max_records: 10,
            }),
        )
        .await
        .unwrap();
    let ClusterRpcResponse::ReplicaFetch(repeated_fetch) = repeated_fetch else {
        panic!("unexpected response variant")
    };
    assert_eq!(repeated_fetch.leader_id, fetch.leader_id);
    assert_eq!(repeated_fetch.leader_epoch, fetch.leader_epoch);
    assert_eq!(repeated_fetch.high_watermark, fetch.high_watermark);
    assert_eq!(repeated_fetch.records.len(), fetch.records.len());

    let restart_reassignment = transport
        .begin_partition_reassignment_to(
            &ClusterRpcTarget {
                node_id: 1,
                host: "127.0.0.1".to_string(),
                port: controller_port,
            },
            BeginPartitionReassignmentRequest {
                topic_name: "process.reassign.valid.topic".to_string(),
                partition_index: 0,
                target_replicas: vec![3, 4],
            },
        )
        .await
        .unwrap();
    assert!(restart_reassignment.accepted);

    let _ = child.kill();
    let _ = child.wait();
}
