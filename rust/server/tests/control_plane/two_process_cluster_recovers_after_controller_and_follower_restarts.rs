use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn two_process_cluster_recovers_after_controller_and_follower_restarts() {
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
    let response = transport
        .send_to(
            &node1.controller_target,
            ClusterRpcRequest::AppendMetadata(AppendMetadataRequest {
                term: 1,
                leader_id: 1,
                prev_metadata_offset: -1,
                records: vec![kafkalite_server::cluster::MetadataRecord::SetController {
                    controller_id: 1,
                }],
            }),
        )
        .await
        .unwrap();
    assert!(matches!(response, ClusterRpcResponse::AppendMetadata(_)));

    create_topic(&node1.bootstrap, "two.process.combined.restart.topic").await;

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

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &node2.bootstrap)
        .create()
        .unwrap();
    producer
        .send(
            FutureRecord::to("two.process.combined.restart.topic")
                .payload("hello")
                .key("k"),
            Duration::from_secs(3),
        )
        .await
        .unwrap();

    let _ = transport
        .update_partition_leader_to(
            &node2.controller_target,
            UpdatePartitionLeaderRequest {
                topic_name: "two.process.combined.restart.topic".to_string(),
                partition_index: 0,
                leader_id: 2,
                leader_epoch: 1,
            },
        )
        .await
        .unwrap();
    let _ = transport
        .update_partition_replication_to(
            &node2.controller_target,
            UpdatePartitionReplicationRequest {
                topic_name: "two.process.combined.restart.topic".to_string(),
                partition_index: 0,
                replicas: vec![2, 1],
                isr: vec![2],
                leader_epoch: 1,
            },
        )
        .await
        .unwrap();
    let fetched = transport
        .send_to(
            &node2.controller_target,
            ClusterRpcRequest::ReplicaFetch(ReplicaFetchRequest {
                topic_name: "two.process.combined.restart.topic".to_string(),
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
    let _ = transport
        .apply_replica_records_to(
            &node1.controller_target,
            ApplyReplicaRecordsRequest {
                topic_name: "two.process.combined.restart.topic".to_string(),
                partition_index: 0,
                records: fetched.records,
                now_ms: 123,
            },
        )
        .await
        .unwrap();
    let _ = transport
        .update_replica_progress_to(
            &node2.controller_target,
            UpdateReplicaProgressRequest {
                topic_name: "two.process.combined.restart.topic".to_string(),
                partition_index: 0,
                leader_epoch: 1,
                broker_id: 1,
                log_end_offset: 1,
                last_caught_up_ms: 123,
            },
        )
        .await
        .unwrap();

    let _ = node1.child.kill();
    let _ = node1.child.wait();
    let _ = node2.child.kill();
    let _ = node2.child.wait();
    node1 = spawn_cluster_process(tempdir.path(), 1, broker1, controller1, &quorum);
    node2 = spawn_cluster_process(tempdir.path(), 2, broker2, controller2, &quorum);
    wait_until_broker_ready(&node1.bootstrap, Duration::from_secs(10)).unwrap();
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

    let state = transport
        .send_to(
            &node2.controller_target,
            ClusterRpcRequest::GetPartitionState(GetPartitionStateRequest {
                topic_name: "two.process.combined.restart.topic".to_string(),
                partition_index: 0,
            }),
        )
        .await
        .unwrap();
    let ClusterRpcResponse::GetPartitionState(state) = state else {
        panic!("unexpected response variant")
    };
    assert!(state.found);
    assert_eq!(state.high_watermark, 1);

    let fetched = transport
        .send_to(
            &node1.controller_target,
            ClusterRpcRequest::ReplicaFetch(ReplicaFetchRequest {
                topic_name: "two.process.combined.restart.topic".to_string(),
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
    assert_eq!(fetched.records.len(), 1);
    assert_eq!(fetched.records[0].offset, 0);

    let _ = node1.child.kill();
    let _ = node1.child.wait();
    let _ = node2.child.kill();
    let _ = node2.child.wait();
}
