use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn two_process_cluster_supports_combined_control_plane_workflow() {
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
            FutureRecord::to("two.process.workflow.topic")
                .payload("hello")
                .key("k"),
            Duration::from_secs(3),
        )
        .await
        .unwrap();

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
    assert_eq!(registration.controller_epoch, 2);
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
    assert_eq!(heartbeat.leader_id, Some(2));
    assert_eq!(heartbeat.controller_epoch, 2);
    let heartbeat_again = transport
        .broker_heartbeat_to(
            &node2.controller_target,
            BrokerHeartbeatRequest {
                node_id: 9,
                broker_epoch: registration.broker_epoch,
                timestamp_ms: 126,
            },
        )
        .await
        .unwrap();
    assert!(heartbeat_again.accepted);
    assert_eq!(heartbeat_again.leader_id, Some(2));
    assert_eq!(heartbeat_again.controller_epoch, 2);
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
    let update = transport
        .update_partition_leader_to(
            &node2.controller_target,
            UpdatePartitionLeaderRequest {
                topic_name: "two.process.workflow.topic".to_string(),
                partition_index: 0,
                leader_id: 2,
                leader_epoch: 2,
            },
        )
        .await
        .unwrap();
    assert!(update.accepted);
    let replication = transport
        .update_partition_replication_to(
            &node2.controller_target,
            UpdatePartitionReplicationRequest {
                topic_name: "two.process.workflow.topic".to_string(),
                partition_index: 0,
                replicas: vec![2, 9],
                isr: vec![2],
                leader_epoch: 2,
            },
        )
        .await
        .unwrap();
    assert!(replication.accepted);
    let progress = transport
        .update_replica_progress_to(
            &node2.controller_target,
            UpdateReplicaProgressRequest {
                topic_name: "two.process.workflow.topic".to_string(),
                partition_index: 0,
                leader_epoch: 2,
                broker_id: 9,
                log_end_offset: 1,
                last_caught_up_ms: 123,
            },
        )
        .await
        .unwrap();
    assert!(progress.accepted);
    assert_eq!(progress.high_watermark, 1);
    let repeated_progress = transport
        .update_replica_progress_to(
            &node2.controller_target,
            UpdateReplicaProgressRequest {
                topic_name: "two.process.workflow.topic".to_string(),
                partition_index: 0,
                leader_epoch: 2,
                broker_id: 9,
                log_end_offset: 1,
                last_caught_up_ms: 124,
            },
        )
        .await
        .unwrap();
    assert!(repeated_progress.accepted);
    assert_eq!(repeated_progress.high_watermark, 1);

    let state = transport
        .send_to(
            &node2.controller_target,
            ClusterRpcRequest::GetPartitionState(GetPartitionStateRequest {
                topic_name: "two.process.workflow.topic".to_string(),
                partition_index: 0,
            }),
        )
        .await
        .unwrap();
    let ClusterRpcResponse::GetPartitionState(state) = state else {
        panic!("unexpected response variant");
    };
    assert!(state.found);
    assert_eq!(state.leader_epoch, 2);
    assert_eq!(state.high_watermark, 1);

    let begin = transport
        .begin_partition_reassignment_to(
            &node2.controller_target,
            BeginPartitionReassignmentRequest {
                topic_name: "two.process.workflow.topic".to_string(),
                partition_index: 0,
                target_replicas: vec![2, 9],
            },
        )
        .await
        .unwrap();
    assert!(begin.accepted);
    let advance = transport
        .send_to(
            &node2.controller_target,
            ClusterRpcRequest::AdvancePartitionReassignment(AdvancePartitionReassignmentRequest {
                topic_name: "two.process.workflow.topic".to_string(),
                partition_index: 0,
                step: kafkalite_server::cluster::ReassignmentStep::Copying,
            }),
        )
        .await
        .unwrap();
    assert!(matches!(
        advance,
        ClusterRpcResponse::AdvancePartitionReassignment(_)
    ));

    let _ = node1.child.kill();
    let _ = node1.child.wait();
    let _ = node2.child.kill();
    let _ = node2.child.wait();
}
