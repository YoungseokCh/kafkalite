use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn two_process_cluster_rejects_metadata_mutation_on_non_controller_node() {
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
            FutureRecord::to("two.process.authority.topic")
                .payload("hello")
                .key("k"),
            Duration::from_secs(3),
        )
        .await
        .unwrap();

    let rejected = transport
        .update_partition_leader_to(
            &node1.controller_target,
            UpdatePartitionLeaderRequest {
                topic_name: "two.process.authority.topic".to_string(),
                partition_index: 0,
                leader_id: 1,
                leader_epoch: 2,
            },
        )
        .await
        .unwrap();
    assert!(!rejected.accepted);

    let rejected_again = transport
        .update_partition_leader_to(
            &node1.controller_target,
            UpdatePartitionLeaderRequest {
                topic_name: "two.process.authority.topic".to_string(),
                partition_index: 0,
                leader_id: 1,
                leader_epoch: 2,
            },
        )
        .await
        .unwrap();
    assert!(!rejected_again.accepted);

    let accepted = transport
        .update_partition_leader_to(
            &node2.controller_target,
            UpdatePartitionLeaderRequest {
                topic_name: "two.process.authority.topic".to_string(),
                partition_index: 0,
                leader_id: 2,
                leader_epoch: 2,
            },
        )
        .await
        .unwrap();
    assert!(accepted.accepted);

    let replication_rejected = transport
        .update_partition_replication_to(
            &node1.controller_target,
            UpdatePartitionReplicationRequest {
                topic_name: "two.process.authority.topic".to_string(),
                partition_index: 0,
                replicas: vec![1, 2],
                isr: vec![1],
                leader_epoch: 2,
            },
        )
        .await
        .unwrap();
    assert!(!replication_rejected.accepted);

    let replication_rejected_again = transport
        .update_partition_replication_to(
            &node1.controller_target,
            UpdatePartitionReplicationRequest {
                topic_name: "two.process.authority.topic".to_string(),
                partition_index: 0,
                replicas: vec![1, 2],
                isr: vec![1],
                leader_epoch: 2,
            },
        )
        .await
        .unwrap();
    assert!(!replication_rejected_again.accepted);

    let reassignment_rejected = transport
        .begin_partition_reassignment_to(
            &node1.controller_target,
            BeginPartitionReassignmentRequest {
                topic_name: "two.process.authority.topic".to_string(),
                partition_index: 0,
                target_replicas: vec![2, 3],
            },
        )
        .await
        .unwrap();
    assert!(!reassignment_rejected.accepted);

    let reassignment_rejected_again = transport
        .begin_partition_reassignment_to(
            &node1.controller_target,
            BeginPartitionReassignmentRequest {
                topic_name: "two.process.authority.topic".to_string(),
                partition_index: 0,
                target_replicas: vec![2, 3],
            },
        )
        .await
        .unwrap();
    assert!(!reassignment_rejected_again.accepted);

    let reassignment_advance_rejected = transport
        .send_to(
            &node1.controller_target,
            ClusterRpcRequest::AdvancePartitionReassignment(AdvancePartitionReassignmentRequest {
                topic_name: "two.process.authority.topic".to_string(),
                partition_index: 0,
                step: kafkalite_server::cluster::ReassignmentStep::Copying,
            }),
        )
        .await
        .unwrap();
    let ClusterRpcResponse::AdvancePartitionReassignment(reassignment_advance_rejected) =
        reassignment_advance_rejected
    else {
        panic!("unexpected response variant")
    };
    assert!(!reassignment_advance_rejected.accepted);

    let reassignment_advance_rejected_again = transport
        .send_to(
            &node1.controller_target,
            ClusterRpcRequest::AdvancePartitionReassignment(AdvancePartitionReassignmentRequest {
                topic_name: "two.process.authority.topic".to_string(),
                partition_index: 0,
                step: kafkalite_server::cluster::ReassignmentStep::Copying,
            }),
        )
        .await
        .unwrap();
    let ClusterRpcResponse::AdvancePartitionReassignment(reassignment_advance_rejected_again) =
        reassignment_advance_rejected_again
    else {
        panic!("unexpected response variant")
    };
    assert!(!reassignment_advance_rejected_again.accepted);

    let progress_rejected = transport
        .update_replica_progress_to(
            &node1.controller_target,
            UpdateReplicaProgressRequest {
                topic_name: "two.process.authority.topic".to_string(),
                partition_index: 0,
                leader_epoch: 2,
                broker_id: 1,
                log_end_offset: 1,
                last_caught_up_ms: 123,
            },
        )
        .await
        .unwrap();
    assert!(!progress_rejected.accepted);

    let _ = node1.child.kill();
    let _ = node1.child.wait();
    let _ = node2.child.kill();
    let _ = node2.child.wait();
}
