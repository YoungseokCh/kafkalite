use std::fs;
use std::net::TcpListener;
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use kafkalite_server::cluster::{
    AdvancePartitionReassignmentRequest, AppendMetadataRequest, ApplyReplicaRecordsRequest,
    BeginPartitionReassignmentRequest, BrokerHeartbeatRequest, ClusterRpcRequest,
    ClusterRpcResponse, ClusterRpcTarget, GetPartitionStateRequest, RegisterBrokerRequest,
    ReplicaFetchRequest, TcpClusterRpcTransport, UpdatePartitionLeaderRequest,
    UpdatePartitionReplicationRequest, UpdateReplicaProgressRequest, VoteRequest,
};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::producer::{FutureProducer, FutureRecord};
use tempfile::tempdir;

struct ClusterProcess {
    bootstrap: String,
    controller_target: ClusterRpcTarget,
    child: Child,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn process_exposes_tcp_control_plane_service() {
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
    let response = transport
        .send_to(
            &ClusterRpcTarget {
                node_id: 1,
                host: "127.0.0.1".to_string(),
                port: controller_port,
            },
            ClusterRpcRequest::Vote(VoteRequest {
                term: 1,
                candidate_id: 1,
                last_metadata_offset: -1,
            }),
        )
        .await
        .unwrap();

    let ClusterRpcResponse::Vote(response) = response else {
        panic!("unexpected response variant");
    };
    assert!(response.vote_granted);

    let _ = child.kill();
    let _ = child.wait();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn two_process_cluster_exposes_control_plane_on_both_nodes() {
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
    let response1 = transport
        .send_to(
            &node1.controller_target,
            ClusterRpcRequest::Vote(VoteRequest {
                term: 1,
                candidate_id: 1,
                last_metadata_offset: -1,
            }),
        )
        .await
        .unwrap();
    let response2 = transport
        .send_to(
            &node2.controller_target,
            ClusterRpcRequest::Vote(VoteRequest {
                term: 1,
                candidate_id: 2,
                last_metadata_offset: -1,
            }),
        )
        .await
        .unwrap();

    assert!(matches!(response1, ClusterRpcResponse::Vote(_)));
    assert!(matches!(response2, ClusterRpcResponse::Vote(_)));

    let _ = node1.child.kill();
    let _ = node1.child.wait();
    let _ = node2.child.kill();
    let _ = node2.child.wait();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn process_control_plane_accepts_partition_leader_mutation() {
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
            FutureRecord::to("process.route.topic")
                .payload("hello")
                .key("k"),
            Duration::from_secs(3),
        )
        .await
        .unwrap();

    let transport = TcpClusterRpcTransport;
    let update = transport
        .update_partition_leader_to(
            &ClusterRpcTarget {
                node_id: 1,
                host: "127.0.0.1".to_string(),
                port: controller_port,
            },
            UpdatePartitionLeaderRequest {
                topic_name: "process.route.topic".to_string(),
                partition_index: 0,
                leader_id: 1,
                leader_epoch: 2,
            },
        )
        .await
        .unwrap();
    assert!(update.accepted);
    let repeated = transport
        .update_partition_leader_to(
            &ClusterRpcTarget {
                node_id: 1,
                host: "127.0.0.1".to_string(),
                port: controller_port,
            },
            UpdatePartitionLeaderRequest {
                topic_name: "process.route.topic".to_string(),
                partition_index: 0,
                leader_id: 1,
                leader_epoch: 2,
            },
        )
        .await
        .unwrap();
    assert!(repeated.accepted);

    let state = transport
        .send_to(
            &ClusterRpcTarget {
                node_id: 1,
                host: "127.0.0.1".to_string(),
                port: controller_port,
            },
            ClusterRpcRequest::GetPartitionState(GetPartitionStateRequest {
                topic_name: "process.route.topic".to_string(),
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

    let _ = child.kill();
    let _ = child.wait();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn process_control_plane_reports_missing_partition_state() {
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
    let state = transport
        .send_to(
            &ClusterRpcTarget {
                node_id: 1,
                host: "127.0.0.1".to_string(),
                port: controller_port,
            },
            ClusterRpcRequest::GetPartitionState(GetPartitionStateRequest {
                topic_name: "missing.partition.state.topic".to_string(),
                partition_index: 0,
            }),
        )
        .await
        .unwrap();
    let ClusterRpcResponse::GetPartitionState(state) = state else {
        panic!("unexpected response variant")
    };
    assert!(!state.found);
    assert_eq!(state.leader_id, -1);
    assert_eq!(state.leader_epoch, -1);
    assert_eq!(state.high_watermark, -1);
    assert_eq!(state.leader_log_end_offset, -1);

    let _ = child.kill();
    let _ = child.wait();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn process_control_plane_reports_missing_partition_for_existing_topic() {
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
            FutureRecord::to("existing.topic.partition.miss")
                .payload("hello")
                .key("k"),
            Duration::from_secs(3),
        )
        .await
        .unwrap();

    let transport = TcpClusterRpcTransport;
    let state = transport
        .send_to(
            &ClusterRpcTarget {
                node_id: 1,
                host: "127.0.0.1".to_string(),
                port: controller_port,
            },
            ClusterRpcRequest::GetPartitionState(GetPartitionStateRequest {
                topic_name: "existing.topic.partition.miss".to_string(),
                partition_index: 1,
            }),
        )
        .await
        .unwrap();
    let ClusterRpcResponse::GetPartitionState(state) = state else {
        panic!("unexpected response variant")
    };
    assert!(!state.found);
    assert_eq!(state.leader_id, -1);
    assert_eq!(state.leader_epoch, -1);
    assert_eq!(state.high_watermark, -1);
    assert_eq!(state.leader_log_end_offset, -1);

    let repeated = transport
        .send_to(
            &ClusterRpcTarget {
                node_id: 1,
                host: "127.0.0.1".to_string(),
                port: controller_port,
            },
            ClusterRpcRequest::GetPartitionState(GetPartitionStateRequest {
                topic_name: "existing.topic.partition.miss".to_string(),
                partition_index: 1,
            }),
        )
        .await
        .unwrap();
    let ClusterRpcResponse::GetPartitionState(repeated) = repeated else {
        panic!("unexpected response variant")
    };
    assert!(!repeated.found);
    assert_eq!(repeated.leader_id, -1);
    assert_eq!(repeated.leader_epoch, -1);
    assert_eq!(repeated.high_watermark, -1);
    assert_eq!(repeated.leader_log_end_offset, -1);

    let _ = child.kill();
    let _ = child.wait();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn process_control_plane_reports_existing_partition_state_after_produce() {
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
            FutureRecord::to("process.partition.state.topic")
                .payload("hello")
                .key("k"),
            Duration::from_secs(3),
        )
        .await
        .unwrap();

    let transport = TcpClusterRpcTransport;
    let state = transport
        .send_to(
            &ClusterRpcTarget {
                node_id: 1,
                host: "127.0.0.1".to_string(),
                port: controller_port,
            },
            ClusterRpcRequest::GetPartitionState(GetPartitionStateRequest {
                topic_name: "process.partition.state.topic".to_string(),
                partition_index: 0,
            }),
        )
        .await
        .unwrap();
    let ClusterRpcResponse::GetPartitionState(state) = state else {
        panic!("unexpected response variant")
    };
    assert!(state.found);
    assert_eq!(state.leader_id, 1);
    assert!(state.leader_epoch >= 0);
    assert!(state.high_watermark >= 0);
    assert_eq!(state.leader_log_end_offset, 1);

    let repeated = transport
        .send_to(
            &ClusterRpcTarget {
                node_id: 1,
                host: "127.0.0.1".to_string(),
                port: controller_port,
            },
            ClusterRpcRequest::GetPartitionState(GetPartitionStateRequest {
                topic_name: "process.partition.state.topic".to_string(),
                partition_index: 0,
            }),
        )
        .await
        .unwrap();
    let ClusterRpcResponse::GetPartitionState(repeated) = repeated else {
        panic!("unexpected response variant")
    };
    assert!(repeated.found);
    assert_eq!(repeated.leader_id, state.leader_id);
    assert_eq!(repeated.leader_epoch, state.leader_epoch);
    assert_eq!(repeated.high_watermark, state.high_watermark);
    assert_eq!(repeated.leader_log_end_offset, state.leader_log_end_offset);

    let _ = child.kill();
    let _ = child.wait();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn process_control_plane_rejects_progress_update_for_missing_partition() {
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
    let response = transport
        .update_replica_progress_to(
            &ClusterRpcTarget {
                node_id: 1,
                host: "127.0.0.1".to_string(),
                port: controller_port,
            },
            UpdateReplicaProgressRequest {
                topic_name: "missing.progress.topic".to_string(),
                partition_index: 0,
                leader_epoch: 1,
                broker_id: 1,
                log_end_offset: 1,
                last_caught_up_ms: 123,
            },
        )
        .await
        .unwrap();

    assert!(!response.accepted);
    assert_eq!(response.high_watermark, 0);

    let repeated = transport
        .update_replica_progress_to(
            &ClusterRpcTarget {
                node_id: 1,
                host: "127.0.0.1".to_string(),
                port: controller_port,
            },
            UpdateReplicaProgressRequest {
                topic_name: "missing.progress.topic".to_string(),
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
    assert_eq!(repeated.high_watermark, 0);

    let _ = child.kill();
    let _ = child.wait();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn process_control_plane_serves_replica_fetch_after_produce() {
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
            FutureRecord::to("process.fetch.topic")
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
                topic_name: "process.fetch.topic".to_string(),
                partition_index: 0,
                start_offset: 0,
                max_records: 10,
            }),
        )
        .await
        .unwrap();
    let ClusterRpcResponse::ReplicaFetch(response) = response else {
        panic!("unexpected response variant");
    };
    assert!(response.found);
    assert_eq!(response.records.len(), 1);
    assert_eq!(response.records[0].offset, 0);

    let _ = child.kill();
    let _ = child.wait();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn two_process_cluster_accepts_control_plane_mutation_on_designated_controller() {
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

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &node2.bootstrap)
        .create()
        .unwrap();
    producer
        .send(
            FutureRecord::to("two.process.route.topic")
                .payload("hello")
                .key("k"),
            Duration::from_secs(3),
        )
        .await
        .unwrap();

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

    let update = transport
        .update_partition_leader_to(
            &node2.controller_target,
            UpdatePartitionLeaderRequest {
                topic_name: "two.process.route.topic".to_string(),
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
    let replica_fetch = transport
        .send_to(
            &node2.controller_target,
            ClusterRpcRequest::ReplicaFetch(ReplicaFetchRequest {
                topic_name: "two.process.workflow.topic".to_string(),
                partition_index: 0,
                start_offset: 0,
                max_records: 10,
            }),
        )
        .await
        .unwrap();
    let ClusterRpcResponse::ReplicaFetch(replica_fetch) = replica_fetch else {
        panic!("unexpected response variant");
    };
    assert!(replica_fetch.found);
    assert_eq!(replica_fetch.records.len(), 1);
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
                topic_name: "two.process.route.topic".to_string(),
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

    let _ = node1.child.kill();
    let _ = node1.child.wait();
    let _ = node2.child.kill();
    let _ = node2.child.wait();
}

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

    let _ = child.kill();
    let _ = child.wait();
}

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

    let _ = child.kill();
    let _ = child.wait();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn two_process_cluster_accepts_register_broker_and_heartbeat_on_designated_controller() {
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
    assert_eq!(registration.leader_id, Some(2));
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

    let _ = node1.child.kill();
    let _ = node1.child.wait();
    let _ = node2.child.kill();
    let _ = node2.child.wait();
}

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
    assert_eq!(registration.controller_epoch, 1);
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
    assert_eq!(heartbeat.controller_epoch, 1);
    assert_eq!(heartbeat.leader_id, Some(2));
    assert_eq!(heartbeat.controller_epoch, 2);
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn two_process_cluster_supports_replica_fetch_and_apply_workflow() {
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

    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", &node1.bootstrap)
        .set("group.id", "replica-apply-seed")
        .create()
        .unwrap();
    let _ = consumer
        .fetch_metadata(Some("two.process.replica.topic"), Duration::from_secs(5))
        .unwrap();

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &node2.bootstrap)
        .create()
        .unwrap();
    producer
        .send(
            FutureRecord::to("two.process.replica.topic")
                .payload("hello")
                .key("k"),
            Duration::from_secs(3),
        )
        .await
        .unwrap();

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
    let _ = transport
        .update_partition_leader_to(
            &node2.controller_target,
            UpdatePartitionLeaderRequest {
                topic_name: "two.process.replica.topic".to_string(),
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
                topic_name: "two.process.replica.topic".to_string(),
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
                topic_name: "two.process.replica.topic".to_string(),
                partition_index: 0,
                start_offset: 0,
                max_records: 10,
            }),
        )
        .await
        .unwrap();
    let ClusterRpcResponse::ReplicaFetch(fetched) = fetched else {
        panic!("unexpected response variant");
    };
    assert_eq!(fetched.records.len(), 1);

    let applied = transport
        .apply_replica_records_to(
            &node1.controller_target,
            ApplyReplicaRecordsRequest {
                topic_name: "two.process.replica.topic".to_string(),
                partition_index: 0,
                records: fetched.records.clone(),
                now_ms: 123,
            },
        )
        .await
        .unwrap();
    assert!(applied.accepted);
    assert_eq!(applied.next_offset, 1);
    let fetched_from_follower = transport
        .send_to(
            &node1.controller_target,
            ClusterRpcRequest::ReplicaFetch(ReplicaFetchRequest {
                topic_name: "two.process.replica.topic".to_string(),
                partition_index: 0,
                start_offset: 0,
                max_records: 10,
            }),
        )
        .await
        .unwrap();
    let ClusterRpcResponse::ReplicaFetch(fetched_from_follower) = fetched_from_follower else {
        panic!("unexpected response variant");
    };
    assert_eq!(fetched_from_follower.records.len(), 1);
    assert_eq!(fetched_from_follower.records[0].offset, 0);

    let progress = transport
        .update_replica_progress_to(
            &node1.controller_target,
            UpdateReplicaProgressRequest {
                topic_name: "two.process.replica.topic".to_string(),
                partition_index: 0,
                leader_epoch: 1,
                broker_id: 1,
                log_end_offset: 1,
                last_caught_up_ms: 123,
            },
        )
        .await
        .unwrap();
    assert!(progress.accepted);
    let state = transport
        .send_to(
            &node1.controller_target,
            ClusterRpcRequest::GetPartitionState(GetPartitionStateRequest {
                topic_name: "two.process.replica.topic".to_string(),
                partition_index: 0,
            }),
        )
        .await
        .unwrap();
    let ClusterRpcResponse::GetPartitionState(state) = state else {
        panic!("unexpected response variant");
    };
    assert!(state.found);
    assert_eq!(state.high_watermark, 1);

    let _ = node1.child.kill();
    let _ = node1.child.wait();
    let _ = node2.child.kill();
    let _ = node2.child.wait();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn two_process_cluster_replica_sync_converges_after_multiple_rounds() {
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

    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", &node1.bootstrap)
        .set("group.id", "replica-apply-seed-2")
        .create()
        .unwrap();
    let _ = consumer
        .fetch_metadata(
            Some("two.process.replica.converge.topic"),
            Duration::from_secs(5),
        )
        .unwrap();

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &node2.bootstrap)
        .create()
        .unwrap();
    for payload in ["first", "second"] {
        producer
            .send(
                FutureRecord::to("two.process.replica.converge.topic")
                    .payload(payload)
                    .key("k"),
                Duration::from_secs(3),
            )
            .await
            .unwrap();
    }

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
    let _ = transport
        .update_partition_leader_to(
            &node2.controller_target,
            UpdatePartitionLeaderRequest {
                topic_name: "two.process.replica.converge.topic".to_string(),
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
                topic_name: "two.process.replica.converge.topic".to_string(),
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
                topic_name: "two.process.replica.converge.topic".to_string(),
                partition_index: 0,
                start_offset: 0,
                max_records: 10,
            }),
        )
        .await
        .unwrap();
    let ClusterRpcResponse::ReplicaFetch(fetched) = fetched else {
        panic!("unexpected response variant");
    };
    assert_eq!(fetched.records.len(), 2);
    let applied = transport
        .apply_replica_records_to(
            &node1.controller_target,
            ApplyReplicaRecordsRequest {
                topic_name: "two.process.replica.converge.topic".to_string(),
                partition_index: 0,
                records: fetched.records.clone(),
                now_ms: 123,
            },
        )
        .await
        .unwrap();
    assert!(applied.accepted);
    let progress = transport
        .update_replica_progress_to(
            &node1.controller_target,
            UpdateReplicaProgressRequest {
                topic_name: "two.process.replica.converge.topic".to_string(),
                partition_index: 0,
                leader_epoch: 1,
                broker_id: 1,
                log_end_offset: 2,
                last_caught_up_ms: 123,
            },
        )
        .await
        .unwrap();
    assert!(progress.accepted);
    assert_eq!(progress.high_watermark, 2);

    let state = transport
        .send_to(
            &node1.controller_target,
            ClusterRpcRequest::GetPartitionState(GetPartitionStateRequest {
                topic_name: "two.process.replica.converge.topic".to_string(),
                partition_index: 0,
            }),
        )
        .await
        .unwrap();
    let ClusterRpcResponse::GetPartitionState(state) = state else {
        panic!("unexpected response variant")
    };
    assert!(state.found);
    assert_eq!(state.high_watermark, 2);

    let _ = node1.child.kill();
    let _ = node1.child.wait();
    let _ = node2.child.kill();
    let _ = node2.child.wait();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn two_process_cluster_preserves_replica_state_after_follower_restart() {
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

    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", &node1.bootstrap)
        .set("group.id", "replica-restart-seed")
        .create()
        .unwrap();
    let _ = consumer
        .fetch_metadata(
            Some("two.process.replica.restart.topic"),
            Duration::from_secs(5),
        )
        .unwrap();
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &node2.bootstrap)
        .create()
        .unwrap();
    producer
        .send(
            FutureRecord::to("two.process.replica.restart.topic")
                .payload("hello")
                .key("k"),
            Duration::from_secs(3),
        )
        .await
        .unwrap();

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
    let _ = transport
        .update_partition_leader_to(
            &node2.controller_target,
            UpdatePartitionLeaderRequest {
                topic_name: "two.process.replica.restart.topic".to_string(),
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
                topic_name: "two.process.replica.restart.topic".to_string(),
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
                topic_name: "two.process.replica.restart.topic".to_string(),
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
                topic_name: "two.process.replica.restart.topic".to_string(),
                partition_index: 0,
                records: fetched.records,
                now_ms: 123,
            },
        )
        .await
        .unwrap();
    let _ = transport
        .update_replica_progress_to(
            &node1.controller_target,
            UpdateReplicaProgressRequest {
                topic_name: "two.process.replica.restart.topic".to_string(),
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
    node1 = spawn_cluster_process(tempdir.path(), 1, broker1, controller1, &quorum);
    wait_until_broker_ready(&node1.bootstrap, Duration::from_secs(10)).unwrap();

    let state = transport
        .send_to(
            &node1.controller_target,
            ClusterRpcRequest::GetPartitionState(GetPartitionStateRequest {
                topic_name: "two.process.replica.restart.topic".to_string(),
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

    let follower_fetch = transport
        .send_to(
            &node1.controller_target,
            ClusterRpcRequest::ReplicaFetch(ReplicaFetchRequest {
                topic_name: "two.process.replica.topic".to_string(),
                partition_index: 0,
                start_offset: 0,
                max_records: 10,
            }),
        )
        .await
        .unwrap();
    let ClusterRpcResponse::ReplicaFetch(follower_fetch) = follower_fetch else {
        panic!("unexpected response variant")
    };
    assert!(follower_fetch.found);
    assert_eq!(follower_fetch.high_watermark, 1);
    assert_eq!(follower_fetch.records.len(), 1);

    let _ = node1.child.kill();
    let _ = node1.child.wait();
    let _ = node2.child.kill();
    let _ = node2.child.wait();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn two_process_cluster_controller_restart_allows_redesignation_and_mutation() {
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

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &node2.bootstrap)
        .create()
        .unwrap();
    producer
        .send(
            FutureRecord::to("two.process.restart.topic")
                .payload("hello")
                .key("k"),
            Duration::from_secs(3),
        )
        .await
        .unwrap();

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

    let _ = node2.child.kill();
    let _ = node2.child.wait();
    node2 = spawn_cluster_process(tempdir.path(), 2, broker2, controller2, &quorum);
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

    let update = transport
        .update_partition_leader_to(
            &node2.controller_target,
            UpdatePartitionLeaderRequest {
                topic_name: "two.process.restart.topic".to_string(),
                partition_index: 0,
                leader_id: 2,
                leader_epoch: 2,
            },
        )
        .await
        .unwrap();
    assert!(update.accepted);
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
    let stale = transport
        .send_to(
            &node2.controller_target,
            ClusterRpcRequest::AppendMetadata(AppendMetadataRequest {
                term: 2,
                leader_id: 1,
                prev_metadata_offset: update.metadata_offset,
                records: vec![kafkalite_server::cluster::MetadataRecord::SetController {
                    controller_id: 1,
                }],
            }),
        )
        .await
        .unwrap();
    let ClusterRpcResponse::AppendMetadata(stale) = stale else {
        panic!("unexpected response variant")
    };
    assert!(!stale.accepted);

    let state = transport
        .send_to(
            &node2.controller_target,
            ClusterRpcRequest::GetPartitionState(GetPartitionStateRequest {
                topic_name: "two.process.restart.topic".to_string(),
                partition_index: 0,
            }),
        )
        .await
        .unwrap();
    let ClusterRpcResponse::GetPartitionState(state) = state else {
        panic!("unexpected response variant")
    };
    assert!(state.found);
    assert_eq!(state.leader_epoch, 2);

    let _ = node1.child.kill();
    let _ = node1.child.wait();
    let _ = node2.child.kill();
    let _ = node2.child.wait();
}

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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn two_process_cluster_rejects_broker_control_on_non_controller_node() {
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

    let rejected = transport
        .register_broker_to(
            &node1.controller_target,
            RegisterBrokerRequest {
                node_id: 9,
                advertised_host: "127.0.0.1".to_string(),
                advertised_port: 39092,
            },
        )
        .await
        .unwrap();
    assert!(!rejected.accepted);
    assert_eq!(rejected.leader_id, Some(2));
    assert_eq!(rejected.controller_epoch, 1);

    let accepted = transport
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
    assert!(accepted.accepted);

    let heartbeat_rejected = transport
        .broker_heartbeat_to(
            &node1.controller_target,
            BrokerHeartbeatRequest {
                node_id: 9,
                broker_epoch: accepted.broker_epoch,
                timestamp_ms: 123,
            },
        )
        .await
        .unwrap();
    assert!(!heartbeat_rejected.accepted);
    assert_eq!(heartbeat_rejected.leader_id, Some(2));
    assert_eq!(heartbeat_rejected.controller_epoch, 1);

    let heartbeat_accepted = transport
        .broker_heartbeat_to(
            &node2.controller_target,
            BrokerHeartbeatRequest {
                node_id: 9,
                broker_epoch: accepted.broker_epoch,
                timestamp_ms: 123,
            },
        )
        .await
        .unwrap();
    assert!(heartbeat_accepted.accepted);

    let _ = node1.child.kill();
    let _ = node1.child.wait();
    let _ = node2.child.kill();
    let _ = node2.child.wait();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn process_control_plane_rejects_empty_reassignment_target() {
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
            FutureRecord::to("process.reassign.empty.topic")
                .payload("hello")
                .key("k"),
            Duration::from_secs(3),
        )
        .await
        .unwrap();

    let transport = TcpClusterRpcTransport;
    let response = transport
        .begin_partition_reassignment_to(
            &ClusterRpcTarget {
                node_id: 1,
                host: "127.0.0.1".to_string(),
                port: controller_port,
            },
            BeginPartitionReassignmentRequest {
                topic_name: "process.reassign.empty.topic".to_string(),
                partition_index: 0,
                target_replicas: vec![],
            },
        )
        .await
        .unwrap();

    assert!(!response.accepted);

    let repeated = transport
        .begin_partition_reassignment_to(
            &ClusterRpcTarget {
                node_id: 1,
                host: "127.0.0.1".to_string(),
                port: controller_port,
            },
            BeginPartitionReassignmentRequest {
                topic_name: "process.reassign.empty.topic".to_string(),
                partition_index: 0,
                target_replicas: vec![],
            },
        )
        .await
        .unwrap();

    assert!(!repeated.accepted);

    let _ = child.kill();
    let _ = child.wait();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn process_control_plane_rejects_duplicate_reassignment_begin() {
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
            FutureRecord::to("process.reassign.duplicate.topic")
                .payload("hello")
                .key("k"),
            Duration::from_secs(3),
        )
        .await
        .unwrap();

    let transport = TcpClusterRpcTransport;
    let accepted = transport
        .begin_partition_reassignment_to(
            &ClusterRpcTarget {
                node_id: 1,
                host: "127.0.0.1".to_string(),
                port: controller_port,
            },
            BeginPartitionReassignmentRequest {
                topic_name: "process.reassign.duplicate.topic".to_string(),
                partition_index: 0,
                target_replicas: vec![2, 3],
            },
        )
        .await
        .unwrap();
    assert!(accepted.accepted);

    let rejected = transport
        .begin_partition_reassignment_to(
            &ClusterRpcTarget {
                node_id: 1,
                host: "127.0.0.1".to_string(),
                port: controller_port,
            },
            BeginPartitionReassignmentRequest {
                topic_name: "process.reassign.duplicate.topic".to_string(),
                partition_index: 0,
                target_replicas: vec![3, 4],
            },
        )
        .await
        .unwrap();
    assert!(!rejected.accepted);

    let _ = child.kill();
    let _ = child.wait();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn process_control_plane_rejects_stale_leader_for_reassignment_begin() {
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
            FutureRecord::to("process.reassign.stale.begin.topic")
                .payload("hello")
                .key("k"),
            Duration::from_secs(3),
        )
        .await
        .unwrap();

    let transport = TcpClusterRpcTransport;
    let _ = transport
        .send_to(
            &ClusterRpcTarget {
                node_id: 1,
                host: "127.0.0.1".to_string(),
                port: controller_port,
            },
            ClusterRpcRequest::AppendMetadata(AppendMetadataRequest {
                term: 2,
                leader_id: 1,
                prev_metadata_offset: -1,
                records: vec![kafkalite_server::cluster::MetadataRecord::SetController {
                    controller_id: 1,
                }],
            }),
        )
        .await
        .unwrap();

    let rejected = transport
        .send_to(
            &ClusterRpcTarget {
                node_id: 1,
                host: "127.0.0.1".to_string(),
                port: controller_port,
            },
            ClusterRpcRequest::AppendMetadata(AppendMetadataRequest {
                term: 2,
                leader_id: 9,
                prev_metadata_offset: 0,
                records: vec![
                    kafkalite_server::cluster::MetadataRecord::BeginPartitionReassignment {
                        topic_name: "process.reassign.stale.begin.topic".to_string(),
                        partition_index: 0,
                        target_replicas: vec![2, 3],
                    },
                ],
            }),
        )
        .await
        .unwrap();
    let ClusterRpcResponse::AppendMetadata(rejected) = rejected else {
        panic!("unexpected response variant")
    };
    assert!(!rejected.accepted);

    let _ = child.kill();
    let _ = child.wait();
}

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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn process_control_plane_rejects_invalid_reassignment_progression() {
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
            FutureRecord::to("process.reassign.order.topic")
                .payload("hello")
                .key("k"),
            Duration::from_secs(3),
        )
        .await
        .unwrap();

    let transport = TcpClusterRpcTransport;
    let begin = transport
        .begin_partition_reassignment_to(
            &ClusterRpcTarget {
                node_id: 1,
                host: "127.0.0.1".to_string(),
                port: controller_port,
            },
            BeginPartitionReassignmentRequest {
                topic_name: "process.reassign.order.topic".to_string(),
                partition_index: 0,
                target_replicas: vec![2, 3],
            },
        )
        .await
        .unwrap();
    assert!(begin.accepted);

    let invalid = transport
        .send_to(
            &ClusterRpcTarget {
                node_id: 1,
                host: "127.0.0.1".to_string(),
                port: controller_port,
            },
            ClusterRpcRequest::AdvancePartitionReassignment(AdvancePartitionReassignmentRequest {
                topic_name: "process.reassign.order.topic".to_string(),
                partition_index: 0,
                step: kafkalite_server::cluster::ReassignmentStep::Shrinking,
            }),
        )
        .await
        .unwrap();
    let ClusterRpcResponse::AdvancePartitionReassignment(invalid) = invalid else {
        panic!("unexpected response variant")
    };
    assert!(!invalid.accepted);

    let _ = child.kill();
    let _ = child.wait();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn process_control_plane_rejects_reassignment_leader_switch_before_catch_up() {
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
            FutureRecord::to("process.reassign.switch.topic")
                .payload("hello")
                .key("k"),
            Duration::from_secs(3),
        )
        .await
        .unwrap();

    let transport = TcpClusterRpcTransport;
    let _ = transport
        .begin_partition_reassignment_to(
            &ClusterRpcTarget {
                node_id: 1,
                host: "127.0.0.1".to_string(),
                port: controller_port,
            },
            BeginPartitionReassignmentRequest {
                topic_name: "process.reassign.switch.topic".to_string(),
                partition_index: 0,
                target_replicas: vec![2, 3],
            },
        )
        .await
        .unwrap();
    let _ = transport
        .send_to(
            &ClusterRpcTarget {
                node_id: 1,
                host: "127.0.0.1".to_string(),
                port: controller_port,
            },
            ClusterRpcRequest::AdvancePartitionReassignment(AdvancePartitionReassignmentRequest {
                topic_name: "process.reassign.switch.topic".to_string(),
                partition_index: 0,
                step: kafkalite_server::cluster::ReassignmentStep::Copying,
            }),
        )
        .await
        .unwrap();

    let response = transport
        .send_to(
            &ClusterRpcTarget {
                node_id: 1,
                host: "127.0.0.1".to_string(),
                port: controller_port,
            },
            ClusterRpcRequest::AdvancePartitionReassignment(AdvancePartitionReassignmentRequest {
                topic_name: "process.reassign.switch.topic".to_string(),
                partition_index: 0,
                step: kafkalite_server::cluster::ReassignmentStep::LeaderSwitch,
            }),
        )
        .await
        .unwrap();
    let ClusterRpcResponse::AdvancePartitionReassignment(response) = response else {
        panic!("unexpected response variant")
    };
    assert!(!response.accepted);

    let _ = child.kill();
    let _ = child.wait();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn process_control_plane_rejects_stale_leader_for_reassignment_advance() {
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
            FutureRecord::to("process.reassign.stale.leader.topic")
                .payload("hello")
                .key("k"),
            Duration::from_secs(3),
        )
        .await
        .unwrap();

    let transport = TcpClusterRpcTransport;
    let _ = transport
        .send_to(
            &ClusterRpcTarget {
                node_id: 1,
                host: "127.0.0.1".to_string(),
                port: controller_port,
            },
            ClusterRpcRequest::AppendMetadata(AppendMetadataRequest {
                term: 2,
                leader_id: 1,
                prev_metadata_offset: -1,
                records: vec![kafkalite_server::cluster::MetadataRecord::SetController {
                    controller_id: 1,
                }],
            }),
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
                topic_name: "process.reassign.stale.leader.topic".to_string(),
                partition_index: 0,
                target_replicas: vec![2, 3],
            },
        )
        .await
        .unwrap();

    let rejected = transport
        .send_to(
            &ClusterRpcTarget {
                node_id: 1,
                host: "127.0.0.1".to_string(),
                port: controller_port,
            },
            ClusterRpcRequest::AppendMetadata(AppendMetadataRequest {
                term: 2,
                leader_id: 9,
                prev_metadata_offset: 1,
                records: vec![
                    kafkalite_server::cluster::MetadataRecord::AdvancePartitionReassignment {
                        topic_name: "process.reassign.stale.leader.topic".to_string(),
                        partition_index: 0,
                        step: kafkalite_server::cluster::ReassignmentStep::Copying,
                    },
                ],
            }),
        )
        .await
        .unwrap();
    let ClusterRpcResponse::AppendMetadata(rejected) = rejected else {
        panic!("unexpected response variant")
    };
    assert!(!rejected.accepted);

    let _ = child.kill();
    let _ = child.wait();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn process_control_plane_rejects_reassignment_complete_before_target_leader() {
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
            FutureRecord::to("process.reassign.complete.topic")
                .payload("hello")
                .key("k"),
            Duration::from_secs(3),
        )
        .await
        .unwrap();

    let transport = TcpClusterRpcTransport;
    let _ = transport
        .begin_partition_reassignment_to(
            &ClusterRpcTarget {
                node_id: 1,
                host: "127.0.0.1".to_string(),
                port: controller_port,
            },
            BeginPartitionReassignmentRequest {
                topic_name: "process.reassign.complete.topic".to_string(),
                partition_index: 0,
                target_replicas: vec![2, 3],
            },
        )
        .await
        .unwrap();
    let _ = transport
        .send_to(
            &ClusterRpcTarget {
                node_id: 1,
                host: "127.0.0.1".to_string(),
                port: controller_port,
            },
            ClusterRpcRequest::AdvancePartitionReassignment(AdvancePartitionReassignmentRequest {
                topic_name: "process.reassign.complete.topic".to_string(),
                partition_index: 0,
                step: kafkalite_server::cluster::ReassignmentStep::Copying,
            }),
        )
        .await
        .unwrap();

    let response = transport
        .send_to(
            &ClusterRpcTarget {
                node_id: 1,
                host: "127.0.0.1".to_string(),
                port: controller_port,
            },
            ClusterRpcRequest::AdvancePartitionReassignment(AdvancePartitionReassignmentRequest {
                topic_name: "process.reassign.complete.topic".to_string(),
                partition_index: 0,
                step: kafkalite_server::cluster::ReassignmentStep::Complete,
            }),
        )
        .await
        .unwrap();
    let ClusterRpcResponse::AdvancePartitionReassignment(response) = response else {
        panic!("unexpected response variant")
    };
    assert!(!response.accepted);

    let _ = child.kill();
    let _ = child.wait();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn process_control_plane_rejects_older_partition_leader_epoch() {
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
            FutureRecord::to("process.epoch.topic")
                .payload("hello")
                .key("k"),
            Duration::from_secs(3),
        )
        .await
        .unwrap();

    let transport = TcpClusterRpcTransport;
    let accepted = transport
        .update_partition_leader_to(
            &ClusterRpcTarget {
                node_id: 1,
                host: "127.0.0.1".to_string(),
                port: controller_port,
            },
            UpdatePartitionLeaderRequest {
                topic_name: "process.epoch.topic".to_string(),
                partition_index: 0,
                leader_id: 1,
                leader_epoch: 3,
            },
        )
        .await
        .unwrap();
    assert!(accepted.accepted);

    let rejected = transport
        .update_partition_leader_to(
            &ClusterRpcTarget {
                node_id: 1,
                host: "127.0.0.1".to_string(),
                port: controller_port,
            },
            UpdatePartitionLeaderRequest {
                topic_name: "process.epoch.topic".to_string(),
                partition_index: 0,
                leader_id: 1,
                leader_epoch: 2,
            },
        )
        .await
        .unwrap();
    assert!(!rejected.accepted);

    let _ = child.kill();
    let _ = child.wait();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn process_control_plane_rejects_older_replication_epoch() {
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
            FutureRecord::to("process.replication.epoch.topic")
                .payload("hello")
                .key("k"),
            Duration::from_secs(3),
        )
        .await
        .unwrap();

    let transport = TcpClusterRpcTransport;
    let accepted = transport
        .update_partition_replication_to(
            &ClusterRpcTarget {
                node_id: 1,
                host: "127.0.0.1".to_string(),
                port: controller_port,
            },
            UpdatePartitionReplicationRequest {
                topic_name: "process.replication.epoch.topic".to_string(),
                partition_index: 0,
                replicas: vec![1, 2],
                isr: vec![1],
                leader_epoch: 3,
            },
        )
        .await
        .unwrap();
    assert!(accepted.accepted);
    let repeated = transport
        .update_partition_replication_to(
            &ClusterRpcTarget {
                node_id: 1,
                host: "127.0.0.1".to_string(),
                port: controller_port,
            },
            UpdatePartitionReplicationRequest {
                topic_name: "process.replication.epoch.topic".to_string(),
                partition_index: 0,
                replicas: vec![1, 2],
                isr: vec![1],
                leader_epoch: 3,
            },
        )
        .await
        .unwrap();
    assert!(repeated.accepted);

    let rejected = transport
        .update_partition_replication_to(
            &ClusterRpcTarget {
                node_id: 1,
                host: "127.0.0.1".to_string(),
                port: controller_port,
            },
            UpdatePartitionReplicationRequest {
                topic_name: "process.replication.epoch.topic".to_string(),
                partition_index: 0,
                replicas: vec![1],
                isr: vec![1],
                leader_epoch: 2,
            },
        )
        .await
        .unwrap();
    assert!(!rejected.accepted);

    let _ = child.kill();
    let _ = child.wait();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn process_control_plane_rejects_stale_leader_for_replication_update() {
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
            FutureRecord::to("process.replication.stale.leader.topic")
                .payload("hello")
                .key("k"),
            Duration::from_secs(3),
        )
        .await
        .unwrap();

    let transport = TcpClusterRpcTransport;
    let _ = transport
        .send_to(
            &ClusterRpcTarget {
                node_id: 1,
                host: "127.0.0.1".to_string(),
                port: controller_port,
            },
            ClusterRpcRequest::AppendMetadata(AppendMetadataRequest {
                term: 2,
                leader_id: 1,
                prev_metadata_offset: -1,
                records: vec![kafkalite_server::cluster::MetadataRecord::SetController {
                    controller_id: 1,
                }],
            }),
        )
        .await
        .unwrap();

    let accepted = transport
        .update_partition_replication_to(
            &ClusterRpcTarget {
                node_id: 1,
                host: "127.0.0.1".to_string(),
                port: controller_port,
            },
            UpdatePartitionReplicationRequest {
                topic_name: "process.replication.stale.leader.topic".to_string(),
                partition_index: 0,
                replicas: vec![1, 2],
                isr: vec![1],
                leader_epoch: 2,
            },
        )
        .await
        .unwrap();
    assert!(accepted.accepted);

    let rejected = transport
        .send_to(
            &ClusterRpcTarget {
                node_id: 1,
                host: "127.0.0.1".to_string(),
                port: controller_port,
            },
            ClusterRpcRequest::AppendMetadata(AppendMetadataRequest {
                term: 2,
                leader_id: 9,
                prev_metadata_offset: accepted.metadata_offset,
                records: vec![
                    kafkalite_server::cluster::MetadataRecord::UpdatePartitionReplication {
                        topic_name: "process.replication.stale.leader.topic".to_string(),
                        partition_index: 0,
                        replicas: vec![9],
                        isr: vec![9],
                        leader_epoch: 2,
                    },
                ],
            }),
        )
        .await
        .unwrap();
    let ClusterRpcResponse::AppendMetadata(rejected) = rejected else {
        panic!("unexpected response variant")
    };
    assert!(!rejected.accepted);

    let _ = child.kill();
    let _ = child.wait();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn process_control_plane_rejects_same_term_conflicting_controller_append() {
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
    let accepted = transport
        .send_to(
            &ClusterRpcTarget {
                node_id: 1,
                host: "127.0.0.1".to_string(),
                port: controller_port,
            },
            ClusterRpcRequest::AppendMetadata(AppendMetadataRequest {
                term: 2,
                leader_id: 1,
                prev_metadata_offset: -1,
                records: vec![kafkalite_server::cluster::MetadataRecord::SetController {
                    controller_id: 1,
                }],
            }),
        )
        .await
        .unwrap();
    let ClusterRpcResponse::AppendMetadata(accepted) = accepted else {
        panic!("unexpected response variant")
    };
    assert!(accepted.accepted);

    let rejected = transport
        .send_to(
            &ClusterRpcTarget {
                node_id: 1,
                host: "127.0.0.1".to_string(),
                port: controller_port,
            },
            ClusterRpcRequest::AppendMetadata(AppendMetadataRequest {
                term: 2,
                leader_id: 9,
                prev_metadata_offset: accepted.last_metadata_offset,
                records: vec![kafkalite_server::cluster::MetadataRecord::SetController {
                    controller_id: 9,
                }],
            }),
        )
        .await
        .unwrap();
    let ClusterRpcResponse::AppendMetadata(rejected) = rejected else {
        panic!("unexpected response variant")
    };
    assert!(!rejected.accepted);

    let _ = child.kill();
    let _ = child.wait();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn process_control_plane_reports_higher_term_vote() {
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
    let response = transport
        .send_to(
            &ClusterRpcTarget {
                node_id: 1,
                host: "127.0.0.1".to_string(),
                port: controller_port,
            },
            ClusterRpcRequest::Vote(VoteRequest {
                term: 5,
                candidate_id: 1,
                last_metadata_offset: -1,
            }),
        )
        .await
        .unwrap();

    let ClusterRpcResponse::Vote(response) = response else {
        panic!("unexpected response variant")
    };
    assert_eq!(response.term, 5);
    assert!(response.vote_granted);

    let repeated = transport
        .send_to(
            &ClusterRpcTarget {
                node_id: 1,
                host: "127.0.0.1".to_string(),
                port: controller_port,
            },
            ClusterRpcRequest::Vote(VoteRequest {
                term: 5,
                candidate_id: 1,
                last_metadata_offset: -1,
            }),
        )
        .await
        .unwrap();
    let ClusterRpcResponse::Vote(repeated) = repeated else {
        panic!("unexpected response variant")
    };
    assert_eq!(repeated.term, 5);
    assert!(repeated.vote_granted);

    let conflicting = transport
        .send_to(
            &ClusterRpcTarget {
                node_id: 1,
                host: "127.0.0.1".to_string(),
                port: controller_port,
            },
            ClusterRpcRequest::Vote(VoteRequest {
                term: 5,
                candidate_id: 2,
                last_metadata_offset: -1,
            }),
        )
        .await
        .unwrap();
    let ClusterRpcResponse::Vote(conflicting) = conflicting else {
        panic!("unexpected response variant")
    };
    assert_eq!(conflicting.term, 5);
    assert!(!conflicting.vote_granted);

    let _ = child.kill();
    let _ = child.wait();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn process_control_plane_rejects_lower_term_vote_after_higher_term_seen() {
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
    let high = transport
        .send_to(
            &ClusterRpcTarget {
                node_id: 1,
                host: "127.0.0.1".to_string(),
                port: controller_port,
            },
            ClusterRpcRequest::Vote(VoteRequest {
                term: 5,
                candidate_id: 1,
                last_metadata_offset: -1,
            }),
        )
        .await
        .unwrap();
    let ClusterRpcResponse::Vote(high) = high else {
        panic!("unexpected response variant")
    };
    assert_eq!(high.term, 5);
    assert!(high.vote_granted);

    let low = transport
        .send_to(
            &ClusterRpcTarget {
                node_id: 1,
                host: "127.0.0.1".to_string(),
                port: controller_port,
            },
            ClusterRpcRequest::Vote(VoteRequest {
                term: 4,
                candidate_id: 1,
                last_metadata_offset: -1,
            }),
        )
        .await
        .unwrap();
    let ClusterRpcResponse::Vote(low) = low else {
        panic!("unexpected response variant")
    };
    assert_eq!(low.term, 5);
    assert!(!low.vote_granted);

    let repeated_low = transport
        .send_to(
            &ClusterRpcTarget {
                node_id: 1,
                host: "127.0.0.1".to_string(),
                port: controller_port,
            },
            ClusterRpcRequest::Vote(VoteRequest {
                term: 4,
                candidate_id: 1,
                last_metadata_offset: -1,
            }),
        )
        .await
        .unwrap();
    let ClusterRpcResponse::Vote(repeated_low) = repeated_low else {
        panic!("unexpected response variant")
    };
    assert_eq!(repeated_low.term, 5);
    assert!(!repeated_low.vote_granted);

    let _ = child.kill();
    let _ = child.wait();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn process_control_plane_rejects_non_voter_vote_candidate() {
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
    let response = transport
        .send_to(
            &ClusterRpcTarget {
                node_id: 1,
                host: "127.0.0.1".to_string(),
                port: controller_port,
            },
            ClusterRpcRequest::Vote(VoteRequest {
                term: 6,
                candidate_id: 9,
                last_metadata_offset: -1,
            }),
        )
        .await
        .unwrap();

    let ClusterRpcResponse::Vote(response) = response else {
        panic!("unexpected response variant")
    };
    assert_eq!(response.term, 0);
    assert!(!response.vote_granted);

    let repeated = transport
        .send_to(
            &ClusterRpcTarget {
                node_id: 1,
                host: "127.0.0.1".to_string(),
                port: controller_port,
            },
            ClusterRpcRequest::Vote(VoteRequest {
                term: 6,
                candidate_id: 9,
                last_metadata_offset: -1,
            }),
        )
        .await
        .unwrap();

    let ClusterRpcResponse::Vote(repeated) = repeated else {
        panic!("unexpected response variant")
    };
    assert_eq!(repeated.term, 0);
    assert!(!repeated.vote_granted);

    let _ = child.kill();
    let _ = child.wait();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn process_control_plane_rejects_stale_lower_term_append() {
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
    let accepted = transport
        .send_to(
            &ClusterRpcTarget {
                node_id: 1,
                host: "127.0.0.1".to_string(),
                port: controller_port,
            },
            ClusterRpcRequest::AppendMetadata(AppendMetadataRequest {
                term: 3,
                leader_id: 1,
                prev_metadata_offset: -1,
                records: vec![kafkalite_server::cluster::MetadataRecord::SetController {
                    controller_id: 1,
                }],
            }),
        )
        .await
        .unwrap();
    let ClusterRpcResponse::AppendMetadata(accepted) = accepted else {
        panic!("unexpected response variant")
    };
    assert!(accepted.accepted);

    let rejected = transport
        .send_to(
            &ClusterRpcTarget {
                node_id: 1,
                host: "127.0.0.1".to_string(),
                port: controller_port,
            },
            ClusterRpcRequest::AppendMetadata(AppendMetadataRequest {
                term: 2,
                leader_id: 1,
                prev_metadata_offset: accepted.last_metadata_offset,
                records: vec![kafkalite_server::cluster::MetadataRecord::SetController {
                    controller_id: 1,
                }],
            }),
        )
        .await
        .unwrap();
    let ClusterRpcResponse::AppendMetadata(rejected) = rejected else {
        panic!("unexpected response variant")
    };
    assert!(!rejected.accepted);
    assert_eq!(rejected.term, 3);

    let _ = child.kill();
    let _ = child.wait();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn process_control_plane_rejects_stale_broker_epoch_heartbeat() {
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

    let rejected = transport
        .broker_heartbeat_to(
            &ClusterRpcTarget {
                node_id: 1,
                host: "127.0.0.1".to_string(),
                port: controller_port,
            },
            BrokerHeartbeatRequest {
                node_id: 9,
                broker_epoch: registration.broker_epoch - 1,
                timestamp_ms: 123,
            },
        )
        .await
        .unwrap();
    assert!(!rejected.accepted);

    let _ = child.kill();
    let _ = child.wait();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn process_control_plane_rejects_heartbeat_before_registration() {
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
    let heartbeat = transport
        .broker_heartbeat_to(
            &ClusterRpcTarget {
                node_id: 1,
                host: "127.0.0.1".to_string(),
                port: controller_port,
            },
            BrokerHeartbeatRequest {
                node_id: 9,
                broker_epoch: 1,
                timestamp_ms: 123,
            },
        )
        .await
        .unwrap();
    assert!(!heartbeat.accepted);

    let _ = child.kill();
    let _ = child.wait();
}

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

    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", &node1.bootstrap)
        .set("group.id", "combined-restart-seed")
        .create()
        .unwrap();
    let _ = consumer
        .fetch_metadata(
            Some("two.process.combined.restart.topic"),
            Duration::from_secs(5),
        )
        .unwrap();
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
            &node1.controller_target,
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
            &node1.controller_target,
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
                topic_name: "two.process.replica.restart.topic".to_string(),
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
    assert_eq!(fetched.high_watermark, 1);
    assert_eq!(fetched.records.len(), 1);
    assert_eq!(fetched.records[0].offset, 0);

    let _ = node1.child.kill();
    let _ = node1.child.wait();
    let _ = node2.child.kill();
    let _ = node2.child.wait();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn process_control_plane_rejects_stale_replica_progress_epoch() {
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
            FutureRecord::to("process.stale.epoch.topic")
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
                topic_name: "process.stale.epoch.topic".to_string(),
                partition_index: 0,
                leader_id: 1,
                leader_epoch: 2,
            },
        )
        .await
        .unwrap();

    let response = transport
        .update_replica_progress_to(
            &ClusterRpcTarget {
                node_id: 1,
                host: "127.0.0.1".to_string(),
                port: controller_port,
            },
            UpdateReplicaProgressRequest {
                topic_name: "process.stale.epoch.topic".to_string(),
                partition_index: 0,
                leader_epoch: 1,
                broker_id: 1,
                log_end_offset: 1,
                last_caught_up_ms: 123,
            },
        )
        .await
        .unwrap();

    assert!(!response.accepted);
    assert_eq!(response.high_watermark, 0);

    let _ = child.kill();
    let _ = child.wait();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn process_control_plane_reports_new_leader_epoch_for_replica_fetch() {
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
            FutureRecord::to("process.fetch.epoch.topic")
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
                topic_name: "process.fetch.epoch.topic".to_string(),
                partition_index: 0,
                leader_id: 1,
                leader_epoch: 3,
            },
        )
        .await
        .unwrap();

    let response = transport
        .send_to(
            &ClusterRpcTarget {
                node_id: 1,
                host: "127.0.0.1".to_string(),
                port: controller_port,
            },
            ClusterRpcRequest::ReplicaFetch(ReplicaFetchRequest {
                topic_name: "process.fetch.epoch.topic".to_string(),
                partition_index: 0,
                start_offset: 0,
                max_records: 10,
            }),
        )
        .await
        .unwrap();
    let ClusterRpcResponse::ReplicaFetch(response) = response else {
        panic!("unexpected response variant")
    };

    assert!(response.found);
    assert_eq!(response.leader_id, 1);
    assert_eq!(response.leader_epoch, 3);
    assert_eq!(response.leader_log_end_offset, 1);

    let _ = child.kill();
    let _ = child.wait();
}

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

    let _ = child.kill();
    let _ = child.wait();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn process_control_plane_replica_fetch_respects_start_offset() {
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
    for payload in ["first", "second"] {
        producer
            .send(
                FutureRecord::to("process.fetch.offset.topic")
                    .payload(payload)
                    .key("k"),
                Duration::from_secs(3),
            )
            .await
            .unwrap();
    }

    let transport = TcpClusterRpcTransport;
    let response = transport
        .send_to(
            &ClusterRpcTarget {
                node_id: 1,
                host: "127.0.0.1".to_string(),
                port: controller_port,
            },
            ClusterRpcRequest::ReplicaFetch(ReplicaFetchRequest {
                topic_name: "process.fetch.offset.topic".to_string(),
                partition_index: 0,
                start_offset: 1,
                max_records: 10,
            }),
        )
        .await
        .unwrap();
    let ClusterRpcResponse::ReplicaFetch(response) = response else {
        panic!("unexpected response variant")
    };

    assert!(response.found);
    assert_eq!(response.records.len(), 1);
    assert_eq!(response.records[0].offset, 1);

    let _ = child.kill();
    let _ = child.wait();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn process_control_plane_replica_fetch_respects_max_records() {
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
    for payload in ["first", "second"] {
        producer
            .send(
                FutureRecord::to("process.fetch.limit.topic")
                    .payload(payload)
                    .key("k"),
                Duration::from_secs(3),
            )
            .await
            .unwrap();
    }

    let transport = TcpClusterRpcTransport;
    let response = transport
        .send_to(
            &ClusterRpcTarget {
                node_id: 1,
                host: "127.0.0.1".to_string(),
                port: controller_port,
            },
            ClusterRpcRequest::ReplicaFetch(ReplicaFetchRequest {
                topic_name: "process.fetch.limit.topic".to_string(),
                partition_index: 0,
                start_offset: 0,
                max_records: 1,
            }),
        )
        .await
        .unwrap();
    let ClusterRpcResponse::ReplicaFetch(response) = response else {
        panic!("unexpected response variant")
    };

    assert!(response.found);
    assert_eq!(response.records.len(), 1);
    assert_eq!(response.records[0].offset, 0);

    let _ = child.kill();
    let _ = child.wait();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn process_control_plane_replica_fetch_zero_max_records_returns_empty() {
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
            FutureRecord::to("process.fetch.zero.topic")
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
                topic_name: "process.fetch.zero.topic".to_string(),
                partition_index: 0,
                start_offset: 0,
                max_records: 0,
            }),
        )
        .await
        .unwrap();
    let ClusterRpcResponse::ReplicaFetch(response) = response else {
        panic!("unexpected response variant")
    };

    assert!(response.found);
    assert!(response.records.is_empty());
    assert_eq!(response.leader_log_end_offset, 1);

    let _ = child.kill();
    let _ = child.wait();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn process_control_plane_replica_fetch_reports_missing_topic() {
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
    let response = transport
        .send_to(
            &ClusterRpcTarget {
                node_id: 1,
                host: "127.0.0.1".to_string(),
                port: controller_port,
            },
            ClusterRpcRequest::ReplicaFetch(ReplicaFetchRequest {
                topic_name: "missing.process.topic".to_string(),
                partition_index: 0,
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
    assert!(response.records.is_empty());
    assert_eq!(response.leader_id, -1);
    assert_eq!(response.leader_epoch, -1);
    assert_eq!(response.high_watermark, -1);
    assert_eq!(response.leader_log_end_offset, -1);

    let repeated = transport
        .send_to(
            &ClusterRpcTarget {
                node_id: 1,
                host: "127.0.0.1".to_string(),
                port: controller_port,
            },
            ClusterRpcRequest::ReplicaFetch(ReplicaFetchRequest {
                topic_name: "missing.process.topic".to_string(),
                partition_index: 0,
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
    assert!(repeated.records.is_empty());
    assert_eq!(repeated.leader_id, -1);
    assert_eq!(repeated.leader_epoch, -1);
    assert_eq!(repeated.high_watermark, -1);
    assert_eq!(repeated.leader_log_end_offset, -1);

    let _ = child.kill();
    let _ = child.wait();
}

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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn process_control_plane_replica_fetch_beyond_log_end_returns_empty() {
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
            FutureRecord::to("process.fetch.eof.topic")
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
                topic_name: "process.fetch.eof.topic".to_string(),
                partition_index: 0,
                start_offset: 10,
                max_records: 10,
            }),
        )
        .await
        .unwrap();
    let ClusterRpcResponse::ReplicaFetch(response) = response else {
        panic!("unexpected response variant")
    };

    assert!(response.found);
    assert!(response.records.is_empty());
    assert_eq!(response.leader_log_end_offset, 1);

    let _ = child.kill();
    let _ = child.wait();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn process_control_plane_replica_fetch_at_log_end_returns_empty() {
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
            FutureRecord::to("process.fetch.leo.topic")
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
                topic_name: "process.fetch.leo.topic".to_string(),
                partition_index: 0,
                start_offset: 1,
                max_records: 10,
            }),
        )
        .await
        .unwrap();
    let ClusterRpcResponse::ReplicaFetch(response) = response else {
        panic!("unexpected response variant")
    };

    assert!(response.found);
    assert!(response.records.is_empty());
    assert_eq!(response.leader_log_end_offset, 1);

    let repeated = transport
        .send_to(
            &ClusterRpcTarget {
                node_id: 1,
                host: "127.0.0.1".to_string(),
                port: controller_port,
            },
            ClusterRpcRequest::ReplicaFetch(ReplicaFetchRequest {
                topic_name: "process.fetch.leo.topic".to_string(),
                partition_index: 0,
                start_offset: 1,
                max_records: 10,
            }),
        )
        .await
        .unwrap();
    let ClusterRpcResponse::ReplicaFetch(repeated) = repeated else {
        panic!("unexpected response variant")
    };

    assert!(repeated.found);
    assert!(repeated.records.is_empty());
    assert_eq!(repeated.leader_log_end_offset, 1);

    let _ = child.kill();
    let _ = child.wait();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn process_control_plane_rejects_replica_apply_offset_mismatch() {
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
            FutureRecord::to("process.apply.mismatch.topic")
                .payload("hello")
                .key("k"),
            Duration::from_secs(3),
        )
        .await
        .unwrap();

    let transport = TcpClusterRpcTransport;
    let fetched = transport
        .send_to(
            &ClusterRpcTarget {
                node_id: 1,
                host: "127.0.0.1".to_string(),
                port: controller_port,
            },
            ClusterRpcRequest::ReplicaFetch(ReplicaFetchRequest {
                topic_name: "process.apply.mismatch.topic".to_string(),
                partition_index: 0,
                start_offset: 0,
                max_records: 10,
            }),
        )
        .await
        .unwrap();
    let ClusterRpcResponse::ReplicaFetch(mut fetched) = fetched else {
        panic!("unexpected response variant")
    };
    assert_eq!(fetched.records.len(), 1);
    fetched.records[0].offset = 5;

    let err = transport
        .apply_replica_records_to(
            &ClusterRpcTarget {
                node_id: 1,
                host: "127.0.0.1".to_string(),
                port: controller_port,
            },
            ApplyReplicaRecordsRequest {
                topic_name: "process.apply.mismatch.topic".to_string(),
                partition_index: 0,
                records: fetched.records,
                now_ms: 123,
            },
        )
        .await
        .unwrap_err()
        .to_string();

    assert!(err.contains("offset mismatch") || err.contains("unexpected replica record offset"));

    let _ = child.kill();
    let _ = child.wait();
}

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

fn spawn_broker(config_path: &Path) -> Child {
    let broker_bin = std::env::var("CARGO_BIN_EXE_kafkalite")
        .expect("CARGO_BIN_EXE_kafkalite should be set for integration tests");
    Command::new(broker_bin)
        .arg("--config")
        .arg(config_path)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn kafkalite process")
}

fn spawn_cluster_process(
    root: &Path,
    node_id: i32,
    broker_port: u16,
    controller_port: u16,
    quorum_voters: &str,
) -> ClusterProcess {
    let node_root = root.join(format!("node-{node_id}"));
    fs::create_dir_all(&node_root).unwrap();
    let config_path = node_root.join("server.properties");
    fs::write(
        &config_path,
        format!(
            concat!(
                "process.roles=broker,controller\n",
                "node.id={node_id}\n",
                "listeners=PLAINTEXT://127.0.0.1:{broker},CONTROLLER://127.0.0.1:{controller}\n",
                "advertised.listeners=PLAINTEXT://127.0.0.1:{broker}\n",
                "controller.listener.names=CONTROLLER\n",
                "controller.quorum.voters={quorum}\n",
                "cluster.id=test-cluster\n",
                "log.dirs={data}\n",
                "num.partitions=1\n"
            ),
            node_id = node_id,
            broker = broker_port,
            controller = controller_port,
            quorum = quorum_voters,
            data = node_root.join("data").display(),
        ),
    )
    .unwrap();
    ClusterProcess {
        bootstrap: format!("127.0.0.1:{broker_port}"),
        controller_target: ClusterRpcTarget {
            node_id,
            host: "127.0.0.1".to_string(),
            port: controller_port,
        },
        child: spawn_broker(&config_path),
    }
}

fn wait_until_broker_ready(bootstrap: &str, timeout: Duration) -> anyhow::Result<()> {
    let started = Instant::now();
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .set("group.id", "control-plane-probe")
        .create()?;
    while started.elapsed() < timeout {
        if consumer
            .fetch_metadata(None, Duration::from_millis(250))
            .is_ok()
        {
            return Ok(());
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    anyhow::bail!("broker did not become ready in time")
}

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}
