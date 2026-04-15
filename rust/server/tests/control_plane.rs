use std::fs;
use std::net::TcpListener;
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use kafkalite_server::cluster::{
    AppendMetadataRequest, ClusterRpcRequest, ClusterRpcResponse, ClusterRpcTarget,
    GetPartitionStateRequest, TcpClusterRpcTransport, UpdatePartitionLeaderRequest, VoteRequest,
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

    let _ = node1.child.kill();
    let _ = node1.child.wait();
    let _ = node2.child.kill();
    let _ = node2.child.wait();
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
