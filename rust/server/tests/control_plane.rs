use std::fs;
use std::net::TcpListener;
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use kafkalite_server::cluster::{
    ClusterRpcRequest, ClusterRpcResponse, ClusterRpcTarget, TcpClusterRpcTransport, VoteRequest,
};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use tempfile::tempdir;

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
