use super::*;

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
                last_metadata_offset: FRESH_CANDIDATE_METADATA_OFFSET,
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
                last_metadata_offset: FRESH_CANDIDATE_METADATA_OFFSET,
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
                last_metadata_offset: FRESH_CANDIDATE_METADATA_OFFSET,
            }),
        )
        .await
        .unwrap();
    let ClusterRpcResponse::Vote(conflicting) = conflicting else {
        panic!("unexpected response variant")
    };
    assert_eq!(conflicting.term, 5);
    assert!(!conflicting.vote_granted);

    let conflicting_again = transport
        .send_to(
            &ClusterRpcTarget {
                node_id: 1,
                host: "127.0.0.1".to_string(),
                port: controller_port,
            },
            ClusterRpcRequest::Vote(VoteRequest {
                term: 5,
                candidate_id: 2,
                last_metadata_offset: FRESH_CANDIDATE_METADATA_OFFSET,
            }),
        )
        .await
        .unwrap();
    let ClusterRpcResponse::Vote(conflicting_again) = conflicting_again else {
        panic!("unexpected response variant")
    };
    assert_eq!(conflicting_again.term, 5);
    assert!(!conflicting_again.vote_granted);

    let _ = child.kill();
    let _ = child.wait();
}
