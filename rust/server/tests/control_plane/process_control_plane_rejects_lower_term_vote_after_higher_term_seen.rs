use super::*;

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
                last_metadata_offset: FRESH_CANDIDATE_METADATA_OFFSET,
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
                last_metadata_offset: FRESH_CANDIDATE_METADATA_OFFSET,
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
                last_metadata_offset: FRESH_CANDIDATE_METADATA_OFFSET,
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
