use super::*;

#[tokio::test]
async fn auto_create_is_disabled_without_local_controller_authority() {
    let dir = tempdir().unwrap().keep();
    let mut config = Config::single_node(dir.join("data"), 9092, 1);
    config.cluster.node_id = 2;
    config.cluster.process_roles = vec![ProcessRole::Broker, ProcessRole::Controller];
    config.cluster.controller_quorum_voters = vec![
        ControllerQuorumVoter {
            node_id: 1,
            host: "node1".to_string(),
            port: 9093,
        },
        ControllerQuorumVoter {
            node_id: 2,
            host: "node2".to_string(),
            port: 9094,
        },
    ];
    let store = Arc::new(FileStore::open(&config.storage.data_dir).unwrap());
    let broker = KafkaBroker::new(config, store).unwrap();

    let response = handle_produce(&broker, produce_request("blocked.topic", -1, -1, 0))
        .await
        .unwrap();

    assert_eq!(
        response.responses[0].partition_responses[0].error_code,
        NOT_LEADER_OR_FOLLOWER
    );
}

#[tokio::test]
async fn auto_create_noops_for_out_of_range_partitions() {
    let broker = test_broker();

    maybe_auto_create_topic(&broker, "bounds.topic", -1, 0).unwrap();
    maybe_auto_create_topic(
        &broker,
        "bounds.topic",
        broker.config().storage.default_partitions,
        0,
    )
    .unwrap();

    let metadata = broker
        .store()
        .topic_metadata(Some(&["bounds.topic".to_string()]), 0)
        .unwrap();
    assert!(metadata.is_empty());
}

#[tokio::test]
async fn auto_create_noops_without_local_controller_authority() {
    let dir = tempdir().unwrap().keep();
    let mut config = Config::single_node(dir.join("data"), 9092, 1);
    config.cluster.node_id = 2;
    config.cluster.process_roles = vec![ProcessRole::Broker, ProcessRole::Controller];
    config.cluster.controller_quorum_voters = vec![
        ControllerQuorumVoter {
            node_id: 1,
            host: "node1".to_string(),
            port: 9093,
        },
        ControllerQuorumVoter {
            node_id: 2,
            host: "node2".to_string(),
            port: 9094,
        },
    ];
    let store = Arc::new(FileStore::open(&config.storage.data_dir).unwrap());
    let broker = KafkaBroker::new(config, store).unwrap();

    maybe_auto_create_topic(&broker, "blocked.topic", 0, 0).unwrap();

    let metadata = broker
        .store()
        .topic_metadata(Some(&["blocked.topic".to_string()]), 0)
        .unwrap();
    assert!(metadata.is_empty());
}
