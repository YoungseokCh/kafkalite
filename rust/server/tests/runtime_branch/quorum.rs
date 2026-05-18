use tempfile::tempdir;

use kafkalite_server::cluster::{
    AppendMetadataRequest, BrokerHeartbeatRequest, ClusterRuntime, MetadataRecord, ProcessRole,
    RegisterBrokerRequest, VoteRequest,
};
use kafkalite_server::config::Config;

#[test]
fn write_and_auto_create_flags_follow_role_and_quorum_shape() {
    let broker_dir = tempdir().unwrap();
    let mut broker_config = Config::single_node(broker_dir.path().join("data"), 19092, 1);
    broker_config.cluster.process_roles = vec![ProcessRole::Broker];
    broker_config.cluster.controller_quorum_voters = super::voters(&[1, 2]);
    let broker_runtime = ClusterRuntime::from_config(&broker_config).unwrap();
    assert!(!broker_runtime.can_write_metadata_locally());
    assert!(!broker_runtime.can_auto_create_topics_locally());

    let single_dir = tempdir().unwrap();
    let mut single_config = Config::single_node(single_dir.path().join("data"), 19093, 1);
    single_config.cluster.process_roles = vec![ProcessRole::Broker];
    let single_runtime = ClusterRuntime::from_config(&single_config).unwrap();
    assert!(single_runtime.can_write_metadata_locally());
    assert!(single_runtime.can_auto_create_topics_locally());
}

#[test]
fn register_and_heartbeat_are_rejected_when_node_is_not_leader() {
    let node1_dir = tempdir().unwrap();
    let mut node1_config = Config::single_node(node1_dir.path().join("data"), 19092, 1);
    node1_config.cluster.node_id = 1;
    node1_config.cluster.process_roles = vec![ProcessRole::Controller];
    node1_config.cluster.controller_quorum_voters = super::voters(&[1, 2]);
    let node1 = ClusterRuntime::from_config(&node1_config).unwrap();

    let _ = node1
        .handle_append_metadata(AppendMetadataRequest {
            term: 1,
            leader_id: 2,
            prev_metadata_offset: node1.metadata_image().metadata_offset,
            records: vec![MetadataRecord::SetController { controller_id: 2 }],
        })
        .unwrap();

    let registration = node1
        .handle_register_broker(
            RegisterBrokerRequest {
                node_id: 99,
                advertised_host: "broker-99.local".to_string(),
                advertised_port: 39092,
            },
            1_000,
        )
        .unwrap();
    assert!(!registration.accepted);

    let heartbeat = node1
        .handle_broker_heartbeat(BrokerHeartbeatRequest {
            node_id: 1,
            broker_epoch: 1,
            timestamp_ms: 1_001,
        })
        .unwrap();
    assert!(!heartbeat.accepted);
    assert_eq!(heartbeat.leader_id, Some(2));
}

#[test]
fn append_and_vote_reject_non_voter_and_stale_offsets() {
    let dir = tempdir().unwrap();
    let mut config = Config::single_node(dir.path().join("data"), 19092, 1);
    config.cluster.node_id = 1;
    config.cluster.process_roles = vec![ProcessRole::Controller];
    config.cluster.controller_quorum_voters = super::voters(&[1, 2]);
    let runtime = ClusterRuntime::from_config(&config).unwrap();

    let non_voter_append = runtime
        .handle_append_metadata(AppendMetadataRequest {
            term: 1,
            leader_id: 9,
            prev_metadata_offset: runtime.metadata_image().metadata_offset,
            records: vec![MetadataRecord::SetController { controller_id: 9 }],
        })
        .unwrap();
    assert!(!non_voter_append.accepted);

    let accepted = runtime
        .handle_append_metadata(AppendMetadataRequest {
            term: 1,
            leader_id: 1,
            prev_metadata_offset: runtime.metadata_image().metadata_offset,
            records: vec![MetadataRecord::SetController { controller_id: 1 }],
        })
        .unwrap();
    assert!(accepted.accepted);

    let advanced = runtime
        .handle_append_metadata(AppendMetadataRequest {
            term: 1,
            leader_id: 1,
            prev_metadata_offset: accepted.last_metadata_offset,
            records: vec![MetadataRecord::SetController { controller_id: 1 }],
        })
        .unwrap();
    assert!(advanced.accepted);

    let stale_vote = runtime
        .handle_vote(VoteRequest {
            term: 2,
            candidate_id: 1,
            last_metadata_offset: accepted.last_metadata_offset,
        })
        .unwrap();
    assert!(!stale_vote.vote_granted);

    let empty_log_vote = runtime
        .handle_vote(VoteRequest {
            term: 2,
            candidate_id: 1,
            last_metadata_offset: -1,
        })
        .unwrap();
    assert!(!empty_log_vote.vote_granted);
}

#[test]
fn run_election_self_majority_promotes_local_leader() {
    let dir = tempdir().unwrap();
    let mut config = Config::single_node(dir.path().join("data"), 19092, 1);
    config.cluster.process_roles = vec![ProcessRole::Controller];
    let runtime = ClusterRuntime::from_config(&config).unwrap();

    let elected = runtime
        .run_election(&runtime.local_transport(), &[])
        .unwrap();

    assert!(elected);
    assert_eq!(
        runtime.quorum_snapshot().leader_id,
        Some(config.cluster.node_id)
    );
    assert_eq!(
        runtime.metadata_image().controller_id,
        config.cluster.node_id
    );
}

#[test]
fn elected_multi_voter_controller_can_write_and_auto_create() {
    let dir = tempdir().unwrap();
    let mut config = Config::single_node(dir.path().join("data"), 19092, 1);
    config.cluster.node_id = 1;
    config.cluster.process_roles = vec![ProcessRole::Controller];
    config.cluster.controller_quorum_voters = super::voters(&[1, 2]);
    let runtime = ClusterRuntime::from_config(&config).unwrap();

    assert!(!runtime.can_write_metadata_locally());
    assert!(!runtime.can_auto_create_topics_locally());

    let _ = runtime
        .handle_append_metadata(AppendMetadataRequest {
            term: 1,
            leader_id: 1,
            prev_metadata_offset: runtime.metadata_image().metadata_offset,
            records: vec![MetadataRecord::SetController { controller_id: 1 }],
        })
        .unwrap();

    assert!(runtime.can_write_metadata_locally());
    assert!(runtime.can_auto_create_topics_locally());
}

#[test]
fn vote_rejects_non_voter_candidate() {
    let dir = tempdir().unwrap();
    let mut config = Config::single_node(dir.path().join("data"), 19092, 1);
    config.cluster.node_id = 1;
    config.cluster.process_roles = vec![ProcessRole::Controller];
    config.cluster.controller_quorum_voters = super::voters(&[1, 2]);
    let runtime = ClusterRuntime::from_config(&config).unwrap();

    let response = runtime
        .handle_vote(VoteRequest {
            term: 1,
            candidate_id: 9,
            last_metadata_offset: 0,
        })
        .unwrap();
    assert!(!response.vote_granted);
}
