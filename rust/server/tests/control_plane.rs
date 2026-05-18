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
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::producer::{FutureProducer, FutureRecord};
use tempfile::tempdir;

struct ClusterProcess {
    bootstrap: String,
    controller_target: ClusterRpcTarget,
    child: Child,
}

const FRESH_CANDIDATE_METADATA_OFFSET: i64 = i64::MAX;

#[path = "control_plane/process_exposes_tcp_control_plane_service.rs"]
mod process_exposes_tcp_control_plane_service;

#[path = "control_plane/two_process_cluster_exposes_control_plane_on_both_nodes.rs"]
mod two_process_cluster_exposes_control_plane_on_both_nodes;

#[path = "control_plane/process_control_plane_accepts_partition_leader_mutation.rs"]
mod process_control_plane_accepts_partition_leader_mutation;

#[path = "control_plane/process_control_plane_reports_missing_partition_state.rs"]
mod process_control_plane_reports_missing_partition_state;

#[path = "control_plane/process_control_plane_reports_missing_partition_for_existing_topic.rs"]
mod process_control_plane_reports_missing_partition_for_existing_topic;

#[path = "control_plane/process_control_plane_reports_existing_partition_state_after_produce.rs"]
mod process_control_plane_reports_existing_partition_state_after_produce;

#[path = "control_plane/process_control_plane_rejects_progress_update_for_missing_partition.rs"]
mod process_control_plane_rejects_progress_update_for_missing_partition;

#[path = "control_plane/process_control_plane_serves_replica_fetch_after_produce.rs"]
mod process_control_plane_serves_replica_fetch_after_produce;

#[path = "control_plane/two_process_cluster_accepts_control_plane_mutation_on_designated_controller.rs"]
mod two_process_cluster_accepts_control_plane_mutation_on_designated_controller;

#[path = "control_plane/process_control_plane_accepts_register_broker_and_heartbeat.rs"]
mod process_control_plane_accepts_register_broker_and_heartbeat;

#[path = "control_plane/process_control_plane_reregistration_bumps_broker_epoch.rs"]
mod process_control_plane_reregistration_bumps_broker_epoch;

#[path = "control_plane/two_process_cluster_accepts_register_broker_and_heartbeat_on_designated_controller.rs"]
mod two_process_cluster_accepts_register_broker_and_heartbeat_on_designated_controller;

#[path = "control_plane/two_process_cluster_supports_combined_control_plane_workflow.rs"]
mod two_process_cluster_supports_combined_control_plane_workflow;

#[path = "control_plane/two_process_cluster_supports_replica_fetch_and_apply_workflow.rs"]
mod two_process_cluster_supports_replica_fetch_and_apply_workflow;

#[path = "control_plane/two_process_cluster_replica_sync_converges_after_multiple_rounds.rs"]
mod two_process_cluster_replica_sync_converges_after_multiple_rounds;

#[path = "control_plane/two_process_cluster_preserves_replica_state_after_follower_restart.rs"]
mod two_process_cluster_preserves_replica_state_after_follower_restart;

#[path = "control_plane/two_process_cluster_controller_restart_allows_redesignation_and_mutation.rs"]
mod two_process_cluster_controller_restart_allows_redesignation_and_mutation;

#[path = "control_plane/two_process_cluster_rejects_metadata_mutation_on_non_controller_node.rs"]
mod two_process_cluster_rejects_metadata_mutation_on_non_controller_node;

#[path = "control_plane/two_process_cluster_rejects_broker_control_on_non_controller_node.rs"]
mod two_process_cluster_rejects_broker_control_on_non_controller_node;

#[path = "control_plane/process_control_plane_rejects_empty_reassignment_target.rs"]
mod process_control_plane_rejects_empty_reassignment_target;

#[path = "control_plane/process_control_plane_rejects_duplicate_reassignment_begin.rs"]
mod process_control_plane_rejects_duplicate_reassignment_begin;

#[path = "control_plane/process_control_plane_rejects_stale_leader_for_reassignment_begin.rs"]
mod process_control_plane_rejects_stale_leader_for_reassignment_begin;

#[path = "control_plane/process_control_plane_completes_valid_reassignment_lifecycle.rs"]
mod process_control_plane_completes_valid_reassignment_lifecycle;

#[path = "control_plane/process_control_plane_rejects_invalid_reassignment_progression.rs"]
mod process_control_plane_rejects_invalid_reassignment_progression;

#[path = "control_plane/process_control_plane_rejects_reassignment_leader_switch_before_catch_up.rs"]
mod process_control_plane_rejects_reassignment_leader_switch_before_catch_up;

#[path = "control_plane/process_control_plane_rejects_stale_leader_for_reassignment_advance.rs"]
mod process_control_plane_rejects_stale_leader_for_reassignment_advance;

#[path = "control_plane/process_control_plane_rejects_reassignment_complete_before_target_leader.rs"]
mod process_control_plane_rejects_reassignment_complete_before_target_leader;

#[path = "control_plane/process_control_plane_rejects_older_partition_leader_epoch.rs"]
mod process_control_plane_rejects_older_partition_leader_epoch;

#[path = "control_plane/process_control_plane_rejects_older_replication_epoch.rs"]
mod process_control_plane_rejects_older_replication_epoch;

#[path = "control_plane/process_control_plane_rejects_stale_leader_for_replication_update.rs"]
mod process_control_plane_rejects_stale_leader_for_replication_update;

#[path = "control_plane/process_control_plane_rejects_same_term_conflicting_controller_append.rs"]
mod process_control_plane_rejects_same_term_conflicting_controller_append;

#[path = "control_plane/process_control_plane_reports_higher_term_vote.rs"]
mod process_control_plane_reports_higher_term_vote;

#[path = "control_plane/process_control_plane_rejects_lower_term_vote_after_higher_term_seen.rs"]
mod process_control_plane_rejects_lower_term_vote_after_higher_term_seen;

#[path = "control_plane/process_control_plane_rejects_non_voter_vote_candidate.rs"]
mod process_control_plane_rejects_non_voter_vote_candidate;

#[path = "control_plane/process_control_plane_rejects_stale_lower_term_append.rs"]
mod process_control_plane_rejects_stale_lower_term_append;

#[path = "control_plane/process_control_plane_rejects_stale_broker_epoch_heartbeat.rs"]
mod process_control_plane_rejects_stale_broker_epoch_heartbeat;

#[path = "control_plane/process_control_plane_rejects_heartbeat_before_registration.rs"]
mod process_control_plane_rejects_heartbeat_before_registration;

#[path = "control_plane/two_process_cluster_recovers_after_controller_and_follower_restarts.rs"]
mod two_process_cluster_recovers_after_controller_and_follower_restarts;

#[path = "control_plane/process_control_plane_rejects_stale_replica_progress_epoch.rs"]
mod process_control_plane_rejects_stale_replica_progress_epoch;

#[path = "control_plane/process_control_plane_reports_new_leader_epoch_for_replica_fetch.rs"]
mod process_control_plane_reports_new_leader_epoch_for_replica_fetch;

#[path = "control_plane/process_control_plane_rejects_stale_progress_after_epoch_bump_and_fetch.rs"]
mod process_control_plane_rejects_stale_progress_after_epoch_bump_and_fetch;

#[path = "control_plane/process_control_plane_replica_fetch_respects_start_offset.rs"]
mod process_control_plane_replica_fetch_respects_start_offset;

#[path = "control_plane/process_control_plane_replica_fetch_respects_max_records.rs"]
mod process_control_plane_replica_fetch_respects_max_records;

#[path = "control_plane/process_control_plane_replica_fetch_zero_max_records_returns_empty.rs"]
mod process_control_plane_replica_fetch_zero_max_records_returns_empty;

#[path = "control_plane/process_control_plane_replica_fetch_reports_missing_topic.rs"]
mod process_control_plane_replica_fetch_reports_missing_topic;

#[path = "control_plane/process_control_plane_replica_fetch_reports_missing_partition_for_existing_topic.rs"]
mod process_control_plane_replica_fetch_reports_missing_partition_for_existing_topic;

#[path = "control_plane/process_control_plane_replica_fetch_beyond_log_end_returns_empty.rs"]
mod process_control_plane_replica_fetch_beyond_log_end_returns_empty;

#[path = "control_plane/process_control_plane_replica_fetch_at_log_end_returns_empty.rs"]
mod process_control_plane_replica_fetch_at_log_end_returns_empty;

#[path = "control_plane/process_control_plane_rejects_replica_apply_offset_mismatch.rs"]
mod process_control_plane_rejects_replica_apply_offset_mismatch;

#[path = "control_plane/process_control_plane_rejects_replica_apply_for_missing_partition.rs"]
mod process_control_plane_rejects_replica_apply_for_missing_partition;

#[path = "control_plane/process_control_plane_accepts_empty_replica_apply_as_noop.rs"]
mod process_control_plane_accepts_empty_replica_apply_as_noop;

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

async fn create_topic(bootstrap: &str, topic: &str) {
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .create()
        .unwrap();
    let topic = NewTopic::new(topic, 1, TopicReplication::Fixed(1));
    let results = admin
        .create_topics(&[topic], &AdminOptions::new())
        .await
        .unwrap();
    for result in results {
        result.unwrap();
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
