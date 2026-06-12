use std::sync::Arc;
use std::time::Duration;

use rdkafka::consumer::Consumer;
use tempfile::tempdir;
use uuid::Uuid;

use kafkalite_server::cluster::UpdatePartitionReplicationRequest;
use kafkalite_server::{Config, FileStore, KafkaBroker};

use super::{bootstrap_available, consumer, free_port};

#[path = "roundtrip_snapshots.rs"]
mod snapshots;

use snapshots::{
    commit_resume_snapshot, fetch_first_batch_exceeds_partition_budget_snapshot,
    fetch_request_max_bytes_across_partitions_snapshot, metadata_snapshot,
    multi_partition_offset_fetch_snapshot, multi_partition_roundtrip_snapshot,
    partition_scoped_resume_snapshot, produce_consume_snapshot,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_kafka_and_local_broker_match_supported_roundtrips() {
    let Some(real_bootstrap) = std::env::var_os("REAL_KAFKA_BOOTSTRAP") else {
        eprintln!(
            "skipping differential test: set REAL_KAFKA_BOOTSTRAP to a reachable Kafka bootstrap server"
        );
        return;
    };

    let (local_bootstrap, handle, _tempdir) = super::start_local_broker().await;
    let real_bootstrap = real_bootstrap
        .into_string()
        .expect("bootstrap must be utf-8");
    if !bootstrap_available(&real_bootstrap) {
        eprintln!("skipping differential test: bootstrap {real_bootstrap} is unreachable");
        handle.abort();
        let _ = handle.await;
        return;
    }

    let suffix = Uuid::new_v4().simple().to_string();

    let metadata_topic = format!("diff.metadata.{suffix}");
    let roundtrip_topic = format!("diff.roundtrip.{suffix}");
    let resume_topic = format!("diff.resume.{suffix}");

    let real_metadata = metadata_snapshot(&real_bootstrap, &metadata_topic).await;
    let local_metadata = metadata_snapshot(&local_bootstrap, &metadata_topic).await;
    assert_eq!(local_metadata, real_metadata);

    let real_roundtrip = produce_consume_snapshot(&real_bootstrap, &roundtrip_topic).await;
    let local_roundtrip = produce_consume_snapshot(&local_bootstrap, &roundtrip_topic).await;
    assert_eq!(local_roundtrip, real_roundtrip);

    let multi_roundtrip_topic = format!("diff.multi-roundtrip.{suffix}");
    let real_multi_roundtrip =
        multi_partition_roundtrip_snapshot(&real_bootstrap, &multi_roundtrip_topic).await;
    let local_multi_roundtrip =
        multi_partition_roundtrip_snapshot(&local_bootstrap, &multi_roundtrip_topic).await;
    assert_eq!(local_multi_roundtrip, real_multi_roundtrip);

    let real_resume =
        commit_resume_snapshot(&real_bootstrap, &resume_topic, &format!("group.{suffix}")).await;
    let local_resume =
        commit_resume_snapshot(&local_bootstrap, &resume_topic, &format!("group.{suffix}")).await;
    assert_eq!(local_resume, real_resume);

    let invalid_partition_topic = format!("diff.invalid-partition.{suffix}");
    let real_invalid =
        super::groups::invalid_partition_snapshot(&real_bootstrap, &invalid_partition_topic).await;
    let local_invalid =
        super::groups::invalid_partition_snapshot(&local_bootstrap, &invalid_partition_topic).await;
    assert_eq!(local_invalid, real_invalid);

    let stale_commit_topic = format!("diff.stale-commit.{suffix}");
    let real_stale_commit = super::groups::stale_commit_after_handoff_snapshot(
        &real_bootstrap,
        &stale_commit_topic,
        &format!("group.stale.{suffix}"),
    )
    .await;
    let local_stale_commit = super::groups::stale_commit_after_handoff_snapshot(
        &local_bootstrap,
        &stale_commit_topic,
        &format!("group.stale.{suffix}"),
    )
    .await;
    assert_eq!(local_stale_commit, real_stale_commit);

    let current_member_stale_commit_topic = format!("diff.current-member-stale-commit.{suffix}");
    let real_current_member_stale_commit = super::groups::current_member_stale_commit_snapshot(
        &real_bootstrap,
        &current_member_stale_commit_topic,
        &format!("group.current-stale.{suffix}"),
    )
    .await;
    let local_current_member_stale_commit = super::groups::current_member_stale_commit_snapshot(
        &local_bootstrap,
        &current_member_stale_commit_topic,
        &format!("group.current-stale.{suffix}"),
    )
    .await;
    assert_eq!(
        local_current_member_stale_commit,
        real_current_member_stale_commit
    );

    let offset_fetch_topic = format!("diff.multi-offsets.{suffix}");
    let real_offset_fetch =
        multi_partition_offset_fetch_snapshot(&real_bootstrap, &offset_fetch_topic).await;
    let local_offset_fetch =
        multi_partition_offset_fetch_snapshot(&local_bootstrap, &offset_fetch_topic).await;
    assert_eq!(local_offset_fetch, real_offset_fetch);

    let oversized_fetch_topic = format!("diff.fetch-oversized.{suffix}");
    let real_oversized_fetch = fetch_first_batch_exceeds_partition_budget_snapshot(
        &real_bootstrap,
        &oversized_fetch_topic,
    )
    .await;
    let local_oversized_fetch = fetch_first_batch_exceeds_partition_budget_snapshot(
        &local_bootstrap,
        &oversized_fetch_topic,
    )
    .await;
    assert_eq!(local_oversized_fetch, real_oversized_fetch);

    let budgeted_fetch_topic = format!("diff.fetch-budget.{suffix}");
    let real_budgeted_fetch =
        fetch_request_max_bytes_across_partitions_snapshot(&real_bootstrap, &budgeted_fetch_topic)
            .await;
    let local_budgeted_fetch =
        fetch_request_max_bytes_across_partitions_snapshot(&local_bootstrap, &budgeted_fetch_topic)
            .await;
    assert_eq!(local_budgeted_fetch, real_budgeted_fetch);

    let partition_scoped_topic = format!("diff.partition-scoped-resume.{suffix}");
    let real_partition_scoped_resume =
        partition_scoped_resume_snapshot(&real_bootstrap, &partition_scoped_topic).await;
    let local_partition_scoped_resume =
        partition_scoped_resume_snapshot(&local_bootstrap, &partition_scoped_topic).await;
    assert_eq!(local_partition_scoped_resume, real_partition_scoped_resume);

    let heartbeat_topic = format!("diff.stale-heartbeat.{suffix}");
    let real_stale_heartbeat = super::groups::stale_heartbeat_after_timeout_snapshot(
        &real_bootstrap,
        &heartbeat_topic,
        &format!("group.heartbeat.{suffix}"),
    )
    .await;
    let local_stale_heartbeat = super::groups::stale_heartbeat_after_timeout_snapshot(
        &local_bootstrap,
        &heartbeat_topic,
        &format!("group.heartbeat.{suffix}"),
    )
    .await;
    assert_eq!(local_stale_heartbeat, real_stale_heartbeat);

    let stale_sync_topic = format!("diff.stale-sync.{suffix}");
    let real_stale_sync = super::assignments::stale_sync_after_handoff_snapshot(
        &real_bootstrap,
        &stale_sync_topic,
        &format!("group.sync.{suffix}"),
    )
    .await;
    let local_stale_sync = super::assignments::stale_sync_after_handoff_snapshot(
        &local_bootstrap,
        &stale_sync_topic,
        &format!("group.sync.{suffix}"),
    )
    .await;
    assert_eq!(local_stale_sync, real_stale_sync);

    let empty_assignment_topic = format!("diff.empty-assignment.{suffix}");
    let real_empty_assignment = super::assignments::empty_assignment_sync_snapshot(
        &real_bootstrap,
        &empty_assignment_topic,
        &format!("group.empty-assignment.{suffix}"),
    )
    .await;
    let local_empty_assignment = super::assignments::empty_assignment_sync_snapshot(
        &local_bootstrap,
        &empty_assignment_topic,
        &format!("group.empty-assignment.{suffix}"),
    )
    .await;
    assert_eq!(local_empty_assignment, real_empty_assignment);

    let leave_group_topic = format!("diff.leave-group.{suffix}");
    let real_leave_group = super::assignments::leave_group_snapshot(
        &real_bootstrap,
        &leave_group_topic,
        &format!("group.leave.{suffix}"),
    )
    .await;
    let local_leave_group = super::assignments::leave_group_snapshot(
        &local_bootstrap,
        &leave_group_topic,
        &format!("group.leave.{suffix}"),
    )
    .await;
    assert_eq!(local_leave_group, real_leave_group);

    handle.abort();
    let _ = handle.await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn metadata_reads_are_side_effect_free_for_existing_topics() {
    let tempdir = tempdir().unwrap();
    let port = free_port();
    let mut config = Config::single_node(tempdir.path().join("kafkalite-data"), port, 1);
    config.cluster.node_id = 1;
    config.cluster.process_roles = vec![
        kafkalite_server::cluster::ProcessRole::Broker,
        kafkalite_server::cluster::ProcessRole::Controller,
    ];
    config.cluster.controller_quorum_voters = vec![
        kafkalite_server::cluster::ControllerQuorumVoter {
            node_id: 1,
            host: "node1".to_string(),
            port: 9093,
        },
        kafkalite_server::cluster::ControllerQuorumVoter {
            node_id: 2,
            host: "node2".to_string(),
            port: 9094,
        },
    ];
    let store = Arc::new(FileStore::open(&config.storage.data_dir).unwrap());
    let broker = KafkaBroker::new(config, store).unwrap();
    let broker_handle = broker.clone();
    let handle = tokio::spawn(async move { broker_handle.run().await });
    tokio::time::sleep(Duration::from_millis(150)).await;
    let bootstrap = format!("127.0.0.1:{port}");

    broker
        .store()
        .ensure_topic("diff.side-effect", 1, 0)
        .unwrap();
    let metadata = broker
        .store()
        .topic_metadata(Some(&["diff.side-effect".to_string()]), 0)
        .unwrap();
    broker.sync_topic_metadata(&metadata).unwrap();
    broker
        .cluster()
        .handle_update_partition_replication(UpdatePartitionReplicationRequest {
            topic_name: "diff.side-effect".to_string(),
            partition_index: 0,
            replicas: vec![1, 2],
            isr: vec![1],
            leader_epoch: 3,
        })
        .unwrap();
    let before = broker.cluster().metadata_image();

    let consumer = consumer(&bootstrap, "metadata-side-effect");
    let _ = consumer
        .fetch_metadata(Some("diff.side-effect"), Duration::from_secs(5))
        .unwrap();

    let after = broker.cluster().metadata_image();
    assert_eq!(
        before.topics[0].partitions[0].replicas,
        after.topics[0].partitions[0].replicas
    );
    assert_eq!(
        before.topics[0].partitions[0].isr,
        after.topics[0].partitions[0].isr
    );
    assert_eq!(
        before.topics[0].partitions[0].leader_epoch,
        after.topics[0].partitions[0].leader_epoch
    );

    handle.abort();
    let _ = handle.await;
}
