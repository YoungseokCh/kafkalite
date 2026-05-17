use crate::broker::handlers::produce_fetch::handle_produce;
use crate::cluster::{
    InMemoryRemoteClusterRpcTransport,
    test_support::{ThreeNodeClusterHarness, TwoNodeClusterHarness},
};

use super::*;

#[tokio::test]
async fn follower_fetches_and_applies_remote_records() {
    let harness = ThreeNodeClusterHarness::new_controller_triplet();
    let voters = voter_trio();
    let leader = test_broker_with_voters(1, 19092, voters.clone());
    let follower = test_broker_with_voters(2, 19093, voters);

    for broker in [&leader, &follower] {
        prepare_topic(broker, "rf.topic", vec![1, 2, 3], vec![1, 2, 3]);
    }
    for sequence in 0..2 {
        let _ = handle_produce(&leader, produce_request("rf.topic", -1, -1, sequence))
            .await
            .unwrap();
    }

    harness.network.register(1, leader.cluster().clone());
    harness.network.register_store(1, leader.store().clone());
    let transport =
        InMemoryRemoteClusterRpcTransport::new(&follower.config().cluster, harness.network);
    let target = transport.resolve_target(1).unwrap();

    let high_watermark = follower
        .fetch_and_apply_from_remote_leader(&transport, &target, "rf.topic", 0, 200)
        .unwrap();

    let fetched = follower
        .store()
        .fetch_records("rf.topic", 0, 0, 10)
        .unwrap();
    assert_eq!(fetched.records.len(), 2);
    assert_eq!(fetched.records[0].offset, 0);
    assert_eq!(fetched.records[1].offset, 1);
    assert_eq!(high_watermark, 0);
}

#[tokio::test]
async fn follower_reconciles_divergence_when_ahead_of_leader() {
    let harness = TwoNodeClusterHarness::new_controller_pair();
    let voters = voter_pair();
    let leader = test_broker_with_voters(1, 19092, voters.clone());
    let follower = test_broker_with_voters(2, 19093, voters);

    for broker in [&leader, &follower] {
        prepare_topic(broker, "diverge.topic", vec![1], vec![1]);
    }
    let _ = handle_produce(&leader, produce_request("diverge.topic", -1, -1, 0))
        .await
        .unwrap();
    follower
        .store()
        .append_replica_records(
            "diverge.topic",
            0,
            &[replica_record(0, 100), replica_record(1, 101)],
            101,
        )
        .unwrap();

    harness.network.register(1, leader.cluster().clone());
    harness.network.register_store(1, leader.store().clone());
    let transport =
        InMemoryRemoteClusterRpcTransport::new(&follower.config().cluster, harness.network);
    let target = transport.resolve_target(1).unwrap();

    let high_watermark = follower
        .fetch_and_apply_from_remote_leader(&transport, &target, "diverge.topic", 0, 200)
        .unwrap();
    let fetched = follower
        .store()
        .fetch_records("diverge.topic", 0, 0, 10)
        .unwrap();

    assert_eq!(high_watermark, 0);
    assert_eq!(fetched.records.len(), 1);
    assert_eq!(fetched.records[0].offset, 0);
}

#[test]
fn sync_follower_progress_returns_minus_one_when_remote_partition_missing() {
    let broker = broker_with_rf_topic();
    let transport = ScriptedTransport::new([ClusterRpcResponse::GetPartitionState(
        missing_partition_state(),
    )]);

    let high_watermark = broker
        .sync_follower_progress_from_remote(&transport, &target(), "rf.topic", 0, 100)
        .unwrap();

    assert_eq!(high_watermark, -1);
}

#[test]
fn sync_follower_progress_rejects_stale_remote_leader_metadata() {
    let broker = broker_with_rf_topic();
    let transport = ScriptedTransport::new([ClusterRpcResponse::GetPartitionState(
        crate::cluster::GetPartitionStateResponse {
            found: true,
            leader_id: 9,
            leader_epoch: 1,
            high_watermark: 0,
            leader_log_end_offset: 0,
        },
    )]);

    let err = broker
        .sync_follower_progress_from_remote(&transport, &target(), "rf.topic", 0, 100)
        .unwrap_err()
        .to_string();

    assert!(err.contains("stale leader or epoch during follower progress sync"));
}

#[test]
fn fetch_and_apply_returns_minus_one_when_remote_partition_missing() {
    let broker = broker_with_rf_topic();
    let transport = ScriptedTransport::new([ClusterRpcResponse::GetPartitionState(
        missing_partition_state(),
    )]);

    let high_watermark = broker
        .fetch_and_apply_from_remote_leader(&transport, &target(), "rf.topic", 0, 100)
        .unwrap();

    assert_eq!(high_watermark, -1);
}

#[test]
fn fetch_and_apply_rejects_changed_leadership_in_fetch_response() {
    let broker = broker_with_rf_topic();
    let transport = ScriptedTransport::new([
        ClusterRpcResponse::GetPartitionState(leader_state(1)),
        ClusterRpcResponse::ReplicaFetch(crate::cluster::ReplicaFetchResponse {
            found: true,
            leader_id: 9,
            leader_epoch: 1,
            high_watermark: 0,
            leader_log_end_offset: 1,
            records: Vec::new(),
        }),
    ]);

    let err = broker
        .fetch_and_apply_from_remote_leader(&transport, &target(), "rf.topic", 0, 100)
        .unwrap_err()
        .to_string();

    assert!(err.contains("leadership changed before replica fetch response applied"));
}

#[test]
fn fetch_and_apply_accepts_empty_fetch_response() {
    let broker = broker_with_rf_topic();
    let transport = ScriptedTransport::new([
        ClusterRpcResponse::GetPartitionState(leader_state(0)),
        ClusterRpcResponse::ReplicaFetch(crate::cluster::ReplicaFetchResponse {
            found: true,
            leader_id: 1,
            leader_epoch: 1,
            high_watermark: 0,
            leader_log_end_offset: 0,
            records: Vec::new(),
        }),
    ]);

    let high_watermark = broker
        .fetch_and_apply_from_remote_leader(&transport, &target(), "rf.topic", 0, 100)
        .unwrap();

    assert_eq!(high_watermark, 0);
    assert!(
        broker
            .store()
            .fetch_records("rf.topic", 0, 0, 10)
            .unwrap()
            .records
            .is_empty()
    );
}

fn broker_with_rf_topic() -> KafkaBroker {
    let broker = test_broker_with_voters(2, 19093, voter_pair());
    prepare_topic(&broker, "rf.topic", vec![1, 2], vec![1, 2]);
    broker
}

fn prepare_topic(broker: &KafkaBroker, topic: &str, replicas: Vec<i32>, isr: Vec<i32>) {
    broker.store().ensure_topic(topic, 1, 0).unwrap();
    let metadata = broker
        .store()
        .topic_metadata(Some(&[topic.to_string()]), 0)
        .unwrap();
    broker.sync_topic_metadata(&metadata).unwrap();
    seed_partition_metadata(broker, topic, 1, replicas, isr, 1);
}

fn target() -> ClusterRpcTarget {
    ClusterRpcTarget {
        node_id: 1,
        host: "node1".to_string(),
        port: 9093,
    }
}

fn missing_partition_state() -> crate::cluster::GetPartitionStateResponse {
    crate::cluster::GetPartitionStateResponse {
        found: false,
        leader_id: -1,
        leader_epoch: -1,
        high_watermark: -1,
        leader_log_end_offset: -1,
    }
}

fn leader_state(leader_log_end_offset: i64) -> crate::cluster::GetPartitionStateResponse {
    crate::cluster::GetPartitionStateResponse {
        found: true,
        leader_id: 1,
        leader_epoch: 1,
        high_watermark: 0,
        leader_log_end_offset,
    }
}
