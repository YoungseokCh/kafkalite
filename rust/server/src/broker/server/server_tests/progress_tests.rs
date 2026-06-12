use crate::broker::handlers::produce_fetch::handle_produce;
use crate::cluster::{
    InMemoryRemoteClusterRpcTransport,
    test_support::{ThreeNodeClusterHarness, TwoNodeClusterHarness},
};
use tokio::time::{Duration, timeout};

use super::*;

#[tokio::test]
async fn produce_updates_local_replica_progress() {
    let broker = test_broker(1, 19092);
    broker.store().ensure_topic("progress.topic", 1, 0).unwrap();
    let metadata = broker
        .store()
        .topic_metadata(Some(&["progress.topic".to_string()]), 0)
        .unwrap();
    broker.sync_topic_metadata(&metadata).unwrap();

    let request = produce_request("progress.topic", -1, -1, 0);
    let _ = handle_produce(&broker, request).await.unwrap();

    assert_eq!(
        broker
            .cluster()
            .metadata_image()
            .partition_state_view("progress.topic", 0)
            .map(|(_, _, _, leo)| leo),
        Some(1)
    );
}

#[test]
fn unknown_partition_leader_fails_closed_in_distributed_mode() {
    let broker = test_broker_with_voters(1, 19092, voter_pair());

    assert!(!broker.is_local_partition_leader("missing.topic", 0));
}

#[test]
fn missing_partition_defaults_to_local_leader_without_quorum() {
    let broker = test_broker(1, 19092);

    assert!(broker.is_local_partition_leader("missing.topic", 0));
}

#[tokio::test]
async fn follower_syncs_progress_from_remote_state() {
    let harness = TwoNodeClusterHarness::new_controller_pair();
    let leader = test_broker_with_voters(1, 19092, voter_pair());
    let follower = test_broker_with_voters(2, 19093, voter_pair());

    prepare_topic(&leader, "replicated.topic", vec![1, 2], vec![1, 2]);
    let _ = handle_produce(&leader, produce_request("replicated.topic", -1, -1, 0))
        .await
        .unwrap();
    prepare_topic(&follower, "replicated.topic", vec![1, 2], vec![1, 2]);

    harness.network.register(1, leader.cluster().clone());
    let transport =
        InMemoryRemoteClusterRpcTransport::new(&leader.config().cluster, harness.network);
    let target = transport.resolve_target(1).unwrap();
    let high_watermark = follower
        .sync_follower_progress_from_remote(&transport, &target, "replicated.topic", 0, 100)
        .unwrap();

    assert_eq!(high_watermark, 0);
    assert_eq!(
        follower
            .cluster()
            .metadata_image()
            .partition_state_view("replicated.topic", 0)
            .map(|(_, _, hw, leo)| (hw, leo)),
        Some((0, 0))
    );
}

#[tokio::test]
async fn follower_syncs_progress_from_remote_state_in_three_node_isr() {
    let harness = ThreeNodeClusterHarness::new_controller_triplet();
    let voters = voter_trio();
    let leader = test_broker_with_voters(1, 19092, voters.clone());
    let follower = test_broker_with_voters(2, 19093, voters);

    for broker in [&leader, &follower] {
        prepare_topic(broker, "replicated.topic", vec![1, 2, 3], vec![1, 2, 3]);
    }
    for sequence in 0..3 {
        let _ = handle_produce(
            &leader,
            produce_request("replicated.topic", -1, -1, sequence),
        )
        .await
        .unwrap();
    }
    follower
        .store()
        .append_replica_records(
            "replicated.topic",
            0,
            &[replica_record(0, 100), replica_record(1, 101)],
            101,
        )
        .unwrap();
    for broker_id in [1, 3] {
        follower
            .cluster()
            .handle_update_replica_progress(crate::cluster::UpdateReplicaProgressRequest {
                topic_name: "replicated.topic".to_string(),
                partition_index: 0,
                leader_epoch: 1,
                broker_id,
                log_end_offset: 3,
                last_caught_up_ms: 100,
            })
            .unwrap();
    }

    harness.network.register(1, leader.cluster().clone());
    let transport =
        InMemoryRemoteClusterRpcTransport::new(&follower.config().cluster, harness.network);
    let target = transport.resolve_target(1).unwrap();
    let high_watermark = follower
        .sync_follower_progress_from_remote(&transport, &target, "replicated.topic", 0, 200)
        .unwrap();

    assert_eq!(high_watermark, 2);
    let image = follower.cluster().metadata_image();
    let partition = &image.topics[0].partitions[0];
    assert_eq!(partition.high_watermark, 2);
    assert_eq!(partition.replica_progress.len(), 3);
    assert_eq!(partition.replica_progress[0].log_end_offset, 3);
    assert_eq!(partition.replica_progress[1].log_end_offset, 2);
    assert_eq!(partition.replica_progress[2].log_end_offset, 3);
}

#[tokio::test]
async fn update_local_replica_progress_notifies_fetch_waiters_when_high_watermark_advances() {
    let broker = test_broker_with_voters(1, 19092, voter_pair());
    prepare_topic(&broker, "long.poll.hw", vec![1, 2], vec![1, 2]);
    broker
        .store()
        .append_replica_records("long.poll.hw", 0, &[replica_record(0, 100)], 101)
        .unwrap();
    let mut receiver = broker.subscribe_fetch_signal("long.poll.hw", 0);

    let high_watermark = broker
        .update_local_replica_progress("long.poll.hw", 0, 200)
        .unwrap();

    assert_eq!(high_watermark, 1);
    timeout(Duration::from_millis(100), receiver.changed())
        .await
        .unwrap()
        .unwrap();
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
