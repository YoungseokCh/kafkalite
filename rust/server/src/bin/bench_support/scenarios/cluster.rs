use std::fs;
use std::path::Path;
use std::time::Instant;

use anyhow::Result;
use kafkalite_server::cluster::test_support::{
    TestClusterNode, ThreeNodeClusterHarness, TwoNodeClusterHarness,
};
use kafkalite_server::cluster::{
    AdvancePartitionReassignmentRequest, BeginPartitionReassignmentRequest, ClusterRpcRequest,
    ClusterRpcTarget, ClusterRpcTransport, ReassignmentStep, UpdatePartitionLeaderRequest,
    UpdatePartitionReplicationRequest, UpdateReplicaProgressRequest,
};
use kafkalite_server::store::{PartitionMetadata, TopicMetadata};

use super::ScenarioSpec;
use super::metrics::{cluster_storage_metrics, runtime_metrics};
use crate::bench_support::report::{MemoryMetrics, ScenarioReport};

pub async fn run_cluster_replication_metadata(
    root: &Path,
    spec: &ScenarioSpec,
) -> Result<ScenarioReport> {
    fs::create_dir_all(root)?;
    let harness = TwoNodeClusterHarness::new_controller_pair();
    harness
        .node2
        .runtime
        .sync_local_topics(&[single_partition_topic(spec.name)], 2)?;

    let transport = harness.transport_from_node(1);
    let target = transport.resolve_target(2)?;

    let started = Instant::now();
    let mut latencies = Vec::with_capacity(spec.messages as usize);
    for index in 0..spec.messages {
        let op_started = Instant::now();
        let _ = transport.send_to(
            &target,
            ClusterRpcRequest::UpdatePartitionLeader(UpdatePartitionLeaderRequest {
                topic_name: spec.name.to_string(),
                partition_index: 0,
                leader_id: 1,
                leader_epoch: index as i32 + 1,
            }),
        )?;
        let _ = transport.send_to(
            &target,
            ClusterRpcRequest::UpdatePartitionReplication(UpdatePartitionReplicationRequest {
                topic_name: spec.name.to_string(),
                partition_index: 0,
                replicas: vec![1, 2],
                isr: vec![1, 2],
                leader_epoch: index as i32 + 1,
            }),
        )?;
        send_replica_progress_pair(&transport, &target, spec.name, index)?;
        latencies.push(op_started.elapsed());
    }

    Ok(cluster_report(
        spec,
        started.elapsed(),
        &latencies,
        &[
            harness.node1.data_dir.as_path(),
            harness.node2.data_dir.as_path(),
        ],
    ))
}

pub async fn run_cluster_reassignment_metadata(
    root: &Path,
    spec: &ScenarioSpec,
) -> Result<ScenarioReport> {
    fs::create_dir_all(root)?;
    let harness = ThreeNodeClusterHarness::new_controller_triplet();
    let transport = harness.transport_from_node(1);
    let target_two = transport.resolve_target(2)?;
    let target_three = transport.resolve_target(3)?;

    for node in [&harness.node2, &harness.node3] {
        seed_reassignment_node(node, spec.name)?;
    }

    let started = Instant::now();
    let mut latencies = Vec::with_capacity(spec.messages as usize);
    for index in 0..spec.messages {
        let op_started = Instant::now();
        let cycle = index / 2;
        let (target, target_replicas) =
            target_replicas_for_step(index, cycle, &target_two, &target_three);
        let _ = transport.send_to(
            target,
            ClusterRpcRequest::BeginPartitionReassignment(BeginPartitionReassignmentRequest {
                topic_name: spec.name.to_string(),
                partition_index: 0,
                target_replicas,
            }),
        )?;
        advance_reassignment(&transport, target, spec.name)?;
        latencies.push(op_started.elapsed());
    }

    Ok(cluster_report(
        spec,
        started.elapsed(),
        &latencies,
        &[
            harness.node1.data_dir.as_path(),
            harness.node2.data_dir.as_path(),
            harness.node3.data_dir.as_path(),
        ],
    ))
}

fn send_replica_progress_pair<T>(
    transport: &T,
    target: &ClusterRpcTarget,
    topic_name: &str,
    index: u32,
) -> Result<()>
where
    T: ClusterRpcTransport,
{
    for (broker_id, log_end_offset) in [(1, index as i64 + 1), (2, index as i64)] {
        let _ = transport.send_to(
            target,
            ClusterRpcRequest::UpdateReplicaProgress(UpdateReplicaProgressRequest {
                topic_name: topic_name.to_string(),
                partition_index: 0,
                leader_epoch: index as i32 + 1,
                broker_id,
                log_end_offset,
                last_caught_up_ms: index as i64,
            }),
        )?;
    }
    Ok(())
}

fn seed_reassignment_node(node: &TestClusterNode, topic_name: &str) -> Result<()> {
    node.runtime
        .sync_local_topics(&[single_partition_topic(topic_name)], 1)?;
    node.runtime
        .handle_update_partition_leader(UpdatePartitionLeaderRequest {
            topic_name: topic_name.to_string(),
            partition_index: 0,
            leader_id: 1,
            leader_epoch: 1,
        })?;
    node.runtime
        .handle_update_partition_replication(UpdatePartitionReplicationRequest {
            topic_name: topic_name.to_string(),
            partition_index: 0,
            replicas: vec![1, 2],
            isr: vec![1, 2],
            leader_epoch: 1,
        })?;
    Ok(())
}

fn target_replicas_for_step<'a, T>(
    index: u32,
    cycle: u32,
    target_two: &'a T,
    target_three: &'a T,
) -> (&'a T, Vec<i32>) {
    match index % 2 {
        0 => {
            let replicas = if cycle.is_multiple_of(2) {
                vec![2, 3]
            } else {
                vec![1, 2]
            };
            (target_two, replicas)
        }
        _ => {
            let replicas = if cycle.is_multiple_of(2) {
                vec![1, 3]
            } else {
                vec![1, 2]
            };
            (target_three, replicas)
        }
    }
}

fn advance_reassignment<T>(transport: &T, target: &ClusterRpcTarget, topic_name: &str) -> Result<()>
where
    T: ClusterRpcTransport,
{
    for step in [
        ReassignmentStep::Copying,
        ReassignmentStep::ExpandingIsr,
        ReassignmentStep::LeaderSwitch,
        ReassignmentStep::Shrinking,
        ReassignmentStep::Complete,
    ] {
        let _ = transport.send_to(
            target,
            ClusterRpcRequest::AdvancePartitionReassignment(AdvancePartitionReassignmentRequest {
                topic_name: topic_name.to_string(),
                partition_index: 0,
                step,
            }),
        )?;
    }
    Ok(())
}

fn single_partition_topic(name: &str) -> TopicMetadata {
    TopicMetadata {
        name: name.to_string(),
        partitions: vec![PartitionMetadata { partition: 0 }],
    }
}

fn cluster_report(
    spec: &ScenarioSpec,
    elapsed: std::time::Duration,
    latencies: &[std::time::Duration],
    data_dirs: &[&Path],
) -> ScenarioReport {
    ScenarioReport {
        name: spec.name.to_string(),
        iterations: 1,
        warmups: 0,
        messages: spec.messages,
        payload_bytes: spec.payload_bytes,
        default_partitions: spec.default_partitions,
        runtime: runtime_metrics(elapsed, latencies, spec.messages, spec.payload_bytes),
        memory: MemoryMetrics {
            peak_rss_kb: 0,
            final_rss_kb: 0,
        },
        storage: cluster_storage_metrics(data_dirs, spec.messages, spec.payload_bytes),
    }
}
