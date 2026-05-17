use super::{
    ClusterMetadataImage, PartitionMetadataImage, PartitionReassignment, ReassignmentStep,
};

impl ClusterMetadataImage {
    pub fn begin_partition_reassignment(
        &mut self,
        topic_name: &str,
        partition_index: i32,
        target_replicas: Vec<i32>,
    ) -> bool {
        if target_replicas.is_empty() {
            return false;
        }
        let Some(partition) = partition_mut(self, topic_name, partition_index) else {
            return false;
        };
        if partition.reassignment.is_some() {
            return false;
        }
        partition.reassignment = Some(PartitionReassignment {
            target_replicas,
            step: ReassignmentStep::Planned,
        });
        true
    }

    pub fn advance_partition_reassignment(
        &mut self,
        topic_name: &str,
        partition_index: i32,
        step: ReassignmentStep,
    ) -> bool {
        let Some(partition) = partition_mut(self, topic_name, partition_index) else {
            return false;
        };
        let Some(reassignment) = partition.reassignment.clone() else {
            return false;
        };
        if reassignment.step == step {
            return false;
        }
        let target_replicas = reassignment.target_replicas.clone();
        if !apply_reassignment_step(partition, step.clone(), &target_replicas) {
            return false;
        }
        if let Some(current) = partition.reassignment.as_mut() {
            current.step = step;
        }
        true
    }

    pub fn complete_partition_reassignment(
        &mut self,
        topic_name: &str,
        partition_index: i32,
    ) -> bool {
        self.advance_partition_reassignment(topic_name, partition_index, ReassignmentStep::Complete)
    }
}

fn apply_reassignment_step(
    partition: &mut PartitionMetadataImage,
    step: ReassignmentStep,
    target_replicas: &[i32],
) -> bool {
    match step {
        ReassignmentStep::ExpandingIsr => expand_isr(partition, target_replicas),
        ReassignmentStep::LeaderSwitch => switch_leader(partition, target_replicas),
        ReassignmentStep::Shrinking => shrink_replicas(partition, target_replicas),
        ReassignmentStep::Complete => complete_reassignment(partition, target_replicas),
        ReassignmentStep::Planned | ReassignmentStep::Copying => true,
    }
}

fn expand_isr(partition: &mut PartitionMetadataImage, target_replicas: &[i32]) -> bool {
    if !targets_caught_up(partition, target_replicas) {
        return false;
    }
    partition.replicas = union_preserving_order(&partition.replicas, target_replicas);
    partition.isr = union_preserving_order(&partition.isr, target_replicas);
    true
}

fn switch_leader(partition: &mut PartitionMetadataImage, target_replicas: &[i32]) -> bool {
    if let Some(new_leader) = target_replicas.first().copied() {
        if !partition.isr.contains(&new_leader) || !targets_caught_up(partition, &[new_leader]) {
            return false;
        }
        partition.leader_id = new_leader;
        partition.leader_epoch += 1;
    }
    true
}

fn shrink_replicas(partition: &mut PartitionMetadataImage, target_replicas: &[i32]) -> bool {
    if partition.leader_id
        != target_replicas
            .first()
            .copied()
            .unwrap_or(partition.leader_id)
    {
        return false;
    }
    partition.replicas = target_replicas.to_vec();
    partition.isr.retain(|id| target_replicas.contains(id));
    true
}

fn complete_reassignment(partition: &mut PartitionMetadataImage, target_replicas: &[i32]) -> bool {
    if !shrink_replicas(partition, target_replicas) {
        return false;
    }
    partition.reassignment = None;
    true
}

fn partition_mut<'a>(
    image: &'a mut ClusterMetadataImage,
    topic_name: &str,
    partition_index: i32,
) -> Option<&'a mut PartitionMetadataImage> {
    image
        .topics
        .iter_mut()
        .find(|topic| topic.name == topic_name)
        .and_then(|topic| {
            topic
                .partitions
                .iter_mut()
                .find(|p| p.partition == partition_index)
        })
}

fn targets_caught_up(partition: &PartitionMetadataImage, targets: &[i32]) -> bool {
    targets.iter().all(|broker_id| {
        partition
            .replica_progress
            .iter()
            .find(|progress| progress.broker_id == *broker_id)
            .is_some_and(|progress| progress.log_end_offset >= partition.high_watermark)
    })
}

fn union_preserving_order(current: &[i32], target: &[i32]) -> Vec<i32> {
    let mut combined = current.to_vec();
    for replica in target {
        if !combined.contains(replica) {
            combined.push(*replica);
        }
    }
    combined
}
