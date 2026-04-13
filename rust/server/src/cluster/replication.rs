use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplicaProgress {
    pub broker_id: i32,
    pub log_end_offset: i64,
    pub last_caught_up_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionReplicationState {
    pub topic_name: String,
    pub partition_index: i32,
    pub leader_id: i32,
    pub leader_epoch: i32,
    pub high_watermark: i64,
    pub replicas: Vec<i32>,
    pub isr: Vec<i32>,
    pub replica_progress: Vec<ReplicaProgress>,
}

impl PartitionReplicationState {
    pub fn update_isr(&mut self, next_isr: Vec<i32>) {
        self.isr = next_isr;
    }

    pub fn update_high_watermark(&mut self, high_watermark: i64) {
        self.high_watermark = high_watermark;
    }

    pub fn update_replica_progress(&mut self, progress: ReplicaProgress) {
        match self
            .replica_progress
            .iter_mut()
            .find(|entry| entry.broker_id == progress.broker_id)
        {
            Some(current) => *current = progress,
            None => self.replica_progress.push(progress),
        }
        self.replica_progress.sort_by_key(|entry| entry.broker_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn updates_replica_progress_in_place() {
        let mut state = PartitionReplicationState {
            topic_name: "topic-a".to_string(),
            partition_index: 0,
            leader_id: 1,
            leader_epoch: 1,
            high_watermark: 0,
            replicas: vec![1, 2],
            isr: vec![1, 2],
            replica_progress: vec![ReplicaProgress {
                broker_id: 2,
                log_end_offset: 10,
                last_caught_up_ms: 100,
            }],
        };

        state.update_replica_progress(ReplicaProgress {
            broker_id: 2,
            log_end_offset: 11,
            last_caught_up_ms: 200,
        });

        assert_eq!(state.replica_progress.len(), 1);
        assert_eq!(state.replica_progress[0].log_end_offset, 11);
    }
}
