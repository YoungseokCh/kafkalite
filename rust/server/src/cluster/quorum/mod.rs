use crate::cluster::config::{ClusterConfig, ControllerQuorumVoter};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QuorumSnapshot {
    pub local_node_id: i32,
    pub current_term: i64,
    pub voted_for: Option<i32>,
    pub leader_id: Option<i32>,
    pub controller_epoch: i64,
    pub voters: Vec<ControllerQuorumVoter>,
}

#[derive(Debug)]
pub struct QuorumState {
    local_node_id: i32,
    current_term: i64,
    voted_for: Option<i32>,
    leader_id: Option<i32>,
    controller_epoch: i64,
    voters: Vec<ControllerQuorumVoter>,
}

impl QuorumState {
    pub fn new(config: &ClusterConfig) -> Self {
        Self {
            local_node_id: config.node_id,
            current_term: 0,
            voted_for: None,
            leader_id: None,
            controller_epoch: 0,
            voters: config.controller_quorum_voters.clone(),
        }
    }

    pub fn become_candidate(&mut self) -> i64 {
        self.current_term += 1;
        self.voted_for = Some(self.local_node_id);
        self.leader_id = None;
        self.current_term
    }

    pub fn record_vote(&mut self, candidate_id: i32, term: i64) -> bool {
        if term < self.current_term {
            return false;
        }
        if term > self.current_term {
            self.current_term = term;
            self.voted_for = None;
            self.leader_id = None;
        }
        if self.voted_for.is_some() && self.voted_for != Some(candidate_id) {
            return false;
        }
        self.voted_for = Some(candidate_id);
        true
    }

    pub fn become_leader(&mut self) {
        self.leader_id = Some(self.local_node_id);
        self.controller_epoch += 1;
    }

    pub fn follow_leader(&mut self, leader_id: i32, term: i64) {
        if term >= self.current_term {
            self.current_term = term;
            self.leader_id = Some(leader_id);
            self.voted_for = None;
        }
    }

    pub fn snapshot(&self) -> QuorumSnapshot {
        QuorumSnapshot {
            local_node_id: self.local_node_id,
            current_term: self.current_term,
            voted_for: self.voted_for,
            leader_id: self.leader_id,
            controller_epoch: self.controller_epoch,
            voters: self.voters.clone(),
        }
    }

    pub fn current_term(&self) -> i64 {
        self.current_term
    }

    pub fn local_node_id(&self) -> i32 {
        self.local_node_id
    }

    pub fn has_majority(&self, votes: usize) -> bool {
        let voters = self.voters.len().max(1);
        votes >= (voters / 2) + 1
    }
}

#[cfg(test)]
mod tests {
    use crate::cluster::config::{ClusterConfig, ControllerQuorumVoter};

    use super::*;

    #[test]
    fn candidate_to_leader_bumps_controller_epoch() {
        let mut config = ClusterConfig::default();
        config.node_id = 2;
        config.controller_quorum_voters = vec![ControllerQuorumVoter {
            node_id: 2,
            host: "node2".to_string(),
            port: 9093,
        }];
        let mut state = QuorumState::new(&config);

        state.become_candidate();
        state.become_leader();

        let snapshot = state.snapshot();
        assert_eq!(snapshot.current_term, 1);
        assert_eq!(snapshot.leader_id, Some(2));
        assert_eq!(snapshot.controller_epoch, 1);
    }

    #[test]
    fn stale_term_vote_is_rejected() {
        let mut state = QuorumState::new(&ClusterConfig::default());
        state.become_candidate();

        assert!(!state.record_vote(3, 0));
    }
}
