use kafkalite_server::cluster::ControllerQuorumVoter;

fn voters(node_ids: &[i32]) -> Vec<ControllerQuorumVoter> {
    node_ids
        .iter()
        .map(|node_id| ControllerQuorumVoter {
            node_id: *node_id,
            host: format!("node{node_id}"),
            port: 9093,
        })
        .collect()
}

#[path = "runtime_branch/quorum.rs"]
mod quorum;

#[path = "runtime_branch/replication_routing.rs"]
mod replication_routing;

#[path = "runtime_branch/replica_state.rs"]
mod replica_state;
