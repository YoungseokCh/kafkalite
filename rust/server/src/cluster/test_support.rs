use std::path::PathBuf;

use tempfile::tempdir;

use crate::cluster::{
    ClusterRuntime, ControllerQuorumVoter, InMemoryClusterNetwork,
    InMemoryRemoteClusterRpcTransport, ProcessRole,
};
use crate::config::Config;

#[derive(Clone)]
pub struct TestClusterNode {
    pub data_dir: PathBuf,
    pub node_id: i32,
    pub runtime: ClusterRuntime,
}

pub struct TwoNodeClusterHarness {
    pub node1: TestClusterNode,
    pub node2: TestClusterNode,
    pub network: InMemoryClusterNetwork,
}

pub struct ThreeNodeClusterHarness {
    pub node1: TestClusterNode,
    pub node2: TestClusterNode,
    pub node3: TestClusterNode,
    pub network: InMemoryClusterNetwork,
}

impl TwoNodeClusterHarness {
    pub fn new_controller_pair() -> Self {
        let voters = controller_voters(&[1, 2]);

        let node1 = build_node(1, 19092, voters.clone());
        let node2 = build_node(2, 19093, voters);
        let network = InMemoryClusterNetwork::default();
        network.register(node1.node_id, node1.runtime.clone());
        network.register(node2.node_id, node2.runtime.clone());

        Self {
            node1,
            node2,
            network,
        }
    }

    pub fn transport_from_node1(&self) -> InMemoryRemoteClusterRpcTransport {
        self.transport_from_node(1)
    }

    pub fn transport_from_node(&self, node_id: i32) -> InMemoryRemoteClusterRpcTransport {
        InMemoryRemoteClusterRpcTransport::new(
            &client_config(node_id, &[1, 2]).cluster,
            self.network.clone(),
        )
    }
}

impl ThreeNodeClusterHarness {
    pub fn new_controller_triplet() -> Self {
        let voters = controller_voters(&[1, 2, 3]);

        let node1 = build_node(1, 19092, voters.clone());
        let node2 = build_node(2, 19093, voters.clone());
        let node3 = build_node(3, 19094, voters);
        let network = InMemoryClusterNetwork::default();
        network.register(node1.node_id, node1.runtime.clone());
        network.register(node2.node_id, node2.runtime.clone());
        network.register(node3.node_id, node3.runtime.clone());

        Self {
            node1,
            node2,
            node3,
            network,
        }
    }

    pub fn transport_from_node(&self, node_id: i32) -> InMemoryRemoteClusterRpcTransport {
        InMemoryRemoteClusterRpcTransport::new(
            &client_config(node_id, &[1, 2, 3]).cluster,
            self.network.clone(),
        )
    }
}

fn build_node(node_id: i32, port: u16, voters: Vec<ControllerQuorumVoter>) -> TestClusterNode {
    let dir = tempdir().unwrap().keep();
    let data_dir = dir.join(format!("node-{node_id}"));
    let mut config = Config::single_node(data_dir.clone(), port, 1);
    config.cluster.node_id = node_id;
    config.cluster.process_roles = vec![ProcessRole::Controller];
    config.cluster.controller_quorum_voters = voters;
    TestClusterNode {
        data_dir,
        node_id,
        runtime: ClusterRuntime::from_config(&config).unwrap(),
    }
}

fn client_config(node_id: i32, voters: &[i32]) -> Config {
    let mut config = Config::single_node(
        tempdir()
            .unwrap()
            .keep()
            .join(format!("node-{node_id}-client")),
        19092,
        1,
    );
    config.cluster.node_id = node_id;
    config.cluster.process_roles = vec![ProcessRole::Controller];
    config.cluster.controller_quorum_voters = controller_voters(voters);
    config
}

fn controller_voters(node_ids: &[i32]) -> Vec<ControllerQuorumVoter> {
    node_ids
        .iter()
        .map(|node_id| ControllerQuorumVoter {
            node_id: *node_id,
            host: format!("node{node_id}"),
            port: 9093,
        })
        .collect()
}
