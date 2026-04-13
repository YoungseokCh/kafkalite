use tempfile::tempdir;

use crate::cluster::{
    ClusterRuntime, ControllerQuorumVoter, InMemoryClusterNetwork,
    InMemoryRemoteClusterRpcTransport, ProcessRole,
};
use crate::config::Config;

#[derive(Clone)]
pub struct TestClusterNode {
    pub node_id: i32,
    pub runtime: ClusterRuntime,
}

pub struct TwoNodeClusterHarness {
    pub node1: TestClusterNode,
    pub node2: TestClusterNode,
    pub network: InMemoryClusterNetwork,
}

impl TwoNodeClusterHarness {
    pub fn new_controller_pair() -> Self {
        let voters = vec![
            ControllerQuorumVoter {
                node_id: 1,
                host: "node1".to_string(),
                port: 9093,
            },
            ControllerQuorumVoter {
                node_id: 2,
                host: "node2".to_string(),
                port: 9093,
            },
        ];

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
        InMemoryRemoteClusterRpcTransport::new(&node1_client_config().cluster, self.network.clone())
    }
}

fn build_node(node_id: i32, port: u16, voters: Vec<ControllerQuorumVoter>) -> TestClusterNode {
    let dir = tempdir().unwrap().keep();
    let mut config = Config::single_node(dir.join(format!("node-{node_id}")), port, 1);
    config.cluster.node_id = node_id;
    config.cluster.process_roles = vec![ProcessRole::Controller];
    config.cluster.controller_quorum_voters = voters;
    TestClusterNode {
        node_id,
        runtime: ClusterRuntime::from_config(&config).unwrap(),
    }
}

fn node1_client_config() -> Config {
    let mut config = Config::single_node(tempdir().unwrap().keep().join("node1-client"), 19092, 1);
    config.cluster.node_id = 1;
    config.cluster.process_roles = vec![ProcessRole::Controller];
    config.cluster.controller_quorum_voters = vec![
        ControllerQuorumVoter {
            node_id: 1,
            host: "node1".to_string(),
            port: 9093,
        },
        ControllerQuorumVoter {
            node_id: 2,
            host: "node2".to_string(),
            port: 9093,
        },
    ];
    config
}
