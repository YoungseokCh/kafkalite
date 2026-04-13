use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use anyhow::{Result, bail};

use crate::cluster::rpc::{
    AppendMetadataRequest, AppendMetadataResponse, BrokerHeartbeatRequest, BrokerHeartbeatResponse,
    RegisterBrokerRequest, RegisterBrokerResponse, UpdatePartitionLeaderRequest,
    UpdatePartitionLeaderResponse,
};
use crate::cluster::{ClusterConfig, ClusterRuntime};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClusterRpcRequest {
    AppendMetadata(AppendMetadataRequest),
    RegisterBroker(RegisterBrokerRequest),
    BrokerHeartbeat(BrokerHeartbeatRequest),
    UpdatePartitionLeader(UpdatePartitionLeaderRequest),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClusterRpcResponse {
    AppendMetadata(AppendMetadataResponse),
    RegisterBroker(RegisterBrokerResponse),
    BrokerHeartbeat(BrokerHeartbeatResponse),
    UpdatePartitionLeader(UpdatePartitionLeaderResponse),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterRpcTarget {
    pub node_id: i32,
    pub host: String,
    pub port: u16,
}

pub trait ClusterRpcTransport {
    fn send(&self, request: ClusterRpcRequest) -> Result<ClusterRpcResponse>;

    fn send_to(
        &self,
        _target: &ClusterRpcTarget,
        request: ClusterRpcRequest,
    ) -> Result<ClusterRpcResponse> {
        self.send(request)
    }

    fn register_broker(&self, request: RegisterBrokerRequest) -> Result<RegisterBrokerResponse> {
        match self.send(ClusterRpcRequest::RegisterBroker(request))? {
            ClusterRpcResponse::RegisterBroker(response) => Ok(response),
            other => bail!("unexpected RPC response: {other:?}"),
        }
    }

    fn append_metadata(&self, request: AppendMetadataRequest) -> Result<AppendMetadataResponse> {
        match self.send(ClusterRpcRequest::AppendMetadata(request))? {
            ClusterRpcResponse::AppendMetadata(response) => Ok(response),
            other => bail!("unexpected RPC response: {other:?}"),
        }
    }

    fn broker_heartbeat(&self, request: BrokerHeartbeatRequest) -> Result<BrokerHeartbeatResponse> {
        match self.send(ClusterRpcRequest::BrokerHeartbeat(request))? {
            ClusterRpcResponse::BrokerHeartbeat(response) => Ok(response),
            other => bail!("unexpected RPC response: {other:?}"),
        }
    }

    fn update_partition_leader(
        &self,
        request: UpdatePartitionLeaderRequest,
    ) -> Result<UpdatePartitionLeaderResponse> {
        match self.send(ClusterRpcRequest::UpdatePartitionLeader(request))? {
            ClusterRpcResponse::UpdatePartitionLeader(response) => Ok(response),
            other => bail!("unexpected RPC response: {other:?}"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct RemoteClusterRpcTransport {
    local_node_id: i32,
    routes: BTreeMap<i32, ClusterRpcTarget>,
}

impl RemoteClusterRpcTransport {
    pub fn new(config: &ClusterConfig) -> Self {
        let routes = config
            .controller_quorum_voters
            .iter()
            .map(|voter| {
                (
                    voter.node_id,
                    ClusterRpcTarget {
                        node_id: voter.node_id,
                        host: voter.host.clone(),
                        port: voter.port,
                    },
                )
            })
            .collect();
        Self {
            local_node_id: config.node_id,
            routes,
        }
    }

    pub fn resolve_target(&self, node_id: i32) -> Result<ClusterRpcTarget> {
        self.routes
            .get(&node_id)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("unknown cluster RPC target node {node_id}"))
    }

    pub fn local_node_id(&self) -> i32 {
        self.local_node_id
    }
}

#[derive(Debug, Clone, Default)]
pub struct InMemoryClusterNetwork {
    runtimes: Arc<Mutex<BTreeMap<i32, ClusterRuntime>>>,
}

impl InMemoryClusterNetwork {
    pub fn register(&self, node_id: i32, runtime: ClusterRuntime) {
        self.runtimes
            .lock()
            .expect("in-memory cluster network mutex poisoned")
            .insert(node_id, runtime);
    }

    fn dispatch(&self, node_id: i32, request: ClusterRpcRequest) -> Result<ClusterRpcResponse> {
        let runtime = self
            .runtimes
            .lock()
            .expect("in-memory cluster network mutex poisoned")
            .get(&node_id)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("unregistered cluster runtime for node {node_id}"))?;
        runtime.dispatch(request)
    }
}

#[derive(Debug, Clone)]
pub struct InMemoryRemoteClusterRpcTransport {
    remote: RemoteClusterRpcTransport,
    network: InMemoryClusterNetwork,
}

impl InMemoryRemoteClusterRpcTransport {
    pub fn new(config: &ClusterConfig, network: InMemoryClusterNetwork) -> Self {
        Self {
            remote: RemoteClusterRpcTransport::new(config),
            network,
        }
    }
}

impl ClusterRpcTransport for RemoteClusterRpcTransport {
    fn send(&self, _request: ClusterRpcRequest) -> Result<ClusterRpcResponse> {
        bail!("remote cluster rpc requires a target node")
    }

    fn send_to(
        &self,
        target: &ClusterRpcTarget,
        request: ClusterRpcRequest,
    ) -> Result<ClusterRpcResponse> {
        bail!(
            "remote cluster rpc not implemented yet for target {}@{}:{} and request {:?}",
            target.node_id,
            target.host,
            target.port,
            request
        )
    }
}

impl ClusterRpcTransport for InMemoryRemoteClusterRpcTransport {
    fn send(&self, _request: ClusterRpcRequest) -> Result<ClusterRpcResponse> {
        bail!("in-memory remote cluster rpc requires a target node")
    }

    fn send_to(
        &self,
        target: &ClusterRpcTarget,
        request: ClusterRpcRequest,
    ) -> Result<ClusterRpcResponse> {
        let resolved = self.remote.resolve_target(target.node_id)?;
        self.network.dispatch(resolved.node_id, request)
    }
}

#[derive(Debug, Clone)]
pub struct LocalClusterRpcTransport {
    runtime: ClusterRuntime,
}

impl LocalClusterRpcTransport {
    pub fn new(runtime: ClusterRuntime) -> Self {
        Self { runtime }
    }
}

impl ClusterRpcTransport for LocalClusterRpcTransport {
    fn send(&self, request: ClusterRpcRequest) -> Result<ClusterRpcResponse> {
        self.runtime.dispatch(request)
    }
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use crate::cluster::{ClusterRuntime, ControllerQuorumVoter, ProcessRole};
    use crate::config::Config;

    use super::*;

    #[test]
    fn local_transport_dispatches_register_and_heartbeat() {
        let dir = tempdir().unwrap();
        let mut config = Config::single_node(dir.path().join("data"), 19092, 1);
        config.cluster.node_id = 4;
        config.cluster.process_roles = vec![ProcessRole::Broker, ProcessRole::Controller];
        let runtime = ClusterRuntime::from_config(&config).unwrap();
        let transport = LocalClusterRpcTransport::new(runtime.clone());

        let registration = transport
            .register_broker(RegisterBrokerRequest {
                node_id: 9,
                advertised_host: "broker-9.local".to_string(),
                advertised_port: 39092,
            })
            .unwrap();
        let heartbeat = transport
            .broker_heartbeat(BrokerHeartbeatRequest {
                node_id: 9,
                broker_epoch: registration.broker_epoch,
                timestamp_ms: 700,
            })
            .unwrap();

        assert_eq!(registration.leader_id, Some(4));
        assert!(heartbeat.accepted);
        assert!(
            runtime
                .metadata_image()
                .brokers
                .iter()
                .any(|broker| broker.node_id == 9)
        );
    }

    #[test]
    fn local_transport_dispatches_append_metadata() {
        let dir = tempdir().unwrap();
        let mut config = Config::single_node(dir.path().join("data"), 19092, 1);
        config.cluster.node_id = 4;
        config.cluster.process_roles = vec![ProcessRole::Broker, ProcessRole::Controller];
        let runtime = ClusterRuntime::from_config(&config).unwrap();
        let transport = LocalClusterRpcTransport::new(runtime.clone());

        let response = transport
            .append_metadata(AppendMetadataRequest {
                term: 1,
                leader_id: 4,
                prev_metadata_offset: runtime.metadata_image().metadata_offset,
                records: vec![crate::cluster::MetadataRecord::SetController { controller_id: 4 }],
            })
            .unwrap();

        assert!(response.accepted);
        assert_eq!(
            response.last_metadata_offset,
            runtime.metadata_image().metadata_offset
        );
    }

    #[test]
    fn local_transport_dispatches_partition_leader_update() {
        let dir = tempdir().unwrap();
        let mut config = Config::single_node(dir.path().join("data"), 19092, 1);
        config.cluster.node_id = 4;
        config.cluster.process_roles = vec![ProcessRole::Broker, ProcessRole::Controller];
        let runtime = ClusterRuntime::from_config(&config).unwrap();
        runtime
            .sync_local_topics(
                &[crate::store::TopicMetadata {
                    name: "leader.topic".to_string(),
                    partitions: vec![crate::store::PartitionMetadata { partition: 0 }],
                }],
                1,
            )
            .unwrap();
        let transport = LocalClusterRpcTransport::new(runtime.clone());

        let response = transport
            .update_partition_leader(UpdatePartitionLeaderRequest {
                topic_name: "leader.topic".to_string(),
                partition_index: 0,
                leader_id: 9,
                leader_epoch: 1,
            })
            .unwrap();

        assert!(response.accepted);
        assert_eq!(
            runtime
                .metadata_image()
                .partition_leader_id("leader.topic", 0),
            Some(9)
        );
    }

    #[test]
    fn remote_transport_resolves_target_from_quorum_voters() {
        let mut config = ClusterConfig {
            node_id: 1,
            ..ClusterConfig::default()
        };
        config.controller_quorum_voters = vec![
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

        let transport = RemoteClusterRpcTransport::new(&config);
        let target = transport.resolve_target(2).unwrap();

        assert_eq!(transport.local_node_id(), 1);
        assert_eq!(target.node_id, 2);
        assert_eq!(target.host, "node2");
        assert_eq!(target.port, 9093);
    }

    #[test]
    fn remote_transport_rejects_unknown_target() {
        let transport = RemoteClusterRpcTransport::new(&ClusterConfig::default());
        let err = transport.resolve_target(99).unwrap_err().to_string();

        assert!(err.contains("unknown cluster RPC target node 99"));
    }

    #[test]
    fn in_memory_remote_transport_dispatches_to_registered_runtime() {
        let mut config1 = Config::single_node(tempdir().unwrap().path().join("node1"), 19092, 1);
        config1.cluster.node_id = 1;
        config1.cluster.process_roles = vec![ProcessRole::Controller];
        config1.cluster.controller_quorum_voters = vec![
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
        let mut config2 = Config::single_node(tempdir().unwrap().path().join("node2"), 19093, 1);
        config2.cluster.node_id = 2;
        config2.cluster.process_roles = vec![ProcessRole::Controller];
        config2.cluster.controller_quorum_voters = config1.cluster.controller_quorum_voters.clone();

        let runtime2 = ClusterRuntime::from_config(&config2).unwrap();
        let network = InMemoryClusterNetwork::default();
        network.register(2, runtime2.clone());
        let transport = InMemoryRemoteClusterRpcTransport::new(&config1.cluster, network);
        let target = transport.remote.resolve_target(2).unwrap();

        let response = transport
            .send_to(
                &target,
                ClusterRpcRequest::AppendMetadata(AppendMetadataRequest {
                    term: 2,
                    leader_id: 1,
                    prev_metadata_offset: runtime2.metadata_image().metadata_offset,
                    records: vec![crate::cluster::MetadataRecord::SetController {
                        controller_id: 1,
                    }],
                }),
            )
            .unwrap();

        assert!(matches!(response, ClusterRpcResponse::AppendMetadata(_)));
        assert_eq!(runtime2.metadata_image().controller_id, 1);
    }
}
