use anyhow::{Result, bail};

use crate::cluster::ClusterRuntime;
use crate::cluster::rpc::{
    BrokerHeartbeatRequest, BrokerHeartbeatResponse, RegisterBrokerRequest, RegisterBrokerResponse,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClusterRpcRequest {
    RegisterBroker(RegisterBrokerRequest),
    BrokerHeartbeat(BrokerHeartbeatRequest),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClusterRpcResponse {
    RegisterBroker(RegisterBrokerResponse),
    BrokerHeartbeat(BrokerHeartbeatResponse),
}

pub trait ClusterRpcTransport {
    fn send(&self, request: ClusterRpcRequest) -> Result<ClusterRpcResponse>;

    fn register_broker(&self, request: RegisterBrokerRequest) -> Result<RegisterBrokerResponse> {
        match self.send(ClusterRpcRequest::RegisterBroker(request))? {
            ClusterRpcResponse::RegisterBroker(response) => Ok(response),
            other => bail!("unexpected RPC response: {other:?}"),
        }
    }

    fn broker_heartbeat(&self, request: BrokerHeartbeatRequest) -> Result<BrokerHeartbeatResponse> {
        match self.send(ClusterRpcRequest::BrokerHeartbeat(request))? {
            ClusterRpcResponse::BrokerHeartbeat(response) => Ok(response),
            other => bail!("unexpected RPC response: {other:?}"),
        }
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

    use crate::cluster::{ClusterRuntime, ProcessRole};
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
}
