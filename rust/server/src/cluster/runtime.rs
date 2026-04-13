use std::sync::{Arc, Mutex};

use anyhow::Result;

use crate::cluster::config::ClusterConfig;
use crate::cluster::controller::{BrokerHeartbeat, ControllerSnapshot, ControllerState};
use crate::cluster::metadata::{BrokerMetadata, ClusterMetadataImage, MetadataStore};
use crate::cluster::quorum::{QuorumSnapshot, QuorumState};
use crate::cluster::rpc::{
    BrokerHeartbeatRequest, BrokerHeartbeatResponse, RegisterBrokerRequest, RegisterBrokerResponse,
};
use crate::cluster::transport::{
    ClusterRpcRequest, ClusterRpcResponse, ClusterRpcTransport, LocalClusterRpcTransport,
    RemoteClusterRpcTransport,
};
use crate::config::Config;
use crate::store::TopicMetadata;

#[derive(Debug, Clone)]
pub struct ClusterRuntime {
    config: ClusterConfig,
    controller: Arc<Mutex<ControllerState>>,
    metadata: Arc<Mutex<MetadataStore>>,
    quorum: Arc<Mutex<QuorumState>>,
}

impl ClusterRuntime {
    pub fn from_config(config: &Config) -> Result<Self> {
        let quorum = Arc::new(Mutex::new(QuorumState::new(&config.cluster)));
        let controller = Arc::new(Mutex::new(ControllerState::new(&config.cluster)));
        let runtime = Self {
            config: config.cluster.clone(),
            controller,
            metadata: Arc::new(Mutex::new(MetadataStore::open(
                &config.storage.data_dir.join("cluster"),
                config,
            )?)),
            quorum,
        };
        runtime.bootstrap_local_state(config);
        Ok(runtime)
    }

    pub fn config(&self) -> &ClusterConfig {
        &self.config
    }

    pub fn controller_snapshot(&self) -> ControllerSnapshot {
        self.controller
            .lock()
            .expect("controller state mutex poisoned")
            .snapshot()
    }

    pub fn metadata_image(&self) -> ClusterMetadataImage {
        self.metadata
            .lock()
            .expect("cluster metadata mutex poisoned")
            .image()
            .clone()
    }

    pub fn sync_local_topics(&self, topics: &[TopicMetadata], broker_id: i32) -> Result<()> {
        self.metadata
            .lock()
            .expect("cluster metadata mutex poisoned")
            .sync_topics(topics, broker_id)?;
        Ok(())
    }

    pub fn handle_broker_heartbeat(
        &self,
        request: BrokerHeartbeatRequest,
    ) -> Result<BrokerHeartbeatResponse> {
        let quorum = self.quorum_snapshot();
        let accepted = {
            let mut controller = self
                .controller
                .lock()
                .expect("controller state mutex poisoned");
            controller.set_leader(quorum.leader_id, quorum.controller_epoch);
            controller.apply_heartbeat(BrokerHeartbeat {
                node_id: request.node_id,
                broker_epoch: request.broker_epoch,
                timestamp_ms: request.timestamp_ms,
            })
        };
        if let Some(leader_id) = quorum.leader_id {
            self.metadata
                .lock()
                .expect("cluster metadata mutex poisoned")
                .sync_controller(leader_id)?;
        }
        Ok(BrokerHeartbeatResponse {
            accepted,
            controller_epoch: quorum.controller_epoch,
            leader_id: quorum.leader_id,
        })
    }

    pub fn handle_register_broker(
        &self,
        request: RegisterBrokerRequest,
        now_ms: i64,
    ) -> Result<RegisterBrokerResponse> {
        let quorum = self.quorum_snapshot();
        let registration = {
            let mut controller = self
                .controller
                .lock()
                .expect("controller state mutex poisoned");
            controller.set_leader(quorum.leader_id, quorum.controller_epoch);
            controller.register_broker(
                request.node_id,
                request.advertised_host,
                request.advertised_port,
                now_ms,
            )
        };
        let mut metadata = self
            .metadata
            .lock()
            .expect("cluster metadata mutex poisoned");
        metadata.sync_broker(BrokerMetadata {
            node_id: registration.node_id,
            host: registration.advertised_host.clone(),
            port: registration.advertised_port,
        })?;
        if let Some(leader_id) = quorum.leader_id {
            metadata.sync_controller(leader_id)?;
        }
        Ok(RegisterBrokerResponse {
            broker_epoch: registration.broker_epoch,
            controller_epoch: quorum.controller_epoch,
            leader_id: quorum.leader_id,
        })
    }

    pub fn dispatch(&self, request: ClusterRpcRequest) -> Result<ClusterRpcResponse> {
        let now_ms = chrono::Utc::now().timestamp_millis();
        match request {
            ClusterRpcRequest::RegisterBroker(request) => Ok(ClusterRpcResponse::RegisterBroker(
                self.handle_register_broker(request, now_ms)?,
            )),
            ClusterRpcRequest::BrokerHeartbeat(request) => Ok(ClusterRpcResponse::BrokerHeartbeat(
                self.handle_broker_heartbeat(request)?,
            )),
        }
    }

    pub fn local_transport(&self) -> LocalClusterRpcTransport {
        LocalClusterRpcTransport::new(self.clone())
    }

    pub fn remote_transport(&self) -> RemoteClusterRpcTransport {
        RemoteClusterRpcTransport::new(&self.config)
    }

    pub fn quorum_snapshot(&self) -> QuorumSnapshot {
        self.quorum
            .lock()
            .expect("quorum state mutex poisoned")
            .snapshot()
    }

    fn bootstrap_local_state(&self, config: &Config) {
        let now_ms = chrono::Utc::now().timestamp_millis();
        {
            let mut quorum = self.quorum.lock().expect("quorum state mutex poisoned");
            if config
                .cluster
                .has_role(crate::cluster::ProcessRole::Controller)
            {
                quorum.become_candidate();
                quorum.become_leader();
            }
        }
        if config.cluster.has_role(crate::cluster::ProcessRole::Broker) {
            let transport = self.local_transport();
            let response = transport
                .register_broker(RegisterBrokerRequest {
                    node_id: config.broker.broker_id,
                    advertised_host: config.broker.advertised_host.clone(),
                    advertised_port: config.broker.advertised_port,
                })
                .expect("local broker registration should succeed");
            let _ = transport.broker_heartbeat(BrokerHeartbeatRequest {
                node_id: config.broker.broker_id,
                broker_epoch: response.broker_epoch,
                timestamp_ms: now_ms,
            });
        } else if config
            .cluster
            .has_role(crate::cluster::ProcessRole::Controller)
            && let Some(leader_id) = self.quorum_snapshot().leader_id
        {
            let _ = self
                .metadata
                .lock()
                .expect("cluster metadata mutex poisoned")
                .sync_controller(leader_id);
        }
    }
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use crate::cluster::ProcessRole;
    use crate::config::Config;

    use super::*;

    #[test]
    fn controller_role_bootstraps_local_leader() {
        let dir = tempdir().unwrap();
        let mut config = Config::single_node(dir.path().join("data"), 19092, 1);
        config.cluster.node_id = 4;
        config.cluster.process_roles = vec![ProcessRole::Broker, ProcessRole::Controller];

        let runtime = ClusterRuntime::from_config(&config).unwrap();

        let quorum = runtime.quorum_snapshot();
        let controller = runtime.controller_snapshot();
        let metadata = runtime.metadata_image();
        assert_eq!(quorum.leader_id, Some(4));
        assert_eq!(quorum.controller_epoch, 1);
        assert_eq!(controller.leader_id, Some(4));
        assert_eq!(controller.registered_brokers.len(), 1);
        assert_eq!(controller.registered_brokers[0].node_id, 1);
        assert_eq!(metadata.controller_id, 4);
        assert_eq!(metadata.brokers.len(), 1);
    }

    #[test]
    fn broker_only_bootstrap_registers_broker_without_controller_leader() {
        let dir = tempdir().unwrap();
        let mut config = Config::single_node(dir.path().join("data"), 19092, 1);
        config.cluster.node_id = 7;
        config.cluster.process_roles = vec![ProcessRole::Broker];

        let runtime = ClusterRuntime::from_config(&config).unwrap();

        assert_eq!(runtime.quorum_snapshot().leader_id, None);
        let controller = runtime.controller_snapshot();
        assert_eq!(controller.leader_id, None);
        assert_eq!(controller.registered_brokers.len(), 1);
        assert_eq!(runtime.metadata_image().brokers.len(), 1);
    }

    #[test]
    fn register_broker_updates_metadata_and_heartbeat_accepts_current_epoch() {
        let dir = tempdir().unwrap();
        let mut config = Config::single_node(dir.path().join("data"), 19092, 1);
        config.cluster.node_id = 4;
        config.cluster.process_roles = vec![ProcessRole::Broker, ProcessRole::Controller];

        let runtime = ClusterRuntime::from_config(&config).unwrap();
        let response = runtime
            .handle_register_broker(
                RegisterBrokerRequest {
                    node_id: 9,
                    advertised_host: "broker-9.local".to_string(),
                    advertised_port: 39092,
                },
                500,
            )
            .unwrap();
        assert_eq!(response.leader_id, Some(4));

        let heartbeat = runtime
            .handle_broker_heartbeat(BrokerHeartbeatRequest {
                node_id: 9,
                broker_epoch: response.broker_epoch,
                timestamp_ms: 600,
            })
            .unwrap();
        assert!(heartbeat.accepted);

        let metadata = runtime.metadata_image();
        assert_eq!(metadata.controller_id, 4);
        assert!(metadata.brokers.iter().any(|broker| broker.node_id == 9));
    }
}
