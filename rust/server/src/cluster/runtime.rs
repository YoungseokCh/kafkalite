use std::sync::{Arc, Mutex};

use anyhow::Result;

use crate::cluster::config::ClusterConfig;
use crate::cluster::controller::{BrokerHeartbeat, ControllerSnapshot, ControllerState};
use crate::cluster::metadata::{BrokerMetadata, ClusterMetadataImage, MetadataStore};
use crate::cluster::quorum::QuorumState;
use crate::config::Config;
use crate::store::TopicMetadata;

mod controller;
mod partition;
mod partition_helpers;
mod routing;

#[cfg(test)]
mod tests;

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

    pub fn can_write_metadata_locally(&self) -> bool {
        if self.config.controller_quorum_voters.is_empty() {
            return true;
        }
        if !self
            .config
            .has_role(crate::cluster::ProcessRole::Controller)
        {
            return false;
        }
        self.quorum_snapshot().leader_id == Some(self.config.node_id)
    }

    pub fn can_auto_create_topics_locally(&self) -> bool {
        if !self
            .config
            .has_role(crate::cluster::ProcessRole::Controller)
        {
            return self.config.controller_quorum_voters.is_empty();
        }
        if self.config.controller_quorum_voters.len() <= 1 {
            return true;
        }
        self.quorum_snapshot().leader_id == Some(self.config.node_id)
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
        request: crate::cluster::BrokerHeartbeatRequest,
    ) -> Result<crate::cluster::BrokerHeartbeatResponse> {
        let quorum = self.quorum_snapshot();
        if !self.config.controller_quorum_voters.is_empty() && !self.can_write_metadata_locally() {
            return Ok(crate::cluster::BrokerHeartbeatResponse {
                accepted: false,
                controller_epoch: quorum.controller_epoch,
                leader_id: quorum.leader_id,
            });
        }
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
        Ok(crate::cluster::BrokerHeartbeatResponse {
            accepted,
            controller_epoch: quorum.controller_epoch,
            leader_id: quorum.leader_id,
        })
    }

    pub fn handle_register_broker(
        &self,
        request: crate::cluster::RegisterBrokerRequest,
        now_ms: i64,
    ) -> Result<crate::cluster::RegisterBrokerResponse> {
        let quorum = self.quorum_snapshot();
        if !self.config.controller_quorum_voters.is_empty() && !self.can_write_metadata_locally() {
            return Ok(crate::cluster::RegisterBrokerResponse {
                accepted: false,
                broker_epoch: -1,
                controller_epoch: quorum.controller_epoch,
                leader_id: quorum.leader_id,
            });
        }
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
        Ok(crate::cluster::RegisterBrokerResponse {
            accepted: true,
            broker_epoch: registration.broker_epoch,
            controller_epoch: quorum.controller_epoch,
            leader_id: quorum.leader_id,
        })
    }
}
