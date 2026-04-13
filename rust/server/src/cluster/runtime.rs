use std::sync::{Arc, Mutex};

use anyhow::Result;

use crate::cluster::config::ClusterConfig;
use crate::cluster::controller::{BrokerHeartbeat, ControllerSnapshot, ControllerState};
use crate::cluster::metadata::{BrokerMetadata, ClusterMetadataImage, MetadataStore};
use crate::cluster::quorum::{QuorumSnapshot, QuorumState};
use crate::cluster::rpc::{
    AppendMetadataRequest, AppendMetadataResponse, BrokerHeartbeatRequest, BrokerHeartbeatResponse,
    RegisterBrokerRequest, RegisterBrokerResponse, UpdatePartitionLeaderRequest,
    UpdatePartitionLeaderResponse, UpdatePartitionReplicationRequest,
    UpdatePartitionReplicationResponse, UpdateReplicaProgressRequest,
    UpdateReplicaProgressResponse,
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

    pub fn handle_append_metadata(
        &self,
        request: AppendMetadataRequest,
    ) -> Result<AppendMetadataResponse> {
        let snapshot = {
            let mut quorum = self.quorum.lock().expect("quorum state mutex poisoned");
            quorum.follow_leader(request.leader_id, request.term);
            quorum.snapshot()
        };
        let mut metadata = self
            .metadata
            .lock()
            .expect("cluster metadata mutex poisoned");
        let accepted =
            metadata.append_remote_records(request.prev_metadata_offset, &request.records)?;
        Ok(AppendMetadataResponse {
            term: snapshot.current_term,
            accepted,
            last_metadata_offset: metadata.metadata_offset(),
        })
    }

    pub fn handle_update_partition_leader(
        &self,
        request: UpdatePartitionLeaderRequest,
    ) -> Result<UpdatePartitionLeaderResponse> {
        let prev_metadata_offset = self.metadata_image().metadata_offset;
        let response = self.handle_append_metadata(AppendMetadataRequest {
            term: self.quorum_snapshot().current_term,
            leader_id: self
                .quorum_snapshot()
                .leader_id
                .unwrap_or(request.leader_id),
            prev_metadata_offset,
            records: vec![crate::cluster::MetadataRecord::UpdatePartitionLeader {
                topic_name: request.topic_name,
                partition_index: request.partition_index,
                leader_id: request.leader_id,
                leader_epoch: request.leader_epoch,
            }],
        })?;
        Ok(UpdatePartitionLeaderResponse {
            accepted: response.accepted,
            metadata_offset: response.last_metadata_offset,
        })
    }

    pub fn handle_update_partition_replication(
        &self,
        request: UpdatePartitionReplicationRequest,
    ) -> Result<UpdatePartitionReplicationResponse> {
        let prev_metadata_offset = self.metadata_image().metadata_offset;
        let response = self.handle_append_metadata(AppendMetadataRequest {
            term: self.quorum_snapshot().current_term,
            leader_id: self
                .quorum_snapshot()
                .leader_id
                .unwrap_or(self.config.node_id),
            prev_metadata_offset,
            records: vec![crate::cluster::MetadataRecord::UpdatePartitionReplication {
                topic_name: request.topic_name,
                partition_index: request.partition_index,
                replicas: request.replicas,
                isr: request.isr,
                leader_epoch: request.leader_epoch,
            }],
        })?;
        Ok(UpdatePartitionReplicationResponse {
            accepted: response.accepted,
            metadata_offset: response.last_metadata_offset,
        })
    }

    pub fn handle_update_replica_progress(
        &self,
        request: UpdateReplicaProgressRequest,
    ) -> Result<UpdateReplicaProgressResponse> {
        let topic_name = request.topic_name.clone();
        let partition_index = request.partition_index;
        let prev_metadata_offset = self.metadata_image().metadata_offset;
        let response = self.handle_append_metadata(AppendMetadataRequest {
            term: self.quorum_snapshot().current_term,
            leader_id: self
                .quorum_snapshot()
                .leader_id
                .unwrap_or(self.config.node_id),
            prev_metadata_offset,
            records: vec![crate::cluster::MetadataRecord::UpdateReplicaProgress {
                topic_name: request.topic_name,
                partition_index: request.partition_index,
                progress: crate::cluster::ReplicaProgress {
                    broker_id: request.broker_id,
                    log_end_offset: request.log_end_offset,
                    last_caught_up_ms: request.last_caught_up_ms,
                },
            }],
        })?;
        let image = self.metadata_image();
        let high_watermark = image
            .partition_high_watermark(&topic_name, partition_index)
            .unwrap_or(0);
        Ok(UpdateReplicaProgressResponse {
            accepted: response.accepted,
            metadata_offset: response.last_metadata_offset,
            high_watermark,
        })
    }

    pub fn dispatch(&self, request: ClusterRpcRequest) -> Result<ClusterRpcResponse> {
        let now_ms = chrono::Utc::now().timestamp_millis();
        match request {
            ClusterRpcRequest::AppendMetadata(request) => Ok(ClusterRpcResponse::AppendMetadata(
                self.handle_append_metadata(request)?,
            )),
            ClusterRpcRequest::UpdatePartitionLeader(request) => {
                Ok(ClusterRpcResponse::UpdatePartitionLeader(
                    self.handle_update_partition_leader(request)?,
                ))
            }
            ClusterRpcRequest::UpdatePartitionReplication(request) => {
                Ok(ClusterRpcResponse::UpdatePartitionReplication(
                    self.handle_update_partition_replication(request)?,
                ))
            }
            ClusterRpcRequest::UpdateReplicaProgress(request) => {
                Ok(ClusterRpcResponse::UpdateReplicaProgress(
                    self.handle_update_replica_progress(request)?,
                ))
            }
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

    #[test]
    fn append_metadata_updates_offset_and_term() {
        let dir = tempdir().unwrap();
        let mut config = Config::single_node(dir.path().join("data"), 19092, 1);
        config.cluster.node_id = 4;
        config.cluster.process_roles = vec![ProcessRole::Controller];

        let runtime = ClusterRuntime::from_config(&config).unwrap();
        let initial_offset = runtime.metadata_image().metadata_offset;
        let response = runtime
            .handle_append_metadata(AppendMetadataRequest {
                term: 2,
                leader_id: 9,
                prev_metadata_offset: initial_offset,
                records: vec![crate::cluster::MetadataRecord::SetController { controller_id: 9 }],
            })
            .unwrap();

        assert!(response.accepted);
        assert_eq!(response.term, 2);
        assert_eq!(runtime.metadata_image().controller_id, 9);
        assert_eq!(runtime.metadata_image().metadata_offset, initial_offset + 1);
    }

    #[test]
    fn update_partition_leader_changes_metadata_image() {
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

        let response = runtime
            .handle_update_partition_leader(UpdatePartitionLeaderRequest {
                topic_name: "leader.topic".to_string(),
                partition_index: 0,
                leader_id: 9,
                leader_epoch: 1,
            })
            .unwrap();

        assert!(response.accepted);
        let image = runtime.metadata_image();
        assert_eq!(image.partition_leader_id("leader.topic", 0), Some(9));
        assert_eq!(image.topics[0].partitions[0].leader_epoch, 1);
    }

    #[test]
    fn update_partition_replication_changes_isr_and_replicas() {
        let dir = tempdir().unwrap();
        let mut config = Config::single_node(dir.path().join("data"), 19092, 1);
        config.cluster.node_id = 4;
        config.cluster.process_roles = vec![ProcessRole::Broker, ProcessRole::Controller];

        let runtime = ClusterRuntime::from_config(&config).unwrap();
        runtime
            .sync_local_topics(
                &[crate::store::TopicMetadata {
                    name: "replication.topic".to_string(),
                    partitions: vec![crate::store::PartitionMetadata { partition: 0 }],
                }],
                1,
            )
            .unwrap();

        let response = runtime
            .handle_update_partition_replication(UpdatePartitionReplicationRequest {
                topic_name: "replication.topic".to_string(),
                partition_index: 0,
                replicas: vec![1, 2, 3],
                isr: vec![1, 2],
                leader_epoch: 2,
            })
            .unwrap();

        assert!(response.accepted);
        let image = runtime.metadata_image();
        assert_eq!(image.topics[0].partitions[0].replicas, vec![1, 2, 3]);
        assert_eq!(image.topics[0].partitions[0].isr, vec![1, 2]);
        assert_eq!(image.topics[0].partitions[0].leader_epoch, 2);
    }

    #[test]
    fn update_replica_progress_computes_high_watermark_from_isr() {
        let dir = tempdir().unwrap();
        let mut config = Config::single_node(dir.path().join("data"), 19092, 1);
        config.cluster.node_id = 4;
        config.cluster.process_roles = vec![ProcessRole::Broker, ProcessRole::Controller];

        let runtime = ClusterRuntime::from_config(&config).unwrap();
        runtime
            .sync_local_topics(
                &[crate::store::TopicMetadata {
                    name: "progress.topic".to_string(),
                    partitions: vec![crate::store::PartitionMetadata { partition: 0 }],
                }],
                1,
            )
            .unwrap();
        runtime
            .handle_update_partition_replication(UpdatePartitionReplicationRequest {
                topic_name: "progress.topic".to_string(),
                partition_index: 0,
                replicas: vec![1, 2],
                isr: vec![1, 2],
                leader_epoch: 1,
            })
            .unwrap();
        runtime
            .handle_update_replica_progress(UpdateReplicaProgressRequest {
                topic_name: "progress.topic".to_string(),
                partition_index: 0,
                broker_id: 1,
                log_end_offset: 10,
                last_caught_up_ms: 100,
            })
            .unwrap();
        let response = runtime
            .handle_update_replica_progress(UpdateReplicaProgressRequest {
                topic_name: "progress.topic".to_string(),
                partition_index: 0,
                broker_id: 2,
                log_end_offset: 8,
                last_caught_up_ms: 100,
            })
            .unwrap();

        assert!(response.accepted);
        assert_eq!(response.high_watermark, 8);
        assert_eq!(
            runtime
                .metadata_image()
                .partition_high_watermark("progress.topic", 0),
            Some(8)
        );
    }
}
