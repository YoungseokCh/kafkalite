use std::sync::{Arc, Mutex};

use anyhow::Result;

use crate::cluster::config::ClusterConfig;
use crate::cluster::controller::{BrokerHeartbeat, ControllerSnapshot, ControllerState};
use crate::cluster::metadata::{BrokerMetadata, ClusterMetadataImage, MetadataStore};
use crate::cluster::quorum::{QuorumSnapshot, QuorumState};
use crate::cluster::rpc::{
    AdvancePartitionReassignmentRequest, AppendMetadataRequest, AppendMetadataResponse,
    BeginPartitionReassignmentRequest, BrokerHeartbeatRequest, BrokerHeartbeatResponse,
    GetPartitionStateRequest, GetPartitionStateResponse, PartitionReassignmentResponse,
    RegisterBrokerRequest, RegisterBrokerResponse, ReplicaFetchRequest, ReplicaFetchResponse,
    UpdatePartitionLeaderRequest, UpdatePartitionLeaderResponse, UpdatePartitionReplicationRequest,
    UpdatePartitionReplicationResponse, UpdateReplicaProgressRequest,
    UpdateReplicaProgressResponse, VoteRequest, VoteResponse,
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
        request: BrokerHeartbeatRequest,
    ) -> Result<BrokerHeartbeatResponse> {
        let quorum = self.quorum_snapshot();
        if !self.config.controller_quorum_voters.is_empty() && !self.can_write_metadata_locally() {
            return Ok(BrokerHeartbeatResponse {
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
        if !self.config.controller_quorum_voters.is_empty() && !self.can_write_metadata_locally() {
            return Ok(RegisterBrokerResponse {
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
        Ok(RegisterBrokerResponse {
            accepted: true,
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
            if !quorum.is_voter(request.leader_id) {
                let snapshot = quorum.snapshot();
                return Ok(AppendMetadataResponse {
                    term: snapshot.current_term,
                    accepted: false,
                    last_metadata_offset: self.metadata_image().metadata_offset,
                });
            }
            if request.term < quorum.current_term() {
                let snapshot = quorum.snapshot();
                return Ok(AppendMetadataResponse {
                    term: snapshot.current_term,
                    accepted: false,
                    last_metadata_offset: self.metadata_image().metadata_offset,
                });
            }
            let current = quorum.snapshot();
            if request.term == current.current_term
                && current.leader_id.is_some()
                && current.leader_id != Some(request.leader_id)
            {
                return Ok(AppendMetadataResponse {
                    term: current.current_term,
                    accepted: false,
                    last_metadata_offset: self.metadata_image().metadata_offset,
                });
            }
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

    pub fn handle_vote(&self, request: VoteRequest) -> Result<VoteResponse> {
        let current_offset = self.metadata_image().metadata_offset;
        let mut quorum = self.quorum.lock().expect("quorum state mutex poisoned");
        let vote_granted = quorum.is_voter(request.candidate_id)
            && request.last_metadata_offset >= current_offset
            && quorum.record_vote(request.candidate_id, request.term);
        Ok(VoteResponse {
            term: quorum.current_term(),
            vote_granted,
        })
    }

    pub fn run_election<T: ClusterRpcTransport>(
        &self,
        transport: &T,
        targets: &[crate::cluster::ClusterRpcTarget],
    ) -> Result<bool> {
        let (term, candidate_id, last_metadata_offset, majority_with_self) = {
            let mut quorum = self.quorum.lock().expect("quorum state mutex poisoned");
            let term = quorum.become_candidate();
            (
                term,
                quorum.local_node_id(),
                self.metadata_image().metadata_offset,
                quorum.has_majority(1),
            )
        };
        if majority_with_self {
            let mut quorum = self.quorum.lock().expect("quorum state mutex poisoned");
            quorum.become_leader();
            if let Some(leader_id) = quorum.snapshot().leader_id {
                self.metadata
                    .lock()
                    .expect("cluster metadata mutex poisoned")
                    .sync_controller(leader_id)?;
            }
            return Ok(true);
        }

        let mut votes = 1_usize;
        for target in targets {
            let response = transport.vote_to(
                target,
                VoteRequest {
                    term,
                    candidate_id,
                    last_metadata_offset,
                },
            )?;
            if response.term > term {
                let mut quorum = self.quorum.lock().expect("quorum state mutex poisoned");
                quorum.step_down(response.term);
                return Ok(false);
            }
            if response.vote_granted {
                votes += 1;
            }
        }
        let mut quorum = self.quorum.lock().expect("quorum state mutex poisoned");
        if quorum.has_majority(votes) {
            quorum.become_leader();
            if let Some(leader_id) = quorum.snapshot().leader_id {
                self.metadata
                    .lock()
                    .expect("cluster metadata mutex poisoned")
                    .sync_controller(leader_id)?;
            }
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn append_with_retry(
        &self,
        build: impl Fn(i64, i64, i32) -> AppendMetadataRequest,
    ) -> Result<AppendMetadataResponse> {
        const MAX_ATTEMPTS: usize = 3;
        for _ in 0..MAX_ATTEMPTS {
            let snapshot = self.quorum_snapshot();
            let leader_id = snapshot.leader_id.unwrap_or(self.config.node_id);
            let request = build(
                self.metadata_image().metadata_offset,
                snapshot.current_term,
                leader_id,
            );
            let response = self.handle_append_metadata(request)?;
            if response.accepted {
                return Ok(response);
            }
        }
        anyhow::bail!("metadata append rejected after retry budget")
    }

    pub fn handle_update_partition_leader(
        &self,
        request: UpdatePartitionLeaderRequest,
    ) -> Result<UpdatePartitionLeaderResponse> {
        if !self.can_write_metadata_locally() {
            return Ok(UpdatePartitionLeaderResponse {
                accepted: false,
                metadata_offset: self.metadata_image().metadata_offset,
            });
        }
        let mut preview = self.metadata_image();
        if !preview.update_partition_leader(
            &request.topic_name,
            request.partition_index,
            request.leader_id,
            request.leader_epoch,
        ) {
            return Ok(UpdatePartitionLeaderResponse {
                accepted: false,
                metadata_offset: preview.metadata_offset,
            });
        }
        let response = self.append_with_retry(|prev_metadata_offset, term, leader_id| {
            AppendMetadataRequest {
                term,
                leader_id,
                prev_metadata_offset,
                records: vec![crate::cluster::MetadataRecord::UpdatePartitionLeader {
                    topic_name: request.topic_name.clone(),
                    partition_index: request.partition_index,
                    leader_id: request.leader_id,
                    leader_epoch: request.leader_epoch,
                }],
            }
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
        if !self.can_write_metadata_locally() {
            return Ok(UpdatePartitionReplicationResponse {
                accepted: false,
                metadata_offset: self.metadata_image().metadata_offset,
            });
        }
        let response = self.append_with_retry(|prev_metadata_offset, term, leader_id| {
            AppendMetadataRequest {
                term,
                leader_id,
                prev_metadata_offset,
                records: vec![crate::cluster::MetadataRecord::UpdatePartitionReplication {
                    topic_name: request.topic_name.clone(),
                    partition_index: request.partition_index,
                    replicas: request.replicas.clone(),
                    isr: request.isr.clone(),
                    leader_epoch: request.leader_epoch,
                }],
            }
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
        if !self.can_write_metadata_locally() {
            return Ok(UpdateReplicaProgressResponse {
                accepted: false,
                metadata_offset: self.metadata_image().metadata_offset,
                high_watermark: self
                    .metadata_image()
                    .partition_high_watermark(&request.topic_name, request.partition_index)
                    .unwrap_or(0),
            });
        }
        let topic_name = request.topic_name.clone();
        let partition_index = request.partition_index;
        let image_before = self.metadata_image();
        let Some((_, current_epoch, _, _)) =
            image_before.partition_state_view(&topic_name, partition_index)
        else {
            return Ok(UpdateReplicaProgressResponse {
                accepted: false,
                metadata_offset: image_before.metadata_offset,
                high_watermark: 0,
            });
        };
        if request.leader_epoch != current_epoch {
            return Ok(UpdateReplicaProgressResponse {
                accepted: false,
                metadata_offset: image_before.metadata_offset,
                high_watermark: image_before
                    .partition_high_watermark(&topic_name, partition_index)
                    .unwrap_or(0),
            });
        }
        let response = self.append_with_retry(|prev_metadata_offset, term, leader_id| {
            AppendMetadataRequest {
                term,
                leader_id,
                prev_metadata_offset,
                records: vec![crate::cluster::MetadataRecord::UpdateReplicaProgress {
                    topic_name: request.topic_name.clone(),
                    partition_index: request.partition_index,
                    leader_epoch: request.leader_epoch,
                    progress: crate::cluster::ReplicaProgress {
                        broker_id: request.broker_id,
                        log_end_offset: request.log_end_offset,
                        last_caught_up_ms: request.last_caught_up_ms,
                    },
                }],
            }
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

    pub fn handle_get_partition_state(
        &self,
        request: GetPartitionStateRequest,
    ) -> Result<GetPartitionStateResponse> {
        let Some((leader_id, leader_epoch, high_watermark, leader_log_end_offset)) = self
            .metadata_image()
            .partition_state_view(&request.topic_name, request.partition_index)
        else {
            return Ok(GetPartitionStateResponse {
                found: false,
                leader_id: -1,
                leader_epoch: -1,
                high_watermark: -1,
                leader_log_end_offset: -1,
            });
        };
        Ok(GetPartitionStateResponse {
            found: true,
            leader_id,
            leader_epoch,
            high_watermark,
            leader_log_end_offset,
        })
    }

    pub fn handle_replica_fetch(
        &self,
        _request: ReplicaFetchRequest,
    ) -> Result<ReplicaFetchResponse> {
        anyhow::bail!("replica fetch requires broker data-plane transport")
    }

    pub fn handle_begin_partition_reassignment(
        &self,
        request: BeginPartitionReassignmentRequest,
    ) -> Result<PartitionReassignmentResponse> {
        if !self.can_write_metadata_locally() {
            return Ok(PartitionReassignmentResponse {
                accepted: false,
                metadata_offset: self.metadata_image().metadata_offset,
            });
        }
        let mut preview = self.metadata_image();
        if !preview.begin_partition_reassignment(
            &request.topic_name,
            request.partition_index,
            request.target_replicas.clone(),
        ) {
            return Ok(PartitionReassignmentResponse {
                accepted: false,
                metadata_offset: preview.metadata_offset,
            });
        }
        let response = self.append_with_retry(|prev_metadata_offset, term, leader_id| {
            AppendMetadataRequest {
                term,
                leader_id,
                prev_metadata_offset,
                records: vec![crate::cluster::MetadataRecord::BeginPartitionReassignment {
                    topic_name: request.topic_name.clone(),
                    partition_index: request.partition_index,
                    target_replicas: request.target_replicas.clone(),
                }],
            }
        })?;
        Ok(PartitionReassignmentResponse {
            accepted: response.accepted,
            metadata_offset: response.last_metadata_offset,
        })
    }

    pub fn handle_advance_partition_reassignment(
        &self,
        request: AdvancePartitionReassignmentRequest,
    ) -> Result<PartitionReassignmentResponse> {
        if !self.can_write_metadata_locally() {
            return Ok(PartitionReassignmentResponse {
                accepted: false,
                metadata_offset: self.metadata_image().metadata_offset,
            });
        }
        let mut preview = self.metadata_image();
        let preview_accepted = if request.step == crate::cluster::ReassignmentStep::Complete {
            preview.complete_partition_reassignment(&request.topic_name, request.partition_index)
        } else {
            preview.advance_partition_reassignment(
                &request.topic_name,
                request.partition_index,
                request.step.clone(),
            )
        };
        if !preview_accepted {
            return Ok(PartitionReassignmentResponse {
                accepted: false,
                metadata_offset: preview.metadata_offset,
            });
        }
        let record = if request.step == crate::cluster::ReassignmentStep::Complete {
            crate::cluster::MetadataRecord::CompletePartitionReassignment {
                topic_name: request.topic_name.clone(),
                partition_index: request.partition_index,
            }
        } else {
            crate::cluster::MetadataRecord::AdvancePartitionReassignment {
                topic_name: request.topic_name.clone(),
                partition_index: request.partition_index,
                step: request.step.clone(),
            }
        };
        let response = self.append_with_retry(|prev_metadata_offset, term, leader_id| {
            AppendMetadataRequest {
                term,
                leader_id,
                prev_metadata_offset,
                records: vec![record.clone()],
            }
        })?;
        Ok(PartitionReassignmentResponse {
            accepted: response.accepted,
            metadata_offset: response.last_metadata_offset,
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
            ClusterRpcRequest::GetPartitionState(request) => Ok(
                ClusterRpcResponse::GetPartitionState(self.handle_get_partition_state(request)?),
            ),
            ClusterRpcRequest::ReplicaFetch(request) => Ok(ClusterRpcResponse::ReplicaFetch(
                self.handle_replica_fetch(request)?,
            )),
            ClusterRpcRequest::BeginPartitionReassignment(request) => {
                Ok(ClusterRpcResponse::BeginPartitionReassignment(
                    self.handle_begin_partition_reassignment(request)?,
                ))
            }
            ClusterRpcRequest::AdvancePartitionReassignment(request) => {
                Ok(ClusterRpcResponse::AdvancePartitionReassignment(
                    self.handle_advance_partition_reassignment(request)?,
                ))
            }
            ClusterRpcRequest::Vote(request) => {
                Ok(ClusterRpcResponse::Vote(self.handle_vote(request)?))
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

    pub fn controller_target(&self) -> Option<crate::cluster::ClusterRpcTarget> {
        let leader_id = self.quorum_snapshot().leader_id?;
        self.remote_transport().resolve_target(leader_id).ok()
    }

    pub fn route_update_partition_leader<T: ClusterRpcTransport>(
        &self,
        transport: &T,
        request: UpdatePartitionLeaderRequest,
    ) -> Result<UpdatePartitionLeaderResponse> {
        if self.can_write_metadata_locally() {
            return self.handle_update_partition_leader(request);
        }
        let Some(target) = self.controller_target() else {
            return Ok(UpdatePartitionLeaderResponse {
                accepted: false,
                metadata_offset: self.metadata_image().metadata_offset,
            });
        };
        transport.update_partition_leader_to(&target, request)
    }

    pub async fn route_update_partition_leader_via_tcp(
        &self,
        transport: &crate::cluster::TcpClusterRpcTransport,
        request: UpdatePartitionLeaderRequest,
    ) -> Result<UpdatePartitionLeaderResponse> {
        if self.can_write_metadata_locally() {
            return self.handle_update_partition_leader(request);
        }
        let Some(target) = self.controller_target() else {
            return Ok(UpdatePartitionLeaderResponse {
                accepted: false,
                metadata_offset: self.metadata_image().metadata_offset,
            });
        };
        transport.update_partition_leader_to(&target, request).await
    }

    pub fn route_update_partition_replication<T: ClusterRpcTransport>(
        &self,
        transport: &T,
        request: UpdatePartitionReplicationRequest,
    ) -> Result<UpdatePartitionReplicationResponse> {
        if self.can_write_metadata_locally() {
            return self.handle_update_partition_replication(request);
        }
        let Some(target) = self.controller_target() else {
            return Ok(UpdatePartitionReplicationResponse {
                accepted: false,
                metadata_offset: self.metadata_image().metadata_offset,
            });
        };
        transport.update_partition_replication_to(&target, request)
    }

    pub fn route_begin_partition_reassignment<T: ClusterRpcTransport>(
        &self,
        transport: &T,
        request: BeginPartitionReassignmentRequest,
    ) -> Result<PartitionReassignmentResponse> {
        if self.can_write_metadata_locally() {
            return self.handle_begin_partition_reassignment(request);
        }
        let Some(target) = self.controller_target() else {
            return Ok(PartitionReassignmentResponse {
                accepted: false,
                metadata_offset: self.metadata_image().metadata_offset,
            });
        };
        transport.begin_partition_reassignment_to(&target, request)
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
                && config.cluster.controller_quorum_voters.len() <= 1
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
            if response.accepted {
                let _ = transport.broker_heartbeat(BrokerHeartbeatRequest {
                    node_id: config.broker.broker_id,
                    broker_epoch: response.broker_epoch,
                    timestamp_ms: now_ms,
                });
            }
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
    use tokio::net::TcpListener;

    use crate::cluster::{
        ProcessRole,
        test_support::{ThreeNodeClusterHarness, TwoNodeClusterHarness},
    };
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
    fn multi_controller_bootstrap_starts_without_self_election() {
        let dir = tempdir().unwrap();
        let mut config = Config::single_node(dir.path().join("data"), 19092, 1);
        config.cluster.node_id = 4;
        config.cluster.process_roles = vec![ProcessRole::Controller];
        config.cluster.controller_quorum_voters = vec![
            crate::cluster::ControllerQuorumVoter {
                node_id: 4,
                host: "node4".to_string(),
                port: 9093,
            },
            crate::cluster::ControllerQuorumVoter {
                node_id: 5,
                host: "node5".to_string(),
                port: 9093,
            },
        ];

        let runtime = ClusterRuntime::from_config(&config).unwrap();

        assert_eq!(runtime.quorum_snapshot().leader_id, None);
    }

    #[test]
    fn three_node_election_requires_majority_votes() {
        let harness = ThreeNodeClusterHarness::new_controller_triplet();
        let transport = harness.transport_from_node(1);
        let targets = vec![
            transport.resolve_target(2).unwrap(),
            transport.resolve_target(3).unwrap(),
        ];

        let elected = harness
            .node1
            .runtime
            .run_election(&transport, &targets)
            .unwrap();

        assert!(elected);
        assert_eq!(harness.node1.runtime.quorum_snapshot().leader_id, Some(1));
        assert_eq!(harness.node1.runtime.metadata_image().controller_id, 1);
    }

    #[test]
    fn three_node_election_fails_without_majority() {
        let harness = ThreeNodeClusterHarness::new_controller_triplet();
        let transport = harness.transport_from_node(1);
        let targets = vec![];

        let elected = harness
            .node1
            .runtime
            .run_election(&transport, &targets)
            .unwrap();

        assert!(!elected);
        assert_eq!(harness.node1.runtime.quorum_snapshot().leader_id, None);
    }

    #[test]
    fn non_leader_controller_routes_partition_leader_update_to_elected_controller() {
        let harness = TwoNodeClusterHarness::new_controller_pair();
        let transport = harness.transport_from_node1();
        let _ = harness
            .node1
            .runtime
            .handle_append_metadata(AppendMetadataRequest {
                term: 1,
                leader_id: 2,
                prev_metadata_offset: harness.node1.runtime.metadata_image().metadata_offset,
                records: vec![crate::cluster::MetadataRecord::SetController { controller_id: 2 }],
            })
            .unwrap();
        let _ = harness
            .node2
            .runtime
            .handle_append_metadata(AppendMetadataRequest {
                term: 1,
                leader_id: 2,
                prev_metadata_offset: harness.node2.runtime.metadata_image().metadata_offset,
                records: vec![crate::cluster::MetadataRecord::SetController { controller_id: 2 }],
            })
            .unwrap();
        harness
            .node2
            .runtime
            .sync_local_topics(
                &[crate::store::TopicMetadata {
                    name: "route.topic".to_string(),
                    partitions: vec![crate::store::PartitionMetadata { partition: 0 }],
                }],
                2,
            )
            .unwrap();

        let response = harness
            .node1
            .runtime
            .route_update_partition_leader(
                &transport,
                UpdatePartitionLeaderRequest {
                    topic_name: "route.topic".to_string(),
                    partition_index: 0,
                    leader_id: 2,
                    leader_epoch: 1,
                },
            )
            .unwrap();

        assert!(response.accepted);
        assert_eq!(
            harness
                .node2
                .runtime
                .metadata_image()
                .partition_leader_id("route.topic", 0),
            Some(2)
        );
    }

    #[tokio::test]
    async fn non_leader_controller_routes_partition_leader_update_via_tcp() {
        let harness = TwoNodeClusterHarness::new_controller_pair();
        let _ = harness
            .node2
            .runtime
            .handle_append_metadata(AppendMetadataRequest {
                term: 1,
                leader_id: 2,
                prev_metadata_offset: harness.node2.runtime.metadata_image().metadata_offset,
                records: vec![crate::cluster::MetadataRecord::SetController { controller_id: 2 }],
            })
            .unwrap();
        harness
            .node2
            .runtime
            .sync_local_topics(
                &[crate::store::TopicMetadata {
                    name: "tcp.route.topic".to_string(),
                    partitions: vec![crate::store::PartitionMetadata { partition: 0 }],
                }],
                2,
            )
            .unwrap();

        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let server_runtime = harness.node2.runtime.clone();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            crate::cluster::TcpClusterRpcTransport::serve_runtime_once(&listener, server_runtime)
                .await
                .unwrap();
        });

        let transport = crate::cluster::TcpClusterRpcTransport;
        let response = transport
            .update_partition_leader_to(
                &crate::cluster::ClusterRpcTarget {
                    node_id: 2,
                    host: addr.ip().to_string(),
                    port: addr.port(),
                },
                UpdatePartitionLeaderRequest {
                    topic_name: "tcp.route.topic".to_string(),
                    partition_index: 0,
                    leader_id: 2,
                    leader_epoch: 1,
                },
            )
            .await
            .unwrap();

        assert!(response.accepted);
        assert_eq!(
            harness
                .node2
                .runtime
                .metadata_image()
                .partition_leader_id("tcp.route.topic", 0),
            Some(2)
        );
        server.await.unwrap();
    }

    #[test]
    fn election_steps_down_on_higher_term_vote_response() {
        let harness = ThreeNodeClusterHarness::new_controller_triplet();
        let transport = harness.transport_from_node(1);
        let target = transport.resolve_target(2).unwrap();
        let _ = harness
            .node2
            .runtime
            .handle_vote(VoteRequest {
                term: 5,
                candidate_id: 2,
                last_metadata_offset: harness.node2.runtime.metadata_image().metadata_offset,
            })
            .unwrap();

        let elected = harness
            .node1
            .runtime
            .run_election(&transport, &[target])
            .unwrap();

        assert!(!elected);
        assert_eq!(harness.node1.runtime.quorum_snapshot().current_term, 5);
        assert_eq!(harness.node1.runtime.quorum_snapshot().leader_id, None);
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
    fn stale_term_append_is_rejected() {
        let dir = tempdir().unwrap();
        let mut config = Config::single_node(dir.path().join("data"), 19092, 1);
        config.cluster.node_id = 4;
        config.cluster.process_roles = vec![ProcessRole::Controller];

        let runtime = ClusterRuntime::from_config(&config).unwrap();
        let _ = runtime
            .handle_append_metadata(AppendMetadataRequest {
                term: 2,
                leader_id: 9,
                prev_metadata_offset: runtime.metadata_image().metadata_offset,
                records: vec![crate::cluster::MetadataRecord::SetController { controller_id: 9 }],
            })
            .unwrap();
        let rejected = runtime
            .handle_append_metadata(AppendMetadataRequest {
                term: 1,
                leader_id: 8,
                prev_metadata_offset: runtime.metadata_image().metadata_offset,
                records: vec![crate::cluster::MetadataRecord::SetController { controller_id: 8 }],
            })
            .unwrap();

        assert!(!rejected.accepted);
        assert_eq!(runtime.metadata_image().controller_id, 9);
    }

    #[test]
    fn same_term_append_with_different_leader_is_rejected() {
        let dir = tempdir().unwrap();
        let mut config = Config::single_node(dir.path().join("data"), 19092, 1);
        config.cluster.node_id = 4;
        config.cluster.process_roles = vec![ProcessRole::Controller];

        let runtime = ClusterRuntime::from_config(&config).unwrap();
        let accepted = runtime
            .handle_append_metadata(AppendMetadataRequest {
                term: 2,
                leader_id: 9,
                prev_metadata_offset: runtime.metadata_image().metadata_offset,
                records: vec![crate::cluster::MetadataRecord::SetController { controller_id: 9 }],
            })
            .unwrap();
        let rejected = runtime
            .handle_append_metadata(AppendMetadataRequest {
                term: 2,
                leader_id: 8,
                prev_metadata_offset: runtime.metadata_image().metadata_offset,
                records: vec![crate::cluster::MetadataRecord::SetController { controller_id: 8 }],
            })
            .unwrap();

        assert!(accepted.accepted);
        assert!(!rejected.accepted);
        assert_eq!(runtime.quorum_snapshot().leader_id, Some(9));
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
    fn update_partition_leader_preserves_replication_state() {
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
        runtime
            .handle_update_partition_replication(UpdatePartitionReplicationRequest {
                topic_name: "leader.topic".to_string(),
                partition_index: 0,
                replicas: vec![1, 2],
                isr: vec![1, 2],
                leader_epoch: 1,
            })
            .unwrap();
        runtime
            .handle_update_replica_progress(UpdateReplicaProgressRequest {
                topic_name: "leader.topic".to_string(),
                partition_index: 0,
                leader_epoch: 1,
                broker_id: 1,
                log_end_offset: 10,
                last_caught_up_ms: 100,
            })
            .unwrap();

        runtime
            .handle_update_partition_leader(UpdatePartitionLeaderRequest {
                topic_name: "leader.topic".to_string(),
                partition_index: 0,
                leader_id: 2,
                leader_epoch: 2,
            })
            .unwrap();

        let partition = &runtime.metadata_image().topics[0].partitions[0];
        assert_eq!(partition.replicas, vec![1, 2]);
        assert!(partition.replica_progress.iter().any(|p| p.broker_id == 1));
        assert_eq!(partition.high_watermark, 10);
    }

    #[test]
    fn update_partition_leader_rejects_older_epoch() {
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
        let _ = runtime
            .handle_update_partition_leader(UpdatePartitionLeaderRequest {
                topic_name: "leader.topic".to_string(),
                partition_index: 0,
                leader_id: 9,
                leader_epoch: 3,
            })
            .unwrap();

        let rejected = runtime
            .handle_update_partition_leader(UpdatePartitionLeaderRequest {
                topic_name: "leader.topic".to_string(),
                partition_index: 0,
                leader_id: 8,
                leader_epoch: 2,
            })
            .unwrap();

        assert!(!rejected.accepted);
        assert_eq!(
            runtime
                .metadata_image()
                .partition_leader_id("leader.topic", 0),
            Some(9)
        );
        assert_eq!(
            runtime.metadata_image().topics[0].partitions[0].leader_epoch,
            3
        );
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
                leader_epoch: 1,
                broker_id: 1,
                log_end_offset: 10,
                last_caught_up_ms: 100,
            })
            .unwrap();
        let response = runtime
            .handle_update_replica_progress(UpdateReplicaProgressRequest {
                topic_name: "progress.topic".to_string(),
                partition_index: 0,
                leader_epoch: 1,
                broker_id: 2,
                log_end_offset: 8,
                last_caught_up_ms: 100,
            })
            .unwrap();

        assert!(response.accepted);
        assert_eq!(response.high_watermark, 10);
        assert_eq!(
            runtime
                .metadata_image()
                .partition_high_watermark("progress.topic", 0),
            Some(10)
        );
    }

    #[test]
    fn replica_progress_reconciles_isr_by_lag() {
        let dir = tempdir().unwrap();
        let mut config = Config::single_node(dir.path().join("data"), 19092, 1);
        config.cluster.node_id = 4;
        config.cluster.process_roles = vec![ProcessRole::Broker, ProcessRole::Controller];

        let runtime = ClusterRuntime::from_config(&config).unwrap();
        runtime
            .sync_local_topics(
                &[crate::store::TopicMetadata {
                    name: "isr.topic".to_string(),
                    partitions: vec![crate::store::PartitionMetadata { partition: 0 }],
                }],
                1,
            )
            .unwrap();
        runtime
            .handle_update_partition_replication(UpdatePartitionReplicationRequest {
                topic_name: "isr.topic".to_string(),
                partition_index: 0,
                replicas: vec![1, 2, 3],
                isr: vec![1, 2, 3],
                leader_epoch: 1,
            })
            .unwrap();
        runtime
            .handle_update_replica_progress(UpdateReplicaProgressRequest {
                topic_name: "isr.topic".to_string(),
                partition_index: 0,
                leader_epoch: 1,
                broker_id: 1,
                log_end_offset: 10,
                last_caught_up_ms: 100,
            })
            .unwrap();
        runtime
            .handle_update_replica_progress(UpdateReplicaProgressRequest {
                topic_name: "isr.topic".to_string(),
                partition_index: 0,
                leader_epoch: 1,
                broker_id: 2,
                log_end_offset: 10,
                last_caught_up_ms: 100,
            })
            .unwrap();
        runtime
            .handle_update_replica_progress(UpdateReplicaProgressRequest {
                topic_name: "isr.topic".to_string(),
                partition_index: 0,
                leader_epoch: 1,
                broker_id: 3,
                log_end_offset: 5,
                last_caught_up_ms: 100,
            })
            .unwrap();

        let image = runtime.metadata_image();
        assert_eq!(image.topics[0].partitions[0].isr, vec![1, 2]);
        assert_eq!(image.topics[0].partitions[0].high_watermark, 10);
    }

    #[test]
    fn reassignment_lifecycle_updates_partition_metadata() {
        let dir = tempdir().unwrap();
        let mut config = Config::single_node(dir.path().join("data"), 19092, 1);
        config.cluster.node_id = 4;
        config.cluster.process_roles = vec![ProcessRole::Broker, ProcessRole::Controller];

        let runtime = ClusterRuntime::from_config(&config).unwrap();
        runtime
            .sync_local_topics(
                &[crate::store::TopicMetadata {
                    name: "reassign.topic".to_string(),
                    partitions: vec![crate::store::PartitionMetadata { partition: 0 }],
                }],
                1,
            )
            .unwrap();

        let begin = runtime
            .handle_begin_partition_reassignment(BeginPartitionReassignmentRequest {
                topic_name: "reassign.topic".to_string(),
                partition_index: 0,
                target_replicas: vec![2, 3],
            })
            .unwrap();
        assert!(begin.accepted);
        assert_eq!(
            runtime
                .metadata_image()
                .partition_reassignment("reassign.topic", 0)
                .unwrap()
                .step,
            crate::cluster::ReassignmentStep::Planned
        );

        runtime
            .handle_update_partition_replication(UpdatePartitionReplicationRequest {
                topic_name: "reassign.topic".to_string(),
                partition_index: 0,
                replicas: vec![1, 2, 3],
                isr: vec![1, 2],
                leader_epoch: 1,
            })
            .unwrap();
        for broker_id in [2, 3] {
            runtime
                .handle_update_replica_progress(UpdateReplicaProgressRequest {
                    topic_name: "reassign.topic".to_string(),
                    partition_index: 0,
                    leader_epoch: 1,
                    broker_id,
                    log_end_offset: 0,
                    last_caught_up_ms: 100,
                })
                .unwrap();
        }

        for step in [
            crate::cluster::ReassignmentStep::Copying,
            crate::cluster::ReassignmentStep::ExpandingIsr,
            crate::cluster::ReassignmentStep::LeaderSwitch,
            crate::cluster::ReassignmentStep::Shrinking,
            crate::cluster::ReassignmentStep::Complete,
        ] {
            runtime
                .handle_advance_partition_reassignment(AdvancePartitionReassignmentRequest {
                    topic_name: "reassign.topic".to_string(),
                    partition_index: 0,
                    step,
                })
                .unwrap();
        }

        let image = runtime.metadata_image();
        let partition = &image.topics[0].partitions[0];
        assert_eq!(partition.replicas, vec![2, 3]);
        assert_eq!(partition.leader_id, 2);
        assert!(partition.reassignment.is_none());
    }

    #[test]
    fn reassignment_requires_targets_caught_up_before_expanding_isr() {
        let dir = tempdir().unwrap();
        let mut config = Config::single_node(dir.path().join("data"), 19092, 1);
        config.cluster.node_id = 4;
        config.cluster.process_roles = vec![ProcessRole::Broker, ProcessRole::Controller];
        let runtime = ClusterRuntime::from_config(&config).unwrap();
        runtime
            .sync_local_topics(
                &[crate::store::TopicMetadata {
                    name: "reassign.topic".to_string(),
                    partitions: vec![crate::store::PartitionMetadata { partition: 0 }],
                }],
                1,
            )
            .unwrap();
        runtime
            .handle_update_partition_replication(UpdatePartitionReplicationRequest {
                topic_name: "reassign.topic".to_string(),
                partition_index: 0,
                replicas: vec![1, 2],
                isr: vec![1, 2],
                leader_epoch: 1,
            })
            .unwrap();
        runtime
            .handle_update_replica_progress(UpdateReplicaProgressRequest {
                topic_name: "reassign.topic".to_string(),
                partition_index: 0,
                leader_epoch: 1,
                broker_id: 1,
                log_end_offset: 10,
                last_caught_up_ms: 100,
            })
            .unwrap();
        runtime
            .handle_update_replica_progress(UpdateReplicaProgressRequest {
                topic_name: "reassign.topic".to_string(),
                partition_index: 0,
                leader_epoch: 1,
                broker_id: 2,
                log_end_offset: 10,
                last_caught_up_ms: 100,
            })
            .unwrap();
        runtime
            .handle_begin_partition_reassignment(BeginPartitionReassignmentRequest {
                topic_name: "reassign.topic".to_string(),
                partition_index: 0,
                target_replicas: vec![2, 3],
            })
            .unwrap();

        let response = runtime
            .handle_advance_partition_reassignment(AdvancePartitionReassignmentRequest {
                topic_name: "reassign.topic".to_string(),
                partition_index: 0,
                step: crate::cluster::ReassignmentStep::ExpandingIsr,
            })
            .unwrap();

        assert!(!response.accepted);
        assert_eq!(
            runtime.metadata_image().topics[0].partitions[0].replicas,
            vec![1, 2]
        );
    }
}
