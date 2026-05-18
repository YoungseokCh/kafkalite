use anyhow::Result;

use crate::cluster::quorum::QuorumSnapshot;
use crate::cluster::rpc::{
    BeginPartitionReassignmentRequest, PartitionReassignmentResponse, UpdatePartitionLeaderRequest,
    UpdatePartitionLeaderResponse, UpdatePartitionReplicationRequest,
    UpdatePartitionReplicationResponse,
};
use crate::cluster::transport::{LocalClusterRpcTransport, RemoteClusterRpcTransport};
use crate::cluster::{ClusterRpcRequest, ClusterRpcResponse, ClusterRpcTransport};
use crate::config::Config;

use super::ClusterRuntime;

impl ClusterRuntime {
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
            ClusterRpcRequest::ApplyReplicaRecords(_) => {
                anyhow::bail!("apply replica records requires broker data-plane transport")
            }
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

    pub async fn route_update_partition_replication_via_tcp(
        &self,
        transport: &crate::cluster::TcpClusterRpcTransport,
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
        transport
            .update_partition_replication_to(&target, request)
            .await
    }

    pub async fn route_begin_partition_reassignment_via_tcp(
        &self,
        transport: &crate::cluster::TcpClusterRpcTransport,
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
        transport
            .begin_partition_reassignment_to(&target, request)
            .await
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

    pub(super) fn bootstrap_local_state(&self, config: &Config) {
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
                .register_broker(crate::cluster::RegisterBrokerRequest {
                    node_id: config.broker.broker_id,
                    advertised_host: config.broker.advertised_host.clone(),
                    advertised_port: config.broker.advertised_port,
                })
                .expect("local broker registration should succeed");
            if response.accepted {
                let _ = transport.broker_heartbeat(crate::cluster::BrokerHeartbeatRequest {
                    node_id: config.broker.broker_id,
                    broker_epoch: response.broker_epoch,
                    timestamp_ms: now_ms,
                });
            }
        } else if config
            .cluster
            .has_role(crate::cluster::ProcessRole::Controller)
        {
            if let Some(leader_id) = self.quorum_snapshot().leader_id {
                let _ = self
                    .metadata
                    .lock()
                    .expect("cluster metadata mutex poisoned")
                    .sync_controller(leader_id);
            }
        }
    }
}
