use anyhow::{Result, bail};

use crate::cluster::rpc::{
    AdvancePartitionReassignmentRequest, AppendMetadataRequest, AppendMetadataResponse,
    BeginPartitionReassignmentRequest, BrokerHeartbeatRequest, BrokerHeartbeatResponse,
    GetPartitionStateRequest, GetPartitionStateResponse, PartitionReassignmentResponse,
    RegisterBrokerRequest, RegisterBrokerResponse, ReplicaFetchRequest, ReplicaFetchResponse,
    UpdatePartitionLeaderRequest, UpdatePartitionLeaderResponse, UpdatePartitionReplicationRequest,
    UpdatePartitionReplicationResponse, UpdateReplicaProgressRequest,
    UpdateReplicaProgressResponse, VoteRequest, VoteResponse,
};

use super::{ClusterRpcRequest, ClusterRpcResponse, ClusterRpcTarget};

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

    fn update_partition_leader_to(
        &self,
        target: &ClusterRpcTarget,
        request: UpdatePartitionLeaderRequest,
    ) -> Result<UpdatePartitionLeaderResponse> {
        match self.send_to(target, ClusterRpcRequest::UpdatePartitionLeader(request))? {
            ClusterRpcResponse::UpdatePartitionLeader(response) => Ok(response),
            other => bail!("unexpected RPC response: {other:?}"),
        }
    }

    fn update_partition_replication(
        &self,
        request: UpdatePartitionReplicationRequest,
    ) -> Result<UpdatePartitionReplicationResponse> {
        match self.send(ClusterRpcRequest::UpdatePartitionReplication(request))? {
            ClusterRpcResponse::UpdatePartitionReplication(response) => Ok(response),
            other => bail!("unexpected RPC response: {other:?}"),
        }
    }

    fn update_partition_replication_to(
        &self,
        target: &ClusterRpcTarget,
        request: UpdatePartitionReplicationRequest,
    ) -> Result<UpdatePartitionReplicationResponse> {
        match self.send_to(
            target,
            ClusterRpcRequest::UpdatePartitionReplication(request),
        )? {
            ClusterRpcResponse::UpdatePartitionReplication(response) => Ok(response),
            other => bail!("unexpected RPC response: {other:?}"),
        }
    }

    fn update_replica_progress(
        &self,
        request: UpdateReplicaProgressRequest,
    ) -> Result<UpdateReplicaProgressResponse> {
        match self.send(ClusterRpcRequest::UpdateReplicaProgress(request))? {
            ClusterRpcResponse::UpdateReplicaProgress(response) => Ok(response),
            other => bail!("unexpected RPC response: {other:?}"),
        }
    }

    fn update_replica_progress_to(
        &self,
        target: &ClusterRpcTarget,
        request: UpdateReplicaProgressRequest,
    ) -> Result<UpdateReplicaProgressResponse> {
        match self.send_to(target, ClusterRpcRequest::UpdateReplicaProgress(request))? {
            ClusterRpcResponse::UpdateReplicaProgress(response) => Ok(response),
            other => bail!("unexpected RPC response: {other:?}"),
        }
    }

    fn get_partition_state(
        &self,
        request: GetPartitionStateRequest,
    ) -> Result<GetPartitionStateResponse> {
        match self.send(ClusterRpcRequest::GetPartitionState(request))? {
            ClusterRpcResponse::GetPartitionState(response) => Ok(response),
            other => bail!("unexpected RPC response: {other:?}"),
        }
    }

    fn replica_fetch_to(
        &self,
        target: &ClusterRpcTarget,
        request: ReplicaFetchRequest,
    ) -> Result<ReplicaFetchResponse> {
        match self.send_to(target, ClusterRpcRequest::ReplicaFetch(request))? {
            ClusterRpcResponse::ReplicaFetch(response) => Ok(response),
            other => bail!("unexpected RPC response: {other:?}"),
        }
    }

    fn begin_partition_reassignment(
        &self,
        request: BeginPartitionReassignmentRequest,
    ) -> Result<PartitionReassignmentResponse> {
        match self.send(ClusterRpcRequest::BeginPartitionReassignment(request))? {
            ClusterRpcResponse::BeginPartitionReassignment(response) => Ok(response),
            other => bail!("unexpected RPC response: {other:?}"),
        }
    }

    fn begin_partition_reassignment_to(
        &self,
        target: &ClusterRpcTarget,
        request: BeginPartitionReassignmentRequest,
    ) -> Result<PartitionReassignmentResponse> {
        match self.send_to(
            target,
            ClusterRpcRequest::BeginPartitionReassignment(request),
        )? {
            ClusterRpcResponse::BeginPartitionReassignment(response) => Ok(response),
            other => bail!("unexpected RPC response: {other:?}"),
        }
    }

    fn advance_partition_reassignment(
        &self,
        request: AdvancePartitionReassignmentRequest,
    ) -> Result<PartitionReassignmentResponse> {
        match self.send(ClusterRpcRequest::AdvancePartitionReassignment(request))? {
            ClusterRpcResponse::AdvancePartitionReassignment(response) => Ok(response),
            other => bail!("unexpected RPC response: {other:?}"),
        }
    }

    fn advance_partition_reassignment_to(
        &self,
        target: &ClusterRpcTarget,
        request: AdvancePartitionReassignmentRequest,
    ) -> Result<PartitionReassignmentResponse> {
        match self.send_to(
            target,
            ClusterRpcRequest::AdvancePartitionReassignment(request),
        )? {
            ClusterRpcResponse::AdvancePartitionReassignment(response) => Ok(response),
            other => bail!("unexpected RPC response: {other:?}"),
        }
    }

    fn vote_to(&self, target: &ClusterRpcTarget, request: VoteRequest) -> Result<VoteResponse> {
        match self.send_to(target, ClusterRpcRequest::Vote(request))? {
            ClusterRpcResponse::Vote(response) => Ok(response),
            other => bail!("unexpected RPC response: {other:?}"),
        }
    }
}
