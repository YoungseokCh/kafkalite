use serde::{Deserialize, Serialize};

use crate::cluster::rpc::{
    AdvancePartitionReassignmentRequest, AppendMetadataRequest, AppendMetadataResponse,
    ApplyReplicaRecordsRequest, ApplyReplicaRecordsResponse, BeginPartitionReassignmentRequest,
    BrokerHeartbeatRequest, BrokerHeartbeatResponse, GetPartitionStateRequest,
    GetPartitionStateResponse, PartitionReassignmentResponse, RegisterBrokerRequest,
    RegisterBrokerResponse, ReplicaFetchRequest, ReplicaFetchResponse,
    UpdatePartitionLeaderRequest, UpdatePartitionLeaderResponse, UpdatePartitionReplicationRequest,
    UpdatePartitionReplicationResponse, UpdateReplicaProgressRequest,
    UpdateReplicaProgressResponse, VoteRequest, VoteResponse,
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ClusterRpcRequest {
    AppendMetadata(AppendMetadataRequest),
    RegisterBroker(RegisterBrokerRequest),
    BrokerHeartbeat(BrokerHeartbeatRequest),
    UpdatePartitionLeader(UpdatePartitionLeaderRequest),
    UpdatePartitionReplication(UpdatePartitionReplicationRequest),
    UpdateReplicaProgress(UpdateReplicaProgressRequest),
    GetPartitionState(GetPartitionStateRequest),
    ReplicaFetch(ReplicaFetchRequest),
    ApplyReplicaRecords(ApplyReplicaRecordsRequest),
    BeginPartitionReassignment(BeginPartitionReassignmentRequest),
    AdvancePartitionReassignment(AdvancePartitionReassignmentRequest),
    Vote(VoteRequest),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ClusterRpcResponse {
    AppendMetadata(AppendMetadataResponse),
    RegisterBroker(RegisterBrokerResponse),
    BrokerHeartbeat(BrokerHeartbeatResponse),
    UpdatePartitionLeader(UpdatePartitionLeaderResponse),
    UpdatePartitionReplication(UpdatePartitionReplicationResponse),
    UpdateReplicaProgress(UpdateReplicaProgressResponse),
    GetPartitionState(GetPartitionStateResponse),
    ReplicaFetch(ReplicaFetchResponse),
    ApplyReplicaRecords(ApplyReplicaRecordsResponse),
    BeginPartitionReassignment(PartitionReassignmentResponse),
    AdvancePartitionReassignment(PartitionReassignmentResponse),
    Vote(VoteResponse),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClusterRpcTarget {
    pub node_id: i32,
    pub host: String,
    pub port: u16,
}

mod local;
mod remote;
mod tcp;
mod traits;

pub use local::LocalClusterRpcTransport;
pub use remote::{
    InMemoryClusterNetwork, InMemoryRemoteClusterRpcTransport, RemoteClusterRpcTransport,
};
pub use tcp::TcpClusterRpcTransport;
pub use traits::ClusterRpcTransport;

#[cfg(test)]
mod tests;
