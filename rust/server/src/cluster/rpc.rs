use crate::cluster::metadata::MetadataRecord;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VoteRequest {
    pub term: i64,
    pub candidate_id: i32,
    pub last_metadata_offset: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VoteResponse {
    pub term: i64,
    pub vote_granted: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppendMetadataRequest {
    pub term: i64,
    pub leader_id: i32,
    pub prev_metadata_offset: i64,
    pub records: Vec<MetadataRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppendMetadataResponse {
    pub term: i64,
    pub accepted: bool,
    pub last_metadata_offset: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegisterBrokerRequest {
    pub node_id: i32,
    pub advertised_host: String,
    pub advertised_port: u16,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegisterBrokerResponse {
    pub broker_epoch: i64,
    pub controller_epoch: i64,
    pub leader_id: Option<i32>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BrokerHeartbeatRequest {
    pub node_id: i32,
    pub broker_epoch: i64,
    pub timestamp_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BrokerHeartbeatResponse {
    pub accepted: bool,
    pub controller_epoch: i64,
    pub leader_id: Option<i32>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdatePartitionLeaderRequest {
    pub topic_name: String,
    pub partition_index: i32,
    pub leader_id: i32,
    pub leader_epoch: i32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdatePartitionLeaderResponse {
    pub accepted: bool,
    pub metadata_offset: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdatePartitionReplicationRequest {
    pub topic_name: String,
    pub partition_index: i32,
    pub replicas: Vec<i32>,
    pub isr: Vec<i32>,
    pub leader_epoch: i32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdatePartitionReplicationResponse {
    pub accepted: bool,
    pub metadata_offset: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdateReplicaProgressRequest {
    pub topic_name: String,
    pub partition_index: i32,
    pub broker_id: i32,
    pub log_end_offset: i64,
    pub last_caught_up_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdateReplicaProgressResponse {
    pub accepted: bool,
    pub metadata_offset: i64,
    pub high_watermark: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GetPartitionStateRequest {
    pub topic_name: String,
    pub partition_index: i32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GetPartitionStateResponse {
    pub found: bool,
    pub leader_id: i32,
    pub leader_epoch: i32,
    pub high_watermark: i64,
    pub leader_log_end_offset: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicaFetchRequest {
    pub topic_name: String,
    pub partition_index: i32,
    pub start_offset: i64,
    pub max_records: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicaFetchResponse {
    pub found: bool,
    pub high_watermark: i64,
    pub leader_log_end_offset: i64,
    pub records: Vec<crate::store::BrokerRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BeginPartitionReassignmentRequest {
    pub topic_name: String,
    pub partition_index: i32,
    pub target_replicas: Vec<i32>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdvancePartitionReassignmentRequest {
    pub topic_name: String,
    pub partition_index: i32,
    pub step: crate::cluster::ReassignmentStep,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionReassignmentResponse {
    pub accepted: bool,
    pub metadata_offset: i64,
}
