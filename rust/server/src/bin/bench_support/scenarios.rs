mod cluster;
mod metrics;
mod single_broker;

pub use cluster::{run_cluster_reassignment_metadata, run_cluster_replication_metadata};
pub use single_broker::{run_commit_resume, run_fetch_tail, run_produce_only, run_roundtrip};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ScenarioKind {
    ProduceOnly,
    Roundtrip,
    FetchTail,
    CommitResume,
    MixedHandoff,
    ClusterReplicationMetadata,
    ClusterReassignmentMetadata,
}

pub struct ScenarioSpec {
    pub name: &'static str,
    pub kind: ScenarioKind,
    pub messages: u32,
    pub payload_bytes: u32,
    pub default_partitions: i32,
}
