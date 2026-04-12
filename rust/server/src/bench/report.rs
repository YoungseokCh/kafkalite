use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchmarkReport {
    pub schema_version: u32,
    pub run_id: String,
    pub git_sha: String,
    pub dirty: bool,
    pub host: HostInfo,
    pub build: BuildMetrics,
    pub scenarios: Vec<ScenarioReport>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HostInfo {
    pub os: String,
    pub arch: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BuildMetrics {
    pub profile: String,
    pub binary_bytes: u64,
    pub package_bytes: u64,
    pub stripped_binary_bytes: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScenarioReport {
    pub name: String,
    pub iterations: u32,
    pub warmups: u32,
    pub messages: u32,
    pub payload_bytes: u32,
    pub default_partitions: i32,
    pub runtime: RuntimeMetrics,
    pub memory: MemoryMetrics,
    pub storage: StorageMetrics,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeMetrics {
    pub elapsed_ms: f64,
    pub throughput_msgs_per_sec: f64,
    pub throughput_bytes_per_sec: f64,
    pub latency_p50_ms: f64,
    pub latency_p95_ms: f64,
    pub latency_p99_ms: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryMetrics {
    pub peak_rss_kb: u64,
    pub final_rss_kb: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageMetrics {
    pub total_bytes: u64,
    pub log_bytes: u64,
    pub index_bytes: u64,
    pub timeindex_bytes: u64,
    pub state_snapshot_bytes: u64,
    pub state_journal_bytes: u64,
    pub bytes_per_record: f64,
    pub bytes_per_payload_byte: f64,
}
