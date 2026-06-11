use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::Result;

use super::ScenarioSpec;
use crate::bench_support::broker_process::BrokerProcess;
use crate::bench_support::report::{MemoryMetrics, RuntimeMetrics, ScenarioReport, StorageMetrics};

pub(super) fn build_report(
    spec: &ScenarioSpec,
    elapsed: Duration,
    latencies: &[Duration],
    broker: &BrokerProcess,
    root: &Path,
    messages: u32,
    payload_bytes: u32,
) -> ScenarioReport {
    let runtime = runtime_metrics(elapsed, latencies, messages, payload_bytes);
    let cpu = broker.cpu_metrics();
    let storage = storage_metrics(root.join("data"), messages, payload_bytes);
    let memory = MemoryMetrics {
        peak_rss_kb: broker.peak_rss_kb(),
        final_rss_kb: broker.final_rss_kb(),
    };
    ScenarioReport {
        name: spec.name.to_string(),
        iterations: 1,
        warmups: 0,
        messages,
        payload_bytes,
        default_partitions: spec.default_partitions,
        runtime,
        cpu,
        memory,
        storage,
    }
}

pub(super) fn runtime_metrics(
    elapsed: Duration,
    latencies: &[Duration],
    messages: u32,
    payload_bytes: u32,
) -> RuntimeMetrics {
    let elapsed_ms = elapsed.as_secs_f64() * 1000.0;
    let throughput_msgs = messages as f64 / elapsed.as_secs_f64().max(0.001);
    let throughput_bytes =
        (messages as f64 * payload_bytes as f64) / elapsed.as_secs_f64().max(0.001);
    let mut millis = latencies
        .iter()
        .map(|d| d.as_secs_f64() * 1000.0)
        .collect::<Vec<_>>();
    millis.sort_by(|a, b| a.partial_cmp(b).unwrap());
    RuntimeMetrics {
        elapsed_ms,
        throughput_msgs_per_sec: throughput_msgs,
        throughput_bytes_per_sec: throughput_bytes,
        latency_p50_ms: percentile(&millis, 0.50),
        latency_p95_ms: percentile(&millis, 0.95),
        latency_p99_ms: percentile(&millis, 0.99),
    }
}

fn storage_metrics(
    data_dir: impl AsRef<Path>,
    messages: u32,
    payload_bytes: u32,
) -> StorageMetrics {
    let mut total = 0_u64;
    let mut log_bytes = 0_u64;
    let mut index_bytes = 0_u64;
    let mut timeindex_bytes = 0_u64;
    let mut state_snapshot_bytes = 0_u64;
    let mut state_journal_bytes = 0_u64;
    if let Ok(entries) = walk(data_dir.as_ref()) {
        for (path, size) in entries {
            total += size;
            match path.extension().and_then(|ext| ext.to_str()) {
                Some("log") => log_bytes += size,
                Some("index") => index_bytes += size,
                Some("timeindex") => timeindex_bytes += size,
                Some("journal") => state_journal_bytes += size,
                Some("snapshot") | Some("json") if path.to_string_lossy().contains("state/") => {
                    state_snapshot_bytes += size
                }
                _ => {}
            }
        }
    }
    let payload_total = messages as f64 * payload_bytes as f64;
    StorageMetrics {
        total_bytes: total,
        log_bytes,
        index_bytes,
        timeindex_bytes,
        state_snapshot_bytes,
        state_journal_bytes,
        bytes_per_record: total as f64 / messages.max(1) as f64,
        bytes_per_payload_byte: total as f64 / payload_total.max(1.0),
    }
}

fn walk(root: &Path) -> Result<Vec<(PathBuf, u64)>> {
    let mut files = Vec::new();
    if !root.exists() {
        return Ok(files);
    }
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        let path = entry.path();
        if entry.file_type()?.is_dir() {
            files.extend(walk(&path)?);
        } else {
            files.push((path, entry.metadata()?.len()));
        }
    }
    Ok(files)
}

fn percentile(values: &[f64], pct: f64) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    let index = ((values.len() - 1) as f64 * pct).round() as usize;
    values[index]
}
