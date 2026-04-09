use std::fs;
use std::path::Path;
use std::time::{Duration, Instant};

use anyhow::Result;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::topic_partition_list::{Offset, TopicPartitionList};

use super::broker_process::BrokerProcess;
use super::report::{MemoryMetrics, RuntimeMetrics, ScenarioReport, StorageMetrics};

pub struct ScenarioSpec {
    pub name: &'static str,
    pub messages: u32,
    pub payload_bytes: u32,
}

pub async fn run_produce_only(root: &Path, broker_bin: &Path, spec: &ScenarioSpec) -> Result<ScenarioReport> {
    let broker = BrokerProcess::start(broker_bin, root)?;
    let producer = producer(&broker.bootstrap)?;
    let payload = vec![b'a'; spec.payload_bytes as usize];
    let mut latencies = Vec::with_capacity(spec.messages as usize);
    let started = Instant::now();
    for index in 0..spec.messages {
        let send_started = Instant::now();
        producer
            .send(
                FutureRecord::to(spec.name)
                    .payload(&payload)
                    .key("bench")
                    .partition(0),
                Duration::from_secs(10),
            )
            .await
            .map_err(|(err, _)| anyhow::anyhow!(err.to_string()))?;
        latencies.push(send_started.elapsed());
        let _ = index;
    }
    Ok(build_report(spec, started.elapsed(), &latencies, &broker, root, spec.messages, spec.payload_bytes))
}

pub async fn run_roundtrip(root: &Path, broker_bin: &Path, spec: &ScenarioSpec) -> Result<ScenarioReport> {
    let broker = BrokerProcess::start(broker_bin, root)?;
    let producer = producer(&broker.bootstrap)?;
    let payload = vec![b'b'; spec.payload_bytes as usize];
    let mut latencies = Vec::with_capacity(spec.messages as usize);
    let started = Instant::now();
    for _ in 0..spec.messages {
        let send_started = Instant::now();
        producer
            .send(
                FutureRecord::to(spec.name)
                    .payload(&payload)
                    .key("bench")
                    .partition(0),
                Duration::from_secs(10),
            )
            .await
            .map_err(|(err, _)| anyhow::anyhow!(err.to_string()))?;
        latencies.push(send_started.elapsed());
    }
    let consumer = consumer(&broker.bootstrap, "bench-roundtrip")?;
    let mut tpl = TopicPartitionList::new();
    tpl.add_partition_offset(spec.name, 0, Offset::Beginning)?;
    consumer.assign(&tpl)?;
    for _ in 0..spec.messages {
        let _ = poll_for_message(&consumer, Duration::from_secs(10))?;
    }
    Ok(build_report(spec, started.elapsed(), &latencies, &broker, root, spec.messages, spec.payload_bytes))
}

pub async fn run_commit_resume(root: &Path, broker_bin: &Path, spec: &ScenarioSpec) -> Result<ScenarioReport> {
    let broker = BrokerProcess::start(broker_bin, root)?;
    let producer = producer(&broker.bootstrap)?;
    let payload = vec![b'c'; spec.payload_bytes as usize];
    let mut latencies = Vec::with_capacity(spec.messages as usize);
    let started = Instant::now();
    for _ in 0..spec.messages {
        let send_started = Instant::now();
        producer
            .send(
                FutureRecord::to(spec.name)
                    .payload(&payload)
                    .key("bench")
                    .partition(0),
                Duration::from_secs(10),
            )
            .await
            .map_err(|(err, _)| anyhow::anyhow!(err.to_string()))?;
        latencies.push(send_started.elapsed());
    }
    let consumer = group_consumer(&broker.bootstrap, "bench-resume")?;
    consumer.subscribe(&[spec.name])?;
    let message = poll_for_message(&consumer, Duration::from_secs(10))?;
    consumer.commit_message(&message, rdkafka::consumer::CommitMode::Sync)?;
    drop(message);
    drop(consumer);
    let consumer = group_consumer(&broker.bootstrap, "bench-resume")?;
    consumer.subscribe(&[spec.name])?;
    let _ = poll_for_message(&consumer, Duration::from_secs(10))?;
    Ok(build_report(spec, started.elapsed(), &latencies, &broker, root, spec.messages, spec.payload_bytes))
}

fn build_report(
    spec: &ScenarioSpec,
    elapsed: Duration,
    latencies: &[Duration],
    broker: &BrokerProcess,
    root: &Path,
    messages: u32,
    payload_bytes: u32,
) -> ScenarioReport {
    let runtime = runtime_metrics(elapsed, latencies, messages, payload_bytes);
    let storage = storage_metrics(root.join("data"), messages, payload_bytes);
    let memory = MemoryMetrics { peak_rss_kb: broker.peak_rss_kb(), final_rss_kb: broker.final_rss_kb() };
    ScenarioReport { name: spec.name.to_string(), iterations: 1, warmups: 0, messages, payload_bytes, runtime, memory, storage }
}

fn runtime_metrics(elapsed: Duration, latencies: &[Duration], messages: u32, payload_bytes: u32) -> RuntimeMetrics {
    let elapsed_ms = elapsed.as_secs_f64() * 1000.0;
    let throughput_msgs = messages as f64 / elapsed.as_secs_f64().max(0.001);
    let throughput_bytes = (messages as f64 * payload_bytes as f64) / elapsed.as_secs_f64().max(0.001);
    let mut millis = latencies.iter().map(|d| d.as_secs_f64() * 1000.0).collect::<Vec<_>>();
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

fn storage_metrics(data_dir: impl AsRef<Path>, messages: u32, payload_bytes: u32) -> StorageMetrics {
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
                Some("snapshot") | Some("json") if path.to_string_lossy().contains("state/") => state_snapshot_bytes += size,
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

fn walk(root: &Path) -> Result<Vec<(std::path::PathBuf, u64)>> {
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

fn producer(bootstrap: &str) -> Result<FutureProducer> {
    Ok(ClientConfig::new().set("bootstrap.servers", bootstrap).set("message.timeout.ms", "5000").set("enable.idempotence", "true").create()?)
}

fn consumer(bootstrap: &str, group_id: &str) -> Result<BaseConsumer> {
    Ok(ClientConfig::new().set("bootstrap.servers", bootstrap).set("group.id", group_id).set("auto.offset.reset", "earliest").create()?)
}

fn group_consumer(bootstrap: &str, group_id: &str) -> Result<BaseConsumer> {
    Ok(ClientConfig::new().set("bootstrap.servers", bootstrap).set("group.id", group_id).set("auto.offset.reset", "earliest").set("enable.auto.commit", "false").create()?)
}

fn poll_for_message(consumer: &BaseConsumer, timeout: Duration) -> Result<rdkafka::message::BorrowedMessage<'_>> {
    let started = Instant::now();
    while started.elapsed() < timeout {
        if let Some(result) = consumer.poll(Duration::from_millis(250)) {
            return Ok(result?);
        }
    }
    anyhow::bail!("expected a message before timeout")
}
