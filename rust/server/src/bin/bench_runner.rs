use std::fs;
use std::path::PathBuf;

use clap::{ArgAction, Parser};

mod bench_modes;
mod bench_support;

use bench_modes::{BenchMode, specs_for_mode};
use bench_support::mixed::run_mixed_handoff;
use bench_support::report::{BenchmarkReport, BuildMetrics, HostInfo, ScenarioReport};
use bench_support::scenarios::{
    ScenarioKind, run_cluster_reassignment_metadata, run_cluster_replication_metadata,
    run_commit_resume, run_fetch_tail, run_produce_only, run_roundtrip,
};

#[derive(Parser, Debug)]
struct Args {
    #[arg(long)]
    output_dir: PathBuf,
    #[arg(long)]
    broker_bin: PathBuf,
    #[arg(long, value_enum, default_value = "full")]
    mode: BenchMode,
    #[arg(long, default_value_t = 0)]
    binary_bytes: u64,
    #[arg(long, default_value_t = 0)]
    package_bytes: u64,
    #[arg(long)]
    stripped_binary_bytes: Option<u64>,
    #[arg(long, default_value = "unknown")]
    git_sha: String,
    #[arg(long, action = ArgAction::SetTrue)]
    dirty: bool,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    fs::create_dir_all(&args.output_dir)?;
    let scenarios = run_mode(&args).await?;
    let report = BenchmarkReport {
        schema_version: 1,
        run_id: chrono::Utc::now().to_rfc3339(),
        git_sha: args.git_sha,
        dirty: args.dirty,
        host: HostInfo {
            os: std::env::consts::OS.to_string(),
            arch: std::env::consts::ARCH.to_string(),
        },
        build: BuildMetrics {
            profile: "release".to_string(),
            binary_bytes: args.binary_bytes,
            package_bytes: args.package_bytes,
            stripped_binary_bytes: args.stripped_binary_bytes,
        },
        scenarios,
    };
    let json = serde_json::to_string_pretty(&report)?;
    fs::write(args.output_dir.join("result.json"), format!("{json}\n"))?;
    fs::write(args.output_dir.join("metrics.csv"), to_csv(&report))?;
    fs::write(args.output_dir.join("summary.md"), to_markdown(&report))?;
    Ok(())
}

async fn run_mode(args: &Args) -> anyhow::Result<Vec<ScenarioReport>> {
    let specs = specs_for_mode(&args.mode);

    let mut reports = Vec::new();
    for spec in specs {
        let scenario_root = tempfile::Builder::new()
            .prefix(&format!("{}-", spec.name.replace('.', "-")))
            .tempdir()?;
        let report = match spec.kind {
            ScenarioKind::ProduceOnly => {
                run_produce_only(scenario_root.path(), &args.broker_bin, &spec).await?
            }
            ScenarioKind::Roundtrip => {
                run_roundtrip(scenario_root.path(), &args.broker_bin, &spec).await?
            }
            ScenarioKind::FetchTail => {
                run_fetch_tail(scenario_root.path(), &args.broker_bin, &spec).await?
            }
            ScenarioKind::CommitResume => {
                run_commit_resume(scenario_root.path(), &args.broker_bin, &spec).await?
            }
            ScenarioKind::MixedHandoff => {
                run_mixed_handoff(scenario_root.path(), &args.broker_bin, &spec).await?
            }
            ScenarioKind::ClusterReplicationMetadata => {
                run_cluster_replication_metadata(scenario_root.path(), &spec).await?
            }
            ScenarioKind::ClusterReassignmentMetadata => {
                run_cluster_reassignment_metadata(scenario_root.path(), &spec).await?
            }
        };
        reports.push(report);
    }
    Ok(reports)
}

fn to_csv(report: &BenchmarkReport) -> String {
    let mut lines = vec!["name,messages,payload_bytes,default_partitions,elapsed_ms,throughput_msgs_per_sec,throughput_bytes_per_sec,latency_p50_ms,latency_p95_ms,latency_p99_ms,peak_rss_kb,final_rss_kb,total_bytes,log_bytes,index_bytes,timeindex_bytes,state_snapshot_bytes,state_journal_bytes".to_string()];
    for scenario in &report.scenarios {
        lines.push(format!(
            "{},{},{},{},{:.2},{:.2},{:.2},{:.2},{:.2},{:.2},{},{},{},{},{},{},{},{}",
            scenario.name,
            scenario.messages,
            scenario.payload_bytes,
            scenario.default_partitions,
            scenario.runtime.elapsed_ms,
            scenario.runtime.throughput_msgs_per_sec,
            scenario.runtime.throughput_bytes_per_sec,
            scenario.runtime.latency_p50_ms,
            scenario.runtime.latency_p95_ms,
            scenario.runtime.latency_p99_ms,
            scenario.memory.peak_rss_kb,
            scenario.memory.final_rss_kb,
            scenario.storage.total_bytes,
            scenario.storage.log_bytes,
            scenario.storage.index_bytes,
            scenario.storage.timeindex_bytes,
            scenario.storage.state_snapshot_bytes,
            scenario.storage.state_journal_bytes,
        ));
    }
    format!("{}\n", lines.join("\n"))
}

fn to_markdown(report: &BenchmarkReport) -> String {
    let mut out = String::from("# Benchmark Summary\n\n");
    out.push_str(&format!(
        "- git_sha: `{}`\n- dirty: `{}`\n- binary_bytes: `{}`\n- package_bytes: `{}`\n\n",
        report.git_sha, report.dirty, report.build.binary_bytes, report.build.package_bytes
    ));
    out.push_str("| scenario | partitions | elapsed_ms | msgs/sec | peak_rss_kb | total_bytes |\n|---|---:|---:|---:|---:|---:|\n");
    for scenario in &report.scenarios {
        out.push_str(&format!(
            "| {} | {} | {:.2} | {:.2} | {} | {} |\n",
            scenario.name,
            scenario.default_partitions,
            scenario.runtime.elapsed_ms,
            scenario.runtime.throughput_msgs_per_sec,
            scenario.memory.peak_rss_kb,
            scenario.storage.total_bytes
        ));
    }
    out
}
