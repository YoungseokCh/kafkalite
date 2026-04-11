use std::path::PathBuf;

use clap::{Parser, Subcommand};
use kafkalite_server::{
    FileStore,
    store::{StorageSummary, TopicSummary},
};

#[derive(Parser, Debug)]
#[command(name = "store_tool", about = "Inspect and repair kafkalite storage")]
struct Args {
    #[arg(long)]
    data_dir: PathBuf,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    TopicSummary { topic: String },
    StorageSummary,
    RebuildIndexes { topic: String },
}

fn main() {
    let args = Args::parse();
    let store = FileStore::open(&args.data_dir).unwrap_or_else(|err| {
        eprintln!("Failed to open kafkalite storage: {err}");
        std::process::exit(1);
    });
    match execute(&store, args.command) {
        Ok(output) => println!("{output}"),
        Err(err) => {
            eprintln!("{err}");
            std::process::exit(1);
        }
    }
}

fn execute(store: &FileStore, command: Command) -> anyhow::Result<String> {
    match command {
        Command::TopicSummary { topic } => {
            let summary = store
                .describe_topic(&topic)
                .ok_or_else(|| anyhow::anyhow!("unknown topic: {topic}"))?;
            Ok(format_topic_summary(&summary))
        }
        Command::StorageSummary => Ok(format_storage_summary(&store.describe_storage()?)),
        Command::RebuildIndexes { topic } => {
            store.rebuild_indexes(&topic)?;
            Ok(format!("rebuilt indexes for topic `{topic}`"))
        }
    }
}

fn format_topic_summary(summary: &TopicSummary) -> String {
    let mut out = vec![
        format!("topic: {}", summary.name),
        format!("partition_count: {}", summary.partition_count),
    ];
    for partition in &summary.partitions {
        out.push(format!(
            "partition={} next_offset={} log_start_offset={} active_segment_base_offset={}",
            partition.partition,
            partition.next_offset,
            partition.log_start_offset,
            partition.active_segment_base_offset
        ));
    }
    out.join("\n")
}

fn format_storage_summary(summary: &StorageSummary) -> String {
    [
        format!("topic_count: {}", summary.topic_count),
        format!("group_count: {}", summary.group_count),
        format!("committed_offset_count: {}", summary.committed_offset_count),
        format!("total_bytes: {}", summary.total_bytes),
        format!("log_bytes: {}", summary.log_bytes),
        format!("index_bytes: {}", summary.index_bytes),
        format!("timeindex_bytes: {}", summary.timeindex_bytes),
        format!("state_bytes: {}", summary.state_bytes),
    ]
    .join("\n")
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use tempfile::tempdir;

    use kafkalite_server::store::{BrokerRecord, Storage};

    use super::*;

    #[test]
    fn storage_summary_reports_existing_bytes() {
        let dir = tempdir().unwrap();
        let store = FileStore::open(dir.path()).unwrap();
        let producer = store.init_producer(10).unwrap();
        store
            .append_records(
                "inspect.events",
                0,
                &[BrokerRecord {
                    offset: 0,
                    timestamp_ms: 10,
                    producer_id: producer.producer_id,
                    producer_epoch: producer.producer_epoch,
                    sequence: 0,
                    key: Some(Bytes::from_static(b"key")),
                    value: Some(Bytes::from_static(b"value")),
                    headers_json: b"[]".to_vec(),
                }],
                20,
            )
            .unwrap();

        let output = execute(&store, Command::StorageSummary).unwrap();

        assert!(output.contains("topic_count: 1"));
        assert!(output.contains("log_bytes: "));
        assert!(output.contains("state_bytes: "));
    }

    #[test]
    fn rebuild_indexes_recreates_missing_index_files() {
        let dir = tempdir().unwrap();
        let store = FileStore::open(dir.path()).unwrap();
        let producer = store.init_producer(10).unwrap();
        store
            .append_records(
                "repair.events",
                0,
                &[BrokerRecord {
                    offset: 0,
                    timestamp_ms: 10,
                    producer_id: producer.producer_id,
                    producer_epoch: producer.producer_epoch,
                    sequence: 0,
                    key: Some(Bytes::from_static(b"key")),
                    value: Some(Bytes::from_static(b"value")),
                    headers_json: b"[]".to_vec(),
                }],
                20,
            )
            .unwrap();

        let partition_dir = dir.path().join("topics/repair.events/partitions/0");
        let index_path = partition_dir.join("00000000000000000000.index");
        let timeindex_path = partition_dir.join("00000000000000000000.timeindex");
        std::fs::remove_file(&index_path).unwrap();
        std::fs::remove_file(&timeindex_path).unwrap();

        let output = execute(
            &store,
            Command::RebuildIndexes {
                topic: "repair.events".to_string(),
            },
        )
        .unwrap();

        assert_eq!(output, "rebuilt indexes for topic `repair.events`");
        assert!(index_path.exists());
        assert!(timeindex_path.exists());
        assert!(std::fs::metadata(index_path).unwrap().len() > 0);
        assert!(std::fs::metadata(timeindex_path).unwrap().len() > 0);
    }
}
