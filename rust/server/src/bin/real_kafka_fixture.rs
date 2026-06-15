use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use std::time::Duration;

#[derive(Parser, Debug)]
#[command(
    name = "real_kafka_fixture",
    about = "Create real Kafka fixture data for differential tests"
)]
struct Args {
    #[arg(long)]
    bootstrap: String,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    CreateTopicRecord(CreateTopicRecordArgs),
}

#[derive(Parser, Debug)]
struct CreateTopicRecordArgs {
    #[arg(long)]
    topic: String,
    #[arg(long, default_value_t = 1)]
    partitions: i32,
    #[arg(long, default_value_t = 0)]
    partition: i32,
    #[arg(long, default_value = "kafka-key")]
    key: String,
    #[arg(long, default_value = "kafka-value")]
    payload: String,
    #[arg(long)]
    payload_bytes: Option<usize>,
    #[arg(long)]
    timestamp_ms: Option<i64>,
    #[arg(long = "topic-config")]
    topic_configs: Vec<String>,
}

#[tokio::main]
async fn main() {
    if let Err(err) = run().await {
        eprintln!("{err:#}");
        std::process::exit(1);
    }
}

async fn run() -> Result<()> {
    let args = Args::parse();
    match args.command {
        Command::CreateTopicRecord(command) => create_topic_record(&args.bootstrap, &command)
            .await
            .with_context(|| {
                format!("creating real Kafka fixture for topic `{}`", command.topic)
            })?,
    }
    Ok(())
}

async fn create_topic_record(bootstrap: &str, args: &CreateTopicRecordArgs) -> Result<()> {
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .create()
        .context("create admin client")?;
    let spec = args.topic_configs.iter().try_fold(
        NewTopic::new(&args.topic, args.partitions, TopicReplication::Fixed(1)),
        |spec, config| {
            let Some((key, value)) = config.split_once('=') else {
                return Err(anyhow::anyhow!(
                    "topic config must be key=value, got `{config}`"
                ));
            };
            Ok(spec.set(key, value))
        },
    )?;
    let _ = admin.create_topics(&[spec], &AdminOptions::new()).await;

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .set("message.timeout.ms", "10000")
        .create()
        .context("create producer")?;
    let payload = args
        .payload_bytes
        .map(|len| {
            args.payload
                .repeat(len.div_ceil(args.payload.len()))
                .chars()
                .take(len)
                .collect()
        })
        .unwrap_or_else(|| args.payload.to_string());
    let record = if let Some(timestamp_ms) = args.timestamp_ms {
        FutureRecord::to(&args.topic)
            .partition(args.partition)
            .key(&args.key)
            .payload(&payload)
            .timestamp(timestamp_ms)
    } else {
        FutureRecord::to(&args.topic)
            .partition(args.partition)
            .key(&args.key)
            .payload(&payload)
    };
    producer
        .send(record, Duration::from_secs(10))
        .await
        .map(|_| ())
        .map_err(|(err, _)| anyhow::anyhow!("produce fixture record: {err}"))
}
