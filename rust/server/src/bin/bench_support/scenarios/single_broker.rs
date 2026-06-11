use std::path::Path;
use std::time::{Duration, Instant};

use anyhow::Result;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::topic_partition_list::{Offset, TopicPartitionList};

use super::ScenarioSpec;
use super::metrics::build_report;
use crate::bench_support::broker_process::BrokerProcess;
use crate::bench_support::report::ScenarioReport;

const BENCH_TIMEOUT: Duration = Duration::from_secs(60);
const IDLE_WINDOW: Duration = Duration::from_secs(5);
const COMMIT_EVERY: u32 = 100;

pub async fn run_busy(
    root: &Path,
    broker_bin: &Path,
    spec: &ScenarioSpec,
) -> Result<ScenarioReport> {
    let broker = BrokerProcess::start(broker_bin, root, spec.default_partitions)?;
    create_topic(&broker.bootstrap, spec).await?;
    let producer = producer(&broker.bootstrap)?;
    let consumer = group_consumer(&broker.bootstrap, "bench-busy")?;
    consumer.subscribe(&[spec.name])?;
    let expected_messages = spec.messages;
    let consumer_handle =
        std::thread::spawn(move || consume_and_commit(consumer, expected_messages));
    std::thread::sleep(Duration::from_millis(250));

    broker.reset_cpu_window();
    let payload = vec![b'b'; spec.payload_bytes as usize];
    let mut latencies = Vec::with_capacity(spec.messages as usize);
    let started = Instant::now();
    for index in 0..spec.messages {
        send_message(
            &producer,
            spec.name,
            &payload,
            index,
            spec.default_partitions,
            &mut latencies,
        )
        .await?;
    }
    let consumed = consumer_handle
        .join()
        .map_err(|_| anyhow::anyhow!("busy consumer thread panicked"))??;
    if consumed != spec.messages {
        anyhow::bail!(
            "busy scenario consumed {consumed}, expected {}",
            spec.messages
        );
    }
    Ok(build_report(
        spec,
        started.elapsed(),
        &latencies,
        &broker,
        root,
        spec.messages,
        spec.payload_bytes,
    ))
}

pub async fn run_idle(
    root: &Path,
    broker_bin: &Path,
    spec: &ScenarioSpec,
) -> Result<ScenarioReport> {
    let broker = BrokerProcess::start(broker_bin, root, spec.default_partitions)?;
    let producer = producer(&broker.bootstrap)?;
    let payload = vec![b'i'; spec.payload_bytes as usize];
    for index in 0..spec.messages {
        send_without_latency(
            &producer,
            spec.name,
            &payload,
            index,
            spec.default_partitions,
        )
        .await?;
    }
    let consumer = consumer(&broker.bootstrap, "bench-idle")?;
    assign_all_partitions(&consumer, spec, Offset::End)?;
    broker.reset_cpu_window();
    let started = Instant::now();
    while started.elapsed() < IDLE_WINDOW {
        let _ = consumer.poll(Duration::from_millis(10));
    }
    let latencies = Vec::new();
    Ok(build_report(
        spec,
        started.elapsed(),
        &latencies,
        &broker,
        root,
        0,
        0,
    ))
}

fn consume_and_commit(consumer: BaseConsumer, expected_messages: u32) -> Result<u32> {
    let started = Instant::now();
    let mut consumed = 0_u32;
    while consumed < expected_messages && started.elapsed() < BENCH_TIMEOUT {
        if let Some(result) = consumer.poll(Duration::from_millis(250)) {
            let message = result?;
            consumed += 1;
            if consumed.is_multiple_of(COMMIT_EVERY) || consumed == expected_messages {
                consumer.commit_message(&message, rdkafka::consumer::CommitMode::Async)?;
            }
        }
    }
    Ok(consumed)
}

async fn send_message(
    producer: &FutureProducer,
    topic: &str,
    payload: &[u8],
    index: u32,
    default_partitions: i32,
    latencies: &mut Vec<Duration>,
) -> Result<()> {
    let send_started = Instant::now();
    send_without_latency(producer, topic, payload, index, default_partitions).await?;
    latencies.push(send_started.elapsed());
    Ok(())
}

async fn send_without_latency(
    producer: &FutureProducer,
    topic: &str,
    payload: &[u8],
    index: u32,
    default_partitions: i32,
) -> Result<()> {
    producer
        .send(
            FutureRecord::to(topic)
                .payload(payload)
                .key("bench")
                .partition(partition_for_message(index, default_partitions)),
            Duration::from_secs(10),
        )
        .await
        .map_err(|(err, _)| anyhow::anyhow!(err.to_string()))?;
    Ok(())
}

fn assign_all_partitions(
    consumer: &BaseConsumer,
    spec: &ScenarioSpec,
    offset: Offset,
) -> Result<()> {
    let mut tpl = TopicPartitionList::new();
    for partition in 0..spec.default_partitions.max(1) {
        tpl.add_partition_offset(spec.name, partition, offset)?;
    }
    consumer.assign(&tpl)?;
    Ok(())
}

fn partition_for_message(index: u32, default_partitions: i32) -> i32 {
    let partitions = default_partitions.max(1) as u32;
    (index % partitions) as i32
}

fn producer(bootstrap: &str) -> Result<FutureProducer> {
    Ok(ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .set("message.timeout.ms", "5000")
        .set("enable.idempotence", "true")
        .create()?)
}

async fn create_topic(bootstrap: &str, spec: &ScenarioSpec) -> Result<()> {
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .create()?;
    let topic = NewTopic::new(
        spec.name,
        spec.default_partitions.max(1),
        TopicReplication::Fixed(1),
    );
    let results = admin
        .create_topics(
            &[topic],
            &AdminOptions::new().operation_timeout(Some(Duration::from_secs(5))),
        )
        .await?;
    for result in results {
        result.map_err(|(name, err)| anyhow::anyhow!("create topic {name} failed: {err}"))?;
    }
    Ok(())
}

fn consumer(bootstrap: &str, group_id: &str) -> Result<BaseConsumer> {
    Ok(ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .set("group.id", group_id)
        .set("auto.offset.reset", "earliest")
        .create()?)
}

fn group_consumer(bootstrap: &str, group_id: &str) -> Result<BaseConsumer> {
    Ok(ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .set("group.id", group_id)
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
        .create()?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn partition_for_message_round_robins_across_partitions() {
        let partitions = (0..6)
            .map(|index| partition_for_message(index, 3))
            .collect::<Vec<_>>();

        assert_eq!(partitions, vec![0, 1, 2, 0, 1, 2]);
    }
}
