use std::path::Path;
use std::time::{Duration, Instant};

use anyhow::Result;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::topic_partition_list::{Offset, TopicPartitionList};

use super::ScenarioSpec;
use super::metrics::build_report;
use crate::bench_support::broker_process::BrokerProcess;
use crate::bench_support::report::ScenarioReport;

pub async fn run_produce_only(
    root: &Path,
    broker_bin: &Path,
    spec: &ScenarioSpec,
) -> Result<ScenarioReport> {
    let broker = BrokerProcess::start(broker_bin, root, spec.default_partitions)?;
    let producer = producer(&broker.bootstrap)?;
    let payload = vec![b'a'; spec.payload_bytes as usize];
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

pub async fn run_roundtrip(
    root: &Path,
    broker_bin: &Path,
    spec: &ScenarioSpec,
) -> Result<ScenarioReport> {
    let broker = BrokerProcess::start(broker_bin, root, spec.default_partitions)?;
    let producer = producer(&broker.bootstrap)?;
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
    let consumer = consumer(&broker.bootstrap, "bench-roundtrip")?;
    assign_all_partitions(&consumer, spec, Offset::Beginning)?;
    for _ in 0..spec.messages {
        let _ = poll_for_message(&consumer, Duration::from_secs(10))?;
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

pub async fn run_fetch_tail(
    root: &Path,
    broker_bin: &Path,
    spec: &ScenarioSpec,
) -> Result<ScenarioReport> {
    let broker = BrokerProcess::start(broker_bin, root, spec.default_partitions)?;
    let producer = producer(&broker.bootstrap)?;
    let payload = vec![b'd'; spec.payload_bytes as usize];
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
    let consumer = consumer(&broker.bootstrap, "bench-fetch-tail")?;
    assign_tail_partition(&consumer, spec)?;
    let started = Instant::now();
    let mut latencies = Vec::with_capacity(10);
    for _ in 0..10 {
        let fetch_started = Instant::now();
        let _ = poll_for_message(&consumer, Duration::from_secs(10))?;
        latencies.push(fetch_started.elapsed());
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

pub async fn run_commit_resume(
    root: &Path,
    broker_bin: &Path,
    spec: &ScenarioSpec,
) -> Result<ScenarioReport> {
    let broker = BrokerProcess::start(broker_bin, root, spec.default_partitions)?;
    let producer = producer(&broker.bootstrap)?;
    let payload = vec![b'c'; spec.payload_bytes as usize];
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
    let consumer = group_consumer(&broker.bootstrap, "bench-resume")?;
    consumer.subscribe(&[spec.name])?;
    let message = poll_for_message(&consumer, Duration::from_secs(10))?;
    consumer.commit_message(&message, rdkafka::consumer::CommitMode::Sync)?;
    drop(message);
    drop(consumer);
    let consumer = group_consumer(&broker.bootstrap, "bench-resume")?;
    consumer.subscribe(&[spec.name])?;
    let _ = poll_for_message(&consumer, Duration::from_secs(10))?;
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

fn assign_tail_partition(consumer: &BaseConsumer, spec: &ScenarioSpec) -> Result<()> {
    let mut tpl = TopicPartitionList::new();
    let target_partition = spec.default_partitions.max(1) - 1;
    tpl.add_partition_offset(
        spec.name,
        target_partition,
        Offset::Offset(
            partition_message_count(spec.messages, spec.default_partitions, target_partition)
                .saturating_sub(10) as i64,
        ),
    )?;
    consumer.assign(&tpl)?;
    Ok(())
}

fn partition_for_message(index: u32, default_partitions: i32) -> i32 {
    let partitions = default_partitions.max(1) as u32;
    (index % partitions) as i32
}

fn partition_message_count(messages: u32, default_partitions: i32, partition: i32) -> u32 {
    if partition < 0 || partition >= default_partitions.max(1) {
        return 0;
    }
    (0..messages)
        .filter(|index| partition_for_message(*index, default_partitions) == partition)
        .count() as u32
}

fn producer(bootstrap: &str) -> Result<FutureProducer> {
    Ok(ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .set("message.timeout.ms", "5000")
        .set("enable.idempotence", "true")
        .create()?)
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

fn poll_for_message(
    consumer: &BaseConsumer,
    timeout: Duration,
) -> Result<rdkafka::message::BorrowedMessage<'_>> {
    let started = Instant::now();
    while started.elapsed() < timeout {
        if let Some(result) = consumer.poll(Duration::from_millis(250)) {
            return Ok(result?);
        }
    }
    anyhow::bail!("expected a message before timeout")
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

    #[test]
    fn partition_message_count_tracks_tail_partition_volume() {
        assert_eq!(partition_message_count(10, 3, 0), 4);
        assert_eq!(partition_message_count(10, 3, 1), 3);
        assert_eq!(partition_message_count(10, 3, 2), 3);
    }
}
