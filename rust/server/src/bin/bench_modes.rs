use clap::ValueEnum;

use super::bench_support::scenarios::{ScenarioKind, ScenarioSpec};

#[derive(Clone, Debug, ValueEnum)]
pub(super) enum BenchMode {
    Quick,
    Full,
    Size,
    Runtime,
    Memory,
    Storage,
}

pub(super) fn specs_for_mode(mode: &BenchMode) -> Vec<ScenarioSpec> {
    match mode {
        BenchMode::Quick => vec![small_produce()],
        BenchMode::Size => Vec::new(),
        BenchMode::Runtime => runtime_specs(),
        BenchMode::Memory => vec![small_produce(), roundtrip(), fetch_tail()],
        BenchMode::Storage => vec![medium_produce()],
        BenchMode::Full => full_specs(),
    }
}

fn runtime_specs() -> Vec<ScenarioSpec> {
    vec![
        small_produce(),
        multi_partition_produce(),
        roundtrip(),
        multi_partition_fetch(),
        fetch_tail(),
        commit_resume(),
        mixed_handoff(),
        cluster_replication_metadata(),
        cluster_reassignment_metadata(),
    ]
}

fn full_specs() -> Vec<ScenarioSpec> {
    vec![
        small_produce(),
        medium_produce(),
        multi_partition_produce(),
        roundtrip(),
        fetch_tail(),
        multi_partition_fetch(),
        commit_resume(),
        mixed_handoff(),
        cluster_replication_metadata(),
        cluster_reassignment_metadata(),
    ]
}

fn small_produce() -> ScenarioSpec {
    scenario(
        "bench.produce.small",
        ScenarioKind::ProduceOnly,
        1_000,
        100,
        1,
    )
}

fn medium_produce() -> ScenarioSpec {
    scenario(
        "bench.produce.medium",
        ScenarioKind::ProduceOnly,
        500,
        1024,
        1,
    )
}

fn multi_partition_produce() -> ScenarioSpec {
    scenario(
        "bench.produce.multi_partition",
        ScenarioKind::ProduceOnly,
        1_000,
        100,
        3,
    )
}

fn roundtrip() -> ScenarioSpec {
    scenario("bench.roundtrip", ScenarioKind::Roundtrip, 200, 512, 1)
}

fn fetch_tail() -> ScenarioSpec {
    scenario("bench.fetch.tail", ScenarioKind::FetchTail, 500, 512, 1)
}

fn multi_partition_fetch() -> ScenarioSpec {
    scenario(
        "bench.fetch.multi_partition",
        ScenarioKind::ProduceOnly,
        500,
        512,
        3,
    )
}

fn commit_resume() -> ScenarioSpec {
    scenario("bench.commit.resume", ScenarioKind::CommitResume, 4, 256, 1)
}

fn mixed_handoff() -> ScenarioSpec {
    scenario(
        "bench.mixed.handoff",
        ScenarioKind::MixedHandoff,
        200,
        256,
        1,
    )
}

fn cluster_replication_metadata() -> ScenarioSpec {
    scenario(
        "bench.cluster.replication.metadata",
        ScenarioKind::ClusterReplicationMetadata,
        200,
        1,
        1,
    )
}

fn cluster_reassignment_metadata() -> ScenarioSpec {
    scenario(
        "bench.cluster.reassignment.metadata",
        ScenarioKind::ClusterReassignmentMetadata,
        100,
        1,
        1,
    )
}

fn scenario(
    name: &'static str,
    kind: ScenarioKind,
    messages: u32,
    payload_bytes: u32,
    default_partitions: i32,
) -> ScenarioSpec {
    ScenarioSpec {
        name,
        kind,
        messages,
        payload_bytes,
        default_partitions,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn runtime_mode_includes_both_cluster_metadata_benchmarks() {
        let specs = specs_for_mode(&BenchMode::Runtime);

        assert!(specs.iter().any(|spec| {
            spec.name == "bench.cluster.replication.metadata"
                && spec.kind == ScenarioKind::ClusterReplicationMetadata
        }));
        assert!(specs.iter().any(|spec| {
            spec.name == "bench.cluster.reassignment.metadata"
                && spec.kind == ScenarioKind::ClusterReassignmentMetadata
        }));
    }

    #[test]
    fn quick_mode_stays_single_produce_scenario() {
        let specs = specs_for_mode(&BenchMode::Quick);

        assert_eq!(specs.len(), 1);
        assert_eq!(specs[0].kind, ScenarioKind::ProduceOnly);
        assert_eq!(specs[0].name, "bench.produce.small");
    }
}
