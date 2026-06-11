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
        BenchMode::Quick => vec![busy()],
        BenchMode::Size => Vec::new(),
        BenchMode::Runtime | BenchMode::Memory | BenchMode::Storage | BenchMode::Full => specs(),
    }
}

fn specs() -> Vec<ScenarioSpec> {
    vec![busy(), idle()]
}

fn busy() -> ScenarioSpec {
    scenario("bench.busy", ScenarioKind::Busy, 500, 512, 3)
}

fn idle() -> ScenarioSpec {
    scenario("bench.idle", ScenarioKind::Idle, 100, 512, 3)
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
    fn runtime_mode_includes_busy_and_idle_benchmarks() {
        let specs = specs_for_mode(&BenchMode::Runtime);

        assert!(
            specs
                .iter()
                .any(|spec| spec.name == "bench.busy" && spec.kind == ScenarioKind::Busy)
        );
        assert!(
            specs
                .iter()
                .any(|spec| spec.name == "bench.idle" && spec.kind == ScenarioKind::Idle)
        );
    }

    #[test]
    fn quick_mode_stays_single_busy_scenario() {
        let specs = specs_for_mode(&BenchMode::Quick);

        assert_eq!(specs.len(), 1);
        assert_eq!(specs[0].kind, ScenarioKind::Busy);
        assert_eq!(specs[0].name, "bench.busy");
    }
}
