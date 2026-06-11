mod metrics;
mod single_broker;

pub use single_broker::{run_busy, run_idle};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ScenarioKind {
    Busy,
    Idle,
}

pub struct ScenarioSpec {
    pub name: &'static str,
    pub kind: ScenarioKind,
    pub messages: u32,
    pub payload_bytes: u32,
    pub default_partitions: i32,
}
