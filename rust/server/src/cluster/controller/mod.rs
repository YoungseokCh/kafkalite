use std::collections::BTreeMap;
use std::time::Duration;

use crate::cluster::config::{ClusterConfig, ProcessRole};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BrokerRegistration {
    pub node_id: i32,
    pub broker_epoch: i64,
    pub advertised_host: String,
    pub advertised_port: u16,
    pub last_heartbeat_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BrokerHeartbeat {
    pub node_id: i32,
    pub broker_epoch: i64,
    pub timestamp_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ControllerSnapshot {
    pub controller_epoch: i64,
    pub leader_id: Option<i32>,
    pub registered_brokers: Vec<BrokerRegistration>,
}

#[derive(Debug)]
pub struct ControllerState {
    local_node_id: i32,
    controller_epoch: i64,
    leader_id: Option<i32>,
    next_broker_epoch: i64,
    heartbeat_timeout: Duration,
    brokers: BTreeMap<i32, BrokerRegistration>,
}

impl ControllerState {
    pub fn new(config: &ClusterConfig) -> Self {
        Self {
            local_node_id: config.node_id,
            controller_epoch: 0,
            leader_id: config
                .has_role(ProcessRole::Controller)
                .then_some(config.node_id),
            next_broker_epoch: 1,
            heartbeat_timeout: Duration::from_secs(30),
            brokers: BTreeMap::new(),
        }
    }

    pub fn set_leader(&mut self, leader_id: Option<i32>, controller_epoch: i64) {
        self.leader_id = leader_id;
        self.controller_epoch = controller_epoch;
    }

    pub fn register_broker(
        &mut self,
        node_id: i32,
        advertised_host: String,
        advertised_port: u16,
        now_ms: i64,
    ) -> BrokerRegistration {
        let registration = BrokerRegistration {
            node_id,
            broker_epoch: self.next_broker_epoch,
            advertised_host,
            advertised_port,
            last_heartbeat_ms: now_ms,
        };
        self.next_broker_epoch += 1;
        self.brokers.insert(node_id, registration.clone());
        registration
    }

    pub fn apply_heartbeat(&mut self, heartbeat: BrokerHeartbeat) -> bool {
        let Some(broker) = self.brokers.get_mut(&heartbeat.node_id) else {
            return false;
        };
        if broker.broker_epoch != heartbeat.broker_epoch {
            return false;
        }
        broker.last_heartbeat_ms = heartbeat.timestamp_ms;
        true
    }

    pub fn expire_stale_brokers(&mut self, now_ms: i64) -> Vec<i32> {
        let timeout_ms = self.heartbeat_timeout.as_millis() as i64;
        let expired = self
            .brokers
            .iter()
            .filter_map(|(node_id, broker)| {
                (now_ms - broker.last_heartbeat_ms > timeout_ms).then_some(*node_id)
            })
            .collect::<Vec<_>>();
        for node_id in &expired {
            self.brokers.remove(node_id);
        }
        expired
    }

    pub fn snapshot(&self) -> ControllerSnapshot {
        ControllerSnapshot {
            controller_epoch: self.controller_epoch,
            leader_id: self.leader_id,
            registered_brokers: self.brokers.values().cloned().collect(),
        }
    }

    pub fn local_node_id(&self) -> i32 {
        self.local_node_id
    }
}

#[cfg(test)]
mod tests {
    use crate::cluster::config::ClusterConfig;

    use super::*;

    #[test]
    fn register_and_heartbeat_round_trip() {
        let mut state = ControllerState::new(&ClusterConfig::default());
        let registration = state.register_broker(1, "broker.local".to_string(), 9092, 100);

        let accepted = state.apply_heartbeat(BrokerHeartbeat {
            node_id: 1,
            broker_epoch: registration.broker_epoch,
            timestamp_ms: 200,
        });

        assert!(accepted);
        assert_eq!(state.snapshot().registered_brokers.len(), 1);
        assert_eq!(
            state.snapshot().registered_brokers[0].last_heartbeat_ms,
            200
        );
    }

    #[test]
    fn stale_broker_epoch_is_rejected() {
        let mut state = ControllerState::new(&ClusterConfig::default());
        let _registration = state.register_broker(1, "broker.local".to_string(), 9092, 100);

        let accepted = state.apply_heartbeat(BrokerHeartbeat {
            node_id: 1,
            broker_epoch: 999,
            timestamp_ms: 200,
        });

        assert!(!accepted);
    }

    #[test]
    fn heartbeat_for_unknown_broker_is_rejected() {
        let mut state = ControllerState::new(&ClusterConfig::default());

        let accepted = state.apply_heartbeat(BrokerHeartbeat {
            node_id: 42,
            broker_epoch: 1,
            timestamp_ms: 200,
        });

        assert!(!accepted);
    }
}
