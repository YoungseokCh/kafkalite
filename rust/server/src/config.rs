use anyhow::Result;
use std::net::SocketAddr;
use std::path::PathBuf;

use crate::cluster::config::{ClusterConfig, ListenerConfig, ProcessRole, load_properties_config};

#[derive(Debug, Clone, Default)]
pub struct Config {
    pub broker: BrokerConfig,
    pub storage: StorageConfig,
    pub cluster: ClusterConfig,
}

#[derive(Debug, Clone)]
pub struct BrokerConfig {
    pub broker_id: i32,
    pub host: String,
    pub port: u16,
    pub advertised_host: String,
    pub advertised_port: u16,
    pub cluster_id: String,
}

#[derive(Debug, Clone)]
pub struct StorageConfig {
    pub data_dir: PathBuf,
    pub default_partitions: i32,
}

impl Default for BrokerConfig {
    fn default() -> Self {
        Self {
            broker_id: default_broker_id(),
            host: default_host(),
            port: default_port(),
            advertised_host: default_advertised_host(),
            advertised_port: default_advertised_port(),
            cluster_id: default_cluster_id(),
        }
    }
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            data_dir: default_data_dir(),
            default_partitions: default_partitions(),
        }
    }
}

fn default_broker_id() -> i32 {
    1
}

fn default_host() -> String {
    "127.0.0.1".to_string()
}

fn default_port() -> u16 {
    9092
}

fn default_advertised_host() -> String {
    "127.0.0.1".to_string()
}

fn default_advertised_port() -> u16 {
    9092
}

fn default_cluster_id() -> String {
    "kafkalite-single-broker".to_string()
}

fn default_data_dir() -> PathBuf {
    PathBuf::from("./data")
}

fn default_partitions() -> i32 {
    1
}

impl Config {
    pub fn load(config_path: Option<&str>) -> Result<Self> {
        load_properties_config(config_path)
    }

    pub fn single_node(data_dir: PathBuf, port: u16, default_partitions: i32) -> Self {
        Self {
            broker: BrokerConfig {
                port,
                advertised_port: port,
                ..BrokerConfig::default()
            },
            storage: StorageConfig {
                data_dir,
                default_partitions,
            },
            cluster: ClusterConfig::default(),
        }
    }

    pub fn socket_addr(&self) -> Result<SocketAddr, std::net::AddrParseError> {
        self.client_listener().socket_addr()
    }

    pub fn client_listener(&self) -> ListenerConfig {
        self.cluster
            .client_listener()
            .cloned()
            .unwrap_or_else(|| ListenerConfig {
                name: ProcessRole::BROKER_DEFAULT_LISTENER.to_string(),
                host: self.broker.host.clone(),
                port: self.broker.port,
            })
    }

    pub fn ensure_cluster_defaults(&mut self) {
        if self.cluster.node_id == 0 {
            self.cluster.node_id = self.broker.broker_id;
        }
        if self.cluster.process_roles.is_empty() {
            self.cluster.process_roles = vec![ProcessRole::Broker];
        }
        let fallback_listener = ListenerConfig {
            name: ProcessRole::BROKER_DEFAULT_LISTENER.to_string(),
            host: self.broker.host.clone(),
            port: self.broker.port,
        };
        self.cluster
            .listeners
            .entry(ProcessRole::BROKER_DEFAULT_LISTENER.to_string())
            .or_insert(fallback_listener);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn loads_kafka_style_properties() {
        let path = temp_config_path("server.properties");
        std::fs::write(
            &path,
            "process.roles=broker,controller\nnode.id=7\nlisteners=PLAINTEXT://:19092,CONTROLLER://:19093\nadvertised.listeners=PLAINTEXT://broker.local:29092,CONTROLLER://broker.local:29093\ncontroller.listener.names=CONTROLLER\ncontroller.quorum.voters=7@node7:19093,8@node8:19093,9@node9:19093\nlog.dirs=/tmp/test-kafkalite-data\nnum.partitions=3\ncluster.id=cluster-a\n",
        )
        .unwrap();

        let config = Config::load(path.to_str()).unwrap();

        assert_eq!(config.broker.broker_id, 7);
        assert_eq!(config.broker.host, "0.0.0.0");
        assert_eq!(config.broker.port, 19092);
        assert_eq!(config.broker.advertised_host, "broker.local");
        assert_eq!(config.broker.advertised_port, 29092);
        assert_eq!(config.broker.cluster_id, "cluster-a");
        assert_eq!(
            config.storage.data_dir,
            PathBuf::from("/tmp/test-kafkalite-data")
        );
        assert_eq!(config.storage.default_partitions, 3);
        assert_eq!(config.cluster.node_id, 7);
        assert_eq!(
            config.cluster.process_roles,
            vec![ProcessRole::Broker, ProcessRole::Controller]
        );
        assert_eq!(
            config.cluster.controller_listener_names,
            vec!["CONTROLLER".to_string()]
        );

        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn advertised_listener_defaults_to_listener_endpoint() {
        let path = temp_config_path("server.properties");
        std::fs::write(&path, "listeners=PLAINTEXT://127.0.0.1:19092\n").unwrap();

        let config = Config::load(path.to_str()).unwrap();

        assert_eq!(config.broker.host, "127.0.0.1");
        assert_eq!(config.broker.port, 19092);
        assert_eq!(config.broker.advertised_host, "127.0.0.1");
        assert_eq!(config.broker.advertised_port, 19092);

        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn rejects_multiple_log_dirs() {
        let path = temp_config_path("server.properties");
        std::fs::write(&path, "log.dirs=/tmp/a,/tmp/b\n").unwrap();

        let err = Config::load(path.to_str()).unwrap_err().to_string();

        assert!(err.contains("log.dirs"));
        assert!(err.contains("exactly one directory"));

        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn controller_role_requires_quorum_settings() {
        let path = temp_config_path("server.properties");
        std::fs::write(
            &path,
            "process.roles=broker,controller\nnode.id=1\nlisteners=PLAINTEXT://:19092,CONTROLLER://:19093\n",
        )
        .unwrap();

        let err = Config::load(path.to_str()).unwrap_err().to_string();

        assert!(err.contains("controller.listener.names"));

        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn parses_controller_quorum_voters_and_named_listeners() {
        let path = temp_config_path("server.properties");
        std::fs::write(
            &path,
            "process.roles=broker,controller\nnode.id=2\nlisteners=PLAINTEXT://:19092,CONTROLLER://:19093\nadvertised.listeners=PLAINTEXT://broker.local:29092\ncontroller.listener.names=CONTROLLER\ncontroller.quorum.voters=1@node1:9093,2@node2:9093,3@node3:9093\n",
        )
        .unwrap();

        let config = Config::load(path.to_str()).unwrap();

        assert_eq!(config.cluster.controller_quorum_voters.len(), 3);
        assert!(config.cluster.listeners.contains_key("CONTROLLER"));
        assert_eq!(
            config.cluster.controller_quorum_voters[1].host,
            "node2".to_string()
        );

        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn broker_role_requires_plaintext_listener() {
        let path = temp_config_path("server.properties");
        std::fs::write(
            &path,
            "process.roles=broker\nlisteners=CONTROLLER://:19093\n",
        )
        .unwrap();

        let err = Config::load(path.to_str()).unwrap_err().to_string();

        assert!(err.contains("PLAINTEXT listener"));

        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn controller_listener_name_must_exist_in_listeners() {
        let path = temp_config_path("server.properties");
        std::fs::write(
            &path,
            "process.roles=broker,controller\nnode.id=1\nlisteners=PLAINTEXT://:19092\ncontroller.listener.names=CONTROLLER\ncontroller.quorum.voters=1@node1:9093\n",
        )
        .unwrap();

        let err = Config::load(path.to_str()).unwrap_err().to_string();

        assert!(err.contains("missing from listeners"));

        std::fs::remove_file(path).unwrap();
    }

    fn temp_config_path(name: &str) -> PathBuf {
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("{}-{}-{}", std::process::id(), unique, name))
    }
}
