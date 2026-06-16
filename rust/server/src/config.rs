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
    pub segment_bytes: u64,
    pub segment_ms: u64,
    pub retention_bytes: Option<u64>,
    pub retention_ms: Option<u64>,
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
            segment_bytes: default_segment_bytes(),
            segment_ms: default_segment_ms(),
            retention_bytes: default_retention_bytes(),
            retention_ms: default_retention_ms(),
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

fn default_segment_bytes() -> u64 {
    crate::store::FileStorePolicy::default().segment_bytes
}

fn default_segment_ms() -> u64 {
    crate::store::FileStorePolicy::default().segment_ms
}

fn default_retention_bytes() -> Option<u64> {
    crate::store::FileStorePolicy::default().retention_bytes
}

fn default_retention_ms() -> Option<u64> {
    crate::store::FileStorePolicy::default().retention_ms
}

impl StorageConfig {
    pub fn policy(&self) -> crate::store::FileStorePolicy {
        crate::store::FileStorePolicy {
            segment_bytes: self.segment_bytes,
            segment_ms: self.segment_ms,
            retention_bytes: self.retention_bytes,
            retention_ms: self.retention_ms,
            ..crate::store::FileStorePolicy::default()
        }
    }
}

impl Config {
    pub fn load(config_path: Option<&str>) -> Result<Self> {
        load_properties_config(config_path)
    }

    pub fn single_node(data_dir: PathBuf, port: u16, default_partitions: i32) -> Self {
        let host = default_host();
        let listener = ListenerConfig {
            name: ProcessRole::BROKER_DEFAULT_LISTENER.to_string(),
            host: host.clone(),
            port,
        };
        Self {
            broker: BrokerConfig {
                host: host.clone(),
                port,
                advertised_host: host.clone(),
                advertised_port: port,
                ..BrokerConfig::default()
            },
            storage: StorageConfig {
                data_dir,
                default_partitions,
                ..StorageConfig::default()
            },
            cluster: ClusterConfig {
                node_id: default_broker_id(),
                process_roles: vec![ProcessRole::Broker],
                listeners: [(
                    ProcessRole::BROKER_DEFAULT_LISTENER.to_string(),
                    listener.clone(),
                )]
                .into_iter()
                .collect(),
                advertised_listeners: [(
                    ProcessRole::BROKER_DEFAULT_LISTENER.to_string(),
                    listener,
                )]
                .into_iter()
                .collect(),
                ..ClusterConfig::default()
            },
        }
    }

    pub fn socket_addr(&self) -> Result<SocketAddr, std::net::AddrParseError> {
        self.client_listener().socket_addr()
    }

    pub fn client_listener(&self) -> &ListenerConfig {
        self.cluster
            .client_listener()
            .expect("client listener must be configured")
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
            "process.roles=broker,controller\nnode.id=7\nlisteners=PLAINTEXT://:19092,CONTROLLER://:19093\nadvertised.listeners=PLAINTEXT://broker.local:29092,CONTROLLER://broker.local:29093\ncontroller.listener.names=CONTROLLER\ncontroller.quorum.voters=7@node7:19093,8@node8:19093,9@node9:19093\nlog.dirs=/tmp/test-kafkalite-data\nnum.partitions=3\nlog.segment.bytes=4096\nlog.roll.ms=5000\nlog.retention.bytes=8192\nlog.retention.ms=15000\ncluster.id=cluster-a\n",
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
        assert_eq!(config.storage.segment_bytes, 4096);
        assert_eq!(config.storage.segment_ms, 5000);
        assert_eq!(config.storage.retention_bytes, Some(8192));
        assert_eq!(config.storage.retention_ms, Some(15000));
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
    fn storage_defaults_match_kafka_baseline() {
        let storage = StorageConfig::default();

        assert_eq!(storage.segment_bytes, 1024 * 1024 * 1024);
        assert_eq!(storage.segment_ms, 24 * 7 * 60 * 60 * 1000);
        assert_eq!(storage.retention_bytes, None);
        assert_eq!(storage.retention_ms, Some(24 * 7 * 60 * 60 * 1000));
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

    #[test]
    fn broker_role_requires_listeners_in_strict_mode() {
        let path = temp_config_path("server.properties");
        std::fs::write(&path, "process.roles=broker\n").unwrap();

        let err = Config::load(path.to_str()).unwrap_err().to_string();

        assert!(err.contains("PLAINTEXT listener"));

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
