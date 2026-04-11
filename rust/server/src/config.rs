use figment::{
    Figment,
    providers::{Format, Toml},
};
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::path::PathBuf;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Config {
    #[serde(default)]
    pub broker: BrokerConfig,
    #[serde(default)]
    pub storage: StorageConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BrokerConfig {
    #[serde(default = "default_broker_id")]
    pub broker_id: i32,
    #[serde(default = "default_host")]
    pub host: String,
    #[serde(default = "default_port")]
    pub port: u16,
    #[serde(default = "default_advertised_host")]
    pub advertised_host: String,
    #[serde(default = "default_advertised_port")]
    pub advertised_port: u16,
    #[serde(default = "default_cluster_id")]
    pub cluster_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageConfig {
    #[serde(default = "default_data_dir")]
    pub data_dir: PathBuf,
    #[serde(default = "default_partitions")]
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
    pub fn load(config_path: Option<&str>) -> Result<Self, Box<figment::Error>> {
        let mut figment = Figment::new();

        if let Some(path) = config_path {
            figment = figment.merge(Toml::file(path).nested());
        }

        if let Ok(env_config) = std::env::var("PALADIN_CONFIG") {
            figment = figment.merge(Toml::file(&env_config).nested());
        }

        if config_path.is_none() && std::env::var("PALADIN_CONFIG").is_err() {
            return Err(Box::new(figment::Error::from(
                "No configuration provided. Use --config or set PALADIN_CONFIG environment variable."
                    .to_string(),
            )));
        }

        figment
            .select("kafkalite")
            .extract::<Config>()
            .map_err(Box::new)
    }

    pub fn socket_addr(&self) -> Result<SocketAddr, std::net::AddrParseError> {
        format!("{}:{}", self.broker.host, self.broker.port).parse()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn loads_broker_section() {
        let path = temp_config_path("kafkalite-config.toml");
        std::fs::write(
            &path,
            "[kafkalite.broker]\nbroker_id = 7\nhost = \"0.0.0.0\"\nport = 19092\nadvertised_host = \"broker.local\"\nadvertised_port = 29092\ncluster_id = \"cluster-a\"\n[kafkalite.storage]\ndata_dir = \"/tmp/test-kafkalite-data\"\ndefault_partitions = 3\n",
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

        std::fs::remove_file(path).unwrap();
    }

    fn temp_config_path(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!("{}-{}", std::process::id(), name))
    }
}
