use anyhow::{Context, Result, bail};
use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::PathBuf;

#[derive(Debug, Clone, Default)]
pub struct Config {
    pub broker: BrokerConfig,
    pub storage: StorageConfig,
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
        let path = config_path
            .map(str::to_owned)
            .or_else(|| std::env::var("KAFKALITE_CONFIG").ok())
            .context("No configuration provided. Use --config or set KAFKALITE_CONFIG.")?;
        let content = std::fs::read_to_string(&path)
            .with_context(|| format!("Failed to read configuration file {path}"))?;
        let properties = parse_properties(&content)?;
        let mut config = Config::default();

        if let Some(value) = properties.get("node.id") {
            config.broker.broker_id = parse_i32("node.id", value)?;
        }

        if let Some((host, port)) = properties
            .get("listeners")
            .map(|value| parse_listener("listeners", value))
            .transpose()?
        {
            config.broker.host = host;
            config.broker.port = port;
        }

        if let Some((host, port)) = properties
            .get("advertised.listeners")
            .map(|value| parse_listener("advertised.listeners", value))
            .transpose()?
        {
            config.broker.advertised_host = host;
            config.broker.advertised_port = port;
        } else {
            config.broker.advertised_host = config.broker.host.clone();
            config.broker.advertised_port = config.broker.port;
        }

        if let Some(value) = properties.get("cluster.id") {
            config.broker.cluster_id = value.clone();
        }

        if let Some(value) = properties.get("log.dirs") {
            config.storage.data_dir = parse_log_dirs(value)?;
        }

        if let Some(value) = properties.get("num.partitions") {
            config.storage.default_partitions = parse_positive_i32("num.partitions", value)?;
        }

        Ok(config)
    }

    pub fn socket_addr(&self) -> Result<SocketAddr, std::net::AddrParseError> {
        format!("{}:{}", self.broker.host, self.broker.port).parse()
    }
}

fn parse_properties(content: &str) -> Result<BTreeMap<String, String>> {
    let mut properties = BTreeMap::new();
    for (index, raw_line) in content.lines().enumerate() {
        let line = raw_line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let (key, value) = line.split_once('=').with_context(|| {
            format!(
                "Invalid properties line {}: expected key=value, got `{}`",
                index + 1,
                raw_line
            )
        })?;
        let key = key.trim();
        let value = value.trim();
        if key.is_empty() {
            bail!("Invalid properties line {}: empty key", index + 1);
        }
        properties.insert(key.to_string(), value.to_string());
    }
    Ok(properties)
}

fn parse_i32(key: &str, value: &str) -> Result<i32> {
    value
        .parse()
        .with_context(|| format!("Invalid integer for {key}: {value}"))
}

fn parse_positive_i32(key: &str, value: &str) -> Result<i32> {
    let parsed = parse_i32(key, value)?;
    if parsed < 1 {
        bail!("Invalid value for {key}: expected >= 1, got {parsed}");
    }
    Ok(parsed)
}

fn parse_log_dirs(value: &str) -> Result<PathBuf> {
    let dirs = value
        .split(',')
        .map(str::trim)
        .filter(|entry| !entry.is_empty())
        .collect::<Vec<_>>();
    match dirs.as_slice() {
        [dir] => Ok(PathBuf::from(dir)),
        [] => bail!("Invalid value for log.dirs: expected one directory"),
        _ => bail!("Invalid value for log.dirs: kafkalite supports exactly one directory"),
    }
}

fn parse_listener(key: &str, value: &str) -> Result<(String, u16)> {
    let listeners = value
        .split(',')
        .map(str::trim)
        .filter(|entry| !entry.is_empty())
        .collect::<Vec<_>>();
    if listeners.is_empty() {
        bail!("Invalid value for {key}: expected at least one listener");
    }

    let selected = listeners
        .iter()
        .find(|entry| {
            listener_name(entry).is_some_and(|name| name.eq_ignore_ascii_case("PLAINTEXT"))
        })
        .copied()
        .or_else(|| (listeners.len() == 1).then_some(listeners[0]))
        .with_context(|| {
            format!("Invalid value for {key}: expected a PLAINTEXT listener or a single listener")
        })?;

    parse_listener_endpoint(key, selected)
}

fn listener_name(listener: &str) -> Option<&str> {
    listener.split_once("://").map(|(name, _)| name.trim())
}

fn parse_listener_endpoint(key: &str, listener: &str) -> Result<(String, u16)> {
    let (_, address) = listener
        .split_once("://")
        .with_context(|| format!("Invalid listener for {key}: {listener}"))?;
    let (host, port) = address
        .rsplit_once(':')
        .with_context(|| format!("Invalid listener for {key}: {listener}"))?;
    let host = if host.is_empty() { "0.0.0.0" } else { host };
    let port = port
        .parse()
        .with_context(|| format!("Invalid port for {key}: {listener}"))?;
    Ok((host.to_string(), port))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn loads_kafka_style_properties() {
        let path = temp_config_path("server.properties");
        std::fs::write(
            &path,
            "process.roles=broker,controller\nnode.id=7\nlisteners=PLAINTEXT://:19092,CONTROLLER://:19093\nadvertised.listeners=PLAINTEXT://broker.local:29092,CONTROLLER://broker.local:29093\nlog.dirs=/tmp/test-kafkalite-data\nnum.partitions=3\ncluster.id=cluster-a\n",
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

    fn temp_config_path(name: &str) -> PathBuf {
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("{}-{}-{}", std::process::id(), unique, name))
    }
}
