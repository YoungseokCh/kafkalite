use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::PathBuf;

use anyhow::{Context, Result, bail};

use crate::config::{BrokerConfig, Config};

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ClusterConfig {
    pub node_id: i32,
    pub process_roles: Vec<ProcessRole>,
    pub listeners: BTreeMap<String, ListenerConfig>,
    pub advertised_listeners: BTreeMap<String, ListenerConfig>,
    pub controller_listener_names: Vec<String>,
    pub controller_quorum_voters: Vec<ControllerQuorumVoter>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProcessRole {
    Broker,
    Controller,
}

impl ProcessRole {
    pub const BROKER_DEFAULT_LISTENER: &'static str = "PLAINTEXT";
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListenerConfig {
    pub name: String,
    pub host: String,
    pub port: u16,
}

impl ListenerConfig {
    pub fn socket_addr(&self) -> Result<SocketAddr, std::net::AddrParseError> {
        format!("{}:{}", self.host, self.port).parse()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ControllerQuorumVoter {
    pub node_id: i32,
    pub host: String,
    pub port: u16,
}

impl ClusterConfig {
    pub fn client_listener(&self) -> Option<&ListenerConfig> {
        self.listeners.get(ProcessRole::BROKER_DEFAULT_LISTENER)
    }

    pub fn advertised_client_listener(&self) -> Option<&ListenerConfig> {
        self.advertised_listeners
            .get(ProcessRole::BROKER_DEFAULT_LISTENER)
    }

    pub fn controller_listener(&self) -> Option<&ListenerConfig> {
        self.controller_listener_names
            .first()
            .and_then(|name| self.listeners.get(name))
    }

    pub fn has_role(&self, role: ProcessRole) -> bool {
        self.process_roles.contains(&role)
    }
}

pub fn load_properties_config(config_path: Option<&str>) -> Result<Config> {
    let path = config_path
        .map(str::to_owned)
        .or_else(|| std::env::var("KAFKALITE_CONFIG").ok())
        .context("No configuration provided. Use --config or set KAFKALITE_CONFIG.")?;
    let content = std::fs::read_to_string(&path)
        .with_context(|| format!("Failed to read configuration file {path}"))?;
    let properties = parse_properties(&content)?;
    let mut config = Config::default();

    let node_id = properties
        .get("node.id")
        .map(|value| parse_i32("node.id", value))
        .transpose()?
        .unwrap_or_else(default_broker_id);
    config.broker.broker_id = node_id;
    config.cluster.node_id = node_id;

    if let Some(value) = properties.get("process.roles") {
        config.cluster.process_roles = parse_process_roles(value)?;
    } else {
        config.cluster.process_roles = vec![ProcessRole::Broker];
    }

    if let Some(value) = properties.get("listeners") {
        config.cluster.listeners = parse_named_listeners("listeners", value)?;
    }

    if let Some(listener) = config.cluster.client_listener() {
        config.broker.host = listener.host.clone();
        config.broker.port = listener.port;
    }

    if let Some(value) = properties.get("advertised.listeners") {
        config.cluster.advertised_listeners = parse_named_listeners("advertised.listeners", value)?;
    } else {
        config.cluster.advertised_listeners = config.cluster.listeners.clone();
    }

    if let Some(listener) = config.cluster.advertised_client_listener() {
        config.broker.advertised_host = listener.host.clone();
        config.broker.advertised_port = listener.port;
    }

    if let Some(value) = properties.get("controller.listener.names") {
        config.cluster.controller_listener_names = parse_csv_list(value);
    }

    if let Some(value) = properties.get("controller.quorum.voters") {
        config.cluster.controller_quorum_voters = parse_controller_quorum_voters(value)?;
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

    validate_cluster_config(&config, &properties)?;
    Ok(config)
}

fn validate_cluster_config(config: &Config, properties: &BTreeMap<String, String>) -> Result<()> {
    if config.cluster.has_role(ProcessRole::Broker) && config.cluster.client_listener().is_none() {
        bail!("Broker role requires a PLAINTEXT listener in listeners");
    }

    if config.cluster.has_role(ProcessRole::Controller) {
        if !properties.contains_key("node.id") {
            bail!("Controller role requires node.id");
        }
        if config.cluster.controller_listener_names.is_empty() {
            bail!("Controller role requires controller.listener.names");
        }
        if config.cluster.controller_quorum_voters.is_empty() {
            bail!("Controller role requires controller.quorum.voters");
        }
        if !config
            .cluster
            .controller_quorum_voters
            .iter()
            .any(|voter| voter.node_id == config.cluster.node_id)
        {
            bail!("Controller role requires node.id to appear in controller.quorum.voters");
        }
        for listener_name in &config.cluster.controller_listener_names {
            if !config.cluster.listeners.contains_key(listener_name) {
                bail!("Controller listener `{listener_name}` missing from listeners");
            }
        }
    }
    Ok(())
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

fn parse_process_roles(value: &str) -> Result<Vec<ProcessRole>> {
    let mut roles = Vec::new();
    for role in parse_csv_list(value) {
        match role.as_str() {
            "broker" => roles.push(ProcessRole::Broker),
            "controller" => roles.push(ProcessRole::Controller),
            _ => bail!("Unsupported process role: {role}"),
        }
    }
    if roles.is_empty() {
        bail!("process.roles must contain at least one role");
    }
    Ok(roles)
}

fn parse_named_listeners(key: &str, value: &str) -> Result<BTreeMap<String, ListenerConfig>> {
    let mut listeners = BTreeMap::new();
    for entry in parse_csv_list(value) {
        let (name, address) = entry
            .split_once("://")
            .with_context(|| format!("Invalid listener for {key}: {entry}"))?;
        let (host, port) = address
            .rsplit_once(':')
            .with_context(|| format!("Invalid listener for {key}: {entry}"))?;
        let port = port
            .parse()
            .with_context(|| format!("Invalid port for {key}: {entry}"))?;
        listeners.insert(
            name.trim().to_string(),
            ListenerConfig {
                name: name.trim().to_string(),
                host: if host.is_empty() { "0.0.0.0" } else { host }.to_string(),
                port,
            },
        );
    }
    if listeners.is_empty() {
        bail!("Invalid value for {key}: expected at least one listener");
    }
    Ok(listeners)
}

fn parse_controller_quorum_voters(value: &str) -> Result<Vec<ControllerQuorumVoter>> {
    let mut voters = Vec::new();
    for voter in parse_csv_list(value) {
        let (node_id, endpoint) = voter
            .split_once('@')
            .with_context(|| format!("Invalid voter entry: {voter}"))?;
        let (host, port) = endpoint
            .rsplit_once(':')
            .with_context(|| format!("Invalid voter entry: {voter}"))?;
        voters.push(ControllerQuorumVoter {
            node_id: parse_i32("controller.quorum.voters", node_id)?,
            host: host.to_string(),
            port: port
                .parse()
                .with_context(|| format!("Invalid voter entry: {voter}"))?,
        });
    }
    Ok(voters)
}

fn parse_log_dirs(value: &str) -> Result<PathBuf> {
    let dirs = parse_csv_list(value);
    match dirs.as_slice() {
        [dir] => Ok(PathBuf::from(dir)),
        [] => bail!("Invalid value for log.dirs: expected one directory"),
        _ => bail!("Invalid value for log.dirs: kafkalite supports exactly one directory"),
    }
}

fn parse_csv_list(value: &str) -> Vec<String> {
    value
        .split(',')
        .map(str::trim)
        .filter(|entry| !entry.is_empty())
        .map(ToOwned::to_owned)
        .collect()
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

fn default_broker_id() -> i32 {
    BrokerConfig::default().broker_id
}
