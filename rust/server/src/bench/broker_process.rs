use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};

use crate::config::{BrokerConfig, Config, StorageConfig};

pub struct BrokerProcess {
    pub bootstrap: String,
    child: Child,
    _config_path: PathBuf,
    peak_rss_kb: Arc<Mutex<u64>>,
    sampler: Option<std::thread::JoinHandle<()>>,
    stop: Arc<std::sync::atomic::AtomicBool>,
}

impl BrokerProcess {
    pub fn start(broker_bin: &Path, root: &Path) -> Result<Self> {
        fs::create_dir_all(root)?;
        let port = free_port()?;
        let config = Config {
            broker: BrokerConfig {
                port,
                advertised_port: port,
                ..BrokerConfig::default()
            },
            storage: StorageConfig {
                data_dir: root.join("data"),
            },
        };
        let config_path = root.join("bench.toml");
        let config_text = format!(
            "[kafkalite.broker]\nbroker_id = {}\nhost = \"{}\"\nport = {}\nadvertised_host = \"{}\"\nadvertised_port = {}\ncluster_id = \"{}\"\n[kafkalite.storage]\ndata_dir = \"{}\"\n",
            config.broker.broker_id,
            config.broker.host,
            config.broker.port,
            config.broker.advertised_host,
            config.broker.advertised_port,
            config.broker.cluster_id,
            config.storage.data_dir.display(),
        );
        fs::write(&config_path, config_text)?;
        let child = Command::new(broker_bin)
            .arg("--config")
            .arg(&config_path)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .with_context(|| format!("spawn broker from {}", broker_bin.display()))?;
        let peak_rss_kb = Arc::new(Mutex::new(0));
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let sampler = Some(start_sampler(child.id(), peak_rss_kb.clone(), stop.clone()));
        let bootstrap = format!("127.0.0.1:{port}");
        wait_until_ready(&bootstrap, Duration::from_secs(10))?;
        Ok(Self {
            bootstrap,
            child,
            _config_path: config_path,
            peak_rss_kb,
            sampler,
            stop,
        })
    }

    pub fn peak_rss_kb(&self) -> u64 {
        *self.peak_rss_kb.lock().expect("rss mutex poisoned")
    }

    pub fn final_rss_kb(&self) -> u64 {
        read_rss_kb(self.child.id()).unwrap_or_default()
    }
}

impl Drop for BrokerProcess {
    fn drop(&mut self) {
        self.stop.store(true, std::sync::atomic::Ordering::Relaxed);
        if let Some(sampler) = self.sampler.take() {
            let _ = sampler.join();
        }
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn start_sampler(
    pid: u32,
    peak_rss_kb: Arc<Mutex<u64>>,
    stop: Arc<std::sync::atomic::AtomicBool>,
) -> std::thread::JoinHandle<()> {
    std::thread::spawn(move || {
        while !stop.load(std::sync::atomic::Ordering::Relaxed) {
            if let Ok(rss) = read_rss_kb(pid) {
                let mut peak = peak_rss_kb.lock().expect("rss mutex poisoned");
                *peak = (*peak).max(rss);
            }
            std::thread::sleep(Duration::from_millis(25));
        }
    })
}

fn wait_until_ready(bootstrap: &str, timeout: Duration) -> Result<()> {
    use rdkafka::config::ClientConfig;
    use rdkafka::consumer::{BaseConsumer, Consumer};

    let started = Instant::now();
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .set("group.id", "bench-probe")
        .create()?;
    while started.elapsed() < timeout {
        if consumer
            .fetch_metadata(None, Duration::from_millis(250))
            .is_ok()
        {
            return Ok(());
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    anyhow::bail!("broker did not become ready in time")
}

fn free_port() -> Result<u16> {
    let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
    Ok(listener.local_addr()?.port())
}

fn read_rss_kb(pid: u32) -> Result<u64> {
    let status = fs::read_to_string(format!("/proc/{pid}/status"))?;
    let value = status
        .lines()
        .find_map(|line| line.strip_prefix("VmRSS:"))
        .and_then(|line| line.split_whitespace().next())
        .context("VmRSS missing")?;
    Ok(value.parse()?)
}
