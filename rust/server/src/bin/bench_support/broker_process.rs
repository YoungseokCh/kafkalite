use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};

use kafkalite_server::Config;

use super::report::CpuMetrics;

#[derive(Debug)]
struct ProcessMetricsState {
    peak_rss_kb: u64,
    cpu_window_start: Instant,
    cpu_window_start_ticks: u64,
    last_sample_at: Instant,
    last_cpu_ticks: u64,
    peak_cpu_percent: f64,
    cpu_samples: u64,
}

pub struct BrokerProcess {
    pub bootstrap: String,
    child: Child,
    _config_path: PathBuf,
    metrics: Arc<Mutex<ProcessMetricsState>>,
    sampler: Option<std::thread::JoinHandle<()>>,
    stop: Arc<std::sync::atomic::AtomicBool>,
}

impl BrokerProcess {
    pub fn start(broker_bin: &Path, root: &Path, default_partitions: i32) -> Result<Self> {
        fs::create_dir_all(root)?;
        let port = free_port()?;
        let config = Config::single_node(root.join("data"), port, default_partitions);
        let config_path = root.join("server.properties");
        let config_text = format!(
            "node.id={}\nlisteners=PLAINTEXT://{}:{}\nadvertised.listeners=PLAINTEXT://{}:{}\ncluster.id={}\nlog.dirs={}\nnum.partitions={}\n",
            config.broker.broker_id,
            config.broker.host,
            config.broker.port,
            config.broker.advertised_host,
            config.broker.advertised_port,
            config.broker.cluster_id,
            config.storage.data_dir.display(),
            config.storage.default_partitions,
        );
        fs::write(&config_path, config_text)?;
        let child = Command::new(broker_bin)
            .arg("--config")
            .arg(&config_path)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .with_context(|| format!("spawn broker from {}", broker_bin.display()))?;
        let now = Instant::now();
        let cpu_ticks = read_cpu_ticks(child.id()).unwrap_or_default();
        let metrics = Arc::new(Mutex::new(ProcessMetricsState {
            peak_rss_kb: 0,
            cpu_window_start: now,
            cpu_window_start_ticks: cpu_ticks,
            last_sample_at: now,
            last_cpu_ticks: cpu_ticks,
            peak_cpu_percent: 0.0,
            cpu_samples: 0,
        }));
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let sampler = Some(start_sampler(child.id(), metrics.clone(), stop.clone()));
        let bootstrap = format!("127.0.0.1:{port}");
        wait_until_ready(&bootstrap, Duration::from_secs(10))?;
        Ok(Self {
            bootstrap,
            child,
            _config_path: config_path,
            metrics,
            sampler,
            stop,
        })
    }

    pub fn reset_cpu_window(&self) {
        let now = Instant::now();
        let cpu_ticks = read_cpu_ticks(self.child.id()).unwrap_or_default();
        let mut metrics = self.metrics.lock().expect("process metrics mutex poisoned");
        metrics.cpu_window_start = now;
        metrics.cpu_window_start_ticks = cpu_ticks;
        metrics.last_sample_at = now;
        metrics.last_cpu_ticks = cpu_ticks;
        metrics.peak_cpu_percent = 0.0;
        metrics.cpu_samples = 0;
    }

    pub fn cpu_metrics(&self) -> CpuMetrics {
        let now = Instant::now();
        let current_ticks = read_cpu_ticks(self.child.id()).unwrap_or_default();
        let metrics = self.metrics.lock().expect("process metrics mutex poisoned");
        let elapsed = now.duration_since(metrics.cpu_window_start);
        let elapsed_ms = elapsed.as_secs_f64() * 1000.0;
        let process_cpu_ms =
            ticks_to_ms(current_ticks.saturating_sub(metrics.cpu_window_start_ticks));
        CpuMetrics {
            elapsed_wall_ms: elapsed_ms,
            process_cpu_ms,
            avg_cpu_percent: cpu_percent(process_cpu_ms, elapsed),
            peak_cpu_percent: metrics.peak_cpu_percent,
            samples: metrics.cpu_samples,
        }
    }

    pub fn peak_rss_kb(&self) -> u64 {
        self.metrics
            .lock()
            .expect("process metrics mutex poisoned")
            .peak_rss_kb
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
    metrics: Arc<Mutex<ProcessMetricsState>>,
    stop: Arc<std::sync::atomic::AtomicBool>,
) -> std::thread::JoinHandle<()> {
    std::thread::spawn(move || {
        while !stop.load(std::sync::atomic::Ordering::Relaxed) {
            let now = Instant::now();
            let cpu_ticks = read_cpu_ticks(pid).ok();
            if let Ok(rss) = read_rss_kb(pid) {
                let mut state = metrics.lock().expect("process metrics mutex poisoned");
                state.peak_rss_kb = state.peak_rss_kb.max(rss);
                if let Some(cpu_ticks) = cpu_ticks {
                    let elapsed = now.duration_since(state.last_sample_at);
                    let delta_ms = ticks_to_ms(cpu_ticks.saturating_sub(state.last_cpu_ticks));
                    state.peak_cpu_percent =
                        state.peak_cpu_percent.max(cpu_percent(delta_ms, elapsed));
                    state.last_sample_at = now;
                    state.last_cpu_ticks = cpu_ticks;
                    state.cpu_samples += 1;
                }
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

fn read_cpu_ticks(pid: u32) -> Result<u64> {
    let stat = fs::read_to_string(format!("/proc/{pid}/stat"))?;
    let after_name = stat
        .rsplit_once(") ")
        .map(|(_, rest)| rest)
        .context("process stat format missing command terminator")?;
    let fields = after_name.split_whitespace().collect::<Vec<_>>();
    let utime = fields
        .get(11)
        .context("process stat missing utime")?
        .parse::<u64>()?;
    let stime = fields
        .get(12)
        .context("process stat missing stime")?
        .parse::<u64>()?;
    Ok(utime + stime)
}

fn ticks_to_ms(ticks: u64) -> f64 {
    ticks as f64 * 1000.0 / clock_ticks_per_second() as f64
}

fn cpu_percent(process_cpu_ms: f64, elapsed: Duration) -> f64 {
    process_cpu_ms / (elapsed.as_secs_f64() * 1000.0).max(1.0) * 100.0
}

fn clock_ticks_per_second() -> i64 {
    let ticks = unsafe { libc::sysconf(libc::_SC_CLK_TCK) };
    if ticks > 0 { ticks } else { 100 }
}
