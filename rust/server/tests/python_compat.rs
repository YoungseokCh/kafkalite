use std::net::TcpListener;
use std::process::Command;
use std::sync::Arc;
use std::time::Duration;

use kafkalite_server::{
    Config, FileStore, KafkaBroker,
    config::{BrokerConfig, StorageConfig},
};
use tempfile::tempdir;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn aiokafka_smoke_script() {
    let _ = env_logger::builder().is_test(true).try_init();
    let _ = tracing_subscriber::fmt()
        .with_test_writer()
        .with_env_filter("kafkalite=debug")
        .try_init();
    let Some(python) = std::env::var_os("AIOKAFKA_PYTHON") else {
        eprintln!(
            "skipping aiokafka smoke: set AIOKAFKA_PYTHON to a python interpreter with aiokafka installed"
        );
        return;
    };

    let tempdir = tempdir().unwrap();
    let port = free_port();
    let config = Config {
        broker: BrokerConfig {
            port,
            advertised_port: port,
            ..BrokerConfig::default()
        },
        storage: StorageConfig {
            data_dir: tempdir.path().join("kafkalite-data"),
            ..StorageConfig::default()
        },
    };
    let store = Arc::new(FileStore::open(&config.storage.data_dir).unwrap());
    let broker = KafkaBroker::new(config, store);
    let handle = tokio::spawn(async move { broker.run().await });
    tokio::time::sleep(Duration::from_millis(150)).await;

    let bootstrap = format!("127.0.0.1:{port}");
    let output = Command::new(python)
        .arg("tests/aiokafka_smoke.py")
        .arg(&bootstrap)
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .output()
        .unwrap();

    handle.abort();
    let _ = handle.await;

    if !output.status.success() {
        panic!(
            "aiokafka smoke failed:\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
}

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}
