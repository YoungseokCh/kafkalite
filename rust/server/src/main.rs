use std::path::Path;
use std::sync::Arc;

use clap::Parser;
use kafkalite_server::{Config, FileStore, KafkaBroker};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[derive(Parser, Debug)]
#[command(name = "kafkalite", about = "Kafka-compatible broker")]
struct Args {
    #[arg(short, long)]
    config: Option<String>,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    init_tracing();

    let config = Config::load(args.config.as_deref()).unwrap_or_else(|err| {
        eprintln!("Failed to load configuration: {err}");
        std::process::exit(1);
    });

    ensure_parent_dir(&config.storage.data_dir);

    let store = Arc::new(FileStore::open(&config.storage.data_dir).unwrap_or_else(|err| {
        eprintln!("Failed to open kafkalite storage: {err}");
        std::process::exit(1);
    }));

    let broker = KafkaBroker::new(config.clone(), store);
    if let Err(err) = broker.run().await {
        eprintln!("Kafka broker failed: {err}");
        std::process::exit(1);
    }
}

fn init_tracing() {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "kafkalite=info".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();
}

fn ensure_parent_dir(path: &Path) {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).unwrap_or_else(|err| {
            eprintln!(
                "Failed to create storage directory {}: {err}",
                parent.display()
            );
            std::process::exit(1);
        });
    }
}
