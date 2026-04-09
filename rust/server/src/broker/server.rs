use std::sync::Arc;

use anyhow::Result;
use tokio::net::TcpListener;
use tracing::{error, info};

use crate::config::Config;
use crate::store::SqliteStore;

use super::dispatcher;

#[derive(Clone)]
pub struct KafkaBroker {
    config: Config,
    store: Arc<SqliteStore>,
}

impl KafkaBroker {
    pub fn new(config: Config, store: Arc<SqliteStore>) -> Self {
        Self { config, store }
    }

    pub async fn run(self) -> Result<()> {
        let addr = self.config.socket_addr()?;
        let listener = TcpListener::bind(addr).await?;
        info!(
            address = %addr,
            broker_id = self.config.broker.broker_id,
            db_path = %self.store.db_path().display(),
            "kafkalite Kafka broker listening"
        );

        loop {
            let (stream, peer) = listener.accept().await?;
            let broker = self.clone();
            tokio::spawn(async move {
                if let Err(err) = dispatcher::serve_connection(stream, peer, broker).await {
                    error!(error = %err, remote = %peer, "connection failed");
                }
            });
        }
    }

    pub fn config(&self) -> &Config {
        &self.config
    }

    pub fn store(&self) -> &Arc<SqliteStore> {
        &self.store
    }
}
