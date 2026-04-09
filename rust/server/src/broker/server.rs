use std::sync::Arc;

use anyhow::Result;
use tokio::net::TcpListener;
use tracing::{debug, error, info};

use crate::config::Config;
use crate::store::Storage;

use super::dispatcher;

#[derive(Clone)]
pub struct KafkaBroker {
    config: Config,
    store: Arc<dyn Storage>,
}

impl KafkaBroker {
    pub fn new(config: Config, store: Arc<dyn Storage>) -> Self {
        Self { config, store }
    }

    pub async fn run(self) -> Result<()> {
        let addr = self.config.socket_addr()?;
        let listener = TcpListener::bind(addr).await?;
        info!(
            address = %addr,
            broker_id = self.config.broker.broker_id,
            "kafkalite Kafka broker listening"
        );

        loop {
            let (stream, peer) = listener.accept().await?;
            let broker = self.clone();
            tokio::spawn(async move {
                if let Err(err) = dispatcher::serve_connection(stream, peer, broker).await {
                    if is_expected_disconnect(&err) {
                        debug!(error = %err, remote = %peer, "connection closed");
                    } else {
                        error!(error = %err, remote = %peer, "connection failed");
                    }
                }
            });
        }
    }

    pub fn config(&self) -> &Config {
        &self.config
    }

    pub fn store(&self) -> &Arc<dyn Storage> {
        &self.store
    }
}

fn is_expected_disconnect(err: &anyhow::Error) -> bool {
    err.chain().any(|cause| {
        cause
            .downcast_ref::<std::io::Error>()
            .is_some_and(|io_err| {
                matches!(
                    io_err.kind(),
                    std::io::ErrorKind::UnexpectedEof
                        | std::io::ErrorKind::ConnectionReset
                        | std::io::ErrorKind::BrokenPipe
                )
            })
    })
}
