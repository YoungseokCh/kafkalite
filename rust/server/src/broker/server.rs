use std::sync::Arc;

use anyhow::Result;
use tokio::net::TcpListener;
use tracing::{debug, error, info};

use crate::cluster::ClusterRuntime;
use crate::config::Config;
use crate::store::Storage;

use super::dispatcher;

#[derive(Clone)]
pub struct KafkaBroker {
    config: Config,
    cluster: ClusterRuntime,
    store: Arc<dyn Storage>,
}

impl KafkaBroker {
    pub fn new(config: Config, store: Arc<dyn Storage>) -> Result<Self> {
        let cluster = ClusterRuntime::from_config(&config)?;
        Ok(Self {
            config,
            cluster,
            store,
        })
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

    pub fn cluster(&self) -> &ClusterRuntime {
        &self.cluster
    }

    pub fn sync_topic_metadata(&self, topics: &[crate::store::TopicMetadata]) -> Result<()> {
        self.cluster
            .sync_local_topics(topics, self.config.broker.broker_id)
    }

    pub fn is_local_partition_leader(&self, topic: &str, partition: i32) -> bool {
        self.cluster()
            .metadata_image()
            .partition_leader_id(topic, partition)
            .is_none_or(|leader_id| leader_id == self.config.broker.broker_id)
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
