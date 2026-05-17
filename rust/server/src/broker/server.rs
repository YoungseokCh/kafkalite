use std::sync::Arc;

use anyhow::Result;
use tokio::net::TcpListener;
use tracing::{debug, error, info};

use crate::cluster::{
    ClusterRpcRequest, ClusterRpcResponse, ClusterRpcTarget, ClusterRpcTransport, ClusterRuntime,
    GetPartitionStateRequest, ReplicaFetchRequest, TcpClusterRpcTransport,
    UpdateReplicaProgressRequest,
};
use crate::config::Config;
use crate::store::Storage;

use self::connection_errors::is_expected_disconnect;
use super::dispatcher;

mod connection_errors;

#[derive(Clone)]
pub struct KafkaBroker {
    config: Config,
    cluster: ClusterRuntime,
    store: Arc<dyn Storage>,
}

impl KafkaBroker {
    pub fn new(config: Config, store: Arc<dyn Storage>) -> Result<Self> {
        let cluster = ClusterRuntime::from_config(&config)?;
        let broker = Self {
            config,
            cluster,
            store,
        };
        if broker.cluster.can_auto_create_topics_locally() {
            let metadata = broker
                .store
                .topic_metadata(None, chrono::Utc::now().timestamp_millis())?;
            broker.sync_topic_metadata(&metadata)?;
        }
        Ok(broker)
    }

    pub async fn run(self) -> Result<()> {
        if let Some(controller_listener) = self.config.cluster.controller_listener() {
            let controller_addr = controller_listener.socket_addr()?;
            let controller_runtime = self.cluster.clone();
            let controller_store = self.store.clone();
            let listener = TcpListener::bind(controller_addr).await?;
            info!(
                address = %controller_addr,
                node_id = self.config.cluster.node_id,
                "kafkalite cluster RPC listening"
            );
            tokio::spawn(async move {
                if let Err(err) = TcpClusterRpcTransport::serve_broker_forever(
                    listener,
                    controller_runtime,
                    controller_store,
                )
                .await
                {
                    error!(error = %err, "cluster rpc service failed");
                }
            });
        }
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
        let leader = self
            .cluster()
            .metadata_image()
            .partition_leader_id(topic, partition);
        match leader {
            Some(leader_id) => leader_id == self.config.broker.broker_id,
            None => self.cluster.config().controller_quorum_voters.is_empty(),
        }
    }

    pub fn partition_high_watermark(&self, topic: &str, partition: i32) -> Option<i64> {
        self.cluster()
            .metadata_image()
            .partition_high_watermark(topic, partition)
    }

    pub fn partition_has_replica_progress(&self, topic: &str, partition: i32) -> bool {
        self.cluster()
            .metadata_image()
            .partition_has_replica_progress(topic, partition)
    }

    pub fn update_local_replica_progress(
        &self,
        topic: &str,
        partition: i32,
        now_ms: i64,
    ) -> Result<i64> {
        let (_, latest) = self.store.list_offsets(topic, partition)?;
        let response =
            self.cluster
                .handle_update_replica_progress(UpdateReplicaProgressRequest {
                    topic_name: topic.to_string(),
                    partition_index: partition,
                    leader_epoch: self
                        .cluster()
                        .metadata_image()
                        .partition_state_view(topic, partition)
                        .map(|(_, epoch, _, _)| epoch)
                        .unwrap_or(0),
                    broker_id: self.config.broker.broker_id,
                    log_end_offset: latest.offset,
                    last_caught_up_ms: now_ms,
                })?;
        Ok(response.high_watermark)
    }

    pub fn sync_follower_progress_from_remote<T: ClusterRpcTransport>(
        &self,
        transport: &T,
        target: &ClusterRpcTarget,
        topic: &str,
        partition: i32,
        now_ms: i64,
    ) -> Result<i64> {
        let local_state = self
            .cluster()
            .metadata_image()
            .partition_state_view(topic, partition)
            .ok_or_else(|| anyhow::anyhow!("missing local partition metadata"))?;
        let ClusterRpcResponse::GetPartitionState(state) = transport.send_to(
            target,
            ClusterRpcRequest::GetPartitionState(GetPartitionStateRequest {
                topic_name: topic.to_string(),
                partition_index: partition,
            }),
        )?
        else {
            unreachable!("unexpected cluster rpc response variant")
        };
        if !state.found {
            return Ok(-1);
        }
        if local_state.0 != 0
            && (state.leader_id != target.node_id
                || state.leader_id != local_state.0
                || state.leader_epoch != local_state.1)
        {
            anyhow::bail!("stale leader or epoch during follower progress sync")
        }
        let (_, latest) = self.store.list_offsets(topic, partition)?;
        let response =
            self.cluster
                .handle_update_replica_progress(UpdateReplicaProgressRequest {
                    topic_name: topic.to_string(),
                    partition_index: partition,
                    leader_epoch: local_state.1,
                    broker_id: self.config.broker.broker_id,
                    log_end_offset: latest.offset.min(state.leader_log_end_offset),
                    last_caught_up_ms: now_ms,
                })?;
        Ok(response.high_watermark)
    }

    pub fn fetch_and_apply_from_remote_leader<T: ClusterRpcTransport>(
        &self,
        transport: &T,
        target: &ClusterRpcTarget,
        topic: &str,
        partition: i32,
        now_ms: i64,
    ) -> Result<i64> {
        let local_state = self
            .cluster()
            .metadata_image()
            .partition_state_view(topic, partition)
            .ok_or_else(|| anyhow::anyhow!("missing local partition metadata"))?;
        let ClusterRpcResponse::GetPartitionState(leader_state) = transport.send_to(
            target,
            ClusterRpcRequest::GetPartitionState(GetPartitionStateRequest {
                topic_name: topic.to_string(),
                partition_index: partition,
            }),
        )?
        else {
            unreachable!("unexpected cluster rpc response variant")
        };
        if !leader_state.found {
            return Ok(-1);
        }
        if local_state.0 != 0
            && (leader_state.leader_id != target.node_id
                || leader_state.leader_id != local_state.0
                || leader_state.leader_epoch != local_state.1)
        {
            anyhow::bail!("stale leader or epoch during replica fetch")
        }
        let (_, latest) = self.store.list_offsets(topic, partition)?;
        let fetched = transport.replica_fetch_to(
            target,
            ReplicaFetchRequest {
                topic_name: topic.to_string(),
                partition_index: partition,
                start_offset: latest.offset,
                max_records: 1_000,
            },
        )?;
        let refreshed_local_state = self
            .cluster()
            .metadata_image()
            .partition_state_view(topic, partition)
            .ok_or_else(|| anyhow::anyhow!("missing local partition metadata"))?;
        if refreshed_local_state.0 != 0
            && (refreshed_local_state.0 != leader_state.leader_id
                || refreshed_local_state.1 != leader_state.leader_epoch)
        {
            anyhow::bail!("leadership changed during replica fetch")
        }
        if !fetched.found {
            return Ok(-1);
        }
        if fetched.leader_id != leader_state.leader_id
            || fetched.leader_epoch != leader_state.leader_epoch
        {
            anyhow::bail!("leadership changed before replica fetch response applied")
        }
        if latest.offset > fetched.leader_log_end_offset {
            self.store
                .truncate_partition(topic, partition, fetched.leader_log_end_offset)?;
        }
        if !fetched.records.is_empty() {
            let _ =
                self.store
                    .append_replica_records(topic, partition, &fetched.records, now_ms)?;
        }
        let response =
            self.cluster
                .handle_update_replica_progress(UpdateReplicaProgressRequest {
                    topic_name: topic.to_string(),
                    partition_index: partition,
                    leader_epoch: local_state.1,
                    broker_id: self.config.broker.broker_id,
                    log_end_offset: fetched
                        .leader_log_end_offset
                        .min(self.store.list_offsets(topic, partition)?.1.offset),
                    last_caught_up_ms: now_ms,
                })?;
        Ok(response.high_watermark)
    }
}

#[cfg(test)]
mod server_tests;
