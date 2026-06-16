use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use anyhow::Result;
use tokio::net::TcpListener;
use tokio::sync::{Notify, watch};
use tokio::task::{JoinError, JoinHandle, JoinSet};
use tracing::{debug, error, info};

use crate::cluster::{
    ClusterRpcRequest, ClusterRpcResponse, ClusterRpcTarget, ClusterRpcTransport, ClusterRuntime,
    GetPartitionStateRequest, ReplicaFetchRequest, TcpClusterRpcTransport,
    UpdateReplicaProgressRequest,
};
use crate::config::Config;
use crate::store::{Storage, TransactionSessionState, TransactionStatus};

use self::connection_errors::is_expected_disconnect;
use super::dispatcher;
use super::fetch_signals::FetchSignals;

mod connection_errors;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StagedOffsetCommit {
    pub group_id: String,
    pub topic: String,
    pub partition: i32,
    pub next_offset: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransactionSession {
    pub producer_id: i64,
    pub producer_epoch: i16,
    pub transaction_timeout_ms: i32,
    pub last_updated_ms: i64,
    pub transaction_start_timestamp_ms: i64,
    pub status: TransactionStatus,
    pub partitions: BTreeSet<(String, i32)>,
    pub pending_offset_commits: Vec<StagedOffsetCommit>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InitTransactionalProducerError {
    ConcurrentTransactions,
}

struct ReadyState {
    ready: AtomicBool,
    notify: Notify,
}

impl ReadyState {
    fn new() -> Self {
        Self {
            ready: AtomicBool::new(false),
            notify: Notify::new(),
        }
    }

    fn mark_ready(&self) {
        self.ready.store(true, Ordering::Release);
        self.notify.notify_waiters();
    }

    async fn wait(&self) {
        if self.ready.load(Ordering::Acquire) {
            return;
        }
        self.notify.notified().await;
    }
}

#[derive(Clone)]
/// Configured broker instance before listener tasks are started.
pub struct KafkaBroker {
    config: Config,
    cluster: ClusterRuntime,
    store: Arc<dyn Storage>,
    pub(super) fetch_signals: Arc<FetchSignals>,
    transactions: Arc<Mutex<BTreeMap<String, TransactionSession>>>,
}

/// Running broker lifecycle handle returned by [`KafkaBroker::start`].
pub struct BrokerHandle {
    client_addr: std::net::SocketAddr,
    controller_addr: Option<std::net::SocketAddr>,
    ready: Arc<ReadyState>,
    shutdown_tx: watch::Sender<bool>,
    client_task: Option<JoinHandle<Result<()>>>,
    controller_task: Option<JoinHandle<Result<()>>>,
}

impl KafkaBroker {
    /// Builds a broker from a validated config and storage backend.
    pub fn new(config: Config, store: Arc<dyn Storage>) -> Result<Self> {
        let cluster = ClusterRuntime::from_config(&config)?;
        let persisted_transactions = store.transaction_sessions()?;
        let mut transactions = BTreeMap::new();
        for (transactional_id, session) in persisted_transactions {
            let pending_offset_commits = store
                .transactional_offset_commits(session.producer_id)?
                .into_iter()
                .map(|commit| StagedOffsetCommit {
                    group_id: commit.group_id,
                    topic: commit.topic,
                    partition: commit.partition,
                    next_offset: commit.next_offset,
                })
                .collect::<Vec<_>>();
            transactions.insert(
                transactional_id,
                TransactionSession {
                    producer_id: session.producer_id,
                    producer_epoch: session.producer_epoch,
                    transaction_timeout_ms: session.transaction_timeout_ms,
                    last_updated_ms: session.last_updated_ms,
                    transaction_start_timestamp_ms: session.transaction_start_timestamp_ms,
                    status: session.status,
                    partitions: session.partitions.into_iter().collect(),
                    pending_offset_commits,
                },
            );
        }
        let broker = Self {
            config,
            cluster,
            store,
            fetch_signals: Arc::new(FetchSignals::default()),
            transactions: Arc::new(Mutex::new(transactions)),
        };
        if broker.cluster.can_auto_create_topics_locally() {
            let metadata = broker
                .store
                .topic_metadata(None, chrono::Utc::now().timestamp_millis())?;
            broker.sync_topic_metadata(&metadata)?;
            broker.recover_local_replica_progress(chrono::Utc::now().timestamp_millis())?;
        }
        broker.recover_transaction_coordinator_state()?;
        Ok(broker)
    }

    /// Binds listeners and spawns broker tasks, returning a handle for lifecycle control.
    pub async fn start(self) -> Result<BrokerHandle> {
        let ready = Arc::new(ReadyState::new());
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        let controller_listener = if let Some(listener) = self.config.cluster.controller_listener()
        {
            let requested_addr = listener.socket_addr()?;
            let listener = TcpListener::bind(requested_addr).await?;
            let controller_addr = listener.local_addr()?;
            info!(
                address = %controller_addr,
                node_id = self.config.cluster.node_id,
                "kafkalite cluster RPC listening"
            );
            Some((controller_addr, listener))
        } else {
            None
        };

        let requested_client_addr = self.config.socket_addr()?;
        let client_listener = TcpListener::bind(requested_client_addr).await?;
        let client_addr = client_listener.local_addr()?;
        info!(
            address = %client_addr,
            broker_id = self.config.broker.broker_id,
            "kafkalite Kafka broker listening"
        );

        let controller_addr = controller_listener.as_ref().map(|(addr, _)| *addr);
        let controller_task = controller_listener.map(|(_, listener)| {
            let broker = self.clone();
            let shutdown_rx = shutdown_rx.clone();
            tokio::spawn(async move { broker.serve_controller(listener, shutdown_rx).await })
        });
        let client_task = {
            let broker = self;
            tokio::spawn(async move { broker.serve_clients(client_listener, shutdown_rx).await })
        };

        ready.mark_ready();

        Ok(BrokerHandle {
            client_addr,
            controller_addr,
            ready,
            shutdown_tx,
            client_task: Some(client_task),
            controller_task,
        })
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

    pub fn init_transactional_producer(
        &self,
        transactional_id: &str,
        transaction_timeout_ms: i32,
        now_ms: i64,
    ) -> Result<std::result::Result<(i64, i16), InitTransactionalProducerError>> {
        if let Some(existing) = self.transaction_session(transactional_id) {
            if matches!(
                existing.status,
                TransactionStatus::Ongoing
                    | TransactionStatus::PrepareCommit
                    | TransactionStatus::PrepareAbort
            ) {
                return Ok(Err(InitTransactionalProducerError::ConcurrentTransactions));
            }
            let next_epoch = existing.producer_epoch.saturating_add(1);
            let session = TransactionSession {
                producer_id: existing.producer_id,
                producer_epoch: next_epoch,
                transaction_timeout_ms,
                last_updated_ms: now_ms,
                transaction_start_timestamp_ms: -1,
                status: TransactionStatus::Empty,
                partitions: BTreeSet::new(),
                pending_offset_commits: Vec::new(),
            };
            self.transactions
                .lock()
                .expect("transaction registry poisoned")
                .insert(transactional_id.to_string(), session.clone());
            self.persist_transaction_session(transactional_id, &session)?;
            return Ok(Ok((session.producer_id, session.producer_epoch)));
        }
        let session = self.store.init_producer()?;
        self.bind_transactional_producer(
            transactional_id,
            session.producer_id,
            session.producer_epoch,
            transaction_timeout_ms,
            now_ms,
        )?;
        Ok(Ok((session.producer_id, session.producer_epoch)))
    }

    pub fn bind_transactional_producer(
        &self,
        transactional_id: &str,
        producer_id: i64,
        producer_epoch: i16,
        transaction_timeout_ms: i32,
        now_ms: i64,
    ) -> Result<()> {
        let session = TransactionSession {
            producer_id,
            producer_epoch,
            transaction_timeout_ms,
            last_updated_ms: now_ms,
            transaction_start_timestamp_ms: -1,
            status: TransactionStatus::Empty,
            partitions: BTreeSet::new(),
            pending_offset_commits: Vec::new(),
        };
        self.transactions
            .lock()
            .expect("transaction registry poisoned")
            .insert(transactional_id.to_string(), session.clone());
        self.persist_transaction_session(transactional_id, &session)
    }

    pub fn transaction_session(&self, transactional_id: &str) -> Option<TransactionSession> {
        self.transactions
            .lock()
            .expect("transaction registry poisoned")
            .get(transactional_id)
            .cloned()
    }

    pub fn transaction_session_by_producer(
        &self,
        producer_id: i64,
    ) -> Option<(String, TransactionSession)> {
        self.transactions
            .lock()
            .expect("transaction registry poisoned")
            .iter()
            .find_map(|(transactional_id, session)| {
                (session.producer_id == producer_id)
                    .then(|| (transactional_id.clone(), session.clone()))
            })
    }

    pub fn add_transaction_partitions(
        &self,
        transactional_id: &str,
        partitions: impl IntoIterator<Item = (String, i32)>,
        now_ms: i64,
    ) -> Result<()> {
        let mut transactions = self
            .transactions
            .lock()
            .expect("transaction registry poisoned");
        if let Some(session) = transactions.get_mut(transactional_id) {
            session.partitions.extend(partitions);
            session.status = TransactionStatus::Ongoing;
            if session.transaction_start_timestamp_ms < 0 {
                session.transaction_start_timestamp_ms = now_ms;
            }
            session.last_updated_ms = now_ms;
            let session = session.clone();
            drop(transactions);
            return self.persist_transaction_session(transactional_id, &session);
        }
        Ok(())
    }

    pub fn transaction_contains_partition(
        &self,
        transactional_id: &str,
        topic: &str,
        partition: i32,
    ) -> bool {
        self.transactions
            .lock()
            .expect("transaction registry poisoned")
            .get(transactional_id)
            .is_some_and(|session| session.partitions.contains(&(topic.to_string(), partition)))
    }

    pub fn transaction_partitions(&self, transactional_id: &str) -> Vec<(String, i32)> {
        self.transactions
            .lock()
            .expect("transaction registry poisoned")
            .get(transactional_id)
            .map(|session| session.partitions.iter().cloned().collect())
            .unwrap_or_default()
    }

    pub fn clear_transaction_partitions(&self, transactional_id: &str) -> Result<()> {
        self.clear_transaction_partitions_with_timestamp(
            transactional_id,
            chrono::Utc::now().timestamp_millis(),
        )
    }

    pub fn clear_transaction_partitions_with_timestamp(
        &self,
        transactional_id: &str,
        now_ms: i64,
    ) -> Result<()> {
        let mut transactions = self
            .transactions
            .lock()
            .expect("transaction registry poisoned");
        if let Some(session) = transactions.get_mut(transactional_id) {
            session.partitions.clear();
            session.status = TransactionStatus::Empty;
            session.last_updated_ms = now_ms;
            session.transaction_start_timestamp_ms = -1;
            let session = session.clone();
            drop(transactions);
            return self.persist_transaction_session(transactional_id, &session);
        }
        Ok(())
    }

    pub fn stage_transaction_offset_commit(
        &self,
        transactional_id: &str,
        commit: StagedOffsetCommit,
        now_ms: i64,
    ) -> Result<()> {
        let mut transactions = self
            .transactions
            .lock()
            .expect("transaction registry poisoned");
        if let Some(session) = transactions.get_mut(transactional_id) {
            session.pending_offset_commits.push(commit);
            session.status = TransactionStatus::Ongoing;
            if session.transaction_start_timestamp_ms < 0 {
                session.transaction_start_timestamp_ms = now_ms;
            }
            session.last_updated_ms = now_ms;
            let session = session.clone();
            drop(transactions);
            return self.persist_transaction_session(transactional_id, &session);
        }
        Ok(())
    }

    pub fn remove_transaction_offset_commit(
        &self,
        transactional_id: &str,
        commit: &StagedOffsetCommit,
        now_ms: i64,
    ) -> Result<()> {
        let mut transactions = self
            .transactions
            .lock()
            .expect("transaction registry poisoned");
        if let Some(session) = transactions.get_mut(transactional_id) {
            if let Some(index) = session.pending_offset_commits.iter().rposition(|existing| {
                existing.group_id == commit.group_id
                    && existing.topic == commit.topic
                    && existing.partition == commit.partition
                    && existing.next_offset == commit.next_offset
            }) {
                session.pending_offset_commits.remove(index);
            }
            if session.pending_offset_commits.is_empty() && session.partitions.is_empty() {
                session.status = TransactionStatus::Empty;
                session.transaction_start_timestamp_ms = -1;
            }
            session.last_updated_ms = now_ms;
            let session = session.clone();
            drop(transactions);
            return self.persist_transaction_session(transactional_id, &session);
        }
        Ok(())
    }

    pub fn transaction_offset_commits(&self, transactional_id: &str) -> Vec<StagedOffsetCommit> {
        self.transactions
            .lock()
            .expect("transaction registry poisoned")
            .get(transactional_id)
            .map(|session| session.pending_offset_commits.clone())
            .unwrap_or_default()
    }

    pub fn clear_transaction_offset_commits(&self, transactional_id: &str) -> Result<()> {
        self.clear_transaction_offset_commits_with_timestamp(
            transactional_id,
            chrono::Utc::now().timestamp_millis(),
        )
    }

    pub fn clear_transaction_offset_commits_with_timestamp(
        &self,
        transactional_id: &str,
        now_ms: i64,
    ) -> Result<()> {
        let mut transactions = self
            .transactions
            .lock()
            .expect("transaction registry poisoned");
        if let Some(session) = transactions.get_mut(transactional_id) {
            session.pending_offset_commits.clear();
            if session.partitions.is_empty() {
                session.status = TransactionStatus::Empty;
                session.transaction_start_timestamp_ms = -1;
            }
            session.last_updated_ms = now_ms;
            let session = session.clone();
            drop(transactions);
            return self.persist_transaction_session(transactional_id, &session);
        }
        Ok(())
    }

    pub fn set_transaction_status(
        &self,
        transactional_id: &str,
        status: TransactionStatus,
        now_ms: i64,
    ) -> Result<()> {
        let mut transactions = self
            .transactions
            .lock()
            .expect("transaction registry poisoned");
        if let Some(session) = transactions.get_mut(transactional_id) {
            session.status = status;
            session.last_updated_ms = now_ms;
            let session = session.clone();
            drop(transactions);
            return self.persist_transaction_session(transactional_id, &session);
        }
        Ok(())
    }

    pub fn finalize_transaction_metadata(
        &self,
        transactional_id: &str,
        status: TransactionStatus,
        now_ms: i64,
    ) -> Result<()> {
        let mut transactions = self
            .transactions
            .lock()
            .expect("transaction registry poisoned");
        if let Some(session) = transactions.get_mut(transactional_id) {
            session.status = status;
            session.partitions.clear();
            session.pending_offset_commits.clear();
            session.last_updated_ms = now_ms;
            session.transaction_start_timestamp_ms = -1;
            let session = session.clone();
            drop(transactions);
            return self.persist_transaction_session(transactional_id, &session);
        }
        Ok(())
    }

    pub fn touch_transaction_by_producer(&self, producer_id: i64, now_ms: i64) -> Result<()> {
        let Some((transactional_id, mut session)) =
            self.transaction_session_by_producer(producer_id)
        else {
            return Ok(());
        };
        session.last_updated_ms = now_ms;
        self.transactions
            .lock()
            .expect("transaction registry poisoned")
            .insert(transactional_id.clone(), session.clone());
        self.persist_transaction_session(&transactional_id, &session)
    }

    pub fn expire_timed_out_transactions(&self, now_ms: i64) -> Result<()> {
        let expired = self
            .transactions
            .lock()
            .expect("transaction registry poisoned")
            .iter()
            .filter(|(_, session)| {
                session.transaction_timeout_ms > 0
                    && now_ms - session.last_updated_ms > i64::from(session.transaction_timeout_ms)
            })
            .map(|(transactional_id, session)| (transactional_id.clone(), session.clone()))
            .collect::<Vec<_>>();
        for (transactional_id, session) in &expired {
            for (topic, partition) in &session.partitions {
                let _ =
                    self.store
                        .write_transaction_marker(crate::store::TransactionMarkerRequest {
                            topic,
                            partition: *partition,
                            producer_id: session.producer_id,
                            producer_epoch: session.producer_epoch,
                            coordinator_epoch: 0,
                            committed: false,
                            partition_leader_epoch: 0,
                            now_ms,
                        });
            }
            self.store
                .delete_transaction_session(transactional_id, now_ms)?;
        }
        if !expired.is_empty() {
            let mut transactions = self
                .transactions
                .lock()
                .expect("transaction registry poisoned");
            for (transactional_id, _) in expired {
                transactions.remove(&transactional_id);
            }
        }
        Ok(())
    }

    fn recover_transaction_coordinator_state(&self) -> Result<()> {
        let sessions = self
            .transactions
            .lock()
            .expect("transaction registry poisoned")
            .iter()
            .map(|(transactional_id, session)| (transactional_id.clone(), session.clone()))
            .collect::<Vec<_>>();
        let now_ms = chrono::Utc::now().timestamp_millis();
        for (transactional_id, session) in sessions {
            match session.status {
                TransactionStatus::PrepareCommit => {
                    self.finish_recovered_transaction(
                        &transactional_id,
                        &session,
                        true,
                        true,
                        now_ms,
                    )?;
                }
                TransactionStatus::CompleteCommit => {
                    self.finish_recovered_transaction(
                        &transactional_id,
                        &session,
                        true,
                        false,
                        now_ms,
                    )?;
                }
                TransactionStatus::PrepareAbort => {
                    self.finish_recovered_transaction(
                        &transactional_id,
                        &session,
                        false,
                        true,
                        now_ms,
                    )?;
                }
                TransactionStatus::CompleteAbort => {
                    self.finish_recovered_transaction(
                        &transactional_id,
                        &session,
                        false,
                        false,
                        now_ms,
                    )?;
                }
                TransactionStatus::Empty | TransactionStatus::Ongoing => {}
            }
        }
        Ok(())
    }

    fn finish_recovered_transaction(
        &self,
        transactional_id: &str,
        session: &TransactionSession,
        committed: bool,
        resolve_in_doubt: bool,
        now_ms: i64,
    ) -> Result<()> {
        if resolve_in_doubt {
            for (topic, partition) in &session.partitions {
                if topic == "__consumer_offsets" {
                    continue;
                }
                self.store
                    .write_transaction_marker(crate::store::TransactionMarkerRequest {
                        topic,
                        partition: *partition,
                        producer_id: session.producer_id,
                        producer_epoch: session.producer_epoch,
                        coordinator_epoch: 0,
                        committed,
                        partition_leader_epoch: 0,
                        now_ms,
                    })?;
                self.update_local_replica_progress(topic, *partition, now_ms)?;
            }
            self.store.complete_transactional_offset_commits(
                session.producer_id,
                session.producer_epoch,
                committed,
                now_ms,
            )?;
        }
        self.finalize_transaction_metadata(
            transactional_id,
            if committed {
                TransactionStatus::CompleteCommit
            } else {
                TransactionStatus::CompleteAbort
            },
            now_ms,
        )?;
        self.set_transaction_status(transactional_id, TransactionStatus::Empty, now_ms)?;
        Ok(())
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

    pub fn partition_leader_epoch(&self, topic: &str, partition: i32) -> Option<i32> {
        self.cluster()
            .metadata_image()
            .partition_state_view(topic, partition)
            .map(|(_, leader_epoch, _, _)| leader_epoch)
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
        let previous_high_watermark = self.partition_high_watermark(topic, partition);
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
        self.notify_fetch_waiters_on_high_watermark_advance(
            topic,
            partition,
            previous_high_watermark,
            response.high_watermark,
        );
        Ok(response.high_watermark)
    }

    fn recover_local_replica_progress(&self, now_ms: i64) -> Result<()> {
        let metadata = self.store.topic_metadata(None, now_ms)?;
        for topic in &metadata {
            for partition in &topic.partitions {
                let _ =
                    self.update_local_replica_progress(&topic.name, partition.partition, now_ms)?;
            }
        }
        Ok(())
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
        let previous_high_watermark = self.partition_high_watermark(topic, partition);
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
        self.notify_fetch_waiters_on_high_watermark_advance(
            topic,
            partition,
            previous_high_watermark,
            response.high_watermark,
        );
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
        let previous_high_watermark = self.partition_high_watermark(topic, partition);
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
        self.notify_fetch_waiters_on_high_watermark_advance(
            topic,
            partition,
            previous_high_watermark,
            response.high_watermark,
        );
        Ok(response.high_watermark)
    }

    fn notify_fetch_waiters_on_high_watermark_advance(
        &self,
        topic: &str,
        partition: i32,
        previous_high_watermark: Option<i64>,
        high_watermark: i64,
    ) {
        if high_watermark > previous_high_watermark.unwrap_or(-1) {
            self.notify_fetch_signal(topic, partition);
        }
    }

    fn persist_transaction_session(
        &self,
        transactional_id: &str,
        session: &TransactionSession,
    ) -> Result<()> {
        self.store.persist_transaction_session(
            transactional_id,
            &TransactionSessionState {
                producer_id: session.producer_id,
                producer_epoch: session.producer_epoch,
                transaction_timeout_ms: session.transaction_timeout_ms,
                last_updated_ms: session.last_updated_ms,
                transaction_start_timestamp_ms: session.transaction_start_timestamp_ms,
                status: session.status,
                partitions: session.partitions.iter().cloned().collect(),
            },
            chrono::Utc::now().timestamp_millis(),
        )?;
        Ok(())
    }

    async fn serve_clients(
        self,
        listener: TcpListener,
        mut shutdown_rx: watch::Receiver<bool>,
    ) -> Result<()> {
        let mut connections = JoinSet::new();

        loop {
            tokio::select! {
                biased;
                _ = shutdown_rx.changed() => {
                    break;
                }
                Some(joined) = connections.join_next(), if !connections.is_empty() => {
                    if let Err(err) = joined {
                        if !err.is_cancelled() {
                            return Err(err.into());
                        }
                    }
                }
                accept_result = listener.accept() => {
                    let (stream, peer) = accept_result?;
                    let broker = self.clone();
                    connections.spawn(async move {
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
        }

        connections.abort_all();
        while let Some(joined) = connections.join_next().await {
            if let Err(err) = joined {
                if !err.is_cancelled() {
                    return Err(err.into());
                }
            }
        }
        Ok(())
    }

    async fn serve_controller(
        self,
        listener: TcpListener,
        mut shutdown_rx: watch::Receiver<bool>,
    ) -> Result<()> {
        loop {
            tokio::select! {
                biased;
                _ = shutdown_rx.changed() => {
                    return Ok(());
                }
                result = TcpClusterRpcTransport::serve_broker_once(
                    &listener,
                    self.cluster.clone(),
                    self.store.clone(),
                    self.fetch_signals.clone(),
                ) => {
                    result?;
                }
            }
        }
    }
}

impl BrokerHandle {
    /// Returns the bound Kafka client listener address.
    pub fn local_addr(&self) -> std::net::SocketAddr {
        self.client_addr
    }

    /// Returns the bound controller RPC address, if controller RPC is enabled.
    pub fn controller_addr(&self) -> Option<std::net::SocketAddr> {
        self.controller_addr
    }

    /// Waits until the broker has finished binding its listeners.
    pub async fn ready(&self) -> Result<()> {
        self.ready.wait().await;
        Ok(())
    }

    /// Waits for the broker tasks to exit, signalling shutdown if one side exits first.
    pub async fn wait(mut self) -> Result<()> {
        match self.controller_task.take() {
            Some(mut controller_task) => {
                tokio::select! {
                    client = self.client_task.as_mut().expect("client task missing") => {
                        self.signal_shutdown();
                        flatten_join_result(client)?;
                        flatten_join_result(controller_task.await)?;
                        Ok(())
                    }
                    controller = &mut controller_task => {
                        self.signal_shutdown();
                        flatten_join_result(controller)?;
                        let client_task = self.client_task.take().expect("client task missing");
                        flatten_join_result(client_task.await)?;
                        Ok(())
                    }
                }
            }
            None => {
                let client_task = self.client_task.take().expect("client task missing");
                flatten_join_result(client_task.await)
            }
        }
    }

    /// Requests graceful shutdown and waits for all broker tasks to stop.
    pub async fn shutdown(mut self) -> Result<()> {
        self.signal_shutdown();
        let client_task = self.client_task.take().expect("client task missing");
        flatten_join_result(client_task.await)?;
        if let Some(controller_task) = self.controller_task.take() {
            flatten_join_result(controller_task.await)
        } else {
            Ok(())
        }
    }

    fn signal_shutdown(&self) {
        let _ = self.shutdown_tx.send(true);
    }
}

impl Drop for BrokerHandle {
    fn drop(&mut self) {
        self.signal_shutdown();
        if let Some(client_task) = &self.client_task {
            client_task.abort();
        }
        if let Some(controller_task) = &self.controller_task {
            controller_task.abort();
        }
    }
}

fn flatten_join_result(result: std::result::Result<Result<()>, JoinError>) -> Result<()> {
    match result {
        Ok(inner) => inner,
        Err(err) => Err(err.into()),
    }
}

#[cfg(test)]
mod server_tests;
