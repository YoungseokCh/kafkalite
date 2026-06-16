use bytes::Bytes;
use kafka_protocol::messages::add_partitions_to_txn_request::{
    AddPartitionsToTxnTopic, AddPartitionsToTxnTransaction,
};
use kafka_protocol::messages::join_group_request::JoinGroupRequestProtocol;
use kafka_protocol::messages::txn_offset_commit_request::{
    TxnOffsetCommitRequestPartition, TxnOffsetCommitRequestTopic,
};
use kafka_protocol::messages::{
    AddOffsetsToTxnRequest, AddPartitionsToTxnRequest, EndTxnRequest, GroupId,
    InitProducerIdRequest, JoinGroupRequest, ProducerId, TopicName, TransactionalId,
    TxnOffsetCommitRequest,
};
use kafka_protocol::protocol::StrBytes;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tempfile::tempdir;
use tokio::time::{Duration, sleep};

use crate::broker::handlers::error_codes::{
    CONCURRENT_TRANSACTIONS, INVALID_PRODUCER_EPOCH, INVALID_PRODUCER_ID_MAPPING,
    INVALID_TRANSACTION_TIMEOUT, INVALID_TXN_STATE, UNKNOWN_PRODUCER_ID,
};
use crate::broker::handlers::groups::handle_txn_offset_commit;
use crate::broker::handlers::produce_fetch::{
    handle_add_offsets_to_txn, handle_add_partitions_to_txn, handle_end_txn,
};
use crate::config::Config;
use crate::store::{BrokerRecord, FileStore, Storage, StoreError, TransactionStatus};

use super::*;

async fn add_offsets_to_txn(
    broker: &KafkaBroker,
    transactional_id: &str,
    group_id: &str,
    producer_id: ProducerId,
    producer_epoch: i16,
) {
    let response = handle_add_offsets_to_txn(
        broker,
        AddOffsetsToTxnRequest::default()
            .with_transactional_id(TransactionalId(StrBytes::from(
                transactional_id.to_string(),
            )))
            .with_group_id(GroupId(StrBytes::from(group_id.to_string())))
            .with_producer_id(producer_id)
            .with_producer_epoch(producer_epoch),
    )
    .await
    .unwrap();
    assert_eq!(response.error_code, 0);
}

struct FailingStorage {
    inner: Arc<FileStore>,
    fail_stage_transactional_offset_commits: AtomicUsize,
    fail_marker_writes: AtomicUsize,
    fail_offset_completions: AtomicUsize,
}

impl FailingStorage {
    fn new(
        inner: Arc<FileStore>,
        fail_stage_transactional_offset_commits: usize,
        fail_marker_writes: usize,
        fail_offset_completions: usize,
    ) -> Self {
        Self {
            inner,
            fail_stage_transactional_offset_commits: AtomicUsize::new(
                fail_stage_transactional_offset_commits,
            ),
            fail_marker_writes: AtomicUsize::new(fail_marker_writes),
            fail_offset_completions: AtomicUsize::new(fail_offset_completions),
        }
    }

    fn consume_failure(counter: &AtomicUsize) -> bool {
        counter
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                if remaining > 0 {
                    Some(remaining - 1)
                } else {
                    None
                }
            })
            .is_ok()
    }
}

impl Storage for FailingStorage {
    fn topic_metadata(
        &self,
        topics: Option<&[String]>,
        now_ms: i64,
    ) -> crate::store::Result<Vec<crate::store::TopicMetadata>> {
        self.inner.topic_metadata(topics, now_ms)
    }

    fn ensure_topic(
        &self,
        topic: &str,
        partition_count: i32,
        now_ms: i64,
    ) -> crate::store::Result<()> {
        self.inner.ensure_topic(topic, partition_count, now_ms)
    }

    fn init_producer(&self) -> crate::store::Result<crate::store::ProducerSession> {
        self.inner.init_producer()
    }

    fn append_records(
        &self,
        topic: &str,
        partition: i32,
        records: &[BrokerRecord],
        now_ms: i64,
    ) -> crate::store::Result<(i64, i64)> {
        self.inner.append_records(topic, partition, records, now_ms)
    }

    fn write_transaction_marker(
        &self,
        request: crate::store::TransactionMarkerRequest<'_>,
    ) -> crate::store::Result<(i64, i64)> {
        if Self::consume_failure(&self.fail_marker_writes) {
            return Err(StoreError::UnknownProducerId {
                producer_id: request.producer_id,
            });
        }
        self.inner.write_transaction_marker(request)
    }

    fn fetch_records(
        &self,
        topic: &str,
        partition: i32,
        start_offset: i64,
        limit: usize,
    ) -> crate::store::Result<crate::store::FetchResult> {
        self.inner
            .fetch_records(topic, partition, start_offset, limit)
    }

    fn replica_fetch_records(
        &self,
        topic: &str,
        partition: i32,
        start_offset: i64,
        limit: usize,
    ) -> crate::store::Result<crate::store::ReplicaFetchResult> {
        self.inner
            .replica_fetch_records(topic, partition, start_offset, limit)
    }

    fn fetch_records_for_client(
        &self,
        topic: &str,
        partition: i32,
        start_offset: i64,
        max_bytes: usize,
    ) -> crate::store::Result<crate::store::FetchResult> {
        self.inner
            .fetch_records_for_client(topic, partition, start_offset, max_bytes)
    }

    fn append_replica_records(
        &self,
        topic: &str,
        partition: i32,
        records: &[BrokerRecord],
        now_ms: i64,
    ) -> crate::store::Result<i64> {
        self.inner
            .append_replica_records(topic, partition, records, now_ms)
    }

    fn apply_replica_records(
        &self,
        topic: &str,
        partition: i32,
        records: &[BrokerRecord],
        leader_high_watermark: i64,
        now_ms: i64,
    ) -> crate::store::Result<crate::store::ReplicaApplyResult> {
        self.inner
            .apply_replica_records(topic, partition, records, leader_high_watermark, now_ms)
    }

    fn truncate_partition(
        &self,
        topic: &str,
        partition: i32,
        next_offset: i64,
    ) -> crate::store::Result<()> {
        self.inner.truncate_partition(topic, partition, next_offset)
    }

    fn list_offsets(
        &self,
        topic: &str,
        partition: i32,
    ) -> crate::store::Result<(
        crate::store::ListOffsetResult,
        crate::store::ListOffsetResult,
    )> {
        self.inner.list_offsets(topic, partition)
    }

    fn list_offset_for_timestamp(
        &self,
        topic: &str,
        partition: i32,
        timestamp_ms: i64,
    ) -> crate::store::Result<Option<crate::store::ListOffsetResult>> {
        self.inner
            .list_offset_for_timestamp(topic, partition, timestamp_ms)
    }

    fn join_group(
        &self,
        request: crate::store::GroupJoinRequest<'_>,
    ) -> crate::store::Result<crate::store::GroupJoinResult> {
        self.inner.join_group(request)
    }

    fn sync_group(
        &self,
        group_id: &str,
        member_id: &str,
        generation_id: i32,
        protocol_name: &str,
        assignments: &[(String, Vec<u8>)],
        now_ms: i64,
    ) -> crate::store::Result<crate::store::SyncGroupResult> {
        self.inner.sync_group(
            group_id,
            member_id,
            generation_id,
            protocol_name,
            assignments,
            now_ms,
        )
    }

    fn heartbeat(
        &self,
        group_id: &str,
        member_id: &str,
        generation_id: i32,
        now_ms: i64,
    ) -> crate::store::Result<()> {
        self.inner
            .heartbeat(group_id, member_id, generation_id, now_ms)
    }

    fn leave_group(
        &self,
        group_id: &str,
        member_id: &str,
        now_ms: i64,
    ) -> crate::store::Result<()> {
        self.inner.leave_group(group_id, member_id, now_ms)
    }

    fn validate_offset_commit(
        &self,
        request: crate::store::OffsetCommitRequest<'_>,
    ) -> crate::store::Result<()> {
        self.inner.validate_offset_commit(request)
    }

    fn commit_offset(
        &self,
        request: crate::store::OffsetCommitRequest<'_>,
    ) -> crate::store::Result<()> {
        self.inner.commit_offset(request)
    }

    fn stage_transactional_offset_commit(
        &self,
        request: crate::store::TransactionalOffsetCommitRequest<'_>,
    ) -> crate::store::Result<()> {
        if Self::consume_failure(&self.fail_stage_transactional_offset_commits) {
            return Err(StoreError::UnknownProducerId {
                producer_id: request.producer_id,
            });
        }
        self.inner.stage_transactional_offset_commit(request)
    }

    fn complete_transactional_offset_commits(
        &self,
        producer_id: i64,
        producer_epoch: i16,
        committed: bool,
        now_ms: i64,
    ) -> crate::store::Result<()> {
        if Self::consume_failure(&self.fail_offset_completions) {
            return Err(StoreError::StaleProducerEpoch {
                producer_id,
                expected: producer_epoch.saturating_add(1),
                actual: producer_epoch,
            });
        }
        self.inner.complete_transactional_offset_commits(
            producer_id,
            producer_epoch,
            committed,
            now_ms,
        )
    }

    fn transactional_offset_commits(
        &self,
        producer_id: i64,
    ) -> crate::store::Result<Vec<crate::store::TransactionalOffsetCommit>> {
        self.inner.transactional_offset_commits(producer_id)
    }

    fn fetch_offset(
        &self,
        group_id: &str,
        topic: &str,
        partition: i32,
    ) -> crate::store::Result<Option<i64>> {
        self.inner.fetch_offset(group_id, topic, partition)
    }

    fn transaction_sessions(
        &self,
    ) -> crate::store::Result<
        std::collections::BTreeMap<String, crate::store::TransactionSessionState>,
    > {
        self.inner.transaction_sessions()
    }

    fn persist_transaction_session(
        &self,
        transactional_id: &str,
        session: &crate::store::TransactionSessionState,
        now_ms: i64,
    ) -> crate::store::Result<()> {
        self.inner
            .persist_transaction_session(transactional_id, session, now_ms)
    }

    fn delete_transaction_session(
        &self,
        transactional_id: &str,
        now_ms: i64,
    ) -> crate::store::Result<()> {
        self.inner
            .delete_transaction_session(transactional_id, now_ms)
    }
}

#[tokio::test]
async fn add_partitions_and_end_txn_commit_appends_control_marker() {
    let broker = test_broker();
    broker.store().ensure_topic("txn.end.topic", 1, 0).unwrap();
    let session = crate::broker::handlers::bootstrap::handle_init_producer_id(
        &broker,
        InitProducerIdRequest::default()
            .with_transactional_id(Some(TransactionalId(StrBytes::from("txn-end".to_string())))),
    )
    .await
    .unwrap();
    let _ = handle_produce(
        &broker,
        produce_request(
            "txn.end.topic",
            session.producer_id.0,
            session.producer_epoch,
            0,
        ),
    )
    .await
    .unwrap();

    let add = handle_add_partitions_to_txn(
        &broker,
        AddPartitionsToTxnRequest::default().with_transactions(vec![
            AddPartitionsToTxnTransaction::default()
                .with_transactional_id(TransactionalId(StrBytes::from("txn-end".to_string())))
                .with_producer_id(session.producer_id)
                .with_producer_epoch(session.producer_epoch)
                .with_topics(vec![
                    AddPartitionsToTxnTopic::default()
                        .with_name(TopicName(StrBytes::from("txn.end.topic".to_string())))
                        .with_partitions(vec![0]),
                ]),
        ]),
        5,
    )
    .await
    .unwrap();
    assert_eq!(add.error_code, 0);
    assert_eq!(
        add.results_by_transaction[0].topic_results[0].results_by_partition[0].partition_error_code,
        0
    );

    let end = handle_end_txn(
        &broker,
        EndTxnRequest::default()
            .with_transactional_id(TransactionalId(StrBytes::from("txn-end".to_string())))
            .with_producer_id(session.producer_id)
            .with_producer_epoch(session.producer_epoch)
            .with_committed(true),
        5,
    )
    .await
    .unwrap();
    assert_eq!(end.error_code, 0);

    let fetched = broker
        .store()
        .fetch_records("txn.end.topic", 0, 0, 10)
        .unwrap();
    assert_eq!(fetched.records.len(), 2);
    assert!(fetched.records[1].control);
}

#[tokio::test]
async fn txn_offset_commit_is_applied_only_on_end_txn_commit() {
    let broker = test_broker();
    broker
        .store()
        .ensure_topic("txn.offset.topic", 1, 0)
        .unwrap();
    let session = crate::broker::handlers::bootstrap::handle_init_producer_id(
        &broker,
        InitProducerIdRequest::default().with_transactional_id(Some(TransactionalId(
            StrBytes::from("txn-offset".to_string()),
        ))),
    )
    .await
    .unwrap();
    let joined = crate::broker::handlers::groups::handle_join_group(
        &broker,
        JoinGroupRequest::default()
            .with_group_id(GroupId(StrBytes::from("txn-group".to_string())))
            .with_member_id(StrBytes::from("member-a".to_string()))
            .with_protocol_type(StrBytes::from("consumer".to_string()))
            .with_session_timeout_ms(5_000)
            .with_rebalance_timeout_ms(5_000)
            .with_protocols(vec![
                JoinGroupRequestProtocol::default()
                    .with_name(StrBytes::from("range".to_string()))
                    .with_metadata(Bytes::new()),
            ]),
    )
    .await
    .unwrap();

    add_offsets_to_txn(
        &broker,
        "txn-offset",
        "txn-group",
        session.producer_id,
        session.producer_epoch,
    )
    .await;

    let response = handle_txn_offset_commit(
        &broker,
        TxnOffsetCommitRequest::default()
            .with_transactional_id(TransactionalId(StrBytes::from("txn-offset".to_string())))
            .with_group_id(GroupId(StrBytes::from("txn-group".to_string())))
            .with_producer_id(session.producer_id)
            .with_producer_epoch(session.producer_epoch)
            .with_generation_id(joined.generation_id)
            .with_member_id(StrBytes::from("member-a".to_string()))
            .with_topics(vec![
                TxnOffsetCommitRequestTopic::default()
                    .with_name(TopicName(StrBytes::from("txn.offset.topic".to_string())))
                    .with_partitions(vec![
                        TxnOffsetCommitRequestPartition::default()
                            .with_partition_index(0)
                            .with_committed_offset(7),
                    ]),
            ]),
    )
    .await
    .unwrap();

    assert_eq!(response.topics[0].partitions[0].error_code, 0);
    assert_eq!(
        broker
            .store()
            .fetch_offset("txn-group", "txn.offset.topic", 0)
            .unwrap(),
        None
    );

    let end = handle_end_txn(
        &broker,
        EndTxnRequest::default()
            .with_transactional_id(TransactionalId(StrBytes::from("txn-offset".to_string())))
            .with_producer_id(session.producer_id)
            .with_producer_epoch(session.producer_epoch)
            .with_committed(true),
        5,
    )
    .await
    .unwrap();
    assert_eq!(end.error_code, 0);

    assert_eq!(
        broker
            .store()
            .fetch_offset("txn-group", "txn.offset.topic", 0)
            .unwrap(),
        Some(7)
    );
}

#[tokio::test]
async fn txn_offset_commit_requires_add_offsets_to_txn() {
    let broker = test_broker();
    broker
        .store()
        .ensure_topic("txn.offset.required.topic", 1, 0)
        .unwrap();
    let session = crate::broker::handlers::bootstrap::handle_init_producer_id(
        &broker,
        InitProducerIdRequest::default().with_transactional_id(Some(TransactionalId(
            StrBytes::from("txn-offset-required".to_string()),
        ))),
    )
    .await
    .unwrap();
    let joined = crate::broker::handlers::groups::handle_join_group(
        &broker,
        JoinGroupRequest::default()
            .with_group_id(GroupId(StrBytes::from(
                "txn-offset-required-group".to_string(),
            )))
            .with_member_id(StrBytes::from("member-a".to_string()))
            .with_protocol_type(StrBytes::from("consumer".to_string()))
            .with_session_timeout_ms(5_000)
            .with_rebalance_timeout_ms(5_000)
            .with_protocols(vec![
                JoinGroupRequestProtocol::default()
                    .with_name(StrBytes::from("range".to_string()))
                    .with_metadata(Bytes::new()),
            ]),
    )
    .await
    .unwrap();

    let response = handle_txn_offset_commit(
        &broker,
        TxnOffsetCommitRequest::default()
            .with_transactional_id(TransactionalId(StrBytes::from(
                "txn-offset-required".to_string(),
            )))
            .with_group_id(GroupId(StrBytes::from(
                "txn-offset-required-group".to_string(),
            )))
            .with_producer_id(session.producer_id)
            .with_producer_epoch(session.producer_epoch)
            .with_generation_id(joined.generation_id)
            .with_member_id(StrBytes::from("member-a".to_string()))
            .with_topics(vec![
                TxnOffsetCommitRequestTopic::default()
                    .with_name(TopicName(StrBytes::from(
                        "txn.offset.required.topic".to_string(),
                    )))
                    .with_partitions(vec![
                        TxnOffsetCommitRequestPartition::default()
                            .with_partition_index(0)
                            .with_committed_offset(3),
                    ]),
            ]),
    )
    .await
    .unwrap();

    assert_eq!(
        response.topics[0].partitions[0].error_code,
        INVALID_TXN_STATE
    );
}

#[tokio::test]
async fn txn_offset_commit_rolls_back_session_state_when_offset_staging_fails() {
    let dir = tempdir().unwrap();
    let data_dir = dir.path().join("data");
    let raw_store = Arc::new(FileStore::open(&data_dir).unwrap());
    let failing_store = Arc::new(FailingStorage::new(raw_store, 1, 0, 0));
    let broker = KafkaBroker::new(
        Config::single_node(data_dir, 9092, 1),
        failing_store as Arc<dyn Storage>,
    )
    .unwrap();
    broker
        .store()
        .ensure_topic("txn.offset.rollback.topic", 1, 0)
        .unwrap();
    let session = crate::broker::handlers::bootstrap::handle_init_producer_id(
        &broker,
        InitProducerIdRequest::default().with_transactional_id(Some(TransactionalId(
            StrBytes::from("txn-offset-rollback".to_string()),
        ))),
    )
    .await
    .unwrap();
    let joined = crate::broker::handlers::groups::handle_join_group(
        &broker,
        JoinGroupRequest::default()
            .with_group_id(GroupId(StrBytes::from(
                "txn-offset-rollback-group".to_string(),
            )))
            .with_member_id(StrBytes::from("member-a".to_string()))
            .with_protocol_type(StrBytes::from("consumer".to_string()))
            .with_session_timeout_ms(5_000)
            .with_rebalance_timeout_ms(5_000)
            .with_protocols(vec![
                JoinGroupRequestProtocol::default()
                    .with_name(StrBytes::from("range".to_string()))
                    .with_metadata(Bytes::new()),
            ]),
    )
    .await
    .unwrap();

    add_offsets_to_txn(
        &broker,
        "txn-offset-rollback",
        "txn-offset-rollback-group",
        session.producer_id,
        session.producer_epoch,
    )
    .await;

    let err = handle_txn_offset_commit(
        &broker,
        TxnOffsetCommitRequest::default()
            .with_transactional_id(TransactionalId(StrBytes::from(
                "txn-offset-rollback".to_string(),
            )))
            .with_group_id(GroupId(StrBytes::from(
                "txn-offset-rollback-group".to_string(),
            )))
            .with_producer_id(session.producer_id)
            .with_producer_epoch(session.producer_epoch)
            .with_generation_id(joined.generation_id)
            .with_member_id(StrBytes::from("member-a".to_string()))
            .with_topics(vec![
                TxnOffsetCommitRequestTopic::default()
                    .with_name(TopicName(StrBytes::from(
                        "txn.offset.rollback.topic".to_string(),
                    )))
                    .with_partitions(vec![
                        TxnOffsetCommitRequestPartition::default()
                            .with_partition_index(0)
                            .with_committed_offset(12),
                    ]),
            ]),
    )
    .await
    .unwrap_err();
    assert!(err.to_string().contains("unknown producer id"));

    let restored = broker
        .transaction_session("txn-offset-rollback")
        .expect("transaction session");
    assert!(restored.pending_offset_commits.is_empty());
    assert_eq!(restored.status, TransactionStatus::Ongoing);
}

#[tokio::test]
async fn add_offsets_to_txn_survives_restart_before_txn_offset_commit() {
    let dir = tempdir().unwrap();
    let broker = broker_for_data_dir(&dir.path().join("data"));
    broker
        .store()
        .ensure_topic("txn.offset.restart.topic", 1, 0)
        .unwrap();
    let session = crate::broker::handlers::bootstrap::handle_init_producer_id(
        &broker,
        InitProducerIdRequest::default().with_transactional_id(Some(TransactionalId(
            StrBytes::from("txn-offset-restart".to_string()),
        ))),
    )
    .await
    .unwrap();
    let joined = crate::broker::handlers::groups::handle_join_group(
        &broker,
        JoinGroupRequest::default()
            .with_group_id(GroupId(StrBytes::from(
                "txn-offset-restart-group".to_string(),
            )))
            .with_member_id(StrBytes::from("member-a".to_string()))
            .with_protocol_type(StrBytes::from("consumer".to_string()))
            .with_session_timeout_ms(5_000)
            .with_rebalance_timeout_ms(5_000)
            .with_protocols(vec![
                JoinGroupRequestProtocol::default()
                    .with_name(StrBytes::from("range".to_string()))
                    .with_metadata(Bytes::new()),
            ]),
    )
    .await
    .unwrap();
    add_offsets_to_txn(
        &broker,
        "txn-offset-restart",
        "txn-offset-restart-group",
        session.producer_id,
        session.producer_epoch,
    )
    .await;

    let reopened = broker_for_data_dir(&dir.path().join("data"));
    let consumer_offsets_partition =
        crate::store::consumer_offsets_partition_for_group_id("txn-offset-restart-group");
    let restored = reopened
        .transaction_session("txn-offset-restart")
        .expect("transaction session");
    assert!(
        restored
            .partitions
            .contains(&("__consumer_offsets".to_string(), consumer_offsets_partition))
    );

    let response = handle_txn_offset_commit(
        &reopened,
        TxnOffsetCommitRequest::default()
            .with_transactional_id(TransactionalId(StrBytes::from(
                "txn-offset-restart".to_string(),
            )))
            .with_group_id(GroupId(StrBytes::from(
                "txn-offset-restart-group".to_string(),
            )))
            .with_producer_id(session.producer_id)
            .with_producer_epoch(session.producer_epoch)
            .with_generation_id(joined.generation_id)
            .with_member_id(StrBytes::from("member-a".to_string()))
            .with_topics(vec![
                TxnOffsetCommitRequestTopic::default()
                    .with_name(TopicName(StrBytes::from(
                        "txn.offset.restart.topic".to_string(),
                    )))
                    .with_partitions(vec![
                        TxnOffsetCommitRequestPartition::default()
                            .with_partition_index(0)
                            .with_committed_offset(14),
                    ]),
            ]),
    )
    .await
    .unwrap();

    assert_eq!(response.topics[0].partitions[0].error_code, 0);
}

#[tokio::test]
async fn txn_offset_commit_is_discarded_on_abort() {
    let broker = test_broker();
    broker
        .store()
        .ensure_topic("txn.abort.offset.topic", 1, 0)
        .unwrap();
    let session = crate::broker::handlers::bootstrap::handle_init_producer_id(
        &broker,
        InitProducerIdRequest::default().with_transactional_id(Some(TransactionalId(
            StrBytes::from("txn-offset-abort".to_string()),
        ))),
    )
    .await
    .unwrap();
    let joined = crate::broker::handlers::groups::handle_join_group(
        &broker,
        JoinGroupRequest::default()
            .with_group_id(GroupId(StrBytes::from("txn-abort-group".to_string())))
            .with_member_id(StrBytes::from("member-a".to_string()))
            .with_protocol_type(StrBytes::from("consumer".to_string()))
            .with_session_timeout_ms(5_000)
            .with_rebalance_timeout_ms(5_000)
            .with_protocols(vec![
                JoinGroupRequestProtocol::default()
                    .with_name(StrBytes::from("range".to_string()))
                    .with_metadata(Bytes::new()),
            ]),
    )
    .await
    .unwrap();

    add_offsets_to_txn(
        &broker,
        "txn-offset-abort",
        "txn-abort-group",
        session.producer_id,
        session.producer_epoch,
    )
    .await;

    let response = handle_txn_offset_commit(
        &broker,
        TxnOffsetCommitRequest::default()
            .with_transactional_id(TransactionalId(StrBytes::from(
                "txn-offset-abort".to_string(),
            )))
            .with_group_id(GroupId(StrBytes::from("txn-abort-group".to_string())))
            .with_producer_id(session.producer_id)
            .with_producer_epoch(session.producer_epoch)
            .with_generation_id(joined.generation_id)
            .with_member_id(StrBytes::from("member-a".to_string()))
            .with_topics(vec![
                TxnOffsetCommitRequestTopic::default()
                    .with_name(TopicName(StrBytes::from(
                        "txn.abort.offset.topic".to_string(),
                    )))
                    .with_partitions(vec![
                        TxnOffsetCommitRequestPartition::default()
                            .with_partition_index(0)
                            .with_committed_offset(9),
                    ]),
            ]),
    )
    .await
    .unwrap();

    assert_eq!(response.topics[0].partitions[0].error_code, 0);

    let end = handle_end_txn(
        &broker,
        EndTxnRequest::default()
            .with_transactional_id(TransactionalId(StrBytes::from(
                "txn-offset-abort".to_string(),
            )))
            .with_producer_id(session.producer_id)
            .with_producer_epoch(session.producer_epoch)
            .with_committed(false),
        5,
    )
    .await
    .unwrap();
    assert_eq!(end.error_code, 0);

    assert_eq!(
        broker
            .store()
            .fetch_offset("txn-abort-group", "txn.abort.offset.topic", 0)
            .unwrap(),
        None
    );
}

#[tokio::test]
async fn transactional_session_survives_broker_restart_with_transactional_offset_staging() {
    let dir = tempdir().unwrap();
    let broker = broker_for_data_dir(&dir.path().join("data"));
    broker
        .store()
        .ensure_topic("txn.restart.topic", 1, 0)
        .unwrap();
    let session = crate::broker::handlers::bootstrap::handle_init_producer_id(
        &broker,
        InitProducerIdRequest::default().with_transactional_id(Some(TransactionalId(
            StrBytes::from("txn-restart".to_string()),
        ))),
    )
    .await
    .unwrap();
    let joined = crate::broker::handlers::groups::handle_join_group(
        &broker,
        JoinGroupRequest::default()
            .with_group_id(GroupId(StrBytes::from("txn-restart-group".to_string())))
            .with_member_id(StrBytes::from("member-a".to_string()))
            .with_protocol_type(StrBytes::from("consumer".to_string()))
            .with_session_timeout_ms(5_000)
            .with_rebalance_timeout_ms(5_000)
            .with_protocols(vec![
                JoinGroupRequestProtocol::default()
                    .with_name(StrBytes::from("range".to_string()))
                    .with_metadata(Bytes::new()),
            ]),
    )
    .await
    .unwrap();
    handle_add_partitions_to_txn(
        &broker,
        AddPartitionsToTxnRequest::default().with_transactions(vec![
            AddPartitionsToTxnTransaction::default()
                .with_transactional_id(TransactionalId(StrBytes::from("txn-restart".to_string())))
                .with_producer_id(session.producer_id)
                .with_producer_epoch(session.producer_epoch)
                .with_topics(vec![
                    AddPartitionsToTxnTopic::default()
                        .with_name(TopicName(StrBytes::from("txn.restart.topic".to_string())))
                        .with_partitions(vec![0]),
                ]),
        ]),
        5,
    )
    .await
    .unwrap();
    let _ = handle_produce(
        &broker,
        produce_request(
            "txn.restart.topic",
            session.producer_id.0,
            session.producer_epoch,
            0,
        ),
    )
    .await
    .unwrap();
    add_offsets_to_txn(
        &broker,
        "txn-restart",
        "txn-restart-group",
        session.producer_id,
        session.producer_epoch,
    )
    .await;
    handle_txn_offset_commit(
        &broker,
        TxnOffsetCommitRequest::default()
            .with_transactional_id(TransactionalId(StrBytes::from("txn-restart".to_string())))
            .with_group_id(GroupId(StrBytes::from("txn-restart-group".to_string())))
            .with_producer_id(session.producer_id)
            .with_producer_epoch(session.producer_epoch)
            .with_generation_id(joined.generation_id)
            .with_member_id(StrBytes::from("member-a".to_string()))
            .with_topics(vec![
                TxnOffsetCommitRequestTopic::default()
                    .with_name(TopicName(StrBytes::from("txn.restart.topic".to_string())))
                    .with_partitions(vec![
                        TxnOffsetCommitRequestPartition::default()
                            .with_partition_index(0)
                            .with_committed_offset(13),
                    ]),
            ]),
    )
    .await
    .unwrap();

    let reopened = broker_for_data_dir(&dir.path().join("data"));
    let restored = reopened.transaction_session("txn-restart").unwrap();
    assert_eq!(restored.producer_id, session.producer_id.0);
    assert!(
        restored
            .partitions
            .contains(&("txn.restart.topic".to_string(), 0))
    );
    assert_eq!(restored.pending_offset_commits.len(), 1);

    let end = handle_end_txn(
        &reopened,
        EndTxnRequest::default()
            .with_transactional_id(TransactionalId(StrBytes::from("txn-restart".to_string())))
            .with_producer_id(session.producer_id)
            .with_producer_epoch(session.producer_epoch)
            .with_committed(true),
        5,
    )
    .await
    .unwrap();
    assert_eq!(end.error_code, 0);
    assert_eq!(
        reopened
            .store()
            .fetch_offset("txn-restart-group", "txn.restart.topic", 0)
            .unwrap(),
        Some(13)
    );
}

#[tokio::test]
async fn prepared_commit_is_completed_during_broker_restart_recovery() {
    let dir = tempdir().unwrap();
    let broker = broker_for_data_dir(&dir.path().join("data"));
    broker
        .store()
        .ensure_topic("txn.recover.commit.topic", 1, 0)
        .unwrap();
    let session = crate::broker::handlers::bootstrap::handle_init_producer_id(
        &broker,
        InitProducerIdRequest::default().with_transactional_id(Some(TransactionalId(
            StrBytes::from("txn-recover-commit".to_string()),
        ))),
    )
    .await
    .unwrap();
    let joined = crate::broker::handlers::groups::handle_join_group(
        &broker,
        JoinGroupRequest::default()
            .with_group_id(GroupId(StrBytes::from("txn-recover-group".to_string())))
            .with_member_id(StrBytes::from("member-a".to_string()))
            .with_protocol_type(StrBytes::from("consumer".to_string()))
            .with_session_timeout_ms(5_000)
            .with_rebalance_timeout_ms(5_000)
            .with_protocols(vec![
                JoinGroupRequestProtocol::default()
                    .with_name(StrBytes::from("range".to_string()))
                    .with_metadata(Bytes::new()),
            ]),
    )
    .await
    .unwrap();
    handle_add_partitions_to_txn(
        &broker,
        AddPartitionsToTxnRequest::default().with_transactions(vec![
            AddPartitionsToTxnTransaction::default()
                .with_transactional_id(TransactionalId(StrBytes::from(
                    "txn-recover-commit".to_string(),
                )))
                .with_producer_id(session.producer_id)
                .with_producer_epoch(session.producer_epoch)
                .with_topics(vec![
                    AddPartitionsToTxnTopic::default()
                        .with_name(TopicName(StrBytes::from(
                            "txn.recover.commit.topic".to_string(),
                        )))
                        .with_partitions(vec![0]),
                ]),
        ]),
        5,
    )
    .await
    .unwrap();
    let _ = handle_produce(
        &broker,
        transactional_produce_request(
            "txn.recover.commit.topic",
            session.producer_id.0,
            session.producer_epoch,
            0,
        ),
    )
    .await
    .unwrap();
    add_offsets_to_txn(
        &broker,
        "txn-recover-commit",
        "txn-recover-group",
        session.producer_id,
        session.producer_epoch,
    )
    .await;
    handle_txn_offset_commit(
        &broker,
        TxnOffsetCommitRequest::default()
            .with_transactional_id(TransactionalId(StrBytes::from(
                "txn-recover-commit".to_string(),
            )))
            .with_group_id(GroupId(StrBytes::from("txn-recover-group".to_string())))
            .with_producer_id(session.producer_id)
            .with_producer_epoch(session.producer_epoch)
            .with_generation_id(joined.generation_id)
            .with_member_id(StrBytes::from("member-a".to_string()))
            .with_topics(vec![
                TxnOffsetCommitRequestTopic::default()
                    .with_name(TopicName(StrBytes::from(
                        "txn.recover.commit.topic".to_string(),
                    )))
                    .with_partitions(vec![
                        TxnOffsetCommitRequestPartition::default()
                            .with_partition_index(0)
                            .with_committed_offset(21),
                    ]),
            ]),
    )
    .await
    .unwrap();
    broker
        .set_transaction_status(
            "txn-recover-commit",
            TransactionStatus::PrepareCommit,
            chrono::Utc::now().timestamp_millis(),
        )
        .unwrap();

    let reopened = broker_for_data_dir(&dir.path().join("data"));
    let restored = reopened
        .transaction_session("txn-recover-commit")
        .expect("restored transaction");
    assert_eq!(restored.status, TransactionStatus::Empty);
    assert!(restored.partitions.is_empty());
    assert!(restored.pending_offset_commits.is_empty());
    assert_eq!(
        reopened
            .store()
            .fetch_offset("txn-recover-group", "txn.recover.commit.topic", 0)
            .unwrap(),
        Some(21)
    );
}

#[tokio::test]
async fn end_txn_commit_retries_from_prepare_commit_state() {
    let dir = tempdir().unwrap();
    let broker = broker_for_data_dir(&dir.path().join("data"));
    broker
        .store()
        .ensure_topic("txn.retry.commit.topic", 1, 0)
        .unwrap();
    let session = crate::broker::handlers::bootstrap::handle_init_producer_id(
        &broker,
        InitProducerIdRequest::default().with_transactional_id(Some(TransactionalId(
            StrBytes::from("txn-retry-commit".to_string()),
        ))),
    )
    .await
    .unwrap();
    let joined = crate::broker::handlers::groups::handle_join_group(
        &broker,
        JoinGroupRequest::default()
            .with_group_id(GroupId(StrBytes::from("txn-retry-group".to_string())))
            .with_member_id(StrBytes::from("member-a".to_string()))
            .with_protocol_type(StrBytes::from("consumer".to_string()))
            .with_session_timeout_ms(5_000)
            .with_rebalance_timeout_ms(5_000)
            .with_protocols(vec![
                JoinGroupRequestProtocol::default()
                    .with_name(StrBytes::from("range".to_string()))
                    .with_metadata(Bytes::new()),
            ]),
    )
    .await
    .unwrap();
    handle_add_partitions_to_txn(
        &broker,
        AddPartitionsToTxnRequest::default().with_transactions(vec![
            AddPartitionsToTxnTransaction::default()
                .with_transactional_id(TransactionalId(StrBytes::from(
                    "txn-retry-commit".to_string(),
                )))
                .with_producer_id(session.producer_id)
                .with_producer_epoch(session.producer_epoch)
                .with_topics(vec![
                    AddPartitionsToTxnTopic::default()
                        .with_name(TopicName(StrBytes::from(
                            "txn.retry.commit.topic".to_string(),
                        )))
                        .with_partitions(vec![0]),
                ]),
        ]),
        5,
    )
    .await
    .unwrap();
    let _ = handle_produce(
        &broker,
        transactional_produce_request(
            "txn.retry.commit.topic",
            session.producer_id.0,
            session.producer_epoch,
            0,
        ),
    )
    .await
    .unwrap();
    add_offsets_to_txn(
        &broker,
        "txn-retry-commit",
        "txn-retry-group",
        session.producer_id,
        session.producer_epoch,
    )
    .await;
    handle_txn_offset_commit(
        &broker,
        TxnOffsetCommitRequest::default()
            .with_transactional_id(TransactionalId(StrBytes::from(
                "txn-retry-commit".to_string(),
            )))
            .with_group_id(GroupId(StrBytes::from("txn-retry-group".to_string())))
            .with_producer_id(session.producer_id)
            .with_producer_epoch(session.producer_epoch)
            .with_generation_id(joined.generation_id)
            .with_member_id(StrBytes::from("member-a".to_string()))
            .with_topics(vec![
                TxnOffsetCommitRequestTopic::default()
                    .with_name(TopicName(StrBytes::from(
                        "txn.retry.commit.topic".to_string(),
                    )))
                    .with_partitions(vec![
                        TxnOffsetCommitRequestPartition::default()
                            .with_partition_index(0)
                            .with_committed_offset(31),
                    ]),
            ]),
    )
    .await
    .unwrap();
    broker
        .set_transaction_status(
            "txn-retry-commit",
            TransactionStatus::PrepareCommit,
            chrono::Utc::now().timestamp_millis(),
        )
        .unwrap();

    let end = handle_end_txn(
        &broker,
        EndTxnRequest::default()
            .with_transactional_id(TransactionalId(StrBytes::from(
                "txn-retry-commit".to_string(),
            )))
            .with_producer_id(session.producer_id)
            .with_producer_epoch(session.producer_epoch)
            .with_committed(true),
        5,
    )
    .await
    .unwrap();

    assert_eq!(end.error_code, 0);
    let restored = broker
        .transaction_session("txn-retry-commit")
        .expect("transaction session");
    assert_eq!(restored.status, TransactionStatus::Empty);
    assert_eq!(
        broker
            .store()
            .fetch_offset("txn-retry-group", "txn.retry.commit.topic", 0)
            .unwrap(),
        Some(31)
    );
}

#[tokio::test]
async fn end_txn_abort_retries_from_prepare_abort_state() {
    let broker = test_broker();
    broker
        .store()
        .ensure_topic("txn.retry.abort.topic", 1, 0)
        .unwrap();
    let session = crate::broker::handlers::bootstrap::handle_init_producer_id(
        &broker,
        InitProducerIdRequest::default().with_transactional_id(Some(TransactionalId(
            StrBytes::from("txn-retry-abort".to_string()),
        ))),
    )
    .await
    .unwrap();
    handle_add_partitions_to_txn(
        &broker,
        AddPartitionsToTxnRequest::default().with_transactions(vec![
            AddPartitionsToTxnTransaction::default()
                .with_transactional_id(TransactionalId(StrBytes::from(
                    "txn-retry-abort".to_string(),
                )))
                .with_producer_id(session.producer_id)
                .with_producer_epoch(session.producer_epoch)
                .with_topics(vec![
                    AddPartitionsToTxnTopic::default()
                        .with_name(TopicName(StrBytes::from(
                            "txn.retry.abort.topic".to_string(),
                        )))
                        .with_partitions(vec![0]),
                ]),
        ]),
        5,
    )
    .await
    .unwrap();
    let _ = handle_produce(
        &broker,
        transactional_produce_request(
            "txn.retry.abort.topic",
            session.producer_id.0,
            session.producer_epoch,
            0,
        ),
    )
    .await
    .unwrap();
    broker
        .set_transaction_status(
            "txn-retry-abort",
            TransactionStatus::PrepareAbort,
            chrono::Utc::now().timestamp_millis(),
        )
        .unwrap();

    let end = handle_end_txn(
        &broker,
        EndTxnRequest::default()
            .with_transactional_id(TransactionalId(StrBytes::from(
                "txn-retry-abort".to_string(),
            )))
            .with_producer_id(session.producer_id)
            .with_producer_epoch(session.producer_epoch)
            .with_committed(false),
        5,
    )
    .await
    .unwrap();

    assert_eq!(end.error_code, 0);
    let restored = broker
        .transaction_session("txn-retry-abort")
        .expect("transaction session");
    assert_eq!(restored.status, TransactionStatus::Empty);

    let fetch = handle_fetch(
        &broker,
        kafka_protocol::messages::FetchRequest::default()
            .with_isolation_level(1)
            .with_topics(vec![
                kafka_protocol::messages::fetch_request::FetchTopic::default()
                    .with_topic(TopicName(StrBytes::from(
                        "txn.retry.abort.topic".to_string(),
                    )))
                    .with_partitions(vec![
                        kafka_protocol::messages::fetch_request::FetchPartition::default()
                            .with_partition(0)
                            .with_fetch_offset(0)
                            .with_partition_max_bytes(4096),
                    ]),
            ]),
    )
    .await
    .unwrap();
    let partition = &fetch.responses[0].partitions[0];
    assert!(partition.records.as_ref().expect("records").is_empty());
    assert_eq!(
        partition
            .aborted_transactions
            .as_ref()
            .expect("aborted transactions")
            .len(),
        1
    );
}

#[tokio::test]
async fn end_txn_commit_succeeds_after_membership_changes_once_offset_is_staged() {
    let broker = test_broker();
    broker
        .store()
        .ensure_topic("txn.invalidated.commit.topic", 1, 0)
        .unwrap();
    let session = crate::broker::handlers::bootstrap::handle_init_producer_id(
        &broker,
        InitProducerIdRequest::default().with_transactional_id(Some(TransactionalId(
            StrBytes::from("txn-invalidated-commit".to_string()),
        ))),
    )
    .await
    .unwrap();
    let joined = crate::broker::handlers::groups::handle_join_group(
        &broker,
        JoinGroupRequest::default()
            .with_group_id(GroupId(StrBytes::from(
                "txn-invalidated-commit-group".to_string(),
            )))
            .with_member_id(StrBytes::from("member-a".to_string()))
            .with_protocol_type(StrBytes::from("consumer".to_string()))
            .with_session_timeout_ms(5_000)
            .with_rebalance_timeout_ms(5_000)
            .with_protocols(vec![
                JoinGroupRequestProtocol::default()
                    .with_name(StrBytes::from("range".to_string()))
                    .with_metadata(Bytes::new()),
            ]),
    )
    .await
    .unwrap();
    handle_add_partitions_to_txn(
        &broker,
        AddPartitionsToTxnRequest::default().with_transactions(vec![
            AddPartitionsToTxnTransaction::default()
                .with_transactional_id(TransactionalId(StrBytes::from(
                    "txn-invalidated-commit".to_string(),
                )))
                .with_producer_id(session.producer_id)
                .with_producer_epoch(session.producer_epoch)
                .with_topics(vec![
                    AddPartitionsToTxnTopic::default()
                        .with_name(TopicName(StrBytes::from(
                            "txn.invalidated.commit.topic".to_string(),
                        )))
                        .with_partitions(vec![0]),
                ]),
        ]),
        5,
    )
    .await
    .unwrap();
    let _ = handle_produce(
        &broker,
        transactional_produce_request(
            "txn.invalidated.commit.topic",
            session.producer_id.0,
            session.producer_epoch,
            0,
        ),
    )
    .await
    .unwrap();
    add_offsets_to_txn(
        &broker,
        "txn-invalidated-commit",
        "txn-invalidated-commit-group",
        session.producer_id,
        session.producer_epoch,
    )
    .await;
    handle_txn_offset_commit(
        &broker,
        TxnOffsetCommitRequest::default()
            .with_transactional_id(TransactionalId(StrBytes::from(
                "txn-invalidated-commit".to_string(),
            )))
            .with_group_id(GroupId(StrBytes::from(
                "txn-invalidated-commit-group".to_string(),
            )))
            .with_producer_id(session.producer_id)
            .with_producer_epoch(session.producer_epoch)
            .with_generation_id(joined.generation_id)
            .with_member_id(StrBytes::from("member-a".to_string()))
            .with_topics(vec![
                TxnOffsetCommitRequestTopic::default()
                    .with_name(TopicName(StrBytes::from(
                        "txn.invalidated.commit.topic".to_string(),
                    )))
                    .with_partitions(vec![
                        TxnOffsetCommitRequestPartition::default()
                            .with_partition_index(0)
                            .with_committed_offset(8),
                    ]),
            ]),
    )
    .await
    .unwrap();
    let _ = crate::broker::handlers::groups::handle_leave_group(
        &broker,
        kafka_protocol::messages::LeaveGroupRequest::default()
            .with_group_id(GroupId(StrBytes::from(
                "txn-invalidated-commit-group".to_string(),
            )))
            .with_member_id(StrBytes::from("member-a".to_string())),
    )
    .await;

    let end = handle_end_txn(
        &broker,
        EndTxnRequest::default()
            .with_transactional_id(TransactionalId(StrBytes::from(
                "txn-invalidated-commit".to_string(),
            )))
            .with_producer_id(session.producer_id)
            .with_producer_epoch(session.producer_epoch)
            .with_committed(true),
        5,
    )
    .await
    .unwrap();

    assert_eq!(end.error_code, 0);
    let restored = broker
        .transaction_session("txn-invalidated-commit")
        .expect("transaction session");
    assert_eq!(restored.status, TransactionStatus::Empty);
    assert_eq!(
        broker
            .store()
            .fetch_offset(
                "txn-invalidated-commit-group",
                "txn.invalidated.commit.topic",
                0
            )
            .unwrap(),
        Some(8)
    );
}

#[tokio::test]
async fn prepared_abort_is_completed_during_broker_restart_recovery() {
    let dir = tempdir().unwrap();
    let broker = broker_for_data_dir(&dir.path().join("data"));
    broker
        .store()
        .ensure_topic("txn.recover.abort.topic", 1, 0)
        .unwrap();
    let session = crate::broker::handlers::bootstrap::handle_init_producer_id(
        &broker,
        InitProducerIdRequest::default().with_transactional_id(Some(TransactionalId(
            StrBytes::from("txn-recover-abort".to_string()),
        ))),
    )
    .await
    .unwrap();
    handle_add_partitions_to_txn(
        &broker,
        AddPartitionsToTxnRequest::default().with_transactions(vec![
            AddPartitionsToTxnTransaction::default()
                .with_transactional_id(TransactionalId(StrBytes::from(
                    "txn-recover-abort".to_string(),
                )))
                .with_producer_id(session.producer_id)
                .with_producer_epoch(session.producer_epoch)
                .with_topics(vec![
                    AddPartitionsToTxnTopic::default()
                        .with_name(TopicName(StrBytes::from(
                            "txn.recover.abort.topic".to_string(),
                        )))
                        .with_partitions(vec![0]),
                ]),
        ]),
        5,
    )
    .await
    .unwrap();
    let _ = handle_produce(
        &broker,
        transactional_produce_request(
            "txn.recover.abort.topic",
            session.producer_id.0,
            session.producer_epoch,
            0,
        ),
    )
    .await
    .unwrap();
    broker
        .set_transaction_status(
            "txn-recover-abort",
            TransactionStatus::PrepareAbort,
            chrono::Utc::now().timestamp_millis(),
        )
        .unwrap();

    let reopened = broker_for_data_dir(&dir.path().join("data"));
    let restored = reopened
        .transaction_session("txn-recover-abort")
        .expect("restored transaction");
    assert_eq!(restored.status, TransactionStatus::Empty);
    assert!(restored.partitions.is_empty());
    assert!(restored.pending_offset_commits.is_empty());

    let fetch = handle_fetch(
        &reopened,
        kafka_protocol::messages::FetchRequest::default()
            .with_isolation_level(1)
            .with_topics(vec![
                kafka_protocol::messages::fetch_request::FetchTopic::default()
                    .with_topic(TopicName(StrBytes::from(
                        "txn.recover.abort.topic".to_string(),
                    )))
                    .with_partitions(vec![
                        kafka_protocol::messages::fetch_request::FetchPartition::default()
                            .with_partition(0)
                            .with_fetch_offset(0)
                            .with_partition_max_bytes(4096),
                    ]),
            ]),
    )
    .await
    .unwrap();
    let partition = &fetch.responses[0].partitions[0];
    assert!(partition.records.as_ref().expect("records").is_empty());
    assert_eq!(
        partition
            .aborted_transactions
            .as_ref()
            .expect("aborted transactions")
            .len(),
        1
    );
}

#[tokio::test]
async fn prepared_commit_recovery_preserves_state_when_marker_write_fails() {
    let dir = tempdir().unwrap();
    let data_dir = dir.path().join("data");
    let broker = broker_for_data_dir(&data_dir);
    broker
        .store()
        .ensure_topic("txn.recover.failure.marker.topic", 1, 0)
        .unwrap();
    let session = crate::broker::handlers::bootstrap::handle_init_producer_id(
        &broker,
        InitProducerIdRequest::default().with_transactional_id(Some(TransactionalId(
            StrBytes::from("txn-recover-failure-marker".to_string()),
        ))),
    )
    .await
    .unwrap();
    let joined = crate::broker::handlers::groups::handle_join_group(
        &broker,
        JoinGroupRequest::default()
            .with_group_id(GroupId(StrBytes::from(
                "txn-recover-failure-marker-group".to_string(),
            )))
            .with_member_id(StrBytes::from("member-a".to_string()))
            .with_protocol_type(StrBytes::from("consumer".to_string()))
            .with_session_timeout_ms(5_000)
            .with_rebalance_timeout_ms(5_000)
            .with_protocols(vec![
                JoinGroupRequestProtocol::default()
                    .with_name(StrBytes::from("range".to_string()))
                    .with_metadata(Bytes::new()),
            ]),
    )
    .await
    .unwrap();
    handle_add_partitions_to_txn(
        &broker,
        AddPartitionsToTxnRequest::default().with_transactions(vec![
            AddPartitionsToTxnTransaction::default()
                .with_transactional_id(TransactionalId(StrBytes::from(
                    "txn-recover-failure-marker".to_string(),
                )))
                .with_producer_id(session.producer_id)
                .with_producer_epoch(session.producer_epoch)
                .with_topics(vec![
                    AddPartitionsToTxnTopic::default()
                        .with_name(TopicName(StrBytes::from(
                            "txn.recover.failure.marker.topic".to_string(),
                        )))
                        .with_partitions(vec![0]),
                ]),
        ]),
        5,
    )
    .await
    .unwrap();
    let _ = handle_produce(
        &broker,
        transactional_produce_request(
            "txn.recover.failure.marker.topic",
            session.producer_id.0,
            session.producer_epoch,
            0,
        ),
    )
    .await
    .unwrap();
    add_offsets_to_txn(
        &broker,
        "txn-recover-failure-marker",
        "txn-recover-failure-marker-group",
        session.producer_id,
        session.producer_epoch,
    )
    .await;
    handle_txn_offset_commit(
        &broker,
        TxnOffsetCommitRequest::default()
            .with_transactional_id(TransactionalId(StrBytes::from(
                "txn-recover-failure-marker".to_string(),
            )))
            .with_group_id(GroupId(StrBytes::from(
                "txn-recover-failure-marker-group".to_string(),
            )))
            .with_producer_id(session.producer_id)
            .with_producer_epoch(session.producer_epoch)
            .with_generation_id(joined.generation_id)
            .with_member_id(StrBytes::from("member-a".to_string()))
            .with_topics(vec![
                TxnOffsetCommitRequestTopic::default()
                    .with_name(TopicName(StrBytes::from(
                        "txn.recover.failure.marker.topic".to_string(),
                    )))
                    .with_partitions(vec![
                        TxnOffsetCommitRequestPartition::default()
                            .with_partition_index(0)
                            .with_committed_offset(44),
                    ]),
            ]),
    )
    .await
    .unwrap();
    broker
        .set_transaction_status(
            "txn-recover-failure-marker",
            TransactionStatus::PrepareCommit,
            chrono::Utc::now().timestamp_millis(),
        )
        .unwrap();
    drop(broker);

    let raw_store = Arc::new(FileStore::open(&data_dir).unwrap());
    let failing_store = Arc::new(FailingStorage::new(raw_store.clone(), 0, 1, 0));
    let broker = KafkaBroker::new(
        Config::single_node(data_dir.clone(), 9092, 1),
        failing_store as Arc<dyn Storage>,
    );
    assert!(
        broker.is_err(),
        "recovery should surface marker write failure"
    );
    let err = broker.err().unwrap();
    assert!(
        err.to_string().contains(&UNKNOWN_PRODUCER_ID.to_string())
            || err.to_string().contains("unknown producer id"),
        "unexpected error: {err:?}"
    );

    let persisted = raw_store.transaction_sessions().unwrap();
    let restored = persisted
        .get("txn-recover-failure-marker")
        .expect("persisted transaction");
    assert_eq!(restored.status, TransactionStatus::PrepareCommit);
    assert_eq!(
        raw_store
            .transactional_offset_commits(session.producer_id.0)
            .unwrap()
            .len(),
        1
    );

    let reopened = broker_for_data_dir(&data_dir);
    let restored = reopened
        .transaction_session("txn-recover-failure-marker")
        .expect("restored transaction");
    assert_eq!(restored.status, TransactionStatus::Empty);
    assert!(restored.pending_offset_commits.is_empty());
    assert_eq!(
        reopened
            .store()
            .fetch_offset(
                "txn-recover-failure-marker-group",
                "txn.recover.failure.marker.topic",
                0
            )
            .unwrap(),
        Some(44)
    );
}

#[tokio::test]
async fn prepared_commit_recovery_preserves_state_when_offset_completion_fails() {
    let dir = tempdir().unwrap();
    let data_dir = dir.path().join("data");
    let broker = broker_for_data_dir(&data_dir);
    broker
        .store()
        .ensure_topic("txn.recover.failure.offset.topic", 1, 0)
        .unwrap();
    let session = crate::broker::handlers::bootstrap::handle_init_producer_id(
        &broker,
        InitProducerIdRequest::default().with_transactional_id(Some(TransactionalId(
            StrBytes::from("txn-recover-failure-offset".to_string()),
        ))),
    )
    .await
    .unwrap();
    let joined = crate::broker::handlers::groups::handle_join_group(
        &broker,
        JoinGroupRequest::default()
            .with_group_id(GroupId(StrBytes::from(
                "txn-recover-failure-offset-group".to_string(),
            )))
            .with_member_id(StrBytes::from("member-a".to_string()))
            .with_protocol_type(StrBytes::from("consumer".to_string()))
            .with_session_timeout_ms(5_000)
            .with_rebalance_timeout_ms(5_000)
            .with_protocols(vec![
                JoinGroupRequestProtocol::default()
                    .with_name(StrBytes::from("range".to_string()))
                    .with_metadata(Bytes::new()),
            ]),
    )
    .await
    .unwrap();
    handle_add_partitions_to_txn(
        &broker,
        AddPartitionsToTxnRequest::default().with_transactions(vec![
            AddPartitionsToTxnTransaction::default()
                .with_transactional_id(TransactionalId(StrBytes::from(
                    "txn-recover-failure-offset".to_string(),
                )))
                .with_producer_id(session.producer_id)
                .with_producer_epoch(session.producer_epoch)
                .with_topics(vec![
                    AddPartitionsToTxnTopic::default()
                        .with_name(TopicName(StrBytes::from(
                            "txn.recover.failure.offset.topic".to_string(),
                        )))
                        .with_partitions(vec![0]),
                ]),
        ]),
        5,
    )
    .await
    .unwrap();
    let _ = handle_produce(
        &broker,
        transactional_produce_request(
            "txn.recover.failure.offset.topic",
            session.producer_id.0,
            session.producer_epoch,
            0,
        ),
    )
    .await
    .unwrap();
    add_offsets_to_txn(
        &broker,
        "txn-recover-failure-offset",
        "txn-recover-failure-offset-group",
        session.producer_id,
        session.producer_epoch,
    )
    .await;
    handle_txn_offset_commit(
        &broker,
        TxnOffsetCommitRequest::default()
            .with_transactional_id(TransactionalId(StrBytes::from(
                "txn-recover-failure-offset".to_string(),
            )))
            .with_group_id(GroupId(StrBytes::from(
                "txn-recover-failure-offset-group".to_string(),
            )))
            .with_producer_id(session.producer_id)
            .with_producer_epoch(session.producer_epoch)
            .with_generation_id(joined.generation_id)
            .with_member_id(StrBytes::from("member-a".to_string()))
            .with_topics(vec![
                TxnOffsetCommitRequestTopic::default()
                    .with_name(TopicName(StrBytes::from(
                        "txn.recover.failure.offset.topic".to_string(),
                    )))
                    .with_partitions(vec![
                        TxnOffsetCommitRequestPartition::default()
                            .with_partition_index(0)
                            .with_committed_offset(45),
                    ]),
            ]),
    )
    .await
    .unwrap();
    broker
        .set_transaction_status(
            "txn-recover-failure-offset",
            TransactionStatus::PrepareCommit,
            chrono::Utc::now().timestamp_millis(),
        )
        .unwrap();
    drop(broker);

    let raw_store = Arc::new(FileStore::open(&data_dir).unwrap());
    let failing_store = Arc::new(FailingStorage::new(raw_store.clone(), 0, 0, 1));
    let broker = KafkaBroker::new(
        Config::single_node(data_dir.clone(), 9092, 1),
        failing_store as Arc<dyn Storage>,
    );
    assert!(
        broker.is_err(),
        "recovery should surface offset completion failure"
    );
    let err = broker.err().unwrap();
    assert!(
        err.to_string().contains("stale producer epoch"),
        "unexpected error: {err:?}"
    );

    let persisted = raw_store.transaction_sessions().unwrap();
    let restored = persisted
        .get("txn-recover-failure-offset")
        .expect("persisted transaction");
    assert_eq!(restored.status, TransactionStatus::PrepareCommit);
    assert_eq!(
        raw_store
            .transactional_offset_commits(session.producer_id.0)
            .unwrap()
            .len(),
        1
    );

    let reopened = broker_for_data_dir(&data_dir);
    let restored = reopened
        .transaction_session("txn-recover-failure-offset")
        .expect("restored transaction");
    assert_eq!(restored.status, TransactionStatus::Empty);
    assert!(restored.pending_offset_commits.is_empty());
    assert_eq!(
        reopened
            .store()
            .fetch_offset(
                "txn-recover-failure-offset-group",
                "txn.recover.failure.offset.topic",
                0
            )
            .unwrap(),
        Some(45)
    );
    assert_eq!(
        reopened
            .store()
            .fetch_records("txn.recover.failure.offset.topic", 0, 0, 10)
            .unwrap()
            .records
            .len(),
        2
    );
}

#[tokio::test]
async fn completed_commit_is_cleaned_up_during_broker_restart_recovery() {
    let dir = tempdir().unwrap();
    let broker = broker_for_data_dir(&dir.path().join("data"));
    broker
        .store()
        .ensure_topic("txn.recover.complete.commit.topic", 1, 0)
        .unwrap();
    let session = crate::broker::handlers::bootstrap::handle_init_producer_id(
        &broker,
        InitProducerIdRequest::default().with_transactional_id(Some(TransactionalId(
            StrBytes::from("txn-complete-commit".to_string()),
        ))),
    )
    .await
    .unwrap();
    handle_add_partitions_to_txn(
        &broker,
        AddPartitionsToTxnRequest::default().with_transactions(vec![
            AddPartitionsToTxnTransaction::default()
                .with_transactional_id(TransactionalId(StrBytes::from(
                    "txn-complete-commit".to_string(),
                )))
                .with_producer_id(session.producer_id)
                .with_producer_epoch(session.producer_epoch)
                .with_topics(vec![
                    AddPartitionsToTxnTopic::default()
                        .with_name(TopicName(StrBytes::from(
                            "txn.recover.complete.commit.topic".to_string(),
                        )))
                        .with_partitions(vec![0]),
                ]),
        ]),
        5,
    )
    .await
    .unwrap();
    broker
        .finalize_transaction_metadata(
            "txn-complete-commit",
            TransactionStatus::CompleteCommit,
            chrono::Utc::now().timestamp_millis(),
        )
        .unwrap();

    let reopened = broker_for_data_dir(&dir.path().join("data"));
    let restored = reopened
        .transaction_session("txn-complete-commit")
        .expect("restored transaction");
    assert_eq!(restored.status, TransactionStatus::Empty);
    assert!(restored.partitions.is_empty());
    assert!(restored.pending_offset_commits.is_empty());
}

#[tokio::test]
async fn completed_abort_is_cleaned_up_during_broker_restart_recovery() {
    let dir = tempdir().unwrap();
    let broker = broker_for_data_dir(&dir.path().join("data"));
    broker
        .store()
        .ensure_topic("txn.recover.complete.abort.topic", 1, 0)
        .unwrap();
    let session = crate::broker::handlers::bootstrap::handle_init_producer_id(
        &broker,
        InitProducerIdRequest::default().with_transactional_id(Some(TransactionalId(
            StrBytes::from("txn-complete-abort".to_string()),
        ))),
    )
    .await
    .unwrap();
    handle_add_partitions_to_txn(
        &broker,
        AddPartitionsToTxnRequest::default().with_transactions(vec![
            AddPartitionsToTxnTransaction::default()
                .with_transactional_id(TransactionalId(StrBytes::from(
                    "txn-complete-abort".to_string(),
                )))
                .with_producer_id(session.producer_id)
                .with_producer_epoch(session.producer_epoch)
                .with_topics(vec![
                    AddPartitionsToTxnTopic::default()
                        .with_name(TopicName(StrBytes::from(
                            "txn.recover.complete.abort.topic".to_string(),
                        )))
                        .with_partitions(vec![0]),
                ]),
        ]),
        5,
    )
    .await
    .unwrap();
    broker
        .finalize_transaction_metadata(
            "txn-complete-abort",
            TransactionStatus::CompleteAbort,
            chrono::Utc::now().timestamp_millis(),
        )
        .unwrap();

    let reopened = broker_for_data_dir(&dir.path().join("data"));
    let restored = reopened
        .transaction_session("txn-complete-abort")
        .expect("restored transaction");
    assert_eq!(restored.status, TransactionStatus::Empty);
    assert!(restored.partitions.is_empty());
    assert!(restored.pending_offset_commits.is_empty());
}

#[tokio::test]
async fn add_partitions_with_unknown_transaction_maps_to_invalid_producer_id_mapping() {
    let broker = test_broker();
    broker
        .store()
        .ensure_topic("txn.missing.topic", 1, 0)
        .unwrap();

    let response = handle_add_partitions_to_txn(
        &broker,
        AddPartitionsToTxnRequest::default().with_transactions(vec![
            AddPartitionsToTxnTransaction::default()
                .with_transactional_id(TransactionalId(StrBytes::from("missing-txn".to_string())))
                .with_producer_id(ProducerId(42))
                .with_producer_epoch(1)
                .with_topics(vec![
                    AddPartitionsToTxnTopic::default()
                        .with_name(TopicName(StrBytes::from("txn.missing.topic".to_string())))
                        .with_partitions(vec![0]),
                ]),
        ]),
        5,
    )
    .await
    .unwrap();

    assert_eq!(response.error_code, 0);
    assert_eq!(
        response.results_by_transaction[0].topic_results[0].results_by_partition[0]
            .partition_error_code,
        INVALID_PRODUCER_ID_MAPPING
    );
}

#[tokio::test]
async fn end_txn_with_stale_epoch_maps_to_invalid_producer_epoch() {
    let broker = test_broker();
    let session = crate::broker::handlers::bootstrap::handle_init_producer_id(
        &broker,
        InitProducerIdRequest::default().with_transactional_id(Some(TransactionalId(
            StrBytes::from("txn-stale".to_string()),
        ))),
    )
    .await
    .unwrap();

    let response = handle_end_txn(
        &broker,
        EndTxnRequest::default()
            .with_transactional_id(TransactionalId(StrBytes::from("txn-stale".to_string())))
            .with_producer_id(session.producer_id)
            .with_producer_epoch(session.producer_epoch + 1)
            .with_committed(true),
        5,
    )
    .await
    .unwrap();

    assert_eq!(response.error_code, INVALID_PRODUCER_EPOCH);
}

#[tokio::test]
async fn init_producer_id_rejects_reinit_during_ongoing_transaction_without_fencing_active_producer()
 {
    let broker = test_broker();
    broker
        .store()
        .ensure_topic("txn.reinit.topic", 1, 0)
        .unwrap();
    let first = crate::broker::handlers::bootstrap::handle_init_producer_id(
        &broker,
        InitProducerIdRequest::default().with_transactional_id(Some(TransactionalId(
            StrBytes::from("txn-reinit".to_string()),
        ))),
    )
    .await
    .unwrap();
    handle_add_partitions_to_txn(
        &broker,
        AddPartitionsToTxnRequest::default().with_transactions(vec![
            AddPartitionsToTxnTransaction::default()
                .with_transactional_id(TransactionalId(StrBytes::from("txn-reinit".to_string())))
                .with_producer_id(first.producer_id)
                .with_producer_epoch(first.producer_epoch)
                .with_topics(vec![
                    AddPartitionsToTxnTopic::default()
                        .with_name(TopicName(StrBytes::from("txn.reinit.topic".to_string())))
                        .with_partitions(vec![0]),
                ]),
        ]),
        5,
    )
    .await
    .unwrap();

    let second = crate::broker::handlers::bootstrap::handle_init_producer_id(
        &broker,
        InitProducerIdRequest::default().with_transactional_id(Some(TransactionalId(
            StrBytes::from("txn-reinit".to_string()),
        ))),
    )
    .await
    .unwrap();

    assert_eq!(second.error_code, CONCURRENT_TRANSACTIONS);
    assert_eq!(second.producer_id.0, -1);
    assert_eq!(second.producer_epoch, -1);
    let session = broker.transaction_session("txn-reinit").unwrap();
    assert_eq!(session.producer_id, first.producer_id.0);
    assert_eq!(session.producer_epoch, first.producer_epoch);
    assert!(session.transaction_start_timestamp_ms >= 0);

    let end = handle_end_txn(
        &broker,
        EndTxnRequest::default()
            .with_transactional_id(TransactionalId(StrBytes::from("txn-reinit".to_string())))
            .with_producer_id(first.producer_id)
            .with_producer_epoch(first.producer_epoch)
            .with_committed(true),
        5,
    )
    .await
    .unwrap();
    assert_eq!(end.error_code, 0);
}

#[tokio::test]
async fn rejected_reinit_does_not_fence_transactional_producer_after_restart() {
    let dir = tempdir().unwrap();
    let broker = broker_for_data_dir(&dir.path().join("data"));
    broker
        .store()
        .ensure_topic("txn.reinit.restart.topic", 1, 0)
        .unwrap();
    let first = crate::broker::handlers::bootstrap::handle_init_producer_id(
        &broker,
        InitProducerIdRequest::default().with_transactional_id(Some(TransactionalId(
            StrBytes::from("txn-reinit-restart".to_string()),
        ))),
    )
    .await
    .unwrap();
    handle_add_partitions_to_txn(
        &broker,
        AddPartitionsToTxnRequest::default().with_transactions(vec![
            AddPartitionsToTxnTransaction::default()
                .with_transactional_id(TransactionalId(StrBytes::from(
                    "txn-reinit-restart".to_string(),
                )))
                .with_producer_id(first.producer_id)
                .with_producer_epoch(first.producer_epoch)
                .with_topics(vec![
                    AddPartitionsToTxnTopic::default()
                        .with_name(TopicName(StrBytes::from(
                            "txn.reinit.restart.topic".to_string(),
                        )))
                        .with_partitions(vec![0]),
                ]),
        ]),
        5,
    )
    .await
    .unwrap();
    let _ = handle_produce(
        &broker,
        transactional_produce_request(
            "txn.reinit.restart.topic",
            first.producer_id.0,
            first.producer_epoch,
            0,
        ),
    )
    .await
    .unwrap();
    let second = crate::broker::handlers::bootstrap::handle_init_producer_id(
        &broker,
        InitProducerIdRequest::default().with_transactional_id(Some(TransactionalId(
            StrBytes::from("txn-reinit-restart".to_string()),
        ))),
    )
    .await
    .unwrap();
    assert_eq!(second.error_code, CONCURRENT_TRANSACTIONS);

    let reopened = broker_for_data_dir(&dir.path().join("data"));
    let restored = reopened
        .transaction_session("txn-reinit-restart")
        .expect("transaction session");
    assert_eq!(restored.producer_id, first.producer_id.0);
    assert_eq!(restored.producer_epoch, first.producer_epoch);

    let end = handle_end_txn(
        &reopened,
        EndTxnRequest::default()
            .with_transactional_id(TransactionalId(StrBytes::from(
                "txn-reinit-restart".to_string(),
            )))
            .with_producer_id(first.producer_id)
            .with_producer_epoch(first.producer_epoch)
            .with_committed(true),
        5,
    )
    .await
    .unwrap();
    assert_eq!(end.error_code, 0);
}

#[test]
fn transaction_start_timestamp_is_set_on_first_activity_and_cleared_on_reset() {
    let broker = test_broker();
    broker
        .bind_transactional_producer("txn-start", 77, 0, 60_000, 100)
        .unwrap();

    let initial = broker.transaction_session("txn-start").unwrap();
    assert_eq!(initial.transaction_start_timestamp_ms, -1);

    broker
        .add_transaction_partitions("txn-start", [("topic-a".to_string(), 0)], 200)
        .unwrap();
    let after_add = broker.transaction_session("txn-start").unwrap();
    assert_eq!(after_add.transaction_start_timestamp_ms, 200);

    broker
        .stage_transaction_offset_commit(
            "txn-start",
            crate::broker::server::StagedOffsetCommit {
                group_id: "group-a".to_string(),
                topic: "topic-a".to_string(),
                partition: 0,
                next_offset: 9,
            },
            300,
        )
        .unwrap();
    let after_commit = broker.transaction_session("txn-start").unwrap();
    assert_eq!(after_commit.transaction_start_timestamp_ms, 200);

    broker
        .finalize_transaction_metadata("txn-start", TransactionStatus::CompleteCommit, 400)
        .unwrap();
    broker
        .set_transaction_status("txn-start", TransactionStatus::Empty, 401)
        .unwrap();
    let after_reset = broker.transaction_session("txn-start").unwrap();
    assert_eq!(after_reset.transaction_start_timestamp_ms, -1);
}

#[test]
fn transaction_start_timestamp_is_set_by_staged_offset_commit_without_partitions() {
    let broker = test_broker();
    broker
        .bind_transactional_producer("txn-offset-start", 88, 0, 60_000, 100)
        .unwrap();

    broker
        .stage_transaction_offset_commit(
            "txn-offset-start",
            crate::broker::server::StagedOffsetCommit {
                group_id: "group-b".to_string(),
                topic: "topic-b".to_string(),
                partition: 1,
                next_offset: 11,
            },
            250,
        )
        .unwrap();

    let session = broker.transaction_session("txn-offset-start").unwrap();
    assert_eq!(session.transaction_start_timestamp_ms, 250);
}

#[tokio::test]
async fn transactional_produce_requires_partition_to_be_added_to_transaction() {
    let broker = test_broker();
    broker
        .store()
        .ensure_topic("txn.produce.topic", 1, 0)
        .unwrap();
    let session = crate::broker::handlers::bootstrap::handle_init_producer_id(
        &broker,
        InitProducerIdRequest::default().with_transactional_id(Some(TransactionalId(
            StrBytes::from("txn-produce".to_string()),
        ))),
    )
    .await
    .unwrap();

    let response = handle_produce(
        &broker,
        transactional_produce_request(
            "txn.produce.topic",
            session.producer_id.0,
            session.producer_epoch,
            0,
        ),
    )
    .await
    .unwrap();

    assert_eq!(
        response.responses[0].partition_responses[0].error_code,
        crate::broker::handlers::error_codes::INVALID_TXN_STATE
    );
}

#[tokio::test]
async fn transactional_produce_rejects_stale_epoch_before_first_append() {
    let broker = test_broker();
    broker
        .store()
        .ensure_topic("txn.produce.epoch.topic", 1, 0)
        .unwrap();
    let session = crate::broker::handlers::bootstrap::handle_init_producer_id(
        &broker,
        InitProducerIdRequest::default().with_transactional_id(Some(TransactionalId(
            StrBytes::from("txn-produce-epoch".to_string()),
        ))),
    )
    .await
    .unwrap();
    handle_add_partitions_to_txn(
        &broker,
        AddPartitionsToTxnRequest::default().with_transactions(vec![
            AddPartitionsToTxnTransaction::default()
                .with_transactional_id(TransactionalId(StrBytes::from(
                    "txn-produce-epoch".to_string(),
                )))
                .with_producer_id(session.producer_id)
                .with_producer_epoch(session.producer_epoch)
                .with_topics(vec![
                    AddPartitionsToTxnTopic::default()
                        .with_name(TopicName(StrBytes::from(
                            "txn.produce.epoch.topic".to_string(),
                        )))
                        .with_partitions(vec![0]),
                ]),
        ]),
        5,
    )
    .await
    .unwrap();

    let response = handle_produce(
        &broker,
        transactional_produce_request(
            "txn.produce.epoch.topic",
            session.producer_id.0,
            session.producer_epoch + 1,
            0,
        ),
    )
    .await
    .unwrap();

    assert_eq!(
        response.responses[0].partition_responses[0].error_code,
        INVALID_PRODUCER_EPOCH
    );
}

#[tokio::test]
async fn transactional_produce_rejects_partition_not_registered_in_transaction() {
    let broker = test_broker();
    broker
        .store()
        .ensure_topic("txn.produce.partition.topic", 2, 0)
        .unwrap();
    let session = crate::broker::handlers::bootstrap::handle_init_producer_id(
        &broker,
        InitProducerIdRequest::default().with_transactional_id(Some(TransactionalId(
            StrBytes::from("txn-produce-partition".to_string()),
        ))),
    )
    .await
    .unwrap();
    handle_add_partitions_to_txn(
        &broker,
        AddPartitionsToTxnRequest::default().with_transactions(vec![
            AddPartitionsToTxnTransaction::default()
                .with_transactional_id(TransactionalId(StrBytes::from(
                    "txn-produce-partition".to_string(),
                )))
                .with_producer_id(session.producer_id)
                .with_producer_epoch(session.producer_epoch)
                .with_topics(vec![
                    AddPartitionsToTxnTopic::default()
                        .with_name(TopicName(StrBytes::from(
                            "txn.produce.partition.topic".to_string(),
                        )))
                        .with_partitions(vec![0]),
                ]),
        ]),
        5,
    )
    .await
    .unwrap();

    let response = handle_produce(
        &broker,
        transactional_produce_request_for_partition(
            "txn.produce.partition.topic",
            1,
            session.producer_id.0,
            session.producer_epoch,
            0,
        ),
    )
    .await
    .unwrap();

    assert_eq!(
        response.responses[0].partition_responses[0].error_code,
        crate::broker::handlers::error_codes::INVALID_TXN_STATE
    );
}

#[tokio::test]
async fn init_producer_id_rejects_non_positive_transaction_timeout() {
    let broker = test_broker();
    let response = crate::broker::handlers::bootstrap::handle_init_producer_id(
        &broker,
        InitProducerIdRequest::default()
            .with_transactional_id(Some(TransactionalId(StrBytes::from(
                "txn-timeout".to_string(),
            ))))
            .with_transaction_timeout_ms(-1),
    )
    .await
    .unwrap();

    assert_eq!(response.error_code, INVALID_TRANSACTION_TIMEOUT);
}

#[tokio::test]
async fn init_producer_id_rejects_transaction_timeout_above_maximum() {
    let broker = test_broker();
    let response = crate::broker::handlers::bootstrap::handle_init_producer_id(
        &broker,
        InitProducerIdRequest::default()
            .with_transactional_id(Some(TransactionalId(StrBytes::from(
                "txn-timeout-max".to_string(),
            ))))
            .with_transaction_timeout_ms(900_001),
    )
    .await
    .unwrap();

    assert_eq!(response.error_code, INVALID_TRANSACTION_TIMEOUT);
}

#[tokio::test]
async fn timed_out_transaction_is_aborted_and_reinitialized_with_new_producer_id() {
    let broker = test_broker();
    broker
        .store()
        .ensure_topic("txn.timeout.topic", 1, 0)
        .unwrap();
    let first = crate::broker::handlers::bootstrap::handle_init_producer_id(
        &broker,
        InitProducerIdRequest::default()
            .with_transactional_id(Some(TransactionalId(StrBytes::from(
                "txn-timeout-reinit".to_string(),
            ))))
            .with_transaction_timeout_ms(50),
    )
    .await
    .unwrap();
    handle_add_partitions_to_txn(
        &broker,
        AddPartitionsToTxnRequest::default().with_transactions(vec![
            AddPartitionsToTxnTransaction::default()
                .with_transactional_id(TransactionalId(StrBytes::from(
                    "txn-timeout-reinit".to_string(),
                )))
                .with_producer_id(first.producer_id)
                .with_producer_epoch(first.producer_epoch)
                .with_topics(vec![
                    AddPartitionsToTxnTopic::default()
                        .with_name(TopicName(StrBytes::from("txn.timeout.topic".to_string())))
                        .with_partitions(vec![0]),
                ]),
        ]),
        5,
    )
    .await
    .unwrap();
    let produce = handle_produce(
        &broker,
        transactional_produce_request(
            "txn.timeout.topic",
            first.producer_id.0,
            first.producer_epoch,
            0,
        ),
    )
    .await
    .unwrap();
    assert_eq!(produce.responses[0].partition_responses[0].error_code, 0);

    sleep(Duration::from_millis(70)).await;

    let second = crate::broker::handlers::bootstrap::handle_init_producer_id(
        &broker,
        InitProducerIdRequest::default()
            .with_transactional_id(Some(TransactionalId(StrBytes::from(
                "txn-timeout-reinit".to_string(),
            ))))
            .with_transaction_timeout_ms(5_000),
    )
    .await
    .unwrap();

    assert_ne!(second.producer_id, first.producer_id);
    assert_eq!(second.producer_epoch, 0);

    let end = handle_end_txn(
        &broker,
        EndTxnRequest::default()
            .with_transactional_id(TransactionalId(StrBytes::from(
                "txn-timeout-reinit".to_string(),
            )))
            .with_producer_id(first.producer_id)
            .with_producer_epoch(first.producer_epoch)
            .with_committed(true),
        5,
    )
    .await
    .unwrap();
    assert_eq!(end.error_code, INVALID_PRODUCER_ID_MAPPING);

    let fetch = handle_fetch(
        &broker,
        kafka_protocol::messages::FetchRequest::default()
            .with_isolation_level(1)
            .with_topics(vec![
                kafka_protocol::messages::fetch_request::FetchTopic::default()
                    .with_topic(TopicName(StrBytes::from("txn.timeout.topic".to_string())))
                    .with_partitions(vec![
                        kafka_protocol::messages::fetch_request::FetchPartition::default()
                            .with_partition(0)
                            .with_fetch_offset(0)
                            .with_partition_max_bytes(4096),
                    ]),
            ]),
    )
    .await
    .unwrap();
    let partition = &fetch.responses[0].partitions[0];
    assert!(partition.records.as_ref().expect("records").is_empty());
    let aborted = partition
        .aborted_transactions
        .as_ref()
        .expect("aborted transactions");
    assert_eq!(aborted.len(), 1);
    assert_eq!(aborted[0].producer_id, first.producer_id);
}

#[tokio::test]
async fn transaction_timeout_survives_restart_and_still_expires_session() {
    let dir = tempdir().unwrap();
    let broker = broker_for_data_dir(&dir.path().join("data"));
    broker
        .store()
        .ensure_topic("txn.timeout.restart.topic", 1, 0)
        .unwrap();
    let first = crate::broker::handlers::bootstrap::handle_init_producer_id(
        &broker,
        InitProducerIdRequest::default()
            .with_transactional_id(Some(TransactionalId(StrBytes::from(
                "txn-timeout-restart".to_string(),
            ))))
            .with_transaction_timeout_ms(25),
    )
    .await
    .unwrap();
    handle_add_partitions_to_txn(
        &broker,
        AddPartitionsToTxnRequest::default().with_transactions(vec![
            AddPartitionsToTxnTransaction::default()
                .with_transactional_id(TransactionalId(StrBytes::from(
                    "txn-timeout-restart".to_string(),
                )))
                .with_producer_id(first.producer_id)
                .with_producer_epoch(first.producer_epoch)
                .with_topics(vec![
                    AddPartitionsToTxnTopic::default()
                        .with_name(TopicName(StrBytes::from(
                            "txn.timeout.restart.topic".to_string(),
                        )))
                        .with_partitions(vec![0]),
                ]),
        ]),
        5,
    )
    .await
    .unwrap();
    let produce = handle_produce(
        &broker,
        transactional_produce_request(
            "txn.timeout.restart.topic",
            first.producer_id.0,
            first.producer_epoch,
            0,
        ),
    )
    .await
    .unwrap();
    assert_eq!(produce.responses[0].partition_responses[0].error_code, 0);

    let reopened = broker_for_data_dir(&dir.path().join("data"));
    let restored = reopened
        .transaction_session("txn-timeout-restart")
        .expect("restored transaction");
    assert_eq!(restored.transaction_timeout_ms, 25);

    sleep(Duration::from_millis(60)).await;

    let second = crate::broker::handlers::bootstrap::handle_init_producer_id(
        &reopened,
        InitProducerIdRequest::default()
            .with_transactional_id(Some(TransactionalId(StrBytes::from(
                "txn-timeout-restart".to_string(),
            ))))
            .with_transaction_timeout_ms(5_000),
    )
    .await
    .unwrap();

    assert_ne!(second.producer_id, first.producer_id);
    assert_eq!(second.producer_epoch, 0);
}
