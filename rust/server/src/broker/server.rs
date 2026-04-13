use std::sync::Arc;

use anyhow::Result;
use tokio::net::TcpListener;
use tracing::{debug, error, info};

use crate::cluster::{
    ClusterRpcRequest, ClusterRpcResponse, ClusterRpcTarget, ClusterRpcTransport, ClusterRuntime,
    GetPartitionStateRequest, ReplicaFetchRequest, UpdateReplicaProgressRequest,
};
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

    pub fn partition_high_watermark(&self, topic: &str, partition: i32) -> Option<i64> {
        self.cluster()
            .metadata_image()
            .partition_high_watermark(topic, partition)
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
                    broker_id: self.config.broker.broker_id,
                    log_end_offset: latest.offset,
                    last_caught_up_ms: now_ms,
                })?;
        Ok(response.high_watermark)
    }

    pub fn sync_follower_progress_from_remote<T: ClusterRpcTransport>(
        &self,
        transport: &T,
        _target: &ClusterRpcTarget,
        topic: &str,
        partition: i32,
        now_ms: i64,
    ) -> Result<i64> {
        let ClusterRpcResponse::GetPartitionState(state) = transport.send_to(
            _target,
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
        let (_, latest) = self.store.list_offsets(topic, partition)?;
        let response =
            self.cluster
                .handle_update_replica_progress(UpdateReplicaProgressRequest {
                    topic_name: topic.to_string(),
                    partition_index: partition,
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
        if !fetched.found {
            return Ok(-1);
        }
        if latest.offset > fetched.leader_log_end_offset {
            return Err(anyhow::anyhow!(
                "replica divergence requires truncate: follower offset {} > leader offset {}",
                latest.offset,
                fetched.leader_log_end_offset
            ));
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
                    broker_id: self.config.broker.broker_id,
                    log_end_offset: fetched
                        .leader_log_end_offset
                        .min(self.store.list_offsets(topic, partition)?.1.offset),
                    last_caught_up_ms: now_ms,
                })?;
        Ok(response.high_watermark)
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bytes::Bytes;
    use bytes::BytesMut;
    use kafka_protocol::messages::produce_request::{PartitionProduceData, TopicProduceData};
    use kafka_protocol::messages::{ProduceRequest, TopicName};
    use kafka_protocol::protocol::StrBytes;
    use kafka_protocol::records::{
        Compression, Record, RecordBatchEncoder, RecordEncodeOptions, TimestampType,
    };
    use tempfile::tempdir;

    use crate::broker::handlers::produce_fetch::handle_produce;
    use crate::cluster::{
        ControllerQuorumVoter, InMemoryRemoteClusterRpcTransport, ProcessRole,
        test_support::{ThreeNodeClusterHarness, TwoNodeClusterHarness},
    };
    use crate::config::Config;
    use crate::store::{BrokerRecord, FileStore};

    use super::*;

    #[tokio::test]
    async fn produce_updates_local_replica_progress() {
        let broker = test_broker(1, 19092);
        broker.store().ensure_topic("progress.topic", 1, 0).unwrap();
        let metadata = broker
            .store()
            .topic_metadata(Some(&["progress.topic".to_string()]), 0)
            .unwrap();
        broker.sync_topic_metadata(&metadata).unwrap();

        let request = produce_request("progress.topic", -1, -1, 0);
        let _ = handle_produce(&broker, request).await.unwrap();

        assert_eq!(
            broker
                .cluster()
                .metadata_image()
                .partition_state_view("progress.topic", 0)
                .map(|(_, _, _, leo)| leo),
            Some(1)
        );
    }

    #[tokio::test]
    async fn follower_syncs_progress_from_remote_state() {
        let harness = TwoNodeClusterHarness::new_controller_pair();
        let leader = test_broker_with_voters(1, 19092, voter_pair());
        let follower = test_broker_with_voters(2, 19093, voter_pair());

        leader
            .store()
            .ensure_topic("replicated.topic", 1, 0)
            .unwrap();
        let metadata = leader
            .store()
            .topic_metadata(Some(&["replicated.topic".to_string()]), 0)
            .unwrap();
        leader.sync_topic_metadata(&metadata).unwrap();
        leader
            .cluster()
            .handle_update_partition_leader(crate::cluster::UpdatePartitionLeaderRequest {
                topic_name: "replicated.topic".to_string(),
                partition_index: 0,
                leader_id: 1,
                leader_epoch: 1,
            })
            .unwrap();
        leader
            .cluster()
            .handle_update_partition_replication(
                crate::cluster::UpdatePartitionReplicationRequest {
                    topic_name: "replicated.topic".to_string(),
                    partition_index: 0,
                    replicas: vec![1, 2],
                    isr: vec![1, 2],
                    leader_epoch: 1,
                },
            )
            .unwrap();
        let request = produce_request("replicated.topic", -1, -1, 0);
        let _ = handle_produce(&leader, request).await.unwrap();

        follower
            .store()
            .ensure_topic("replicated.topic", 1, 0)
            .unwrap();
        let follower_metadata = follower
            .store()
            .topic_metadata(Some(&["replicated.topic".to_string()]), 0)
            .unwrap();
        follower.sync_topic_metadata(&follower_metadata).unwrap();
        follower
            .cluster()
            .handle_update_partition_leader(crate::cluster::UpdatePartitionLeaderRequest {
                topic_name: "replicated.topic".to_string(),
                partition_index: 0,
                leader_id: 1,
                leader_epoch: 1,
            })
            .unwrap();
        follower
            .cluster()
            .handle_update_partition_replication(
                crate::cluster::UpdatePartitionReplicationRequest {
                    topic_name: "replicated.topic".to_string(),
                    partition_index: 0,
                    replicas: vec![1, 2],
                    isr: vec![1, 2],
                    leader_epoch: 1,
                },
            )
            .unwrap();

        harness.network.register(1, leader.cluster().clone());
        let transport =
            InMemoryRemoteClusterRpcTransport::new(&leader.config().cluster, harness.network);
        let target = transport.resolve_target(1).unwrap();
        let high_watermark = follower
            .sync_follower_progress_from_remote(&transport, &target, "replicated.topic", 0, 100)
            .unwrap();

        assert_eq!(high_watermark, 0);
        assert_eq!(
            follower
                .cluster()
                .metadata_image()
                .partition_state_view("replicated.topic", 0)
                .map(|(_, _, hw, leo)| (hw, leo)),
            Some((0, 0))
        );
    }

    #[tokio::test]
    async fn follower_syncs_progress_from_remote_state_in_three_node_isr() {
        let harness = ThreeNodeClusterHarness::new_controller_triplet();
        let voters = voter_trio();
        let leader = test_broker_with_voters(1, 19092, voters.clone());
        let follower = test_broker_with_voters(2, 19093, voters);

        for broker in [&leader, &follower] {
            broker
                .store()
                .ensure_topic("replicated.topic", 1, 0)
                .unwrap();
            let metadata = broker
                .store()
                .topic_metadata(Some(&["replicated.topic".to_string()]), 0)
                .unwrap();
            broker.sync_topic_metadata(&metadata).unwrap();
            broker
                .cluster()
                .handle_update_partition_leader(crate::cluster::UpdatePartitionLeaderRequest {
                    topic_name: "replicated.topic".to_string(),
                    partition_index: 0,
                    leader_id: 1,
                    leader_epoch: 1,
                })
                .unwrap();
            broker
                .cluster()
                .handle_update_partition_replication(
                    crate::cluster::UpdatePartitionReplicationRequest {
                        topic_name: "replicated.topic".to_string(),
                        partition_index: 0,
                        replicas: vec![1, 2, 3],
                        isr: vec![1, 2, 3],
                        leader_epoch: 1,
                    },
                )
                .unwrap();
        }

        let _ = handle_produce(&leader, produce_request("replicated.topic", -1, -1, 0))
            .await
            .unwrap();
        let _ = handle_produce(&leader, produce_request("replicated.topic", -1, -1, 1))
            .await
            .unwrap();
        let _ = handle_produce(&leader, produce_request("replicated.topic", -1, -1, 2))
            .await
            .unwrap();

        follower
            .store()
            .append_replica_records(
                "replicated.topic",
                0,
                &[replica_record(0, 100), replica_record(1, 101)],
                101,
            )
            .unwrap();
        follower
            .cluster()
            .handle_update_replica_progress(crate::cluster::UpdateReplicaProgressRequest {
                topic_name: "replicated.topic".to_string(),
                partition_index: 0,
                broker_id: 1,
                log_end_offset: 3,
                last_caught_up_ms: 100,
            })
            .unwrap();
        follower
            .cluster()
            .handle_update_replica_progress(crate::cluster::UpdateReplicaProgressRequest {
                topic_name: "replicated.topic".to_string(),
                partition_index: 0,
                broker_id: 3,
                log_end_offset: 3,
                last_caught_up_ms: 100,
            })
            .unwrap();

        harness.network.register(1, leader.cluster().clone());
        let transport =
            InMemoryRemoteClusterRpcTransport::new(&follower.config().cluster, harness.network);
        let target = transport.resolve_target(1).unwrap();
        let high_watermark = follower
            .sync_follower_progress_from_remote(&transport, &target, "replicated.topic", 0, 200)
            .unwrap();

        assert_eq!(high_watermark, 2);
        let image = follower.cluster().metadata_image();
        let partition = &image.topics[0].partitions[0];
        assert_eq!(partition.high_watermark, 2);
        assert_eq!(partition.replica_progress.len(), 3);
        assert_eq!(partition.replica_progress[0].log_end_offset, 3);
        assert_eq!(partition.replica_progress[1].log_end_offset, 2);
        assert_eq!(partition.replica_progress[2].log_end_offset, 3);
    }

    #[tokio::test]
    async fn follower_fetches_and_applies_remote_records() {
        let harness = ThreeNodeClusterHarness::new_controller_triplet();
        let voters = voter_trio();
        let leader = test_broker_with_voters(1, 19092, voters.clone());
        let follower = test_broker_with_voters(2, 19093, voters);

        for broker in [&leader, &follower] {
            broker.store().ensure_topic("rf.topic", 1, 0).unwrap();
            let metadata = broker
                .store()
                .topic_metadata(Some(&["rf.topic".to_string()]), 0)
                .unwrap();
            broker.sync_topic_metadata(&metadata).unwrap();
            broker
                .cluster()
                .handle_update_partition_leader(crate::cluster::UpdatePartitionLeaderRequest {
                    topic_name: "rf.topic".to_string(),
                    partition_index: 0,
                    leader_id: 1,
                    leader_epoch: 1,
                })
                .unwrap();
            broker
                .cluster()
                .handle_update_partition_replication(
                    crate::cluster::UpdatePartitionReplicationRequest {
                        topic_name: "rf.topic".to_string(),
                        partition_index: 0,
                        replicas: vec![1, 2, 3],
                        isr: vec![1, 2, 3],
                        leader_epoch: 1,
                    },
                )
                .unwrap();
        }

        let _ = handle_produce(&leader, produce_request("rf.topic", -1, -1, 0))
            .await
            .unwrap();
        let _ = handle_produce(&leader, produce_request("rf.topic", -1, -1, 1))
            .await
            .unwrap();

        harness.network.register(1, leader.cluster().clone());
        harness.network.register_store(1, leader.store().clone());
        let transport =
            InMemoryRemoteClusterRpcTransport::new(&follower.config().cluster, harness.network);
        let target = transport.resolve_target(1).unwrap();

        let high_watermark = follower
            .fetch_and_apply_from_remote_leader(&transport, &target, "rf.topic", 0, 200)
            .unwrap();

        let fetched = follower
            .store()
            .fetch_records("rf.topic", 0, 0, 10)
            .unwrap();
        assert_eq!(fetched.records.len(), 2);
        assert_eq!(fetched.records[0].offset, 0);
        assert_eq!(fetched.records[1].offset, 1);
        assert_eq!(high_watermark, 0);
    }

    #[tokio::test]
    async fn follower_detects_divergence_when_ahead_of_leader() {
        let harness = TwoNodeClusterHarness::new_controller_pair();
        let voters = voter_pair();
        let leader = test_broker_with_voters(1, 19092, voters.clone());
        let follower = test_broker_with_voters(2, 19093, voters);

        for broker in [&leader, &follower] {
            broker.store().ensure_topic("diverge.topic", 1, 0).unwrap();
            let metadata = broker
                .store()
                .topic_metadata(Some(&["diverge.topic".to_string()]), 0)
                .unwrap();
            broker.sync_topic_metadata(&metadata).unwrap();
            broker
                .cluster()
                .handle_update_partition_leader(crate::cluster::UpdatePartitionLeaderRequest {
                    topic_name: "diverge.topic".to_string(),
                    partition_index: 0,
                    leader_id: 1,
                    leader_epoch: 1,
                })
                .unwrap();
        }

        let _ = handle_produce(&leader, produce_request("diverge.topic", -1, -1, 0))
            .await
            .unwrap();
        follower
            .store()
            .append_replica_records(
                "diverge.topic",
                0,
                &[replica_record(0, 100), replica_record(1, 101)],
                101,
            )
            .unwrap();

        harness.network.register(1, leader.cluster().clone());
        harness.network.register_store(1, leader.store().clone());
        let transport =
            InMemoryRemoteClusterRpcTransport::new(&follower.config().cluster, harness.network);
        let target = transport.resolve_target(1).unwrap();

        let err = follower
            .fetch_and_apply_from_remote_leader(&transport, &target, "diverge.topic", 0, 200)
            .unwrap_err()
            .to_string();

        assert!(err.contains("replica divergence requires truncate"));
    }

    fn test_broker(node_id: i32, port: u16) -> KafkaBroker {
        test_broker_with_voters(node_id, port, voter_pair())
    }

    fn test_broker_with_voters(
        node_id: i32,
        port: u16,
        voters: Vec<ControllerQuorumVoter>,
    ) -> KafkaBroker {
        let dir = tempdir().unwrap().keep();
        let mut config = Config::single_node(dir.join(format!("node-{node_id}")), port, 1);
        config.cluster.node_id = node_id;
        config.cluster.process_roles = vec![ProcessRole::Broker, ProcessRole::Controller];
        config.cluster.controller_quorum_voters = voters;
        config.broker.broker_id = node_id;
        let store = Arc::new(FileStore::open(&config.storage.data_dir).unwrap());
        KafkaBroker::new(config, store).unwrap()
    }

    fn voter_pair() -> Vec<ControllerQuorumVoter> {
        vec![
            ControllerQuorumVoter {
                node_id: 1,
                host: "node1".to_string(),
                port: 9093,
            },
            ControllerQuorumVoter {
                node_id: 2,
                host: "node2".to_string(),
                port: 9093,
            },
        ]
    }

    fn voter_trio() -> Vec<ControllerQuorumVoter> {
        vec![
            ControllerQuorumVoter {
                node_id: 1,
                host: "node1".to_string(),
                port: 9093,
            },
            ControllerQuorumVoter {
                node_id: 2,
                host: "node2".to_string(),
                port: 9093,
            },
            ControllerQuorumVoter {
                node_id: 3,
                host: "node3".to_string(),
                port: 9093,
            },
        ]
    }

    fn replica_record(offset: i64, timestamp_ms: i64) -> BrokerRecord {
        BrokerRecord {
            offset,
            timestamp_ms,
            producer_id: -1,
            producer_epoch: -1,
            sequence: offset as i32,
            key: Some(Bytes::from_static(b"key")),
            value: Some(Bytes::from_static(b"value")),
            headers_json: vec![],
        }
    }

    fn produce_request(
        topic: &str,
        producer_id: i64,
        producer_epoch: i16,
        sequence: i32,
    ) -> ProduceRequest {
        let records = vec![Record {
            transactional: false,
            control: false,
            partition_leader_epoch: 0,
            producer_id,
            producer_epoch,
            timestamp_type: TimestampType::Creation,
            offset: 0,
            sequence,
            timestamp: 100,
            key: Some(Bytes::from_static(b"key")),
            value: Some(Bytes::from_static(b"value")),
            headers: Default::default(),
        }];
        let mut encoded = BytesMut::new();
        RecordBatchEncoder::encode(
            &mut encoded,
            &records,
            &RecordEncodeOptions {
                version: 2,
                compression: Compression::None,
            },
        )
        .unwrap();
        ProduceRequest::default()
            .with_acks(1)
            .with_timeout_ms(5_000)
            .with_topic_data(vec![
                TopicProduceData::default()
                    .with_name(TopicName(StrBytes::from(topic.to_string())))
                    .with_partition_data(vec![
                        PartitionProduceData::default()
                            .with_index(0)
                            .with_records(Some(encoded.freeze())),
                    ]),
            ])
    }
}
