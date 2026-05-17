use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use bytes::{Bytes, BytesMut};
use kafka_protocol::messages::produce_request::{PartitionProduceData, TopicProduceData};
use kafka_protocol::messages::{ProduceRequest, TopicName};
use kafka_protocol::protocol::StrBytes;
use kafka_protocol::records::{
    Compression, Record, RecordBatchEncoder, RecordEncodeOptions, TimestampType,
};
use tempfile::tempdir;

use super::*;
use crate::cluster::{ControllerQuorumVoter, ProcessRole};
use crate::config::Config;
use crate::store::{BrokerRecord, FileStore};

mod fetch_apply_tests;
mod progress_tests;

#[derive(Clone)]
struct ScriptedTransport {
    responses: Arc<Mutex<VecDeque<ClusterRpcResponse>>>,
}

impl ScriptedTransport {
    fn new(responses: impl IntoIterator<Item = ClusterRpcResponse>) -> Self {
        Self {
            responses: Arc::new(Mutex::new(responses.into_iter().collect())),
        }
    }
}

impl ClusterRpcTransport for ScriptedTransport {
    fn send(&self, _request: ClusterRpcRequest) -> Result<ClusterRpcResponse> {
        unreachable!("scripted transport only supports targeted sends")
    }

    fn send_to(
        &self,
        _target: &ClusterRpcTarget,
        _request: ClusterRpcRequest,
    ) -> Result<ClusterRpcResponse> {
        self.responses
            .lock()
            .unwrap()
            .pop_front()
            .ok_or_else(|| anyhow::anyhow!("missing scripted response"))
    }
}

fn test_broker(node_id: i32, port: u16) -> KafkaBroker {
    let dir = tempdir().unwrap().keep();
    let mut config = Config::single_node(dir.join(format!("node-{node_id}")), port, 1);
    config.broker.broker_id = node_id;
    let store = Arc::new(FileStore::open(&config.storage.data_dir).unwrap());
    KafkaBroker::new(config, store).unwrap()
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
    let broker = KafkaBroker::new(config, store).unwrap();
    let _ = broker
        .cluster()
        .handle_append_metadata(crate::cluster::AppendMetadataRequest {
            term: 1,
            leader_id: node_id,
            prev_metadata_offset: broker.cluster().metadata_image().metadata_offset,
            records: vec![crate::cluster::MetadataRecord::SetController {
                controller_id: node_id,
            }],
        })
        .unwrap();
    broker
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

fn seed_partition_metadata(
    broker: &KafkaBroker,
    topic: &str,
    leader_id: i32,
    replicas: Vec<i32>,
    isr: Vec<i32>,
    leader_epoch: i32,
) {
    let append_leader = broker
        .cluster()
        .quorum_snapshot()
        .leader_id
        .unwrap_or(leader_id);
    let prev = broker.cluster().metadata_image().metadata_offset;
    broker
        .cluster()
        .handle_append_metadata(crate::cluster::AppendMetadataRequest {
            term: i64::from(leader_epoch.max(1)),
            leader_id: append_leader,
            prev_metadata_offset: prev,
            records: vec![crate::cluster::MetadataRecord::UpdatePartitionLeader {
                topic_name: topic.to_string(),
                partition_index: 0,
                leader_id,
                leader_epoch,
            }],
        })
        .unwrap();
    let prev = broker.cluster().metadata_image().metadata_offset;
    broker
        .cluster()
        .handle_append_metadata(crate::cluster::AppendMetadataRequest {
            term: i64::from(leader_epoch.max(1)),
            leader_id: append_leader,
            prev_metadata_offset: prev,
            records: vec![crate::cluster::MetadataRecord::UpdatePartitionReplication {
                topic_name: topic.to_string(),
                partition_index: 0,
                replicas,
                isr,
                leader_epoch,
            }],
        })
        .unwrap();
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
