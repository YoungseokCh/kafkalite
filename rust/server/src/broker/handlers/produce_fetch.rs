use anyhow::Result;
use bytes::BytesMut;
use kafka_protocol::messages::fetch_response::{FetchableTopicResponse, PartitionData};
use kafka_protocol::messages::list_offsets_response::{
    ListOffsetsPartitionResponse, ListOffsetsTopicResponse,
};
use kafka_protocol::messages::produce_response::{PartitionProduceResponse, TopicProduceResponse};
use kafka_protocol::messages::{
    BrokerId, FetchRequest, FetchResponse, ListOffsetsRequest, ListOffsetsResponse, ProduceRequest,
    ProduceResponse, TopicName,
};
use kafka_protocol::protocol::StrBytes;
use kafka_protocol::records::{
    Compression, Record, RecordBatchDecoder, RecordBatchEncoder, RecordEncodeOptions, TimestampType,
};

use crate::store::{BrokerRecord, StoreError};

use super::super::KafkaBroker;

const UNKNOWN_TOPIC_OR_PARTITION: i16 = 3;
const NOT_LEADER_OR_FOLLOWER: i16 = 6;
const OUT_OF_ORDER_SEQUENCE_NUMBER: i16 = 45;
const INVALID_PRODUCER_EPOCH: i16 = 47;
const UNKNOWN_PRODUCER_ID: i16 = 59;

pub async fn handle_produce(
    broker: &KafkaBroker,
    request: ProduceRequest,
) -> Result<ProduceResponse> {
    let mut topics = Vec::new();
    for topic_data in request.topic_data {
        let topic_name = topic_data.name.to_string();
        let mut partitions = Vec::new();
        for partition_data in topic_data.partition_data {
            let raw_records = partition_data.records.unwrap_or_default();
            let mut record_bytes = raw_records.clone();
            let decoded = RecordBatchDecoder::decode_all(&mut record_bytes)?;
            let flattened = decoded
                .into_iter()
                .flat_map(|set| set.records)
                .map(to_broker_record)
                .collect::<Vec<_>>();
            let now = chrono::Utc::now().timestamp_millis();
            maybe_auto_create_topic(broker, &topic_name, partition_data.index, now)?;
            if !broker.is_local_partition_leader(&topic_name, partition_data.index) {
                partitions.push(
                    PartitionProduceResponse::default()
                        .with_index(partition_data.index)
                        .with_error_code(NOT_LEADER_OR_FOLLOWER)
                        .with_base_offset(-1)
                        .with_log_append_time_ms(-1)
                        .with_log_start_offset(0)
                        .with_record_errors(vec![])
                        .with_error_message(None),
                );
                continue;
            }
            let produce_result =
                broker
                    .store()
                    .append_records(&topic_name, partition_data.index, &flattened, now);
            let (error_code, base_offset) = match produce_result {
                Ok((base_offset, _)) => {
                    let _ = broker.update_local_replica_progress(
                        &topic_name,
                        partition_data.index,
                        now,
                    );
                    (0, base_offset)
                }
                Err(StoreError::UnknownTopicOrPartition { .. }) => (UNKNOWN_TOPIC_OR_PARTITION, -1),
                Err(StoreError::InvalidProducerSequence { .. }) => {
                    (OUT_OF_ORDER_SEQUENCE_NUMBER, -1)
                }
                Err(StoreError::StaleProducerEpoch { .. }) => (INVALID_PRODUCER_EPOCH, -1),
                Err(StoreError::UnknownProducerId { .. }) => (UNKNOWN_PRODUCER_ID, -1),
                Err(err) => return Err(err.into()),
            };
            partitions.push(
                PartitionProduceResponse::default()
                    .with_index(partition_data.index)
                    .with_error_code(error_code)
                    .with_base_offset(base_offset)
                    .with_log_append_time_ms(-1)
                    .with_log_start_offset(0)
                    .with_record_errors(vec![])
                    .with_error_message(None),
            );
        }
        topics.push(
            TopicProduceResponse::default()
                .with_name(TopicName(StrBytes::from(topic_name.clone())))
                .with_partition_responses(partitions),
        );
    }

    Ok(ProduceResponse::default()
        .with_responses(topics)
        .with_throttle_time_ms(0))
}

pub async fn handle_fetch(broker: &KafkaBroker, request: FetchRequest) -> Result<FetchResponse> {
    let mut responses = Vec::new();
    for topic in request.topics {
        let mut partitions = Vec::new();
        let topic_name = topic.topic.to_string();
        for partition in topic.partitions {
            if !broker.is_local_partition_leader(&topic_name, partition.partition) {
                partitions.push(
                    PartitionData::default()
                        .with_partition_index(partition.partition)
                        .with_error_code(NOT_LEADER_OR_FOLLOWER)
                        .with_high_watermark(-1)
                        .with_last_stable_offset(-1)
                        .with_log_start_offset(-1)
                        .with_aborted_transactions(None)
                        .with_preferred_read_replica(BrokerId(-1))
                        .with_records(None),
                );
                continue;
            }
            match broker.store().fetch_records(
                &topic_name,
                partition.partition,
                partition.fetch_offset,
                1_000,
            ) {
                Ok(fetched) => {
                    let records = encode_records(&fetched.records)?;
                    let high_watermark = broker
                        .partition_high_watermark(&topic_name, partition.partition)
                        .unwrap_or(fetched.high_watermark);
                    partitions.push(
                        PartitionData::default()
                            .with_partition_index(partition.partition)
                            .with_error_code(0)
                            .with_high_watermark(high_watermark)
                            .with_last_stable_offset(high_watermark)
                            .with_log_start_offset(0)
                            .with_aborted_transactions(None)
                            .with_preferred_read_replica(BrokerId(-1))
                            .with_records(Some(records)),
                    );
                }
                Err(StoreError::UnknownTopicOrPartition { .. }) => {
                    partitions.push(
                        PartitionData::default()
                            .with_partition_index(partition.partition)
                            .with_error_code(UNKNOWN_TOPIC_OR_PARTITION)
                            .with_high_watermark(-1)
                            .with_last_stable_offset(-1)
                            .with_log_start_offset(-1)
                            .with_aborted_transactions(None)
                            .with_preferred_read_replica(BrokerId(-1))
                            .with_records(None),
                    );
                }
                Err(err) => return Err(err.into()),
            }
        }
        responses.push(
            FetchableTopicResponse::default()
                .with_topic(TopicName(StrBytes::from(topic_name.clone())))
                .with_partitions(partitions),
        );
    }
    Ok(FetchResponse::default()
        .with_throttle_time_ms(0)
        .with_error_code(0)
        .with_session_id(0)
        .with_responses(responses))
}

pub async fn handle_list_offsets(
    broker: &KafkaBroker,
    request: ListOffsetsRequest,
    api_version: i16,
) -> Result<ListOffsetsResponse> {
    let mut topics = Vec::new();
    for topic in request.topics {
        let topic_name = topic.name.to_string();
        let mut partitions = Vec::new();
        for partition in topic.partitions {
            if !broker.is_local_partition_leader(&topic_name, partition.partition_index) {
                partitions.push(
                    ListOffsetsPartitionResponse::default()
                        .with_partition_index(partition.partition_index)
                        .with_error_code(NOT_LEADER_OR_FOLLOWER)
                        .with_timestamp(-1)
                        .with_offset(-1)
                        .with_leader_epoch(-1),
                );
                continue;
            }
            match broker
                .store()
                .list_offsets(&topic_name, partition.partition_index)
            {
                Ok((earliest, latest)) => {
                    let result = match partition.timestamp {
                        -2 => earliest,
                        -1 => latest,
                        _ => earliest,
                    };
                    partitions.push(
                        ListOffsetsPartitionResponse::default()
                            .with_partition_index(partition.partition_index)
                            .with_error_code(0)
                            .with_timestamp(result.timestamp_ms)
                            .with_offset(result.offset)
                            .with_leader_epoch(if api_version >= 4 { 0 } else { -1 }),
                    );
                }
                Err(StoreError::UnknownTopicOrPartition { .. }) => {
                    partitions.push(
                        ListOffsetsPartitionResponse::default()
                            .with_partition_index(partition.partition_index)
                            .with_error_code(UNKNOWN_TOPIC_OR_PARTITION)
                            .with_timestamp(-1)
                            .with_offset(-1)
                            .with_leader_epoch(-1),
                    );
                }
                Err(err) => return Err(err.into()),
            }
        }
        topics.push(
            ListOffsetsTopicResponse::default()
                .with_name(TopicName(StrBytes::from(topic_name.clone())))
                .with_partitions(partitions),
        );
    }
    Ok(ListOffsetsResponse::default()
        .with_throttle_time_ms(0)
        .with_topics(topics))
}

fn maybe_auto_create_topic(
    broker: &KafkaBroker,
    topic: &str,
    partition: i32,
    now_ms: i64,
) -> Result<()> {
    let known = broker
        .store()
        .topic_metadata(Some(&[topic.to_string()]), now_ms)?;
    if !known.is_empty() {
        return Ok(());
    }
    if partition < 0 || partition >= broker.config().storage.default_partitions {
        return Ok(());
    }
    if !broker.cluster().can_auto_create_topics_locally() {
        return Ok(());
    }
    broker
        .store()
        .ensure_topic(topic, broker.config().storage.default_partitions, now_ms)?;
    let metadata = broker
        .store()
        .topic_metadata(Some(&[topic.to_string()]), now_ms)?;
    broker.sync_topic_metadata(&metadata)?;
    Ok(())
}

fn to_broker_record(record: Record) -> BrokerRecord {
    BrokerRecord {
        offset: record.offset,
        timestamp_ms: record.timestamp,
        producer_id: record.producer_id,
        producer_epoch: record.producer_epoch,
        sequence: record.sequence,
        key: record.key,
        value: record.value,
        headers_json: serde_json::to_vec(
            &record
                .headers
                .iter()
                .map(|(key, value)| (key.to_string(), value.clone().map(|bytes| bytes.to_vec())))
                .collect::<Vec<_>>(),
        )
        .unwrap_or_else(|_| b"[]".to_vec()),
    }
}

fn encode_records(records: &[BrokerRecord]) -> Result<bytes::Bytes> {
    let kafka_records = records
        .iter()
        .enumerate()
        .map(|(index, record)| Record {
            transactional: false,
            control: false,
            partition_leader_epoch: 0,
            producer_id: -1,
            producer_epoch: -1,
            timestamp_type: TimestampType::Creation,
            offset: record.offset.max(index as i64),
            sequence: record.sequence,
            timestamp: record.timestamp_ms,
            key: record.key.clone(),
            value: record.value.clone(),
            headers: Default::default(),
        })
        .collect::<Vec<_>>();
    let mut encoded = BytesMut::new();
    RecordBatchEncoder::encode(
        &mut encoded,
        &kafka_records,
        &RecordEncodeOptions {
            version: 2,
            compression: Compression::None,
        },
    )?;
    Ok(encoded.freeze())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bytes::Bytes;
    use kafka_protocol::messages::produce_request::{PartitionProduceData, TopicProduceData};
    use kafka_protocol::messages::{ProduceRequest, TopicName};
    use tempfile::tempdir;

    use crate::cluster::{ControllerQuorumVoter, ProcessRole};
    use crate::config::Config;
    use crate::store::FileStore;

    use super::*;

    #[tokio::test]
    async fn duplicate_retry_returns_same_base_offset() {
        let broker = test_broker();
        let session = broker.store().init_producer(0).unwrap();
        let first = handle_produce(
            &broker,
            produce_request(
                "retry.topic",
                session.producer_id,
                session.producer_epoch,
                0,
            ),
        )
        .await
        .unwrap();
        let duplicate = handle_produce(
            &broker,
            produce_request(
                "retry.topic",
                session.producer_id,
                session.producer_epoch,
                0,
            ),
        )
        .await
        .unwrap();

        let first_partition = &first.responses[0].partition_responses[0];
        let duplicate_partition = &duplicate.responses[0].partition_responses[0];
        assert_eq!(first_partition.error_code, 0);
        assert_eq!(duplicate_partition.error_code, 0);
        assert_eq!(first_partition.base_offset, duplicate_partition.base_offset);
        let fetched = broker
            .store()
            .fetch_records("retry.topic", 0, 0, 10)
            .unwrap();
        assert_eq!(fetched.records.len(), 1);
    }

    #[tokio::test]
    async fn stale_epoch_maps_to_invalid_producer_epoch() {
        let broker = test_broker();
        let session = broker.store().init_producer(0).unwrap();
        let _ = handle_produce(
            &broker,
            produce_request(
                "epoch.topic",
                session.producer_id,
                session.producer_epoch + 1,
                0,
            ),
        )
        .await
        .unwrap();
        let stale = handle_produce(
            &broker,
            produce_request(
                "epoch.topic",
                session.producer_id,
                session.producer_epoch,
                1,
            ),
        )
        .await
        .unwrap();

        let partition = &stale.responses[0].partition_responses[0];
        assert_eq!(partition.error_code, INVALID_PRODUCER_EPOCH);
        assert_eq!(partition.base_offset, -1);
    }

    #[tokio::test]
    async fn produce_is_rejected_when_local_broker_is_not_leader() {
        let broker = test_broker();
        broker.store().ensure_topic("remote.topic", 1, 0).unwrap();
        let metadata = broker
            .store()
            .topic_metadata(Some(&["remote.topic".to_string()]), 0)
            .unwrap();
        broker.sync_topic_metadata(&metadata).unwrap();
        let initial_offset = broker.cluster().metadata_image().metadata_offset;
        broker
            .cluster()
            .handle_append_metadata(crate::cluster::AppendMetadataRequest {
                term: 1,
                leader_id: 9,
                prev_metadata_offset: initial_offset,
                records: vec![crate::cluster::MetadataRecord::UpsertTopic(
                    crate::cluster::TopicMetadataImage {
                        name: "remote.topic".to_string(),
                        partitions: vec![crate::cluster::PartitionMetadataImage {
                            partition: 0,
                            leader_id: 9,
                            leader_epoch: 1,
                            high_watermark: 0,
                            replicas: vec![9],
                            isr: vec![9],
                            replica_progress: vec![],
                            reassignment: None,
                        }],
                    },
                )],
            })
            .unwrap();

        let response = handle_produce(&broker, produce_request("remote.topic", -1, -1, 0))
            .await
            .unwrap();

        assert_eq!(
            response.responses[0].partition_responses[0].error_code,
            NOT_LEADER_OR_FOLLOWER
        );
    }

    #[tokio::test]
    async fn fetch_and_list_offsets_are_rejected_when_local_broker_is_not_leader() {
        let broker = test_broker();
        broker.store().ensure_topic("remote.fetch", 1, 0).unwrap();
        let metadata = broker
            .store()
            .topic_metadata(Some(&["remote.fetch".to_string()]), 0)
            .unwrap();
        broker.sync_topic_metadata(&metadata).unwrap();
        let initial_offset = broker.cluster().metadata_image().metadata_offset;
        broker
            .cluster()
            .handle_append_metadata(crate::cluster::AppendMetadataRequest {
                term: 1,
                leader_id: 9,
                prev_metadata_offset: initial_offset,
                records: vec![crate::cluster::MetadataRecord::UpsertTopic(
                    crate::cluster::TopicMetadataImage {
                        name: "remote.fetch".to_string(),
                        partitions: vec![crate::cluster::PartitionMetadataImage {
                            partition: 0,
                            leader_id: 9,
                            leader_epoch: 1,
                            high_watermark: 0,
                            replicas: vec![9],
                            isr: vec![9],
                            replica_progress: vec![],
                            reassignment: None,
                        }],
                    },
                )],
            })
            .unwrap();

        let fetch = handle_fetch(
            &broker,
            FetchRequest::default().with_topics(vec![
                kafka_protocol::messages::fetch_request::FetchTopic::default()
                    .with_topic(TopicName(StrBytes::from("remote.fetch".to_string())))
                    .with_partitions(vec![
                        kafka_protocol::messages::fetch_request::FetchPartition::default()
                            .with_partition(0)
                            .with_fetch_offset(0)
                            .with_partition_max_bytes(1024),
                    ]),
            ]),
        )
        .await
        .unwrap();
        assert_eq!(
            fetch.responses[0].partitions[0].error_code,
            NOT_LEADER_OR_FOLLOWER
        );

        let offsets = handle_list_offsets(
            &broker,
            ListOffsetsRequest::default().with_topics(vec![
                kafka_protocol::messages::list_offsets_request::ListOffsetsTopic::default()
                    .with_name(TopicName(StrBytes::from("remote.fetch".to_string())))
                    .with_partitions(vec![
                        kafka_protocol::messages::list_offsets_request::ListOffsetsPartition::default()
                            .with_partition_index(0)
                            .with_timestamp(-1),
                    ]),
            ]),
            4,
        )
        .await
        .unwrap();
        assert_eq!(
            offsets.topics[0].partitions[0].error_code,
            NOT_LEADER_OR_FOLLOWER
        );
    }

    #[tokio::test]
    async fn fetch_uses_metadata_high_watermark_when_replication_progress_exists() {
        let broker = test_broker();
        broker.store().ensure_topic("hw.topic", 1, 0).unwrap();
        let metadata = broker
            .store()
            .topic_metadata(Some(&["hw.topic".to_string()]), 0)
            .unwrap();
        broker.sync_topic_metadata(&metadata).unwrap();

        let _ = handle_produce(&broker, produce_request("hw.topic", -1, -1, 0))
            .await
            .unwrap();

        broker
            .cluster()
            .handle_update_partition_replication(
                crate::cluster::UpdatePartitionReplicationRequest {
                    topic_name: "hw.topic".to_string(),
                    partition_index: 0,
                    replicas: vec![1, 2],
                    isr: vec![1, 2],
                    leader_epoch: 1,
                },
            )
            .unwrap();
        broker
            .cluster()
            .handle_update_replica_progress(crate::cluster::UpdateReplicaProgressRequest {
                topic_name: "hw.topic".to_string(),
                partition_index: 0,
                broker_id: 1,
                log_end_offset: 1,
                last_caught_up_ms: 100,
            })
            .unwrap();
        broker
            .cluster()
            .handle_update_replica_progress(crate::cluster::UpdateReplicaProgressRequest {
                topic_name: "hw.topic".to_string(),
                partition_index: 0,
                broker_id: 2,
                log_end_offset: 0,
                last_caught_up_ms: 100,
            })
            .unwrap();

        let fetch = handle_fetch(
            &broker,
            FetchRequest::default().with_topics(vec![
                kafka_protocol::messages::fetch_request::FetchTopic::default()
                    .with_topic(TopicName(StrBytes::from("hw.topic".to_string())))
                    .with_partitions(vec![
                        kafka_protocol::messages::fetch_request::FetchPartition::default()
                            .with_partition(0)
                            .with_fetch_offset(0)
                            .with_partition_max_bytes(1024),
                    ]),
            ]),
        )
        .await
        .unwrap();

        assert_eq!(fetch.responses[0].partitions[0].high_watermark, 0);
        assert_eq!(fetch.responses[0].partitions[0].last_stable_offset, 0);
    }

    #[tokio::test]
    async fn auto_create_is_disabled_without_local_controller_authority() {
        let dir = tempdir().unwrap().keep();
        let mut config = Config::single_node(dir.join("data"), 9092, 1);
        config.cluster.node_id = 2;
        config.cluster.process_roles = vec![ProcessRole::Broker, ProcessRole::Controller];
        config.cluster.controller_quorum_voters = vec![
            ControllerQuorumVoter {
                node_id: 1,
                host: "node1".to_string(),
                port: 9093,
            },
            ControllerQuorumVoter {
                node_id: 2,
                host: "node2".to_string(),
                port: 9094,
            },
        ];
        let store = Arc::new(FileStore::open(&config.storage.data_dir).unwrap());
        let broker = KafkaBroker::new(config, store).unwrap();

        let response = handle_produce(&broker, produce_request("blocked.topic", -1, -1, 0))
            .await
            .unwrap();

        assert_eq!(
            response.responses[0].partition_responses[0].error_code,
            NOT_LEADER_OR_FOLLOWER
        );
    }

    fn test_broker() -> KafkaBroker {
        let dir = tempdir().unwrap().keep();
        let config = Config::single_node(dir.join("data"), 9092, 1);
        let store = Arc::new(FileStore::open(&config.storage.data_dir).unwrap());
        KafkaBroker::new(config, store).unwrap()
    }

    fn produce_request(
        topic: &str,
        producer_id: i64,
        producer_epoch: i16,
        sequence: i32,
    ) -> ProduceRequest {
        produce_request_for_partition(topic, 0, producer_id, producer_epoch, sequence)
    }

    fn produce_request_for_partition(
        topic: &str,
        partition: i32,
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
                            .with_index(partition)
                            .with_records(Some(encoded.freeze())),
                    ]),
            ])
    }
}
