use anyhow::Result;
use kafka_protocol::messages::add_partitions_to_txn_request::AddPartitionsToTxnTransaction;
use kafka_protocol::messages::add_partitions_to_txn_response::{
    AddPartitionsToTxnPartitionResult, AddPartitionsToTxnResponse, AddPartitionsToTxnResult,
    AddPartitionsToTxnTopicResult,
};
use kafka_protocol::messages::list_offsets_response::{
    ListOffsetsPartitionResponse, ListOffsetsTopicResponse,
};
use kafka_protocol::messages::produce_response::{PartitionProduceResponse, TopicProduceResponse};
use kafka_protocol::messages::write_txn_markers_response::{
    WritableTxnMarkerPartitionResult, WritableTxnMarkerResult, WritableTxnMarkerTopicResult,
};
use kafka_protocol::messages::{
    AddPartitionsToTxnRequest, EndTxnRequest, EndTxnResponse, FetchRequest, FetchResponse,
    ListOffsetsRequest, ListOffsetsResponse, ProduceRequest, ProduceResponse, TopicName,
    WriteTxnMarkersRequest, WriteTxnMarkersResponse,
};
use kafka_protocol::protocol::StrBytes;
use kafka_protocol::records::RecordBatchDecoder;

use self::auto_create::maybe_auto_create_topic;
#[cfg(test)]
use self::records::encode_records;
use self::records::to_broker_record;
use super::error_codes::{
    INVALID_PRODUCER_EPOCH, INVALID_PRODUCER_ID_MAPPING, INVALID_TXN_STATE, NOT_LEADER_OR_FOLLOWER,
    OUT_OF_ORDER_SEQUENCE_NUMBER, PRODUCER_FENCED, UNKNOWN_PRODUCER_ID, UNKNOWN_TOPIC_OR_PARTITION,
};
use crate::store::StoreError;
use crate::store::TransactionMarkerRequest;
use crate::store::TransactionStatus;

use super::super::KafkaBroker;

mod auto_create;
mod fetch_long_poll;
mod records;

struct MarkerWrite<'a> {
    topic_name: &'a str,
    partition_index: i32,
    producer_id: i64,
    producer_epoch: i16,
    coordinator_epoch: i32,
    committed: bool,
}

pub async fn handle_produce(
    broker: &KafkaBroker,
    request: ProduceRequest,
) -> Result<ProduceResponse> {
    let request_now_ms = chrono::Utc::now().timestamp_millis();
    broker.expire_timed_out_transactions(request_now_ms)?;
    let mut topics = Vec::new();
    for topic_data in request.topic_data {
        let topic_name = topic_data.name.to_string();
        let mut partitions = Vec::new();
        for partition_data in topic_data.partition_data {
            // Decode the Kafka wire batches into the broker's internal record shape.
            let raw_records = partition_data.records.unwrap_or_default();
            let mut record_bytes = raw_records.clone();
            let decoded = RecordBatchDecoder::decode_all(&mut record_bytes)?;
            let flattened = decoded
                .into_iter()
                .flat_map(|set| set.records)
                .map(to_broker_record)
                .collect::<Vec<_>>();
            if let Some(error_code) = validate_transactional_produce(
                broker,
                &topic_name,
                partition_data.index,
                &flattened,
            ) {
                partitions.push(
                    PartitionProduceResponse::default()
                        .with_index(partition_data.index)
                        .with_error_code(error_code)
                        .with_base_offset(-1)
                        .with_log_append_time_ms(-1)
                        .with_log_start_offset(0)
                        .with_record_errors(vec![])
                        .with_error_message(None),
                );
                continue;
            }
            let now = request_now_ms;

            // Auto-create can materialize metadata before we validate leadership or storage state.
            maybe_auto_create_topic(broker, &topic_name, partition_data.index, now)?;

            // Produce requests are only accepted by the current leader for the partition.
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

            // Reject requests for partitions that are not backed by the local store.
            let known_local = broker
                .store()
                .topic_metadata(Some(std::slice::from_ref(&topic_name)), now)
                .map(|topics| {
                    topics.iter().any(|topic| {
                        topic
                            .partitions
                            .iter()
                            .any(|p| p.partition == partition_data.index)
                    })
                })
                .unwrap_or(false);
            if !known_local {
                partitions.push(
                    PartitionProduceResponse::default()
                        .with_index(partition_data.index)
                        .with_error_code(UNKNOWN_TOPIC_OR_PARTITION)
                        .with_base_offset(-1)
                        .with_log_append_time_ms(-1)
                        .with_log_start_offset(0)
                        .with_record_errors(vec![])
                        .with_error_message(None),
                );
                continue;
            }

            // Capture the current tail so we can tell whether append made new data visible.
            let previous_log_end = broker
                .store()
                .list_offsets(&topic_name, partition_data.index)
                .map(|(_, latest)| latest.offset)
                .ok();

            // Append to the local log, then translate storage/leadership outcomes to Kafka errors.
            let produce_result =
                broker
                    .store()
                    .append_records(&topic_name, partition_data.index, &flattened, now);
            let (error_code, base_offset) = match produce_result {
                Ok((base_offset, last_offset)) => {
                    if !broker.is_local_partition_leader(&topic_name, partition_data.index) {
                        let _ = broker.store().truncate_partition(
                            &topic_name,
                            partition_data.index,
                            base_offset,
                        );
                        (NOT_LEADER_OR_FOLLOWER, -1)
                    } else {
                        if flattened.iter().any(|record| record.transactional) {
                            broker.touch_transaction_by_producer(
                                flattened
                                    .first()
                                    .map(|record| record.producer_id)
                                    .unwrap_or(-1),
                                now,
                            )?;
                        }
                        let _ = broker.update_local_replica_progress(
                            &topic_name,
                            partition_data.index,
                            now,
                        );
                        // Wake long-poll fetch waiters only when this append advanced visible data.
                        if should_notify_fetch_waiters(&flattened, previous_log_end, last_offset) {
                            broker.notify_fetch_signal(&topic_name, partition_data.index);
                        }
                        (0, base_offset)
                    }
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

fn should_notify_fetch_waiters(
    records: &[crate::store::BrokerRecord],
    previous_log_end: Option<i64>,
    last_offset: i64,
) -> bool {
    !records.is_empty() && previous_log_end.is_some_and(|log_end| last_offset >= log_end)
}

fn validate_transactional_produce(
    broker: &KafkaBroker,
    topic: &str,
    partition: i32,
    records: &[crate::store::BrokerRecord],
) -> Option<i16> {
    for record in records.iter().filter(|record| record.transactional) {
        let Some((_, session)) = broker.transaction_session_by_producer(record.producer_id) else {
            return Some(INVALID_PRODUCER_ID_MAPPING);
        };
        if session.fenced {
            return Some(PRODUCER_FENCED);
        }
        if session.producer_epoch != record.producer_epoch {
            return Some(INVALID_PRODUCER_EPOCH);
        }
        if !matches!(
            session.status,
            TransactionStatus::Empty | TransactionStatus::Ongoing
        ) {
            return Some(INVALID_TXN_STATE);
        }
        if !session.partitions.contains(&(topic.to_string(), partition)) {
            return Some(INVALID_TXN_STATE);
        }
    }
    None
}

pub async fn handle_fetch(broker: &KafkaBroker, request: FetchRequest) -> Result<FetchResponse> {
    fetch_long_poll::handle_fetch(broker, request).await
}

pub async fn handle_write_txn_markers(
    broker: &KafkaBroker,
    request: WriteTxnMarkersRequest,
) -> Result<WriteTxnMarkersResponse> {
    let now_ms = chrono::Utc::now().timestamp_millis();
    broker.expire_timed_out_transactions(now_ms)?;
    let mut markers = Vec::new();
    for marker in request.markers {
        let mut topics = Vec::new();
        for topic in marker.topics {
            let topic_name = topic.name.to_string();
            let mut partitions = Vec::new();
            for partition_index in topic.partition_indexes {
                let error_code = write_transaction_marker_for_partition(
                    broker,
                    MarkerWrite {
                        topic_name: &topic_name,
                        partition_index,
                        producer_id: marker.producer_id.0,
                        producer_epoch: marker.producer_epoch,
                        coordinator_epoch: marker.coordinator_epoch,
                        committed: marker.transaction_result,
                    },
                    now_ms,
                )?;
                partitions.push(
                    WritableTxnMarkerPartitionResult::default()
                        .with_partition_index(partition_index)
                        .with_error_code(error_code),
                );
            }
            topics.push(
                WritableTxnMarkerTopicResult::default()
                    .with_name(TopicName(StrBytes::from(topic_name)))
                    .with_partitions(partitions),
            );
        }
        markers.push(
            WritableTxnMarkerResult::default()
                .with_producer_id(marker.producer_id)
                .with_topics(topics),
        );
    }
    Ok(WriteTxnMarkersResponse::default().with_markers(markers))
}

pub async fn handle_add_partitions_to_txn(
    broker: &KafkaBroker,
    request: AddPartitionsToTxnRequest,
    api_version: i16,
) -> Result<AddPartitionsToTxnResponse> {
    broker.expire_timed_out_transactions(chrono::Utc::now().timestamp_millis())?;
    let response = if api_version <= 3 {
        let transaction = AddPartitionsToTxnTransaction::default()
            .with_transactional_id(request.v3_and_below_transactional_id)
            .with_producer_id(request.v3_and_below_producer_id)
            .with_producer_epoch(request.v3_and_below_producer_epoch)
            .with_topics(request.v3_and_below_topics);
        add_partitions_response_v3_and_below(broker, transaction)?
    } else {
        add_partitions_response_v4_and_above(broker, request.transactions)?
    };
    Ok(response.with_throttle_time_ms(0))
}

pub async fn handle_end_txn(
    broker: &KafkaBroker,
    request: EndTxnRequest,
    api_version: i16,
) -> Result<EndTxnResponse> {
    broker.expire_timed_out_transactions(chrono::Utc::now().timestamp_millis())?;
    let error_code = if let Some(session) = validated_transaction_session(
        broker,
        request.transactional_id.as_ref(),
        request.producer_id.0,
        request.producer_epoch,
    ) {
        if !matches!(
            session.status,
            TransactionStatus::Ongoing | TransactionStatus::Empty
        ) {
            INVALID_TXN_STATE
        } else {
            let now_ms = chrono::Utc::now().timestamp_millis();
            let partitions = broker.transaction_partitions(request.transactional_id.as_ref());
            let staged_offset_commits =
                broker.transaction_offset_commits(request.transactional_id.as_ref());
            let prepare_status = if request.committed {
                TransactionStatus::PrepareCommit
            } else {
                TransactionStatus::PrepareAbort
            };
            broker.set_transaction_status(
                request.transactional_id.as_ref(),
                prepare_status,
                now_ms,
            )?;
            let mut first_error = 0;
            for (topic, partition) in &partitions {
                let error = write_transaction_marker_for_partition(
                    broker,
                    MarkerWrite {
                        topic_name: topic,
                        partition_index: *partition,
                        producer_id: request.producer_id.0,
                        producer_epoch: request.producer_epoch,
                        coordinator_epoch: 0,
                        committed: request.committed,
                    },
                    now_ms,
                )?;
                if first_error == 0 && error != 0 {
                    first_error = error;
                }
            }
            if first_error == 0 && request.committed {
                for staged in &staged_offset_commits {
                    let commit_result =
                        broker
                            .store()
                            .commit_offset(crate::store::OffsetCommitRequest {
                                group_id: &staged.group_id,
                                member_id: &staged.member_id,
                                generation_id: staged.generation_id,
                                topic: &staged.topic,
                                partition: staged.partition,
                                next_offset: staged.next_offset,
                                now_ms,
                            });
                    let error = match commit_result {
                        Ok(()) => 0,
                        Err(StoreError::UnknownTopicOrPartition { .. }) => {
                            UNKNOWN_TOPIC_OR_PARTITION
                        }
                        Err(StoreError::UnknownMember { .. }) => 25,
                        Err(StoreError::StaleGeneration { .. }) => 22,
                        Err(err) => return Err(err.into()),
                    };
                    if first_error == 0 && error != 0 {
                        first_error = error;
                    }
                }
            }
            if first_error == 0 {
                let complete_status = if request.committed {
                    TransactionStatus::CompleteCommit
                } else {
                    TransactionStatus::CompleteAbort
                };
                broker.finalize_transaction_metadata(
                    request.transactional_id.as_ref(),
                    complete_status,
                    now_ms,
                )?;
                broker.set_transaction_status(
                    request.transactional_id.as_ref(),
                    TransactionStatus::Empty,
                    now_ms,
                )?;
            } else if !request.committed {
                broker.clear_transaction_offset_commits_with_timestamp(
                    request.transactional_id.as_ref(),
                    now_ms,
                )?;
                broker.set_transaction_status(
                    request.transactional_id.as_ref(),
                    TransactionStatus::Ongoing,
                    now_ms,
                )?;
            } else {
                broker.set_transaction_status(
                    request.transactional_id.as_ref(),
                    TransactionStatus::Ongoing,
                    now_ms,
                )?;
            }
            first_error
        }
    } else {
        transaction_session_error(
            broker,
            request.transactional_id.as_ref(),
            request.producer_id.0,
            request.producer_epoch,
        )
        .unwrap_or(INVALID_TXN_STATE)
    };

    let mut response = EndTxnResponse::default()
        .with_throttle_time_ms(0)
        .with_error_code(error_code);
    if api_version >= 5 {
        response = response
            .with_producer_id(request.producer_id)
            .with_producer_epoch(request.producer_epoch);
    }
    Ok(response)
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
                    let leader_epoch = broker
                        .partition_leader_epoch(&topic_name, partition.partition_index)
                        .unwrap_or(0);
                    let result = match partition.timestamp {
                        -2 => Some(earliest),
                        -1 => Some(latest),
                        timestamp => broker.store().list_offset_for_timestamp(
                            &topic_name,
                            partition.partition_index,
                            timestamp,
                        )?,
                    };
                    let (timestamp, offset) = result
                        .map(|result| (result.timestamp_ms, result.offset))
                        .unwrap_or((-1, -1));
                    partitions.push(
                        ListOffsetsPartitionResponse::default()
                            .with_partition_index(partition.partition_index)
                            .with_error_code(0)
                            .with_timestamp(timestamp)
                            .with_offset(offset)
                            .with_leader_epoch(if api_version >= 4 { leader_epoch } else { -1 }),
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

#[cfg(test)]
#[path = "produce_fetch/produce_fetch_tests.rs"]
mod produce_fetch_tests;

fn add_partitions_response_v3_and_below(
    broker: &KafkaBroker,
    transaction: AddPartitionsToTxnTransaction,
) -> Result<AddPartitionsToTxnResponse> {
    let (_, topic_results) = add_transaction_partitions(broker, transaction)?;
    Ok(AddPartitionsToTxnResponse::default().with_results_by_topic_v3_and_below(topic_results))
}

fn add_partitions_response_v4_and_above(
    broker: &KafkaBroker,
    transactions: Vec<AddPartitionsToTxnTransaction>,
) -> Result<AddPartitionsToTxnResponse> {
    let mut results = Vec::new();
    let mut top_level_error = 0;
    for transaction in transactions {
        let (transaction_error, topic_results) =
            add_transaction_partitions(broker, transaction.clone())?;
        if top_level_error == 0
            && transaction_error != 0
            && transaction_error != INVALID_PRODUCER_ID_MAPPING
        {
            top_level_error = transaction_error;
        }
        results.push(
            AddPartitionsToTxnResult::default()
                .with_transactional_id(transaction.transactional_id)
                .with_topic_results(topic_results),
        );
    }
    Ok(AddPartitionsToTxnResponse::default()
        .with_error_code(top_level_error)
        .with_results_by_transaction(results))
}

fn add_transaction_partitions(
    broker: &KafkaBroker,
    transaction: AddPartitionsToTxnTransaction,
) -> Result<(i16, Vec<AddPartitionsToTxnTopicResult>)> {
    let validated_session = validated_transaction_session(
        broker,
        transaction.transactional_id.as_ref(),
        transaction.producer_id.0,
        transaction.producer_epoch,
    );
    let session_error = validated_session
        .as_ref()
        .and_then(|session| {
            (!matches!(
                session.status,
                TransactionStatus::Empty | TransactionStatus::Ongoing
            ))
            .then_some(INVALID_TXN_STATE)
        })
        .or_else(|| {
            if validated_session.is_none() {
                transaction_session_error(
                    broker,
                    transaction.transactional_id.as_ref(),
                    transaction.producer_id.0,
                    transaction.producer_epoch,
                )
            } else {
                None
            }
        });
    let mut topic_results = Vec::new();
    let mut accepted = Vec::new();
    let mut transaction_error = 0;

    for topic in transaction.topics {
        let topic_name = topic.name.to_string();
        let mut partition_results = Vec::new();
        for partition in topic.partitions {
            let error_code = if let Some(error_code) = session_error {
                transaction_error = error_code;
                error_code
            } else if !broker.is_local_partition_leader(&topic_name, partition) {
                NOT_LEADER_OR_FOLLOWER
            } else {
                let now_ms = chrono::Utc::now().timestamp_millis();
                let known_local = broker
                    .store()
                    .topic_metadata(Some(std::slice::from_ref(&topic_name)), now_ms)
                    .map(|topics| {
                        topics
                            .iter()
                            .any(|topic| topic.partitions.iter().any(|p| p.partition == partition))
                    })
                    .unwrap_or(false);
                if !known_local {
                    UNKNOWN_TOPIC_OR_PARTITION
                } else if transaction.verify_only
                    && !broker.transaction_contains_partition(
                        transaction.transactional_id.as_ref(),
                        &topic_name,
                        partition,
                    )
                {
                    transaction_error = INVALID_TXN_STATE;
                    INVALID_TXN_STATE
                } else {
                    accepted.push((topic_name.clone(), partition));
                    0
                }
            };
            partition_results.push(
                AddPartitionsToTxnPartitionResult::default()
                    .with_partition_index(partition)
                    .with_partition_error_code(error_code),
            );
        }
        topic_results.push(
            AddPartitionsToTxnTopicResult::default()
                .with_name(TopicName(StrBytes::from(topic_name)))
                .with_results_by_partition(partition_results),
        );
    }

    if session_error.is_none() && !transaction.verify_only {
        broker.add_transaction_partitions(
            transaction.transactional_id.as_ref(),
            accepted,
            chrono::Utc::now().timestamp_millis(),
        )?;
    }
    Ok((transaction_error, topic_results))
}

fn validated_transaction_session(
    broker: &KafkaBroker,
    transactional_id: &str,
    producer_id: i64,
    producer_epoch: i16,
) -> Option<super::super::server::TransactionSession> {
    let session = broker.transaction_session(transactional_id)?;
    if session.fenced {
        return None;
    }
    if session.producer_id != producer_id || session.producer_epoch != producer_epoch {
        return None;
    }
    Some(session)
}

fn transaction_session_error(
    broker: &KafkaBroker,
    transactional_id: &str,
    producer_id: i64,
    producer_epoch: i16,
) -> Option<i16> {
    let Some(session) = broker.transaction_session(transactional_id) else {
        return Some(INVALID_PRODUCER_ID_MAPPING);
    };
    if session.fenced
        && session.producer_id == producer_id
        && session.producer_epoch == producer_epoch
    {
        return Some(PRODUCER_FENCED);
    }
    if session.producer_id != producer_id {
        return Some(INVALID_PRODUCER_ID_MAPPING);
    }
    if session.producer_epoch != producer_epoch {
        return Some(INVALID_PRODUCER_EPOCH);
    }
    None
}

fn write_transaction_marker_for_partition(
    broker: &KafkaBroker,
    marker: MarkerWrite<'_>,
    now_ms: i64,
) -> Result<i16> {
    if !broker.is_local_partition_leader(marker.topic_name, marker.partition_index) {
        return Ok(NOT_LEADER_OR_FOLLOWER);
    }
    let leader_epoch = broker
        .partition_leader_epoch(marker.topic_name, marker.partition_index)
        .unwrap_or(0);
    match broker
        .store()
        .write_transaction_marker(TransactionMarkerRequest {
            topic: marker.topic_name,
            partition: marker.partition_index,
            producer_id: marker.producer_id,
            producer_epoch: marker.producer_epoch,
            coordinator_epoch: marker.coordinator_epoch,
            committed: marker.committed,
            partition_leader_epoch: leader_epoch,
            now_ms,
        }) {
        Ok(_) => {
            let _ = broker.update_local_replica_progress(
                marker.topic_name,
                marker.partition_index,
                now_ms,
            );
            broker.notify_fetch_signal(marker.topic_name, marker.partition_index);
            Ok(0)
        }
        Err(StoreError::UnknownTopicOrPartition { .. }) => Ok(UNKNOWN_TOPIC_OR_PARTITION),
        Err(StoreError::InvalidProducerSequence { .. }) => Ok(OUT_OF_ORDER_SEQUENCE_NUMBER),
        Err(StoreError::StaleProducerEpoch { .. }) => Ok(INVALID_PRODUCER_EPOCH),
        Err(StoreError::UnknownProducerId { .. }) => Ok(UNKNOWN_PRODUCER_ID),
        Err(err) => Err(err.into()),
    }
}
