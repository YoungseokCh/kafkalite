use anyhow::Result;
use kafka_protocol::messages::api_versions_response::ApiVersion;
use kafka_protocol::messages::metadata_response::{
    MetadataResponseBroker, MetadataResponsePartition, MetadataResponseTopic,
};
use kafka_protocol::messages::{
    ApiKey, ApiVersionsResponse, BrokerId, InitProducerIdResponse, MetadataRequest,
    MetadataResponse, ProducerId, TopicName,
};
use kafka_protocol::protocol::StrBytes;

use crate::protocol;

use super::super::KafkaBroker;

pub fn handle_api_versions() -> ApiVersionsResponse {
    let apis = vec![
        api(ApiKey::ApiVersions, 0, protocol::API_VERSIONS_VERSION),
        api(ApiKey::Metadata, 1, protocol::METADATA_VERSION),
        api(ApiKey::InitProducerId, 0, protocol::INIT_PRODUCER_ID_VERSION),
        api(ApiKey::Produce, 3, protocol::PRODUCE_VERSION),
        api(ApiKey::Fetch, 4, protocol::FETCH_VERSION),
        api(ApiKey::ListOffsets, 1, protocol::LIST_OFFSETS_VERSION),
        api(ApiKey::FindCoordinator, 0, protocol::FIND_COORDINATOR_VERSION),
        api(ApiKey::JoinGroup, 0, protocol::JOIN_GROUP_VERSION),
        api(ApiKey::SyncGroup, 0, protocol::SYNC_GROUP_VERSION),
        api(ApiKey::Heartbeat, 0, protocol::HEARTBEAT_VERSION),
        api(ApiKey::LeaveGroup, 0, protocol::LEAVE_GROUP_VERSION),
        api(ApiKey::OffsetCommit, 0, protocol::OFFSET_COMMIT_VERSION),
        api(ApiKey::OffsetFetch, 1, protocol::OFFSET_FETCH_VERSION),
    ];

    ApiVersionsResponse::default()
        .with_error_code(0)
        .with_api_keys(apis)
        .with_throttle_time_ms(0)
}

pub async fn handle_metadata(broker: &KafkaBroker, request: MetadataRequest) -> Result<MetadataResponse> {
    let names = request.topics.map(|topics| {
        topics
            .into_iter()
            .filter_map(|topic| topic.name.map(|name| name.to_string()))
            .collect::<Vec<_>>()
    });
    let metadata = broker
        .store()
        .topic_metadata(names.as_deref(), chrono::Utc::now().timestamp_millis())?;

    let node_id = BrokerId(broker.config().broker.broker_id);
    let partition = MetadataResponsePartition::default()
        .with_error_code(0)
        .with_partition_index(0)
        .with_leader_id(node_id)
        .with_leader_epoch(0)
        .with_replica_nodes(vec![node_id])
        .with_isr_nodes(vec![node_id])
        .with_offline_replicas(vec![]);

    let topics = metadata
        .into_iter()
        .map(|topic| {
            MetadataResponseTopic::default()
                .with_error_code(0)
                .with_name(Some(TopicName(StrBytes::from(topic.name.clone()))))
                .with_is_internal(false)
                .with_partitions(vec![partition.clone()])
        })
        .collect();

    let broker_node = MetadataResponseBroker::default()
        .with_node_id(node_id)
        .with_host(StrBytes::from(broker.config().broker.advertised_host.clone()))
        .with_port(i32::from(broker.config().broker.advertised_port))
        .with_rack(None);

    Ok(MetadataResponse::default()
        .with_throttle_time_ms(0)
        .with_brokers(vec![broker_node])
        .with_cluster_id(Some(StrBytes::from(broker.config().broker.cluster_id.clone())))
        .with_controller_id(node_id)
        .with_topics(topics))
}

pub async fn handle_init_producer_id(broker: &KafkaBroker) -> Result<InitProducerIdResponse> {
    let session = broker
        .store()
        .init_producer(chrono::Utc::now().timestamp_millis())?;
    Ok(InitProducerIdResponse::default()
        .with_throttle_time_ms(0)
        .with_error_code(0)
        .with_producer_id(ProducerId(session.producer_id))
        .with_producer_epoch(session.producer_epoch))
}

fn api(api_key: ApiKey, min_version: i16, max_version: i16) -> ApiVersion {
    ApiVersion::default()
        .with_api_key(api_key as i16)
        .with_min_version(min_version)
        .with_max_version(max_version)
}
