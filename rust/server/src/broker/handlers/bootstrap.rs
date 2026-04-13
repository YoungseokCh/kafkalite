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

use crate::cluster::{ClusterMetadataImage, TopicMetadataImage};
use crate::protocol;

use super::super::KafkaBroker;

pub fn handle_api_versions() -> ApiVersionsResponse {
    let apis = vec![
        api(ApiKey::ApiVersions, 0, protocol::API_VERSIONS_VERSION),
        api(ApiKey::Metadata, 1, protocol::METADATA_VERSION),
        api(
            ApiKey::InitProducerId,
            0,
            protocol::INIT_PRODUCER_ID_VERSION,
        ),
        api(ApiKey::Produce, 3, protocol::PRODUCE_VERSION),
        api(ApiKey::Fetch, 4, protocol::FETCH_VERSION),
        api(ApiKey::ListOffsets, 1, protocol::LIST_OFFSETS_VERSION),
        api(
            ApiKey::FindCoordinator,
            0,
            protocol::FIND_COORDINATOR_VERSION,
        ),
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

pub async fn handle_metadata(
    broker: &KafkaBroker,
    request: MetadataRequest,
) -> Result<MetadataResponse> {
    let names = request.topics.map(|topics| {
        topics
            .into_iter()
            .filter_map(|topic| topic.name.map(|name| name.to_string()))
            .collect::<Vec<_>>()
    });
    let now_ms = chrono::Utc::now().timestamp_millis();
    if request.allow_auto_topic_creation
        && let Some(requested) = names.as_ref()
    {
        for topic in requested {
            broker.store().ensure_topic(
                topic,
                broker.config().storage.default_partitions,
                now_ms,
            )?;
        }
    }
    let metadata = broker.store().topic_metadata(names.as_deref(), now_ms)?;
    broker.sync_topic_metadata(&metadata)?;
    let image = broker.cluster().metadata_image();

    let topics = if let Some(requested) = names {
        requested
            .into_iter()
            .map(
                |name| match image.topics.iter().find(|topic| topic.name == name) {
                    Some(topic) => MetadataResponseTopic::default()
                        .with_error_code(0)
                        .with_name(Some(TopicName(StrBytes::from(topic.name.clone()))))
                        .with_is_internal(false)
                        .with_partitions(topic_partitions(topic)),
                    None => MetadataResponseTopic::default()
                        .with_error_code(3)
                        .with_name(Some(TopicName(StrBytes::from(name))))
                        .with_is_internal(false)
                        .with_partitions(vec![]),
                },
            )
            .collect()
    } else {
        image
            .topics
            .iter()
            .map(|topic| {
                MetadataResponseTopic::default()
                    .with_error_code(0)
                    .with_name(Some(TopicName(StrBytes::from(topic.name.clone()))))
                    .with_is_internal(false)
                    .with_partitions(topic_partitions(topic))
            })
            .collect()
    };

    let brokers = metadata_brokers(&image);

    Ok(MetadataResponse::default()
        .with_throttle_time_ms(0)
        .with_brokers(brokers)
        .with_cluster_id(Some(StrBytes::from(image.cluster_id.clone())))
        .with_controller_id(BrokerId(image.controller_id))
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

fn metadata_brokers(image: &ClusterMetadataImage) -> Vec<MetadataResponseBroker> {
    image
        .brokers
        .iter()
        .map(|broker| {
            MetadataResponseBroker::default()
                .with_node_id(BrokerId(broker.node_id))
                .with_host(StrBytes::from(broker.host.clone()))
                .with_port(i32::from(broker.port))
                .with_rack(None)
        })
        .collect()
}

fn topic_partitions(topic: &TopicMetadataImage) -> Vec<MetadataResponsePartition> {
    topic
        .partitions
        .iter()
        .map(|partition| {
            MetadataResponsePartition::default()
                .with_error_code(0)
                .with_partition_index(partition.partition)
                .with_leader_id(BrokerId(partition.leader_id))
                .with_leader_epoch(partition.leader_epoch)
                .with_replica_nodes(partition.replicas.iter().copied().map(BrokerId).collect())
                .with_isr_nodes(partition.isr.iter().copied().map(BrokerId).collect())
                .with_offline_replicas(vec![])
        })
        .collect()
}
