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
    let mut created = false;
    if request.allow_auto_topic_creation
        && let Some(requested) = names.as_ref()
        && broker.cluster().can_auto_create_topics_locally()
    {
        for topic in requested {
            broker.store().ensure_topic(
                topic,
                broker.config().storage.default_partitions,
                now_ms,
            )?;
            created = true;
        }
    }
    if created {
        let metadata = broker.store().topic_metadata(names.as_deref(), now_ms)?;
        broker.sync_topic_metadata(&metadata)?;
    }
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

    let brokers = metadata_brokers(&image, broker);
    let controller_id = broker
        .cluster()
        .quorum_snapshot()
        .leader_id
        .unwrap_or(image.controller_id);

    Ok(MetadataResponse::default()
        .with_throttle_time_ms(0)
        .with_brokers(brokers)
        .with_cluster_id(Some(StrBytes::from(image.cluster_id.clone())))
        .with_controller_id(BrokerId(controller_id))
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

fn metadata_brokers(
    image: &ClusterMetadataImage,
    broker: &KafkaBroker,
) -> Vec<MetadataResponseBroker> {
    let brokers = if image.brokers.is_empty() {
        vec![crate::cluster::BrokerMetadata {
            node_id: broker.config().broker.broker_id,
            host: broker.config().broker.advertised_host.clone(),
            port: broker.config().broker.advertised_port,
        }]
    } else {
        image.brokers.clone()
    };
    brokers
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use kafka_protocol::messages::metadata_request::MetadataRequestTopic;
    use tempfile::tempdir;

    use super::*;
    use crate::broker::KafkaBroker;
    use crate::config::Config;
    use crate::store::FileStore;

    #[tokio::test]
    async fn metadata_without_topic_filter_returns_all_known_topics() {
        let broker = test_broker();

        let response = handle_metadata(&broker, MetadataRequest::default())
            .await
            .unwrap();

        assert!(response.topics.is_empty());
    }

    #[tokio::test]
    async fn metadata_can_auto_create_requested_topic_locally() {
        let broker = test_broker();
        let request = MetadataRequest::default()
            .with_allow_auto_topic_creation(true)
            .with_topics(Some(vec![MetadataRequestTopic::default().with_name(Some(
                TopicName(StrBytes::from("autocreate.topic".to_string())),
            ))]));

        let response = handle_metadata(&broker, request).await.unwrap();

        assert!(response.topics.iter().any(|topic| {
            topic.error_code == 0
                && topic
                    .name
                    .as_ref()
                    .map(|name| name.0.to_string())
                    .as_deref()
                    == Some("autocreate.topic")
        }));
    }

    fn test_broker() -> KafkaBroker {
        let dir = tempdir().unwrap().keep();
        let config = Config::single_node(dir.join("data"), 9092, 1);
        let store = Arc::new(FileStore::open(&config.storage.data_dir).unwrap());
        KafkaBroker::new(config, store).unwrap()
    }
}
