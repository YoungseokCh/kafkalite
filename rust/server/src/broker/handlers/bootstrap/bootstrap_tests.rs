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
    let create = MetadataRequest::default()
        .with_allow_auto_topic_creation(true)
        .with_topics(Some(vec![MetadataRequestTopic::default().with_name(Some(
            TopicName(StrBytes::from("all.topic".to_string())),
        ))]));
    let _ = handle_metadata(&broker, create).await.unwrap();

    let mut request = MetadataRequest::default();
    request.topics = None;

    let response = handle_metadata(&broker, request).await.unwrap();

    assert_eq!(response.topics.len(), 1);
    assert_eq!(response.topics[0].error_code, 0);
    assert_eq!(
        response.topics[0].name.as_ref().unwrap().0.to_string(),
        "all.topic"
    );
}

#[tokio::test]
async fn metadata_can_auto_create_requested_topic_locally() {
    let broker = test_broker();
    assert!(broker.cluster().can_auto_create_topics_locally());
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

#[tokio::test]
async fn metadata_requested_topic_without_auto_create_returns_unknown_topic() {
    let broker = test_broker();
    let request = MetadataRequest::default()
        .with_allow_auto_topic_creation(false)
        .with_topics(Some(vec![MetadataRequestTopic::default().with_name(Some(
            TopicName(StrBytes::from("missing.topic".to_string())),
        ))]));

    let response = handle_metadata(&broker, request).await.unwrap();

    assert_eq!(response.topics.len(), 1);
    assert_eq!(response.topics[0].error_code, 3);
}

#[tokio::test]
async fn metadata_auto_create_is_ignored_without_local_controller_authority() {
    let broker = non_writable_broker();
    assert!(!broker.cluster().can_auto_create_topics_locally());
    let request = MetadataRequest::default()
        .with_allow_auto_topic_creation(true)
        .with_topics(Some(vec![MetadataRequestTopic::default().with_name(Some(
            TopicName(StrBytes::from("blocked.topic".to_string())),
        ))]));

    let response = handle_metadata(&broker, request).await.unwrap();

    assert_eq!(response.topics.len(), 1);
    assert_eq!(response.topics[0].error_code, 3);
}

#[tokio::test]
async fn metadata_uses_registered_broker_list_when_present() {
    let broker = test_broker();
    let registration = broker
        .cluster()
        .handle_register_broker(
            crate::cluster::RegisterBrokerRequest {
                node_id: 7,
                advertised_host: "registered-broker".to_string(),
                advertised_port: 39092,
            },
            1,
        )
        .unwrap();
    assert!(registration.accepted);

    let response = handle_metadata(&broker, MetadataRequest::default())
        .await
        .unwrap();

    assert!(
        response
            .brokers
            .iter()
            .any(|entry| entry.node_id.0 == 7 && entry.host.to_string() == "registered-broker")
    );
}

#[tokio::test]
async fn metadata_auto_create_with_unnamed_topics_is_noop() {
    let broker = test_broker();
    let request = MetadataRequest::default()
        .with_allow_auto_topic_creation(true)
        .with_topics(Some(vec![MetadataRequestTopic::default().with_name(None)]));

    let response = handle_metadata(&broker, request).await.unwrap();

    assert!(response.topics.is_empty());
    let metadata = broker.store().topic_metadata(None, 0).unwrap();
    assert!(metadata.is_empty());
}

#[tokio::test]
async fn metadata_auto_create_enabled_without_topics_does_not_create() {
    let broker = test_broker();
    let request = MetadataRequest::default()
        .with_allow_auto_topic_creation(true)
        .with_topics(None);

    let response = handle_metadata(&broker, request).await.unwrap();

    assert!(response.topics.is_empty());
    let metadata = broker.store().topic_metadata(None, 0).unwrap();
    assert!(metadata.is_empty());
}

#[tokio::test]
async fn metadata_with_explicit_empty_topic_list_returns_empty_topics() {
    let broker = test_broker();
    let request = MetadataRequest::default().with_topics(Some(vec![]));

    let response = handle_metadata(&broker, request).await.unwrap();

    assert!(response.topics.is_empty());
}

fn test_broker() -> KafkaBroker {
    let dir = tempdir().unwrap().keep();
    let config = Config::single_node(dir.join("data"), 9092, 1);
    let store = Arc::new(FileStore::open(&config.storage.data_dir).unwrap());
    KafkaBroker::new(config, store).unwrap()
}

fn non_writable_broker() -> KafkaBroker {
    let dir = tempdir().unwrap().keep();
    let mut config = Config::single_node(dir.join("data"), 9092, 1);
    config.cluster.node_id = 2;
    config.cluster.process_roles = vec![
        crate::cluster::ProcessRole::Broker,
        crate::cluster::ProcessRole::Controller,
    ];
    config.cluster.controller_quorum_voters = vec![
        crate::cluster::ControllerQuorumVoter {
            node_id: 1,
            host: "node1".to_string(),
            port: 9093,
        },
        crate::cluster::ControllerQuorumVoter {
            node_id: 2,
            host: "node2".to_string(),
            port: 9094,
        },
    ];
    let store = Arc::new(FileStore::open(&config.storage.data_dir).unwrap());
    KafkaBroker::new(config, store).unwrap()
}
