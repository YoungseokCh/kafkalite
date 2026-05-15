use anyhow::Result;
use kafka_protocol::messages::create_topics_response::CreatableTopicResult;
use kafka_protocol::messages::{CreateTopicsRequest, CreateTopicsResponse, TopicName};
use kafka_protocol::protocol::StrBytes;

use super::super::KafkaBroker;

const INVALID_PARTITIONS: i16 = 37;
const INVALID_REPLICATION_FACTOR: i16 = 38;
const INVALID_REPLICA_ASSIGNMENT: i16 = 39;
const NOT_CONTROLLER: i16 = 41;

pub async fn handle_create_topics(
    broker: &KafkaBroker,
    request: CreateTopicsRequest,
) -> Result<CreateTopicsResponse> {
    let now_ms = chrono::Utc::now().timestamp_millis();
    let mut results = Vec::new();
    let mut created_names = Vec::new();

    for topic in request.topics {
        let name = topic.name.to_string();
        let partition_count = partition_count(&topic, broker.config().storage.default_partitions);
        let error_code = validate_topic_request(broker, &topic, partition_count);

        if error_code == 0 && !request.validate_only {
            broker
                .store()
                .ensure_topic(&name, partition_count, now_ms)?;
            created_names.push(name.clone());
        }

        results.push(topic_result(name, error_code, partition_count));
    }

    if !created_names.is_empty() {
        let metadata = broker
            .store()
            .topic_metadata(Some(&created_names), now_ms)?;
        broker.sync_topic_metadata(&metadata)?;
    }

    Ok(CreateTopicsResponse::default()
        .with_throttle_time_ms(0)
        .with_topics(results))
}

fn partition_count(
    topic: &kafka_protocol::messages::create_topics_request::CreatableTopic,
    default_partitions: i32,
) -> i32 {
    if !topic.assignments.is_empty() {
        topic.assignments.len() as i32
    } else if topic.num_partitions == -1 {
        default_partitions
    } else {
        topic.num_partitions
    }
}

fn validate_topic_request(
    broker: &KafkaBroker,
    topic: &kafka_protocol::messages::create_topics_request::CreatableTopic,
    partition_count: i32,
) -> i16 {
    if !broker.cluster().can_auto_create_topics_locally() {
        return NOT_CONTROLLER;
    }
    if partition_count <= 0 {
        return INVALID_PARTITIONS;
    }
    if topic.replication_factor > 1 {
        return INVALID_REPLICATION_FACTOR;
    }
    if topic
        .assignments
        .iter()
        .any(|assignment| assignment.broker_ids.len() > 1)
    {
        return INVALID_REPLICA_ASSIGNMENT;
    }
    0
}

fn topic_result(name: String, error_code: i16, partition_count: i32) -> CreatableTopicResult {
    CreatableTopicResult::default()
        .with_name(TopicName(StrBytes::from(name)))
        .with_error_code(error_code)
        .with_error_message(None)
        .with_num_partitions(partition_count)
        .with_replication_factor(1)
        .with_configs(None)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use kafka_protocol::messages::create_topics_request::CreatableTopic;
    use tempfile::tempdir;

    use super::*;
    use crate::broker::KafkaBroker;
    use crate::config::Config;
    use crate::store::FileStore;

    #[tokio::test]
    async fn create_topics_materializes_and_syncs_metadata() {
        let broker = test_broker(3);
        let request = CreateTopicsRequest::default().with_topics(vec![
            CreatableTopic::default()
                .with_name(TopicName(StrBytes::from("admin.topic".to_string())))
                .with_num_partitions(2)
                .with_replication_factor(1),
        ]);

        let response = handle_create_topics(&broker, request).await.unwrap();

        assert_eq!(response.topics[0].error_code, 0);
        assert_eq!(response.topics[0].num_partitions, 2);
        assert_eq!(
            broker
                .cluster()
                .metadata_image()
                .partition_leader_id("admin.topic", 1),
            Some(1)
        );
    }

    #[tokio::test]
    async fn create_topics_validate_only_does_not_materialize() {
        let broker = test_broker(1);
        let request = CreateTopicsRequest::default()
            .with_validate_only(true)
            .with_topics(vec![
                CreatableTopic::default()
                    .with_name(TopicName(StrBytes::from("dry.topic".to_string())))
                    .with_num_partitions(1)
                    .with_replication_factor(1),
            ]);

        let response = handle_create_topics(&broker, request).await.unwrap();

        assert_eq!(response.topics[0].error_code, 0);
        assert!(broker.store().topic_metadata(None, 0).unwrap().is_empty());
    }

    fn test_broker(default_partitions: i32) -> KafkaBroker {
        let dir = tempdir().unwrap().keep();
        let config = Config::single_node(dir.join("data"), 9092, default_partitions);
        let store = Arc::new(FileStore::open(&config.storage.data_dir).unwrap());
        KafkaBroker::new(config, store).unwrap()
    }
}
