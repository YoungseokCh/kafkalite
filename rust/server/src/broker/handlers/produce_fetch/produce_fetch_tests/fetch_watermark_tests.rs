use super::*;

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
        .handle_update_partition_replication(crate::cluster::UpdatePartitionReplicationRequest {
            topic_name: "hw.topic".to_string(),
            partition_index: 0,
            replicas: vec![1, 2],
            isr: vec![1, 2],
            leader_epoch: 1,
        })
        .unwrap();
    broker
        .cluster()
        .handle_update_replica_progress(crate::cluster::UpdateReplicaProgressRequest {
            topic_name: "hw.topic".to_string(),
            partition_index: 0,
            leader_epoch: 1,
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
            leader_epoch: 1,
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
async fn empty_fetch_uses_metadata_high_watermark_when_replication_progress_exists() {
    let broker = test_broker();
    broker.store().ensure_topic("hw.empty.topic", 1, 0).unwrap();
    let metadata = broker
        .store()
        .topic_metadata(Some(&["hw.empty.topic".to_string()]), 0)
        .unwrap();
    broker.sync_topic_metadata(&metadata).unwrap();

    let _ = handle_produce(&broker, produce_request("hw.empty.topic", -1, -1, 0))
        .await
        .unwrap();

    broker
        .cluster()
        .handle_update_partition_replication(crate::cluster::UpdatePartitionReplicationRequest {
            topic_name: "hw.empty.topic".to_string(),
            partition_index: 0,
            replicas: vec![1, 2],
            isr: vec![1, 2],
            leader_epoch: 1,
        })
        .unwrap();
    broker
        .cluster()
        .handle_update_replica_progress(crate::cluster::UpdateReplicaProgressRequest {
            topic_name: "hw.empty.topic".to_string(),
            partition_index: 0,
            leader_epoch: 1,
            broker_id: 1,
            log_end_offset: 1,
            last_caught_up_ms: 100,
        })
        .unwrap();
    broker
        .cluster()
        .handle_update_replica_progress(crate::cluster::UpdateReplicaProgressRequest {
            topic_name: "hw.empty.topic".to_string(),
            partition_index: 0,
            leader_epoch: 1,
            broker_id: 2,
            log_end_offset: 0,
            last_caught_up_ms: 100,
        })
        .unwrap();

    let fetch = handle_fetch(
        &broker,
        FetchRequest::default().with_topics(vec![
            kafka_protocol::messages::fetch_request::FetchTopic::default()
                .with_topic(TopicName(StrBytes::from("hw.empty.topic".to_string())))
                .with_partitions(vec![
                    kafka_protocol::messages::fetch_request::FetchPartition::default()
                        .with_partition(0)
                        .with_fetch_offset(1)
                        .with_partition_max_bytes(1024),
                ]),
        ]),
    )
    .await
    .unwrap();

    let partition = &fetch.responses[0].partitions[0];
    assert_eq!(partition.high_watermark, 0);
    assert_eq!(partition.last_stable_offset, 0);
    assert!(partition.records.as_ref().unwrap().is_empty());
}
