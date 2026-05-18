use super::*;

#[test]
fn local_transport_dispatches_register_and_heartbeat() {
    let dir = tempdir().unwrap();
    let mut config = Config::single_node(dir.path().join("data"), 19092, 1);
    config.cluster.node_id = 4;
    config.cluster.process_roles = vec![ProcessRole::Broker, ProcessRole::Controller];
    let runtime = ClusterRuntime::from_config(&config).unwrap();
    let transport = LocalClusterRpcTransport::new(runtime.clone());

    let registration = transport
        .register_broker(RegisterBrokerRequest {
            node_id: 9,
            advertised_host: "broker-9.local".to_string(),
            advertised_port: 39092,
        })
        .unwrap();
    let heartbeat = transport
        .broker_heartbeat(BrokerHeartbeatRequest {
            node_id: 9,
            broker_epoch: registration.broker_epoch,
            timestamp_ms: 700,
        })
        .unwrap();

    assert_eq!(registration.leader_id, Some(4));
    assert!(heartbeat.accepted);
    assert!(
        runtime
            .metadata_image()
            .brokers
            .iter()
            .any(|broker| broker.node_id == 9)
    );
}

#[test]
fn local_transport_dispatches_append_metadata() {
    let dir = tempdir().unwrap();
    let mut config = Config::single_node(dir.path().join("data"), 19092, 1);
    config.cluster.node_id = 4;
    config.cluster.process_roles = vec![ProcessRole::Broker, ProcessRole::Controller];
    let runtime = ClusterRuntime::from_config(&config).unwrap();
    let transport = LocalClusterRpcTransport::new(runtime.clone());

    let response = transport
        .append_metadata(AppendMetadataRequest {
            term: 1,
            leader_id: 4,
            prev_metadata_offset: runtime.metadata_image().metadata_offset,
            records: vec![crate::cluster::MetadataRecord::SetController { controller_id: 4 }],
        })
        .unwrap();

    assert!(response.accepted);
    assert_eq!(
        response.last_metadata_offset,
        runtime.metadata_image().metadata_offset
    );
}

#[test]
fn local_transport_dispatches_partition_leader_update() {
    let dir = tempdir().unwrap();
    let mut config = Config::single_node(dir.path().join("data"), 19092, 1);
    config.cluster.node_id = 4;
    config.cluster.process_roles = vec![ProcessRole::Broker, ProcessRole::Controller];
    let runtime = ClusterRuntime::from_config(&config).unwrap();
    runtime
        .sync_local_topics(
            &[crate::store::TopicMetadata {
                name: "leader.topic".to_string(),
                partitions: vec![crate::store::PartitionMetadata { partition: 0 }],
            }],
            1,
        )
        .unwrap();
    let transport = LocalClusterRpcTransport::new(runtime.clone());

    let response = transport
        .update_partition_leader(UpdatePartitionLeaderRequest {
            topic_name: "leader.topic".to_string(),
            partition_index: 0,
            leader_id: 9,
            leader_epoch: 1,
        })
        .unwrap();

    assert!(response.accepted);
    assert_eq!(
        runtime
            .metadata_image()
            .partition_leader_id("leader.topic", 0),
        Some(9)
    );
}

#[test]
fn local_transport_dispatches_partition_replication_update() {
    let dir = tempdir().unwrap();
    let mut config = Config::single_node(dir.path().join("data"), 19092, 1);
    config.cluster.node_id = 4;
    config.cluster.process_roles = vec![ProcessRole::Broker, ProcessRole::Controller];
    let runtime = ClusterRuntime::from_config(&config).unwrap();
    runtime
        .sync_local_topics(
            &[crate::store::TopicMetadata {
                name: "replication.topic".to_string(),
                partitions: vec![crate::store::PartitionMetadata { partition: 0 }],
            }],
            1,
        )
        .unwrap();
    let transport = LocalClusterRpcTransport::new(runtime.clone());

    let response = transport
        .update_partition_replication(UpdatePartitionReplicationRequest {
            topic_name: "replication.topic".to_string(),
            partition_index: 0,
            replicas: vec![1, 2, 3],
            isr: vec![1, 2],
            leader_epoch: 2,
        })
        .unwrap();

    assert!(response.accepted);
    let image = runtime.metadata_image();
    assert_eq!(image.topics[0].partitions[0].replicas, vec![1, 2, 3]);
    assert_eq!(image.topics[0].partitions[0].isr, vec![1, 2]);
}

#[test]
fn local_transport_dispatches_replica_progress_update() {
    let dir = tempdir().unwrap();
    let mut config = Config::single_node(dir.path().join("data"), 19092, 1);
    config.cluster.node_id = 4;
    config.cluster.process_roles = vec![ProcessRole::Broker, ProcessRole::Controller];
    let runtime = ClusterRuntime::from_config(&config).unwrap();
    runtime
        .sync_local_topics(
            &[crate::store::TopicMetadata {
                name: "progress.topic".to_string(),
                partitions: vec![crate::store::PartitionMetadata { partition: 0 }],
            }],
            1,
        )
        .unwrap();
    let transport = LocalClusterRpcTransport::new(runtime.clone());
    transport
        .update_partition_replication(UpdatePartitionReplicationRequest {
            topic_name: "progress.topic".to_string(),
            partition_index: 0,
            replicas: vec![1, 2],
            isr: vec![1, 2],
            leader_epoch: 1,
        })
        .unwrap();

    transport
        .update_replica_progress(UpdateReplicaProgressRequest {
            topic_name: "progress.topic".to_string(),
            partition_index: 0,
            leader_epoch: 1,
            broker_id: 1,
            log_end_offset: 10,
            last_caught_up_ms: 100,
        })
        .unwrap();
    let response = transport
        .update_replica_progress(UpdateReplicaProgressRequest {
            topic_name: "progress.topic".to_string(),
            partition_index: 0,
            leader_epoch: 1,
            broker_id: 2,
            log_end_offset: 8,
            last_caught_up_ms: 100,
        })
        .unwrap();

    assert!(response.accepted);
    assert_eq!(response.high_watermark, 10);
}
