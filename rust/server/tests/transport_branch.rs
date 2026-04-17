use std::sync::Arc;

use bytes::Bytes;
use kafkalite_server::cluster::{
    ClusterRpcRequest, ClusterRpcResponse, ClusterRpcTarget, ClusterRpcTransport, ClusterRuntime,
    ControllerQuorumVoter, InMemoryClusterNetwork, InMemoryRemoteClusterRpcTransport, ProcessRole,
    ReplicaFetchRequest, VoteRequest,
};
use kafkalite_server::config::Config;
use kafkalite_server::store::{BrokerRecord, FileStore, Storage};
use tempfile::tempdir;

fn voters() -> Vec<ControllerQuorumVoter> {
    vec![
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
    ]
}

fn controller_config(node_id: i32) -> Config {
    let dir = tempdir().unwrap().keep();
    let mut config = Config::single_node(
        dir.join(format!("node-{node_id}")),
        19090 + node_id as u16,
        1,
    );
    config.cluster.node_id = node_id;
    config.cluster.process_roles = vec![ProcessRole::Controller];
    config.cluster.controller_quorum_voters = voters();
    config
}

fn target(node_id: i32) -> ClusterRpcTarget {
    ClusterRpcTarget {
        node_id,
        host: format!("node{node_id}"),
        port: 9092 + node_id as u16,
    }
}

fn replica_record(offset: i64) -> BrokerRecord {
    BrokerRecord {
        offset,
        timestamp_ms: 100 + offset,
        producer_id: -1,
        producer_epoch: -1,
        sequence: offset as i32,
        key: Some(Bytes::from_static(b"key")),
        value: Some(Bytes::from_static(b"value")),
        headers_json: vec![],
    }
}

#[test]
fn in_memory_transport_routes_non_replica_requests_to_registered_runtime() {
    let network = InMemoryClusterNetwork::default();
    let runtime = ClusterRuntime::from_config(&controller_config(2)).unwrap();
    network.register(2, runtime);
    let transport = InMemoryRemoteClusterRpcTransport::new(&controller_config(1).cluster, network);

    let response = transport
        .send_to(
            &target(2),
            ClusterRpcRequest::Vote(VoteRequest {
                term: 7,
                candidate_id: 1,
                last_metadata_offset: -1,
            }),
        )
        .unwrap();

    assert!(matches!(
        response,
        ClusterRpcResponse::Vote(ref vote) if vote.term == 7
    ));
}

#[test]
fn in_memory_transport_replica_fetch_reports_missing_without_registered_store() {
    let transport = InMemoryRemoteClusterRpcTransport::new(
        &controller_config(1).cluster,
        InMemoryClusterNetwork::default(),
    );

    let response = transport
        .replica_fetch_to(
            &target(2),
            ReplicaFetchRequest {
                topic_name: "missing.topic".to_string(),
                partition_index: 0,
                start_offset: 0,
                max_records: 10,
            },
        )
        .unwrap();

    assert!(!response.found);
    assert_eq!(response.leader_log_end_offset, -1);
    assert!(response.records.is_empty());
}

#[test]
fn in_memory_transport_replica_fetch_reads_from_registered_store() {
    let network = InMemoryClusterNetwork::default();
    let store_dir = tempdir().unwrap();
    let store = Arc::new(FileStore::open(store_dir.path().join("node-2-data")).unwrap());
    store.ensure_topic("replicated.topic", 1, 0).unwrap();
    store
        .append_replica_records("replicated.topic", 0, &[replica_record(0)], 101)
        .unwrap();
    network.register_store(2, store);
    let transport = InMemoryRemoteClusterRpcTransport::new(&controller_config(1).cluster, network);

    let response = transport
        .replica_fetch_to(
            &target(2),
            ReplicaFetchRequest {
                topic_name: "replicated.topic".to_string(),
                partition_index: 0,
                start_offset: 0,
                max_records: 10,
            },
        )
        .unwrap();

    assert!(response.found);
    assert_eq!(response.leader_log_end_offset, 1);
    assert_eq!(response.records.len(), 1);
    assert_eq!(response.records[0].offset, 0);
}
