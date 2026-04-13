use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use anyhow::{Result, bail};

use crate::cluster::rpc::{
    AdvancePartitionReassignmentRequest, AppendMetadataRequest, AppendMetadataResponse,
    BeginPartitionReassignmentRequest, BrokerHeartbeatRequest, BrokerHeartbeatResponse,
    GetPartitionStateRequest, GetPartitionStateResponse, PartitionReassignmentResponse,
    RegisterBrokerRequest, RegisterBrokerResponse, ReplicaFetchRequest, ReplicaFetchResponse,
    UpdatePartitionLeaderRequest, UpdatePartitionLeaderResponse, UpdatePartitionReplicationRequest,
    UpdatePartitionReplicationResponse, UpdateReplicaProgressRequest,
    UpdateReplicaProgressResponse,
};
use crate::cluster::{ClusterConfig, ClusterRuntime};
use crate::store::{BrokerRecord, Storage};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClusterRpcRequest {
    AppendMetadata(AppendMetadataRequest),
    RegisterBroker(RegisterBrokerRequest),
    BrokerHeartbeat(BrokerHeartbeatRequest),
    UpdatePartitionLeader(UpdatePartitionLeaderRequest),
    UpdatePartitionReplication(UpdatePartitionReplicationRequest),
    UpdateReplicaProgress(UpdateReplicaProgressRequest),
    GetPartitionState(GetPartitionStateRequest),
    ReplicaFetch(ReplicaFetchRequest),
    BeginPartitionReassignment(BeginPartitionReassignmentRequest),
    AdvancePartitionReassignment(AdvancePartitionReassignmentRequest),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClusterRpcResponse {
    AppendMetadata(AppendMetadataResponse),
    RegisterBroker(RegisterBrokerResponse),
    BrokerHeartbeat(BrokerHeartbeatResponse),
    UpdatePartitionLeader(UpdatePartitionLeaderResponse),
    UpdatePartitionReplication(UpdatePartitionReplicationResponse),
    UpdateReplicaProgress(UpdateReplicaProgressResponse),
    GetPartitionState(GetPartitionStateResponse),
    ReplicaFetch(ReplicaFetchResponse),
    BeginPartitionReassignment(PartitionReassignmentResponse),
    AdvancePartitionReassignment(PartitionReassignmentResponse),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterRpcTarget {
    pub node_id: i32,
    pub host: String,
    pub port: u16,
}

pub trait ClusterRpcTransport {
    fn send(&self, request: ClusterRpcRequest) -> Result<ClusterRpcResponse>;

    fn send_to(
        &self,
        _target: &ClusterRpcTarget,
        request: ClusterRpcRequest,
    ) -> Result<ClusterRpcResponse> {
        self.send(request)
    }

    fn register_broker(&self, request: RegisterBrokerRequest) -> Result<RegisterBrokerResponse> {
        match self.send(ClusterRpcRequest::RegisterBroker(request))? {
            ClusterRpcResponse::RegisterBroker(response) => Ok(response),
            other => bail!("unexpected RPC response: {other:?}"),
        }
    }

    fn append_metadata(&self, request: AppendMetadataRequest) -> Result<AppendMetadataResponse> {
        match self.send(ClusterRpcRequest::AppendMetadata(request))? {
            ClusterRpcResponse::AppendMetadata(response) => Ok(response),
            other => bail!("unexpected RPC response: {other:?}"),
        }
    }

    fn broker_heartbeat(&self, request: BrokerHeartbeatRequest) -> Result<BrokerHeartbeatResponse> {
        match self.send(ClusterRpcRequest::BrokerHeartbeat(request))? {
            ClusterRpcResponse::BrokerHeartbeat(response) => Ok(response),
            other => bail!("unexpected RPC response: {other:?}"),
        }
    }

    fn update_partition_leader(
        &self,
        request: UpdatePartitionLeaderRequest,
    ) -> Result<UpdatePartitionLeaderResponse> {
        match self.send(ClusterRpcRequest::UpdatePartitionLeader(request))? {
            ClusterRpcResponse::UpdatePartitionLeader(response) => Ok(response),
            other => bail!("unexpected RPC response: {other:?}"),
        }
    }

    fn update_partition_replication(
        &self,
        request: UpdatePartitionReplicationRequest,
    ) -> Result<UpdatePartitionReplicationResponse> {
        match self.send(ClusterRpcRequest::UpdatePartitionReplication(request))? {
            ClusterRpcResponse::UpdatePartitionReplication(response) => Ok(response),
            other => bail!("unexpected RPC response: {other:?}"),
        }
    }

    fn update_replica_progress(
        &self,
        request: UpdateReplicaProgressRequest,
    ) -> Result<UpdateReplicaProgressResponse> {
        match self.send(ClusterRpcRequest::UpdateReplicaProgress(request))? {
            ClusterRpcResponse::UpdateReplicaProgress(response) => Ok(response),
            other => bail!("unexpected RPC response: {other:?}"),
        }
    }

    fn get_partition_state(
        &self,
        request: GetPartitionStateRequest,
    ) -> Result<GetPartitionStateResponse> {
        match self.send(ClusterRpcRequest::GetPartitionState(request))? {
            ClusterRpcResponse::GetPartitionState(response) => Ok(response),
            other => bail!("unexpected RPC response: {other:?}"),
        }
    }

    fn replica_fetch_to(
        &self,
        target: &ClusterRpcTarget,
        request: ReplicaFetchRequest,
    ) -> Result<ReplicaFetchResponse> {
        match self.send_to(target, ClusterRpcRequest::ReplicaFetch(request))? {
            ClusterRpcResponse::ReplicaFetch(response) => Ok(response),
            other => bail!("unexpected RPC response: {other:?}"),
        }
    }

    fn begin_partition_reassignment(
        &self,
        request: BeginPartitionReassignmentRequest,
    ) -> Result<PartitionReassignmentResponse> {
        match self.send(ClusterRpcRequest::BeginPartitionReassignment(request))? {
            ClusterRpcResponse::BeginPartitionReassignment(response) => Ok(response),
            other => bail!("unexpected RPC response: {other:?}"),
        }
    }

    fn advance_partition_reassignment(
        &self,
        request: AdvancePartitionReassignmentRequest,
    ) -> Result<PartitionReassignmentResponse> {
        match self.send(ClusterRpcRequest::AdvancePartitionReassignment(request))? {
            ClusterRpcResponse::AdvancePartitionReassignment(response) => Ok(response),
            other => bail!("unexpected RPC response: {other:?}"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct RemoteClusterRpcTransport {
    local_node_id: i32,
    routes: BTreeMap<i32, ClusterRpcTarget>,
}

impl RemoteClusterRpcTransport {
    pub fn new(config: &ClusterConfig) -> Self {
        let routes = config
            .controller_quorum_voters
            .iter()
            .map(|voter| {
                (
                    voter.node_id,
                    ClusterRpcTarget {
                        node_id: voter.node_id,
                        host: voter.host.clone(),
                        port: voter.port,
                    },
                )
            })
            .collect();
        Self {
            local_node_id: config.node_id,
            routes,
        }
    }

    pub fn resolve_target(&self, node_id: i32) -> Result<ClusterRpcTarget> {
        self.routes
            .get(&node_id)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("unknown cluster RPC target node {node_id}"))
    }

    pub fn local_node_id(&self) -> i32 {
        self.local_node_id
    }
}

#[derive(Clone, Default)]
pub struct InMemoryClusterNetwork {
    runtimes: Arc<Mutex<BTreeMap<i32, ClusterRuntime>>>,
    stores: Arc<Mutex<BTreeMap<i32, Arc<dyn Storage>>>>,
}

impl InMemoryClusterNetwork {
    pub fn register(&self, node_id: i32, runtime: ClusterRuntime) {
        self.runtimes
            .lock()
            .expect("in-memory cluster network mutex poisoned")
            .insert(node_id, runtime);
    }

    pub fn register_store(&self, node_id: i32, store: Arc<dyn Storage>) {
        self.stores
            .lock()
            .expect("in-memory cluster network store mutex poisoned")
            .insert(node_id, store);
    }

    fn dispatch(&self, node_id: i32, request: ClusterRpcRequest) -> Result<ClusterRpcResponse> {
        if let ClusterRpcRequest::ReplicaFetch(request) = request {
            return self.dispatch_replica_fetch(node_id, request);
        }
        let runtime = self
            .runtimes
            .lock()
            .expect("in-memory cluster network mutex poisoned")
            .get(&node_id)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("unregistered cluster runtime for node {node_id}"))?;
        runtime.dispatch(request)
    }

    fn dispatch_replica_fetch(
        &self,
        node_id: i32,
        request: ReplicaFetchRequest,
    ) -> Result<ClusterRpcResponse> {
        let Some(store) = self
            .stores
            .lock()
            .expect("in-memory cluster network store mutex poisoned")
            .get(&node_id)
            .cloned()
        else {
            return Ok(ClusterRpcResponse::ReplicaFetch(ReplicaFetchResponse {
                found: false,
                high_watermark: -1,
                leader_log_end_offset: -1,
                records: Vec::<BrokerRecord>::new(),
            }));
        };
        match store.fetch_records(
            &request.topic_name,
            request.partition_index,
            request.start_offset,
            request.max_records,
        ) {
            Ok(fetched) => {
                let (_, latest) =
                    store.list_offsets(&request.topic_name, request.partition_index)?;
                Ok(ClusterRpcResponse::ReplicaFetch(ReplicaFetchResponse {
                    found: true,
                    high_watermark: fetched.high_watermark,
                    leader_log_end_offset: latest.offset,
                    records: fetched.records,
                }))
            }
            Err(crate::store::StoreError::UnknownTopicOrPartition { .. }) => {
                Ok(ClusterRpcResponse::ReplicaFetch(ReplicaFetchResponse {
                    found: false,
                    high_watermark: -1,
                    leader_log_end_offset: -1,
                    records: Vec::new(),
                }))
            }
            Err(err) => Err(err.into()),
        }
    }
}

#[derive(Clone)]
pub struct InMemoryRemoteClusterRpcTransport {
    remote: RemoteClusterRpcTransport,
    network: InMemoryClusterNetwork,
}

impl InMemoryRemoteClusterRpcTransport {
    pub fn new(config: &ClusterConfig, network: InMemoryClusterNetwork) -> Self {
        Self {
            remote: RemoteClusterRpcTransport::new(config),
            network,
        }
    }

    pub fn resolve_target(&self, node_id: i32) -> Result<ClusterRpcTarget> {
        self.remote.resolve_target(node_id)
    }
}

impl ClusterRpcTransport for RemoteClusterRpcTransport {
    fn send(&self, _request: ClusterRpcRequest) -> Result<ClusterRpcResponse> {
        bail!("remote cluster rpc requires a target node")
    }

    fn send_to(
        &self,
        target: &ClusterRpcTarget,
        request: ClusterRpcRequest,
    ) -> Result<ClusterRpcResponse> {
        bail!(
            "remote cluster rpc not implemented yet for target {}@{}:{} and request {:?}",
            target.node_id,
            target.host,
            target.port,
            request
        )
    }
}

impl ClusterRpcTransport for InMemoryRemoteClusterRpcTransport {
    fn send(&self, _request: ClusterRpcRequest) -> Result<ClusterRpcResponse> {
        bail!("in-memory remote cluster rpc requires a target node")
    }

    fn send_to(
        &self,
        target: &ClusterRpcTarget,
        request: ClusterRpcRequest,
    ) -> Result<ClusterRpcResponse> {
        let resolved = self.remote.resolve_target(target.node_id)?;
        self.network.dispatch(resolved.node_id, request)
    }
}

#[derive(Debug, Clone)]
pub struct LocalClusterRpcTransport {
    runtime: ClusterRuntime,
}

impl LocalClusterRpcTransport {
    pub fn new(runtime: ClusterRuntime) -> Self {
        Self { runtime }
    }
}

impl ClusterRpcTransport for LocalClusterRpcTransport {
    fn send(&self, request: ClusterRpcRequest) -> Result<ClusterRpcResponse> {
        self.runtime.dispatch(request)
    }
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use crate::cluster::{
        ClusterRuntime, ControllerQuorumVoter, ProcessRole, test_support::TwoNodeClusterHarness,
    };
    use crate::config::Config;

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
                broker_id: 1,
                log_end_offset: 10,
                last_caught_up_ms: 100,
            })
            .unwrap();
        let response = transport
            .update_replica_progress(UpdateReplicaProgressRequest {
                topic_name: "progress.topic".to_string(),
                partition_index: 0,
                broker_id: 2,
                log_end_offset: 8,
                last_caught_up_ms: 100,
            })
            .unwrap();

        assert!(response.accepted);
        assert_eq!(response.high_watermark, 10);
    }

    #[test]
    fn remote_transport_resolves_target_from_quorum_voters() {
        let mut config = ClusterConfig {
            node_id: 1,
            ..ClusterConfig::default()
        };
        config.controller_quorum_voters = vec![
            ControllerQuorumVoter {
                node_id: 1,
                host: "node1".to_string(),
                port: 9093,
            },
            ControllerQuorumVoter {
                node_id: 2,
                host: "node2".to_string(),
                port: 9093,
            },
        ];

        let transport = RemoteClusterRpcTransport::new(&config);
        let target = transport.resolve_target(2).unwrap();

        assert_eq!(transport.local_node_id(), 1);
        assert_eq!(target.node_id, 2);
        assert_eq!(target.host, "node2");
        assert_eq!(target.port, 9093);
    }

    #[test]
    fn remote_transport_rejects_unknown_target() {
        let transport = RemoteClusterRpcTransport::new(&ClusterConfig::default());
        let err = transport.resolve_target(99).unwrap_err().to_string();

        assert!(err.contains("unknown cluster RPC target node 99"));
    }

    #[test]
    fn in_memory_remote_transport_dispatches_to_registered_runtime() {
        let harness = TwoNodeClusterHarness::new_controller_pair();
        let mut config1 =
            Config::single_node(tempdir().unwrap().path().join("node1-client"), 19092, 1);
        config1.cluster.node_id = 1;
        config1.cluster.process_roles = vec![ProcessRole::Controller];
        config1.cluster.controller_quorum_voters = vec![
            ControllerQuorumVoter {
                node_id: 1,
                host: "node1".to_string(),
                port: 9093,
            },
            ControllerQuorumVoter {
                node_id: 2,
                host: "node2".to_string(),
                port: 9093,
            },
        ];
        let transport = InMemoryRemoteClusterRpcTransport::new(&config1.cluster, harness.network);
        let target = transport.remote.resolve_target(2).unwrap();

        let response = transport
            .send_to(
                &target,
                ClusterRpcRequest::AppendMetadata(AppendMetadataRequest {
                    term: 2,
                    leader_id: 1,
                    prev_metadata_offset: harness.node2.runtime.metadata_image().metadata_offset,
                    records: vec![crate::cluster::MetadataRecord::SetController {
                        controller_id: 1,
                    }],
                }),
            )
            .unwrap();

        assert!(matches!(response, ClusterRpcResponse::AppendMetadata(_)));
        assert_eq!(harness.node2.runtime.metadata_image().controller_id, 1);
    }

    #[test]
    fn in_memory_remote_transport_propagates_replication_scenario() {
        let harness = TwoNodeClusterHarness::new_controller_pair();
        harness
            .node2
            .runtime
            .sync_local_topics(
                &[crate::store::TopicMetadata {
                    name: "replicated.topic".to_string(),
                    partitions: vec![crate::store::PartitionMetadata { partition: 0 }],
                }],
                2,
            )
            .unwrap();

        let mut config1 =
            Config::single_node(tempdir().unwrap().path().join("node1-client"), 19092, 1);
        config1.cluster.node_id = 1;
        config1.cluster.process_roles = vec![ProcessRole::Controller];
        config1.cluster.controller_quorum_voters = vec![
            ControllerQuorumVoter {
                node_id: 1,
                host: "node1".to_string(),
                port: 9093,
            },
            ControllerQuorumVoter {
                node_id: 2,
                host: "node2".to_string(),
                port: 9093,
            },
        ];
        let transport = InMemoryRemoteClusterRpcTransport::new(&config1.cluster, harness.network);
        let target = transport.remote.resolve_target(2).unwrap();

        let _ = transport
            .send_to(
                &target,
                ClusterRpcRequest::UpdatePartitionLeader(UpdatePartitionLeaderRequest {
                    topic_name: "replicated.topic".to_string(),
                    partition_index: 0,
                    leader_id: 1,
                    leader_epoch: 1,
                }),
            )
            .unwrap();
        let _ = transport
            .send_to(
                &target,
                ClusterRpcRequest::UpdatePartitionReplication(UpdatePartitionReplicationRequest {
                    topic_name: "replicated.topic".to_string(),
                    partition_index: 0,
                    replicas: vec![1, 2],
                    isr: vec![1, 2],
                    leader_epoch: 1,
                }),
            )
            .unwrap();
        let _ = transport
            .send_to(
                &target,
                ClusterRpcRequest::UpdateReplicaProgress(UpdateReplicaProgressRequest {
                    topic_name: "replicated.topic".to_string(),
                    partition_index: 0,
                    broker_id: 1,
                    log_end_offset: 11,
                    last_caught_up_ms: 100,
                }),
            )
            .unwrap();
        let response = transport
            .send_to(
                &target,
                ClusterRpcRequest::UpdateReplicaProgress(UpdateReplicaProgressRequest {
                    topic_name: "replicated.topic".to_string(),
                    partition_index: 0,
                    broker_id: 2,
                    log_end_offset: 9,
                    last_caught_up_ms: 100,
                }),
            )
            .unwrap();

        let ClusterRpcResponse::UpdateReplicaProgress(response) = response else {
            panic!("unexpected response variant");
        };
        assert_eq!(response.high_watermark, 11);
        assert_eq!(
            harness
                .node2
                .runtime
                .metadata_image()
                .partition_high_watermark("replicated.topic", 0),
            Some(11)
        );
    }

    #[test]
    fn in_memory_remote_transport_applies_controller_failover() {
        let harness = TwoNodeClusterHarness::new_controller_pair();
        let transport = harness.transport_from_node1();
        let target = transport.resolve_target(2).unwrap();

        let _ = transport
            .send_to(
                &target,
                ClusterRpcRequest::AppendMetadata(AppendMetadataRequest {
                    term: 2,
                    leader_id: 1,
                    prev_metadata_offset: harness.node2.runtime.metadata_image().metadata_offset,
                    records: vec![crate::cluster::MetadataRecord::SetController {
                        controller_id: 1,
                    }],
                }),
            )
            .unwrap();
        let after_first = harness.node2.runtime.quorum_snapshot();
        assert_eq!(after_first.leader_id, Some(1));
        assert_eq!(after_first.current_term, 2);

        let _ = transport
            .send_to(
                &target,
                ClusterRpcRequest::AppendMetadata(AppendMetadataRequest {
                    term: 3,
                    leader_id: 2,
                    prev_metadata_offset: harness.node2.runtime.metadata_image().metadata_offset,
                    records: vec![crate::cluster::MetadataRecord::SetController {
                        controller_id: 2,
                    }],
                }),
            )
            .unwrap();
        let after_failover = harness.node2.runtime.quorum_snapshot();
        assert_eq!(after_failover.leader_id, Some(2));
        assert_eq!(after_failover.current_term, 3);
        assert_eq!(harness.node2.runtime.metadata_image().controller_id, 2);
    }

    #[test]
    fn in_memory_remote_transport_keeps_partition_state_coherent_across_failover() {
        let harness = TwoNodeClusterHarness::new_controller_pair();
        harness
            .node2
            .runtime
            .sync_local_topics(
                &[crate::store::TopicMetadata {
                    name: "replicated.topic".to_string(),
                    partitions: vec![crate::store::PartitionMetadata { partition: 0 }],
                }],
                2,
            )
            .unwrap();
        harness
            .node2
            .runtime
            .handle_update_partition_leader(UpdatePartitionLeaderRequest {
                topic_name: "replicated.topic".to_string(),
                partition_index: 0,
                leader_id: 1,
                leader_epoch: 1,
            })
            .unwrap();
        harness
            .node2
            .runtime
            .handle_update_partition_replication(UpdatePartitionReplicationRequest {
                topic_name: "replicated.topic".to_string(),
                partition_index: 0,
                replicas: vec![1, 2],
                isr: vec![1, 2],
                leader_epoch: 1,
            })
            .unwrap();
        harness
            .node2
            .runtime
            .handle_update_replica_progress(UpdateReplicaProgressRequest {
                topic_name: "replicated.topic".to_string(),
                partition_index: 0,
                broker_id: 1,
                log_end_offset: 7,
                last_caught_up_ms: 100,
            })
            .unwrap();
        harness
            .node2
            .runtime
            .handle_update_replica_progress(UpdateReplicaProgressRequest {
                topic_name: "replicated.topic".to_string(),
                partition_index: 0,
                broker_id: 2,
                log_end_offset: 5,
                last_caught_up_ms: 100,
            })
            .unwrap();

        let transport = harness.transport_from_node1();
        let target = transport.resolve_target(2).unwrap();
        let before_failover = transport
            .send_to(
                &target,
                ClusterRpcRequest::GetPartitionState(GetPartitionStateRequest {
                    topic_name: "replicated.topic".to_string(),
                    partition_index: 0,
                }),
            )
            .unwrap();
        let ClusterRpcResponse::GetPartitionState(before_failover) = before_failover else {
            panic!("unexpected response variant");
        };

        let _ = transport
            .send_to(
                &target,
                ClusterRpcRequest::AppendMetadata(AppendMetadataRequest {
                    term: 3,
                    leader_id: 2,
                    prev_metadata_offset: harness.node2.runtime.metadata_image().metadata_offset,
                    records: vec![crate::cluster::MetadataRecord::SetController {
                        controller_id: 2,
                    }],
                }),
            )
            .unwrap();
        let after_failover = transport
            .send_to(
                &target,
                ClusterRpcRequest::GetPartitionState(GetPartitionStateRequest {
                    topic_name: "replicated.topic".to_string(),
                    partition_index: 0,
                }),
            )
            .unwrap();
        let ClusterRpcResponse::GetPartitionState(after_failover) = after_failover else {
            panic!("unexpected response variant");
        };

        assert!(before_failover.found);
        assert_eq!(before_failover.leader_id, 1);
        assert_eq!(before_failover.high_watermark, 7);
        assert_eq!(before_failover.leader_log_end_offset, 7);
        assert_eq!(after_failover, before_failover);
        assert_eq!(harness.node2.runtime.metadata_image().controller_id, 2);
    }

    #[test]
    fn in_memory_remote_transport_advances_reassignment_lifecycle() {
        let harness = TwoNodeClusterHarness::new_controller_pair();
        harness
            .node2
            .runtime
            .sync_local_topics(
                &[crate::store::TopicMetadata {
                    name: "reassign.topic".to_string(),
                    partitions: vec![crate::store::PartitionMetadata { partition: 0 }],
                }],
                1,
            )
            .unwrap();
        let transport = harness.transport_from_node1();
        let target = transport.resolve_target(2).unwrap();

        let _ = transport
            .send_to(
                &target,
                ClusterRpcRequest::BeginPartitionReassignment(BeginPartitionReassignmentRequest {
                    topic_name: "reassign.topic".to_string(),
                    partition_index: 0,
                    target_replicas: vec![2, 3],
                }),
            )
            .unwrap();
        let _ = transport
            .send_to(
                &target,
                ClusterRpcRequest::AdvancePartitionReassignment(
                    AdvancePartitionReassignmentRequest {
                        topic_name: "reassign.topic".to_string(),
                        partition_index: 0,
                        step: crate::cluster::ReassignmentStep::ExpandingIsr,
                    },
                ),
            )
            .unwrap();
        let _ = transport
            .send_to(
                &target,
                ClusterRpcRequest::AdvancePartitionReassignment(
                    AdvancePartitionReassignmentRequest {
                        topic_name: "reassign.topic".to_string(),
                        partition_index: 0,
                        step: crate::cluster::ReassignmentStep::LeaderSwitch,
                    },
                ),
            )
            .unwrap();

        let reassignment = harness
            .node2
            .runtime
            .metadata_image()
            .partition_reassignment("reassign.topic", 0)
            .unwrap();
        assert_eq!(
            reassignment.step,
            crate::cluster::ReassignmentStep::LeaderSwitch
        );
        assert_eq!(
            harness.node2.runtime.metadata_image().topics[0].partitions[0].leader_id,
            2
        );
    }

    #[test]
    fn in_memory_remote_transport_rejects_stale_append_after_failover() {
        let harness = TwoNodeClusterHarness::new_controller_pair();
        let transport = harness.transport_from_node1();
        let target = transport.resolve_target(2).unwrap();

        let initial_offset = harness.node2.runtime.metadata_image().metadata_offset;
        let _ = transport
            .send_to(
                &target,
                ClusterRpcRequest::AppendMetadata(AppendMetadataRequest {
                    term: 3,
                    leader_id: 2,
                    prev_metadata_offset: initial_offset,
                    records: vec![crate::cluster::MetadataRecord::SetController {
                        controller_id: 2,
                    }],
                }),
            )
            .unwrap();

        let response = transport
            .send_to(
                &target,
                ClusterRpcRequest::AppendMetadata(AppendMetadataRequest {
                    term: 2,
                    leader_id: 1,
                    prev_metadata_offset: initial_offset,
                    records: vec![crate::cluster::MetadataRecord::SetController {
                        controller_id: 1,
                    }],
                }),
            )
            .unwrap();

        let ClusterRpcResponse::AppendMetadata(response) = response else {
            panic!("unexpected response variant");
        };
        assert!(!response.accepted);
        assert_eq!(harness.node2.runtime.metadata_image().controller_id, 2);
    }
}
