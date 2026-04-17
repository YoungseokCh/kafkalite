use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use anyhow::{Result, bail};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use crate::cluster::codec::{decode_request, decode_response, encode_request, encode_response};
use crate::cluster::rpc::{
    AdvancePartitionReassignmentRequest, AppendMetadataRequest, AppendMetadataResponse,
    ApplyReplicaRecordsRequest, ApplyReplicaRecordsResponse, BeginPartitionReassignmentRequest,
    BrokerHeartbeatRequest, BrokerHeartbeatResponse, GetPartitionStateRequest,
    GetPartitionStateResponse, PartitionReassignmentResponse, RegisterBrokerRequest,
    RegisterBrokerResponse, ReplicaFetchRequest, ReplicaFetchResponse,
    UpdatePartitionLeaderRequest, UpdatePartitionLeaderResponse, UpdatePartitionReplicationRequest,
    UpdatePartitionReplicationResponse, UpdateReplicaProgressRequest,
    UpdateReplicaProgressResponse, VoteRequest, VoteResponse,
};
use crate::cluster::{ClusterConfig, ClusterRuntime};
use crate::store::{BrokerRecord, Storage, StoreError};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ClusterRpcRequest {
    AppendMetadata(AppendMetadataRequest),
    RegisterBroker(RegisterBrokerRequest),
    BrokerHeartbeat(BrokerHeartbeatRequest),
    UpdatePartitionLeader(UpdatePartitionLeaderRequest),
    UpdatePartitionReplication(UpdatePartitionReplicationRequest),
    UpdateReplicaProgress(UpdateReplicaProgressRequest),
    GetPartitionState(GetPartitionStateRequest),
    ReplicaFetch(ReplicaFetchRequest),
    ApplyReplicaRecords(ApplyReplicaRecordsRequest),
    BeginPartitionReassignment(BeginPartitionReassignmentRequest),
    AdvancePartitionReassignment(AdvancePartitionReassignmentRequest),
    Vote(VoteRequest),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ClusterRpcResponse {
    AppendMetadata(AppendMetadataResponse),
    RegisterBroker(RegisterBrokerResponse),
    BrokerHeartbeat(BrokerHeartbeatResponse),
    UpdatePartitionLeader(UpdatePartitionLeaderResponse),
    UpdatePartitionReplication(UpdatePartitionReplicationResponse),
    UpdateReplicaProgress(UpdateReplicaProgressResponse),
    GetPartitionState(GetPartitionStateResponse),
    ReplicaFetch(ReplicaFetchResponse),
    ApplyReplicaRecords(ApplyReplicaRecordsResponse),
    BeginPartitionReassignment(PartitionReassignmentResponse),
    AdvancePartitionReassignment(PartitionReassignmentResponse),
    Vote(VoteResponse),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClusterRpcTarget {
    pub node_id: i32,
    pub host: String,
    pub port: u16,
}

#[derive(Debug, Clone, Default)]
pub struct TcpClusterRpcTransport;

impl TcpClusterRpcTransport {
    pub async fn send_to(
        &self,
        target: &ClusterRpcTarget,
        request: ClusterRpcRequest,
    ) -> Result<ClusterRpcResponse> {
        let mut stream = TcpStream::connect((target.host.as_str(), target.port)).await?;
        let bytes = encode_request(&request)?;
        stream.write_all(&bytes).await?;
        stream.flush().await?;

        let mut len_bytes = [0_u8; 4];
        stream.read_exact(&mut len_bytes).await?;
        let len = u32::from_be_bytes(len_bytes) as usize;
        let mut payload = vec![0_u8; len];
        stream.read_exact(&mut payload).await?;

        let mut frame = Vec::with_capacity(4 + payload.len());
        frame.extend_from_slice(&len_bytes);
        frame.extend_from_slice(&payload);
        decode_response(&frame)
    }

    pub async fn serve_once(
        listener: &TcpListener,
        handler: impl Fn(ClusterRpcRequest) -> Result<ClusterRpcResponse>,
    ) -> Result<()> {
        let (mut stream, _) = listener.accept().await?;
        let mut len_bytes = [0_u8; 4];
        stream.read_exact(&mut len_bytes).await?;
        let len = u32::from_be_bytes(len_bytes) as usize;
        let mut payload = vec![0_u8; len];
        stream.read_exact(&mut payload).await?;

        let mut frame = Vec::with_capacity(4 + payload.len());
        frame.extend_from_slice(&len_bytes);
        frame.extend_from_slice(&payload);
        let request = decode_request(&frame)?;
        let response = handler(request)?;
        let encoded = encode_response(&response)?;
        stream.write_all(&encoded).await?;
        stream.flush().await?;
        Ok(())
    }

    pub async fn serve_runtime_once(listener: &TcpListener, runtime: ClusterRuntime) -> Result<()> {
        Self::serve_once(listener, move |request| runtime.dispatch(request)).await
    }

    pub async fn serve_runtime_forever(
        listener: TcpListener,
        runtime: ClusterRuntime,
    ) -> Result<()> {
        loop {
            Self::serve_runtime_once(&listener, runtime.clone()).await?;
        }
    }

    pub async fn serve_broker_forever(
        listener: TcpListener,
        runtime: ClusterRuntime,
        store: Arc<dyn Storage>,
    ) -> Result<()> {
        loop {
            let runtime = runtime.clone();
            let store = store.clone();
            Self::serve_once(&listener, move |request| match request {
                ClusterRpcRequest::ReplicaFetch(request) => match store.fetch_records(
                    &request.topic_name,
                    request.partition_index,
                    request.start_offset,
                    request.max_records,
                ) {
                    Ok(fetched) => {
                        let (_, latest) =
                            store.list_offsets(&request.topic_name, request.partition_index)?;
                        let (leader_id, leader_epoch, high_watermark, _) = runtime
                            .metadata_image()
                            .partition_state_view(&request.topic_name, request.partition_index)
                            .unwrap_or((-1, -1, fetched.high_watermark, latest.offset));
                        Ok(ClusterRpcResponse::ReplicaFetch(ReplicaFetchResponse {
                            found: true,
                            leader_id,
                            leader_epoch,
                            high_watermark,
                            leader_log_end_offset: latest.offset,
                            records: fetched.records,
                        }))
                    }
                    Err(StoreError::UnknownTopicOrPartition { .. }) => {
                        Ok(ClusterRpcResponse::ReplicaFetch(ReplicaFetchResponse {
                            found: false,
                            leader_id: -1,
                            leader_epoch: -1,
                            high_watermark: -1,
                            leader_log_end_offset: -1,
                            records: Vec::new(),
                        }))
                    }
                    Err(err) => Err(err.into()),
                },
                ClusterRpcRequest::ApplyReplicaRecords(request) => {
                    let next_offset = store.append_replica_records(
                        &request.topic_name,
                        request.partition_index,
                        &request.records,
                        request.now_ms,
                    )?;
                    Ok(ClusterRpcResponse::ApplyReplicaRecords(
                        ApplyReplicaRecordsResponse {
                            accepted: true,
                            next_offset,
                        },
                    ))
                }
                other => runtime.dispatch(other),
            })
            .await?;
        }
    }

    pub async fn update_partition_leader_to(
        &self,
        target: &ClusterRpcTarget,
        request: UpdatePartitionLeaderRequest,
    ) -> Result<UpdatePartitionLeaderResponse> {
        match self
            .send_to(target, ClusterRpcRequest::UpdatePartitionLeader(request))
            .await?
        {
            ClusterRpcResponse::UpdatePartitionLeader(response) => Ok(response),
            other => bail!("unexpected RPC response: {other:?}"),
        }
    }

    pub async fn register_broker_to(
        &self,
        target: &ClusterRpcTarget,
        request: RegisterBrokerRequest,
    ) -> Result<RegisterBrokerResponse> {
        match self
            .send_to(target, ClusterRpcRequest::RegisterBroker(request))
            .await?
        {
            ClusterRpcResponse::RegisterBroker(response) => Ok(response),
            other => bail!("unexpected RPC response: {other:?}"),
        }
    }

    pub async fn broker_heartbeat_to(
        &self,
        target: &ClusterRpcTarget,
        request: BrokerHeartbeatRequest,
    ) -> Result<BrokerHeartbeatResponse> {
        match self
            .send_to(target, ClusterRpcRequest::BrokerHeartbeat(request))
            .await?
        {
            ClusterRpcResponse::BrokerHeartbeat(response) => Ok(response),
            other => bail!("unexpected RPC response: {other:?}"),
        }
    }

    pub async fn update_partition_replication_to(
        &self,
        target: &ClusterRpcTarget,
        request: UpdatePartitionReplicationRequest,
    ) -> Result<UpdatePartitionReplicationResponse> {
        match self
            .send_to(
                target,
                ClusterRpcRequest::UpdatePartitionReplication(request),
            )
            .await?
        {
            ClusterRpcResponse::UpdatePartitionReplication(response) => Ok(response),
            other => bail!("unexpected RPC response: {other:?}"),
        }
    }

    pub async fn begin_partition_reassignment_to(
        &self,
        target: &ClusterRpcTarget,
        request: BeginPartitionReassignmentRequest,
    ) -> Result<PartitionReassignmentResponse> {
        match self
            .send_to(
                target,
                ClusterRpcRequest::BeginPartitionReassignment(request),
            )
            .await?
        {
            ClusterRpcResponse::BeginPartitionReassignment(response) => Ok(response),
            other => bail!("unexpected RPC response: {other:?}"),
        }
    }

    pub async fn update_replica_progress_to(
        &self,
        target: &ClusterRpcTarget,
        request: UpdateReplicaProgressRequest,
    ) -> Result<UpdateReplicaProgressResponse> {
        match self
            .send_to(target, ClusterRpcRequest::UpdateReplicaProgress(request))
            .await?
        {
            ClusterRpcResponse::UpdateReplicaProgress(response) => Ok(response),
            other => bail!("unexpected RPC response: {other:?}"),
        }
    }

    pub async fn apply_replica_records_to(
        &self,
        target: &ClusterRpcTarget,
        request: ApplyReplicaRecordsRequest,
    ) -> Result<ApplyReplicaRecordsResponse> {
        match self
            .send_to(target, ClusterRpcRequest::ApplyReplicaRecords(request))
            .await?
        {
            ClusterRpcResponse::ApplyReplicaRecords(response) => Ok(response),
            other => bail!("unexpected RPC response: {other:?}"),
        }
    }
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

    fn update_partition_leader_to(
        &self,
        target: &ClusterRpcTarget,
        request: UpdatePartitionLeaderRequest,
    ) -> Result<UpdatePartitionLeaderResponse> {
        match self.send_to(target, ClusterRpcRequest::UpdatePartitionLeader(request))? {
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

    fn update_partition_replication_to(
        &self,
        target: &ClusterRpcTarget,
        request: UpdatePartitionReplicationRequest,
    ) -> Result<UpdatePartitionReplicationResponse> {
        match self.send_to(
            target,
            ClusterRpcRequest::UpdatePartitionReplication(request),
        )? {
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

    fn update_replica_progress_to(
        &self,
        target: &ClusterRpcTarget,
        request: UpdateReplicaProgressRequest,
    ) -> Result<UpdateReplicaProgressResponse> {
        match self.send_to(target, ClusterRpcRequest::UpdateReplicaProgress(request))? {
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

    fn begin_partition_reassignment_to(
        &self,
        target: &ClusterRpcTarget,
        request: BeginPartitionReassignmentRequest,
    ) -> Result<PartitionReassignmentResponse> {
        match self.send_to(
            target,
            ClusterRpcRequest::BeginPartitionReassignment(request),
        )? {
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

    fn advance_partition_reassignment_to(
        &self,
        target: &ClusterRpcTarget,
        request: AdvancePartitionReassignmentRequest,
    ) -> Result<PartitionReassignmentResponse> {
        match self.send_to(
            target,
            ClusterRpcRequest::AdvancePartitionReassignment(request),
        )? {
            ClusterRpcResponse::AdvancePartitionReassignment(response) => Ok(response),
            other => bail!("unexpected RPC response: {other:?}"),
        }
    }

    fn vote_to(&self, target: &ClusterRpcTarget, request: VoteRequest) -> Result<VoteResponse> {
        match self.send_to(target, ClusterRpcRequest::Vote(request))? {
            ClusterRpcResponse::Vote(response) => Ok(response),
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
                leader_id: -1,
                leader_epoch: -1,
                high_watermark: -1,
                leader_log_end_offset: -1,
                records: Vec::<BrokerRecord>::new(),
            }));
        };
        let state = self
            .runtimes
            .lock()
            .expect("in-memory cluster network mutex poisoned")
            .get(&node_id)
            .map(|runtime| {
                runtime
                    .metadata_image()
                    .partition_state_view(&request.topic_name, request.partition_index)
            });
        match store.fetch_records(
            &request.topic_name,
            request.partition_index,
            request.start_offset,
            request.max_records,
        ) {
            Ok(fetched) => {
                let (_, latest) =
                    store.list_offsets(&request.topic_name, request.partition_index)?;
                let (leader_id, leader_epoch, high_watermark, _) =
                    state
                        .flatten()
                        .unwrap_or((-1, -1, fetched.high_watermark, latest.offset));
                Ok(ClusterRpcResponse::ReplicaFetch(ReplicaFetchResponse {
                    found: true,
                    leader_id,
                    leader_epoch,
                    high_watermark,
                    leader_log_end_offset: latest.offset,
                    records: fetched.records,
                }))
            }
            Err(crate::store::StoreError::UnknownTopicOrPartition { .. }) => {
                Ok(ClusterRpcResponse::ReplicaFetch(ReplicaFetchResponse {
                    found: false,
                    leader_id: -1,
                    leader_epoch: -1,
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
    use std::collections::VecDeque;
    use std::sync::Arc;
    use std::sync::Mutex;

    use tempfile::tempdir;
    use tokio::net::TcpListener;

    use crate::cluster::{
        ClusterRuntime, ControllerQuorumVoter, ProcessRole, test_support::TwoNodeClusterHarness,
    };
    use crate::config::Config;
    use crate::store::FileStore;

    use super::*;

    #[derive(Clone)]
    struct ScriptedTransport {
        responses: Arc<Mutex<VecDeque<ClusterRpcResponse>>>,
    }

    impl ScriptedTransport {
        fn new(responses: impl IntoIterator<Item = ClusterRpcResponse>) -> Self {
            Self {
                responses: Arc::new(Mutex::new(responses.into_iter().collect())),
            }
        }
    }

    impl ClusterRpcTransport for ScriptedTransport {
        fn send(&self, _request: ClusterRpcRequest) -> Result<ClusterRpcResponse> {
            self.responses
                .lock()
                .unwrap()
                .pop_front()
                .ok_or_else(|| anyhow::anyhow!("missing scripted response"))
        }
    }

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
        let _ = harness
            .node2
            .runtime
            .handle_append_metadata(AppendMetadataRequest {
                term: 1,
                leader_id: 2,
                prev_metadata_offset: harness.node2.runtime.metadata_image().metadata_offset,
                records: vec![crate::cluster::MetadataRecord::SetController { controller_id: 2 }],
            })
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
                    leader_epoch: 1,
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
                    leader_epoch: 1,
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
        let _ = harness
            .node2
            .runtime
            .handle_append_metadata(AppendMetadataRequest {
                term: 1,
                leader_id: 2,
                prev_metadata_offset: harness.node2.runtime.metadata_image().metadata_offset,
                records: vec![crate::cluster::MetadataRecord::SetController { controller_id: 2 }],
            })
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
                leader_epoch: 1,
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
                leader_epoch: 1,
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
        let _ = harness
            .node2
            .runtime
            .handle_append_metadata(AppendMetadataRequest {
                term: 1,
                leader_id: 2,
                prev_metadata_offset: harness.node2.runtime.metadata_image().metadata_offset,
                records: vec![crate::cluster::MetadataRecord::SetController { controller_id: 2 }],
            })
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
                ClusterRpcRequest::UpdateReplicaProgress(UpdateReplicaProgressRequest {
                    topic_name: "reassign.topic".to_string(),
                    partition_index: 0,
                    leader_epoch: 0,
                    broker_id: 2,
                    log_end_offset: 0,
                    last_caught_up_ms: 100,
                }),
            )
            .unwrap();
        let _ = transport
            .send_to(
                &target,
                ClusterRpcRequest::UpdateReplicaProgress(UpdateReplicaProgressRequest {
                    topic_name: "reassign.topic".to_string(),
                    partition_index: 0,
                    leader_epoch: 0,
                    broker_id: 3,
                    log_end_offset: 0,
                    last_caught_up_ms: 100,
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

    #[tokio::test]
    async fn tcp_transport_round_trips_cluster_rpc() {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            TcpClusterRpcTransport::serve_once(&listener, |request| match request {
                ClusterRpcRequest::Vote(request) => Ok(ClusterRpcResponse::Vote(VoteResponse {
                    term: request.term,
                    vote_granted: true,
                })),
                other => panic!("unexpected request {other:?}"),
            })
            .await
            .unwrap();
        });

        let transport = TcpClusterRpcTransport;
        let response = transport
            .send_to(
                &ClusterRpcTarget {
                    node_id: 1,
                    host: addr.ip().to_string(),
                    port: addr.port(),
                },
                ClusterRpcRequest::Vote(VoteRequest {
                    term: 7,
                    candidate_id: 1,
                    last_metadata_offset: 3,
                }),
            )
            .await
            .unwrap();

        let ClusterRpcResponse::Vote(response) = response else {
            panic!("unexpected response variant")
        };
        assert_eq!(response.term, 7);
        assert!(response.vote_granted);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn tcp_transport_can_dispatch_to_runtime() {
        let dir = tempdir().unwrap();
        let mut config = Config::single_node(dir.path().join("data"), 19092, 1);
        config.cluster.node_id = 4;
        config.cluster.process_roles = vec![ProcessRole::Controller];
        let runtime = ClusterRuntime::from_config(&config).unwrap();
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server_runtime = runtime.clone();
        let server = tokio::spawn(async move {
            TcpClusterRpcTransport::serve_runtime_once(&listener, server_runtime)
                .await
                .unwrap();
        });

        let transport = TcpClusterRpcTransport;
        let response = transport
            .send_to(
                &ClusterRpcTarget {
                    node_id: 4,
                    host: addr.ip().to_string(),
                    port: addr.port(),
                },
                ClusterRpcRequest::AppendMetadata(AppendMetadataRequest {
                    term: 1,
                    leader_id: 4,
                    prev_metadata_offset: runtime.metadata_image().metadata_offset,
                    records: vec![crate::cluster::MetadataRecord::SetController {
                        controller_id: 4,
                    }],
                }),
            )
            .await
            .unwrap();

        let ClusterRpcResponse::AppendMetadata(response) = response else {
            panic!("unexpected response variant")
        };
        assert!(response.accepted);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn tcp_transport_routes_partition_leader_update_to_controller_runtime() {
        let dir = tempdir().unwrap();
        let mut config = Config::single_node(dir.path().join("data"), 19092, 1);
        config.cluster.node_id = 2;
        config.cluster.process_roles = vec![ProcessRole::Controller];
        let runtime = ClusterRuntime::from_config(&config).unwrap();
        runtime
            .sync_local_topics(
                &[crate::store::TopicMetadata {
                    name: "tcp.route.topic".to_string(),
                    partitions: vec![crate::store::PartitionMetadata { partition: 0 }],
                }],
                2,
            )
            .unwrap();
        let _ = runtime
            .handle_append_metadata(AppendMetadataRequest {
                term: 1,
                leader_id: 2,
                prev_metadata_offset: runtime.metadata_image().metadata_offset,
                records: vec![crate::cluster::MetadataRecord::SetController { controller_id: 2 }],
            })
            .unwrap();

        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server_runtime = runtime.clone();
        let server = tokio::spawn(async move {
            TcpClusterRpcTransport::serve_runtime_once(&listener, server_runtime)
                .await
                .unwrap();
        });

        let transport = TcpClusterRpcTransport;
        let response = transport
            .update_partition_leader_to(
                &ClusterRpcTarget {
                    node_id: 2,
                    host: addr.ip().to_string(),
                    port: addr.port(),
                },
                UpdatePartitionLeaderRequest {
                    topic_name: "tcp.route.topic".to_string(),
                    partition_index: 0,
                    leader_id: 2,
                    leader_epoch: 1,
                },
            )
            .await
            .unwrap();

        assert!(response.accepted);
        assert_eq!(
            runtime
                .metadata_image()
                .partition_leader_id("tcp.route.topic", 0),
            Some(2)
        );
        server.await.unwrap();
    }

    #[tokio::test]
    async fn tcp_transport_round_trips_register_broker_and_heartbeat() {
        let dir = tempdir().unwrap();
        let mut config = Config::single_node(dir.path().join("data"), 19092, 1);
        config.cluster.node_id = 4;
        config.cluster.process_roles = vec![ProcessRole::Broker, ProcessRole::Controller];
        let runtime = ClusterRuntime::from_config(&config).unwrap();
        let _ = runtime
            .handle_append_metadata(AppendMetadataRequest {
                term: 1,
                leader_id: 4,
                prev_metadata_offset: runtime.metadata_image().metadata_offset,
                records: vec![crate::cluster::MetadataRecord::SetController { controller_id: 4 }],
            })
            .unwrap();

        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server_runtime = runtime.clone();
        let server = tokio::spawn(async move {
            TcpClusterRpcTransport::serve_runtime_once(&listener, server_runtime)
                .await
                .unwrap();
        });

        let transport = TcpClusterRpcTransport;
        let registration = transport
            .register_broker_to(
                &ClusterRpcTarget {
                    node_id: 4,
                    host: addr.ip().to_string(),
                    port: addr.port(),
                },
                RegisterBrokerRequest {
                    node_id: 9,
                    advertised_host: "broker-9.local".to_string(),
                    advertised_port: 39092,
                },
            )
            .await
            .unwrap();
        assert!(registration.accepted);
        server.await.unwrap();

        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server_runtime = runtime.clone();
        let server = tokio::spawn(async move {
            TcpClusterRpcTransport::serve_runtime_once(&listener, server_runtime)
                .await
                .unwrap();
        });

        let heartbeat = transport
            .broker_heartbeat_to(
                &ClusterRpcTarget {
                    node_id: 4,
                    host: addr.ip().to_string(),
                    port: addr.port(),
                },
                BrokerHeartbeatRequest {
                    node_id: 9,
                    broker_epoch: registration.broker_epoch,
                    timestamp_ms: 123,
                },
            )
            .await
            .unwrap();
        assert!(heartbeat.accepted);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn tcp_transport_forever_server_handles_multiple_requests() {
        let dir = tempdir().unwrap();
        let mut config = Config::single_node(dir.path().join("data"), 19092, 1);
        config.cluster.node_id = 4;
        config.cluster.process_roles = vec![ProcessRole::Controller];
        let runtime = ClusterRuntime::from_config(&config).unwrap();
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server_runtime = runtime.clone();
        let server = tokio::spawn(async move {
            TcpClusterRpcTransport::serve_runtime_forever(listener, server_runtime)
                .await
                .unwrap();
        });

        let transport = TcpClusterRpcTransport;
        for term in [1_i64, 2_i64] {
            let response = transport
                .send_to(
                    &ClusterRpcTarget {
                        node_id: 4,
                        host: addr.ip().to_string(),
                        port: addr.port(),
                    },
                    ClusterRpcRequest::Vote(VoteRequest {
                        term,
                        candidate_id: 4,
                        last_metadata_offset: runtime.metadata_image().metadata_offset,
                    }),
                )
                .await
                .unwrap();
            let ClusterRpcResponse::Vote(response) = response else {
                panic!("unexpected response variant")
            };
            assert_eq!(response.term, term);
        }

        server.abort();
        let _ = server.await;
    }

    #[test]
    fn in_memory_remote_transport_replica_fetch_without_store_reports_missing() {
        let harness = TwoNodeClusterHarness::new_controller_pair();
        let transport = harness.transport_from_node1();
        let target = transport.resolve_target(2).unwrap();

        let response = transport
            .replica_fetch_to(
                &target,
                ReplicaFetchRequest {
                    topic_name: "missing.topic".to_string(),
                    partition_index: 0,
                    start_offset: 0,
                    max_records: 10,
                },
            )
            .unwrap();

        assert!(!response.found);
        assert!(response.records.is_empty());
    }

    #[test]
    fn in_memory_remote_transport_replica_fetch_unknown_topic_reports_missing() {
        let harness = TwoNodeClusterHarness::new_controller_pair();
        let transport = harness.transport_from_node1();
        let dir = tempdir().unwrap();
        harness.network.register_store(
            2,
            Arc::new(FileStore::open(dir.path().join("node-2-data")).unwrap()),
        );
        let target = transport.resolve_target(2).unwrap();

        let response = transport
            .replica_fetch_to(
                &target,
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
    }

    #[tokio::test]
    async fn tcp_transport_typed_wrappers_reject_unexpected_response_variants() {
        async fn expect_unexpected_response<T>(fut: impl std::future::Future<Output = Result<T>>) {
            let err = match fut.await {
                Ok(_) => panic!("expected unexpected response error"),
                Err(err) => err.to_string(),
            };
            assert!(err.contains("unexpected RPC response"));
        }

        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            for _ in 0..6 {
                TcpClusterRpcTransport::serve_once(&listener, |_| {
                    Ok(ClusterRpcResponse::Vote(VoteResponse {
                        term: 1,
                        vote_granted: true,
                    }))
                })
                .await
                .unwrap();
            }
        });
        let target = ClusterRpcTarget {
            node_id: 4,
            host: addr.ip().to_string(),
            port: addr.port(),
        };
        let transport = TcpClusterRpcTransport;

        expect_unexpected_response(transport.register_broker_to(
            &target,
            RegisterBrokerRequest {
                node_id: 9,
                advertised_host: "broker-9.local".to_string(),
                advertised_port: 39092,
            },
        ))
        .await;
        expect_unexpected_response(transport.broker_heartbeat_to(
            &target,
            BrokerHeartbeatRequest {
                node_id: 9,
                broker_epoch: 1,
                timestamp_ms: 123,
            },
        ))
        .await;
        expect_unexpected_response(transport.update_partition_replication_to(
            &target,
            UpdatePartitionReplicationRequest {
                topic_name: "missing.topic".to_string(),
                partition_index: 0,
                replicas: vec![4],
                isr: vec![4],
                leader_epoch: 1,
            },
        ))
        .await;
        expect_unexpected_response(transport.begin_partition_reassignment_to(
            &target,
            BeginPartitionReassignmentRequest {
                topic_name: "missing.topic".to_string(),
                partition_index: 0,
                target_replicas: vec![4],
            },
        ))
        .await;
        expect_unexpected_response(transport.update_replica_progress_to(
            &target,
            UpdateReplicaProgressRequest {
                topic_name: "missing.topic".to_string(),
                partition_index: 0,
                leader_epoch: 1,
                broker_id: 4,
                log_end_offset: 0,
                last_caught_up_ms: 1,
            },
        ))
        .await;
        expect_unexpected_response(transport.apply_replica_records_to(
            &target,
            ApplyReplicaRecordsRequest {
                topic_name: "missing.topic".to_string(),
                partition_index: 0,
                records: Vec::new(),
                now_ms: 1,
            },
        ))
        .await;

        server.abort();
        let _ = server.await;
    }

    #[tokio::test]
    async fn tcp_transport_typed_wrappers_accept_expected_response_variants() {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            for _ in 0..4 {
                TcpClusterRpcTransport::serve_once(&listener, |request| match request {
                    ClusterRpcRequest::UpdatePartitionReplication(_) => {
                        Ok(ClusterRpcResponse::UpdatePartitionReplication(
                            UpdatePartitionReplicationResponse {
                                accepted: true,
                                metadata_offset: 11,
                            },
                        ))
                    }
                    ClusterRpcRequest::BeginPartitionReassignment(_) => {
                        Ok(ClusterRpcResponse::BeginPartitionReassignment(
                            PartitionReassignmentResponse {
                                accepted: true,
                                metadata_offset: 12,
                            },
                        ))
                    }
                    ClusterRpcRequest::UpdateReplicaProgress(_) => Ok(
                        ClusterRpcResponse::UpdateReplicaProgress(UpdateReplicaProgressResponse {
                            accepted: true,
                            metadata_offset: 13,
                            high_watermark: 13,
                        }),
                    ),
                    ClusterRpcRequest::ApplyReplicaRecords(_) => Ok(
                        ClusterRpcResponse::ApplyReplicaRecords(ApplyReplicaRecordsResponse {
                            accepted: true,
                            next_offset: 14,
                        }),
                    ),
                    other => panic!("unexpected request {other:?}"),
                })
                .await
                .unwrap();
            }
        });
        let target = ClusterRpcTarget {
            node_id: 4,
            host: addr.ip().to_string(),
            port: addr.port(),
        };
        let transport = TcpClusterRpcTransport;

        let replication = transport
            .update_partition_replication_to(
                &target,
                UpdatePartitionReplicationRequest {
                    topic_name: "typed-success.topic".to_string(),
                    partition_index: 0,
                    replicas: vec![1, 2],
                    isr: vec![1],
                    leader_epoch: 1,
                },
            )
            .await
            .unwrap();
        assert!(replication.accepted);
        assert_eq!(replication.metadata_offset, 11);

        let reassignment = transport
            .begin_partition_reassignment_to(
                &target,
                BeginPartitionReassignmentRequest {
                    topic_name: "typed-success.topic".to_string(),
                    partition_index: 0,
                    target_replicas: vec![2, 3],
                },
            )
            .await
            .unwrap();
        assert!(reassignment.accepted);
        assert_eq!(reassignment.metadata_offset, 12);

        let progress = transport
            .update_replica_progress_to(
                &target,
                UpdateReplicaProgressRequest {
                    topic_name: "typed-success.topic".to_string(),
                    partition_index: 0,
                    leader_epoch: 1,
                    broker_id: 2,
                    log_end_offset: 9,
                    last_caught_up_ms: 100,
                },
            )
            .await
            .unwrap();
        assert!(progress.accepted);
        assert_eq!(progress.high_watermark, 13);

        let applied = transport
            .apply_replica_records_to(
                &target,
                ApplyReplicaRecordsRequest {
                    topic_name: "typed-success.topic".to_string(),
                    partition_index: 0,
                    records: Vec::new(),
                    now_ms: 100,
                },
            )
            .await
            .unwrap();
        assert!(applied.accepted);
        assert_eq!(applied.next_offset, 14);

        server.abort();
        let _ = server.await;
    }

    #[tokio::test]
    async fn tcp_transport_update_partition_leader_wrapper_rejects_unexpected_response() {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            TcpClusterRpcTransport::serve_once(&listener, |_| {
                Ok(ClusterRpcResponse::Vote(VoteResponse {
                    term: 1,
                    vote_granted: true,
                }))
            })
            .await
            .unwrap();
        });
        let transport = TcpClusterRpcTransport;

        let err = transport
            .update_partition_leader_to(
                &ClusterRpcTarget {
                    node_id: 4,
                    host: addr.ip().to_string(),
                    port: addr.port(),
                },
                UpdatePartitionLeaderRequest {
                    topic_name: "typed-error.topic".to_string(),
                    partition_index: 0,
                    leader_id: 4,
                    leader_epoch: 1,
                },
            )
            .await
            .unwrap_err()
            .to_string();

        assert!(err.contains("unexpected RPC response"));
        server.await.unwrap();
    }

    #[tokio::test]
    async fn tcp_broker_transport_serves_replica_fetch_apply_and_runtime_dispatch() {
        let dir = tempdir().unwrap();
        let mut config = Config::single_node(dir.path().join("data"), 19092, 1);
        config.cluster.node_id = 1;
        config.cluster.process_roles = vec![ProcessRole::Broker, ProcessRole::Controller];
        let runtime = ClusterRuntime::from_config(&config).unwrap();
        runtime
            .sync_local_topics(
                &[crate::store::TopicMetadata {
                    name: "broker.topic".to_string(),
                    partitions: vec![crate::store::PartitionMetadata { partition: 0 }],
                }],
                1,
            )
            .unwrap();
        let store = Arc::new(FileStore::open(dir.path().join("broker-store")).unwrap());
        store.ensure_topic("broker.topic", 1, 0).unwrap();
        store.ensure_topic("store-only.topic", 1, 0).unwrap();
        store
            .append_replica_records(
                "store-only.topic",
                0,
                &[crate::store::BrokerRecord {
                    offset: 0,
                    timestamp_ms: 100,
                    producer_id: -1,
                    producer_epoch: -1,
                    sequence: 0,
                    key: Some(bytes::Bytes::from_static(b"k")),
                    value: Some(bytes::Bytes::from_static(b"v")),
                    headers_json: vec![],
                }],
                100,
            )
            .unwrap();

        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(TcpClusterRpcTransport::serve_broker_forever(
            listener,
            runtime.clone(),
            store.clone(),
        ));
        let transport = TcpClusterRpcTransport;
        let target = ClusterRpcTarget {
            node_id: 1,
            host: addr.ip().to_string(),
            port: addr.port(),
        };

        let applied = transport
            .apply_replica_records_to(
                &target,
                ApplyReplicaRecordsRequest {
                    topic_name: "broker.topic".to_string(),
                    partition_index: 0,
                    records: vec![crate::store::BrokerRecord {
                        offset: 0,
                        timestamp_ms: 101,
                        producer_id: -1,
                        producer_epoch: -1,
                        sequence: 0,
                        key: Some(bytes::Bytes::from_static(b"k1")),
                        value: Some(bytes::Bytes::from_static(b"v1")),
                        headers_json: vec![],
                    }],
                    now_ms: 101,
                },
            )
            .await
            .unwrap();
        assert!(applied.accepted);
        assert_eq!(applied.next_offset, 1);

        let fetched = transport
            .send_to(
                &target,
                ClusterRpcRequest::ReplicaFetch(ReplicaFetchRequest {
                    topic_name: "broker.topic".to_string(),
                    partition_index: 0,
                    start_offset: 0,
                    max_records: 10,
                }),
            )
            .await
            .unwrap();
        let ClusterRpcResponse::ReplicaFetch(fetched) = fetched else {
            panic!("unexpected response variant");
        };
        assert!(fetched.found);
        assert_eq!(fetched.leader_id, 1);
        assert_eq!(fetched.leader_log_end_offset, 1);
        assert_eq!(fetched.records.len(), 1);

        let fallback_fetch = transport
            .send_to(
                &target,
                ClusterRpcRequest::ReplicaFetch(ReplicaFetchRequest {
                    topic_name: "store-only.topic".to_string(),
                    partition_index: 0,
                    start_offset: 0,
                    max_records: 10,
                }),
            )
            .await
            .unwrap();
        let ClusterRpcResponse::ReplicaFetch(fallback_fetch) = fallback_fetch else {
            panic!("unexpected response variant");
        };
        assert!(fallback_fetch.found);
        assert_eq!(fallback_fetch.leader_id, -1);
        assert_eq!(fallback_fetch.high_watermark, 1);

        let missing = transport
            .send_to(
                &target,
                ClusterRpcRequest::ReplicaFetch(ReplicaFetchRequest {
                    topic_name: "missing.topic".to_string(),
                    partition_index: 0,
                    start_offset: 0,
                    max_records: 10,
                }),
            )
            .await
            .unwrap();
        let ClusterRpcResponse::ReplicaFetch(missing) = missing else {
            panic!("unexpected response variant");
        };
        assert!(!missing.found);

        let vote = transport
            .send_to(
                &target,
                ClusterRpcRequest::Vote(VoteRequest {
                    term: 2,
                    candidate_id: 1,
                    last_metadata_offset: runtime.metadata_image().metadata_offset,
                }),
            )
            .await
            .unwrap();
        assert!(matches!(vote, ClusterRpcResponse::Vote(_)));

        server.abort();
        let _ = server.await;
    }

    #[test]
    fn scripted_transport_covers_remaining_wrapper_variants() {
        let target = ClusterRpcTarget {
            node_id: 2,
            host: "node2".to_string(),
            port: 9093,
        };
        let transport = ScriptedTransport::new([
            ClusterRpcResponse::GetPartitionState(GetPartitionStateResponse {
                found: true,
                leader_id: 2,
                leader_epoch: 3,
                high_watermark: 5,
                leader_log_end_offset: 8,
            }),
            ClusterRpcResponse::ReplicaFetch(ReplicaFetchResponse {
                found: true,
                leader_id: 2,
                leader_epoch: 3,
                high_watermark: 5,
                leader_log_end_offset: 8,
                records: Vec::new(),
            }),
            ClusterRpcResponse::BeginPartitionReassignment(PartitionReassignmentResponse {
                accepted: true,
                metadata_offset: 21,
            }),
            ClusterRpcResponse::AdvancePartitionReassignment(PartitionReassignmentResponse {
                accepted: true,
                metadata_offset: 22,
            }),
            ClusterRpcResponse::AdvancePartitionReassignment(PartitionReassignmentResponse {
                accepted: true,
                metadata_offset: 23,
            }),
            ClusterRpcResponse::Vote(VoteResponse {
                term: 7,
                vote_granted: true,
            }),
        ]);

        let state = transport
            .get_partition_state(GetPartitionStateRequest {
                topic_name: "scripted.topic".to_string(),
                partition_index: 0,
            })
            .unwrap();
        assert!(state.found);
        assert_eq!(state.leader_epoch, 3);

        let fetched = transport
            .replica_fetch_to(
                &target,
                ReplicaFetchRequest {
                    topic_name: "scripted.topic".to_string(),
                    partition_index: 0,
                    start_offset: 0,
                    max_records: 1,
                },
            )
            .unwrap();
        assert!(fetched.found);
        assert_eq!(fetched.leader_log_end_offset, 8);

        let begin = transport
            .begin_partition_reassignment(BeginPartitionReassignmentRequest {
                topic_name: "scripted.topic".to_string(),
                partition_index: 0,
                target_replicas: vec![2, 3],
            })
            .unwrap();
        assert!(begin.accepted);
        assert_eq!(begin.metadata_offset, 21);

        let advance = transport
            .advance_partition_reassignment(AdvancePartitionReassignmentRequest {
                topic_name: "scripted.topic".to_string(),
                partition_index: 0,
                step: crate::cluster::ReassignmentStep::ExpandingIsr,
            })
            .unwrap();
        assert!(advance.accepted);
        assert_eq!(advance.metadata_offset, 22);

        let advance_to = transport
            .advance_partition_reassignment_to(
                &target,
                AdvancePartitionReassignmentRequest {
                    topic_name: "scripted.topic".to_string(),
                    partition_index: 0,
                    step: crate::cluster::ReassignmentStep::LeaderSwitch,
                },
            )
            .unwrap();
        assert!(advance_to.accepted);
        assert_eq!(advance_to.metadata_offset, 23);

        let vote = transport
            .vote_to(
                &target,
                VoteRequest {
                    term: 7,
                    candidate_id: 2,
                    last_metadata_offset: 8,
                },
            )
            .unwrap();
        assert_eq!(vote.term, 7);
        assert!(vote.vote_granted);
    }

    #[test]
    fn scripted_transport_covers_targeted_wrapper_success_variants() {
        let target = ClusterRpcTarget {
            node_id: 2,
            host: "node2".to_string(),
            port: 9093,
        };
        let transport = ScriptedTransport::new([
            ClusterRpcResponse::UpdatePartitionReplication(UpdatePartitionReplicationResponse {
                accepted: true,
                metadata_offset: 31,
            }),
            ClusterRpcResponse::UpdateReplicaProgress(UpdateReplicaProgressResponse {
                accepted: true,
                metadata_offset: 32,
                high_watermark: 5,
            }),
            ClusterRpcResponse::UpdateReplicaProgress(UpdateReplicaProgressResponse {
                accepted: true,
                metadata_offset: 33,
                high_watermark: 6,
            }),
            ClusterRpcResponse::BeginPartitionReassignment(PartitionReassignmentResponse {
                accepted: true,
                metadata_offset: 34,
            }),
            ClusterRpcResponse::AdvancePartitionReassignment(PartitionReassignmentResponse {
                accepted: true,
                metadata_offset: 35,
            }),
        ]);

        let replication = transport
            .update_partition_replication_to(
                &target,
                UpdatePartitionReplicationRequest {
                    topic_name: "scripted.topic".to_string(),
                    partition_index: 0,
                    replicas: vec![2],
                    isr: vec![2],
                    leader_epoch: 1,
                },
            )
            .unwrap();
        assert!(replication.accepted);
        assert_eq!(replication.metadata_offset, 31);

        let progress = transport
            .update_replica_progress(UpdateReplicaProgressRequest {
                topic_name: "scripted.topic".to_string(),
                partition_index: 0,
                leader_epoch: 1,
                broker_id: 2,
                log_end_offset: 5,
                last_caught_up_ms: 100,
            })
            .unwrap();
        assert!(progress.accepted);
        assert_eq!(progress.high_watermark, 5);

        let progress_to = transport
            .update_replica_progress_to(
                &target,
                UpdateReplicaProgressRequest {
                    topic_name: "scripted.topic".to_string(),
                    partition_index: 0,
                    leader_epoch: 1,
                    broker_id: 2,
                    log_end_offset: 6,
                    last_caught_up_ms: 101,
                },
            )
            .unwrap();
        assert!(progress_to.accepted);
        assert_eq!(progress_to.high_watermark, 6);

        let begin = transport
            .begin_partition_reassignment_to(
                &target,
                BeginPartitionReassignmentRequest {
                    topic_name: "scripted.topic".to_string(),
                    partition_index: 0,
                    target_replicas: vec![2, 3],
                },
            )
            .unwrap();
        assert!(begin.accepted);
        assert_eq!(begin.metadata_offset, 34);

        let advance = transport
            .advance_partition_reassignment_to(
                &target,
                AdvancePartitionReassignmentRequest {
                    topic_name: "scripted.topic".to_string(),
                    partition_index: 0,
                    step: crate::cluster::ReassignmentStep::LeaderSwitch,
                },
            )
            .unwrap();
        assert!(advance.accepted);
        assert_eq!(advance.metadata_offset, 35);
    }

    #[test]
    fn scripted_transport_rejects_unexpected_remaining_wrapper_variants() {
        let target = ClusterRpcTarget {
            node_id: 2,
            host: "node2".to_string(),
            port: 9093,
        };
        let transport = ScriptedTransport::new([
            ClusterRpcResponse::Vote(VoteResponse {
                term: 1,
                vote_granted: true,
            }),
            ClusterRpcResponse::Vote(VoteResponse {
                term: 1,
                vote_granted: true,
            }),
            ClusterRpcResponse::Vote(VoteResponse {
                term: 1,
                vote_granted: true,
            }),
            ClusterRpcResponse::Vote(VoteResponse {
                term: 1,
                vote_granted: true,
            }),
            ClusterRpcResponse::Vote(VoteResponse {
                term: 1,
                vote_granted: true,
            }),
            ClusterRpcResponse::AppendMetadata(AppendMetadataResponse {
                term: 1,
                accepted: true,
                last_metadata_offset: 1,
            }),
        ]);

        for err in [
            transport
                .get_partition_state(GetPartitionStateRequest {
                    topic_name: "scripted.topic".to_string(),
                    partition_index: 0,
                })
                .unwrap_err()
                .to_string(),
            transport
                .replica_fetch_to(
                    &target,
                    ReplicaFetchRequest {
                        topic_name: "scripted.topic".to_string(),
                        partition_index: 0,
                        start_offset: 0,
                        max_records: 1,
                    },
                )
                .unwrap_err()
                .to_string(),
            transport
                .begin_partition_reassignment(BeginPartitionReassignmentRequest {
                    topic_name: "scripted.topic".to_string(),
                    partition_index: 0,
                    target_replicas: vec![2, 3],
                })
                .unwrap_err()
                .to_string(),
            transport
                .advance_partition_reassignment(AdvancePartitionReassignmentRequest {
                    topic_name: "scripted.topic".to_string(),
                    partition_index: 0,
                    step: crate::cluster::ReassignmentStep::ExpandingIsr,
                })
                .unwrap_err()
                .to_string(),
            transport
                .advance_partition_reassignment_to(
                    &target,
                    AdvancePartitionReassignmentRequest {
                        topic_name: "scripted.topic".to_string(),
                        partition_index: 0,
                        step: crate::cluster::ReassignmentStep::LeaderSwitch,
                    },
                )
                .unwrap_err()
                .to_string(),
            transport
                .vote_to(
                    &target,
                    VoteRequest {
                        term: 7,
                        candidate_id: 2,
                        last_metadata_offset: 8,
                    },
                )
                .unwrap_err()
                .to_string(),
        ] {
            assert!(err.contains("unexpected RPC response"));
        }
    }

    #[test]
    fn scripted_transport_rejects_unexpected_core_wrapper_variants() {
        let target = ClusterRpcTarget {
            node_id: 2,
            host: "node2".to_string(),
            port: 9093,
        };
        let transport = ScriptedTransport::new([
            ClusterRpcResponse::Vote(VoteResponse {
                term: 1,
                vote_granted: true,
            }),
            ClusterRpcResponse::Vote(VoteResponse {
                term: 1,
                vote_granted: true,
            }),
            ClusterRpcResponse::Vote(VoteResponse {
                term: 1,
                vote_granted: true,
            }),
            ClusterRpcResponse::Vote(VoteResponse {
                term: 1,
                vote_granted: true,
            }),
            ClusterRpcResponse::Vote(VoteResponse {
                term: 1,
                vote_granted: true,
            }),
            ClusterRpcResponse::Vote(VoteResponse {
                term: 1,
                vote_granted: true,
            }),
            ClusterRpcResponse::Vote(VoteResponse {
                term: 1,
                vote_granted: true,
            }),
            ClusterRpcResponse::Vote(VoteResponse {
                term: 1,
                vote_granted: true,
            }),
        ]);

        for err in [
            transport
                .register_broker(RegisterBrokerRequest {
                    node_id: 2,
                    advertised_host: "node2".to_string(),
                    advertised_port: 9092,
                })
                .unwrap_err()
                .to_string(),
            transport
                .append_metadata(AppendMetadataRequest {
                    term: 1,
                    leader_id: 1,
                    prev_metadata_offset: -1,
                    records: vec![],
                })
                .unwrap_err()
                .to_string(),
            transport
                .broker_heartbeat(BrokerHeartbeatRequest {
                    node_id: 2,
                    broker_epoch: 1,
                    timestamp_ms: 1,
                })
                .unwrap_err()
                .to_string(),
            transport
                .update_partition_leader(UpdatePartitionLeaderRequest {
                    topic_name: "scripted.topic".to_string(),
                    partition_index: 0,
                    leader_id: 2,
                    leader_epoch: 1,
                })
                .unwrap_err()
                .to_string(),
            transport
                .update_partition_leader_to(
                    &target,
                    UpdatePartitionLeaderRequest {
                        topic_name: "scripted.topic".to_string(),
                        partition_index: 0,
                        leader_id: 2,
                        leader_epoch: 1,
                    },
                )
                .unwrap_err()
                .to_string(),
            transport
                .update_partition_replication(UpdatePartitionReplicationRequest {
                    topic_name: "scripted.topic".to_string(),
                    partition_index: 0,
                    replicas: vec![2],
                    isr: vec![2],
                    leader_epoch: 1,
                })
                .unwrap_err()
                .to_string(),
            transport
                .update_partition_replication_to(
                    &target,
                    UpdatePartitionReplicationRequest {
                        topic_name: "scripted.topic".to_string(),
                        partition_index: 0,
                        replicas: vec![2],
                        isr: vec![2],
                        leader_epoch: 1,
                    },
                )
                .unwrap_err()
                .to_string(),
            transport
                .update_replica_progress(UpdateReplicaProgressRequest {
                    topic_name: "scripted.topic".to_string(),
                    partition_index: 0,
                    leader_epoch: 1,
                    broker_id: 2,
                    log_end_offset: 1,
                    last_caught_up_ms: 1,
                })
                .unwrap_err()
                .to_string(),
        ] {
            assert!(err.contains("unexpected RPC response"));
        }
    }

    #[test]
    fn scripted_transport_rejects_unexpected_targeted_wrapper_variants() {
        let target = ClusterRpcTarget {
            node_id: 2,
            host: "node2".to_string(),
            port: 9093,
        };
        let transport = ScriptedTransport::new([
            ClusterRpcResponse::Vote(VoteResponse {
                term: 1,
                vote_granted: true,
            }),
            ClusterRpcResponse::Vote(VoteResponse {
                term: 1,
                vote_granted: true,
            }),
            ClusterRpcResponse::Vote(VoteResponse {
                term: 1,
                vote_granted: true,
            }),
        ]);

        for err in [
            transport
                .update_replica_progress_to(
                    &target,
                    UpdateReplicaProgressRequest {
                        topic_name: "scripted.topic".to_string(),
                        partition_index: 0,
                        leader_epoch: 1,
                        broker_id: 2,
                        log_end_offset: 1,
                        last_caught_up_ms: 1,
                    },
                )
                .unwrap_err()
                .to_string(),
            transport
                .begin_partition_reassignment_to(
                    &target,
                    BeginPartitionReassignmentRequest {
                        topic_name: "scripted.topic".to_string(),
                        partition_index: 0,
                        target_replicas: vec![2, 3],
                    },
                )
                .unwrap_err()
                .to_string(),
            transport
                .update_partition_replication_to(
                    &target,
                    UpdatePartitionReplicationRequest {
                        topic_name: "scripted.topic".to_string(),
                        partition_index: 0,
                        replicas: vec![2],
                        isr: vec![2],
                        leader_epoch: 1,
                    },
                )
                .unwrap_err()
                .to_string(),
        ] {
            assert!(err.contains("unexpected RPC response"));
        }
    }

    #[test]
    fn remote_transports_require_explicit_target_nodes() {
        let remote = RemoteClusterRpcTransport::new(&ClusterConfig::default());
        let err = remote
            .send(ClusterRpcRequest::Vote(VoteRequest {
                term: 1,
                candidate_id: 1,
                last_metadata_offset: 0,
            }))
            .unwrap_err()
            .to_string();
        assert!(err.contains("requires a target node"));

        let err = remote
            .send_to(
                &ClusterRpcTarget {
                    node_id: 1,
                    host: "node1".to_string(),
                    port: 9093,
                },
                ClusterRpcRequest::Vote(VoteRequest {
                    term: 1,
                    candidate_id: 1,
                    last_metadata_offset: 0,
                }),
            )
            .unwrap_err()
            .to_string();
        assert!(err.contains("remote cluster rpc not implemented yet"));

        let in_memory = InMemoryRemoteClusterRpcTransport::new(
            &ClusterConfig::default(),
            InMemoryClusterNetwork::default(),
        );
        let err = in_memory
            .send(ClusterRpcRequest::Vote(VoteRequest {
                term: 1,
                candidate_id: 1,
                last_metadata_offset: 0,
            }))
            .unwrap_err()
            .to_string();
        assert!(err.contains("requires a target node"));
    }
}
