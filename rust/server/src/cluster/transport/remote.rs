use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use anyhow::{Result, bail};

use crate::cluster::rpc::{ReplicaFetchRequest, ReplicaFetchResponse};
use crate::cluster::{ClusterConfig, ClusterRuntime};
use crate::store::{BrokerRecord, Storage};

use super::traits::ClusterRpcTransport;
use super::{ClusterRpcRequest, ClusterRpcResponse, ClusterRpcTarget};

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
    pub(super) remote: RemoteClusterRpcTransport,
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
