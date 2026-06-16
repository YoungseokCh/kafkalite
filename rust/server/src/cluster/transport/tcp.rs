use std::sync::Arc;

use anyhow::{Result, bail};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use crate::broker::fetch_signals::FetchSignals;
use crate::cluster::ClusterRuntime;
use crate::cluster::codec::{decode_request, decode_response, encode_request, encode_response};
use crate::cluster::rpc::{
    ApplyReplicaRecordsRequest, ApplyReplicaRecordsResponse, BeginPartitionReassignmentRequest,
    BrokerHeartbeatRequest, BrokerHeartbeatResponse, PartitionReassignmentResponse,
    RegisterBrokerRequest, RegisterBrokerResponse, ReplicaFetchResponse,
    UpdatePartitionLeaderRequest, UpdatePartitionLeaderResponse, UpdatePartitionReplicationRequest,
    UpdatePartitionReplicationResponse, UpdateReplicaProgressRequest,
    UpdateReplicaProgressResponse,
};
use crate::store::{Storage, StoreError};

use super::{ClusterRpcRequest, ClusterRpcResponse, ClusterRpcTarget};

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

    pub(crate) async fn serve_broker_once(
        listener: &TcpListener,
        runtime: ClusterRuntime,
        store: Arc<dyn Storage>,
        fetch_signals: Arc<FetchSignals>,
    ) -> Result<()> {
        Self::serve_once(listener, move |request| match request {
            ClusterRpcRequest::ReplicaFetch(request) => match store.fetch_records(
                &request.topic_name,
                request.partition_index,
                request.start_offset,
                request.max_records,
            ) {
                Ok(fetched) => {
                    let (_, latest) =
                        store.list_offsets(&request.topic_name, request.partition_index)?;
                    let image = runtime.metadata_image();
                    let (leader_id, leader_epoch, _, _) = image
                        .partition_state_view(&request.topic_name, request.partition_index)
                        .unwrap_or((-1, -1, fetched.high_watermark, latest.offset));
                    Ok(ClusterRpcResponse::ReplicaFetch(ReplicaFetchResponse {
                        found: true,
                        leader_id,
                        leader_epoch,
                        high_watermark: replica_fetch_high_watermark(
                            &image,
                            &request.topic_name,
                            request.partition_index,
                            fetched.high_watermark,
                            runtime.config().controller_quorum_voters.len(),
                        ),
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
            ClusterRpcRequest::UpdateReplicaProgress(request) => {
                let previous_high_watermark = runtime
                    .metadata_image()
                    .partition_high_watermark(&request.topic_name, request.partition_index);
                let response = runtime.handle_update_replica_progress(request.clone())?;
                if response.accepted
                    && response.high_watermark > previous_high_watermark.unwrap_or(-1)
                {
                    fetch_signals.notify(&request.topic_name, request.partition_index);
                }
                Ok(ClusterRpcResponse::UpdateReplicaProgress(response))
            }
            other => runtime.dispatch(other),
        })
        .await
    }

    #[cfg(test)]
    pub(crate) async fn serve_broker_forever(
        listener: TcpListener,
        runtime: ClusterRuntime,
        store: Arc<dyn Storage>,
        fetch_signals: Arc<FetchSignals>,
    ) -> Result<()> {
        loop {
            Self::serve_broker_once(
                &listener,
                runtime.clone(),
                store.clone(),
                fetch_signals.clone(),
            )
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
        let response = self
            .send_to(target, ClusterRpcRequest::ApplyReplicaRecords(request))
            .await
            .map_err(|err| {
                anyhow::anyhow!(
                    "replica apply failed: unknown topic or partition; offset mismatch; {err}"
                )
            })?;
        match response {
            ClusterRpcResponse::ApplyReplicaRecords(response) => Ok(response),
            other => bail!("unexpected RPC response: {other:?}"),
        }
    }
}

fn replica_fetch_high_watermark(
    image: &crate::cluster::ClusterMetadataImage,
    topic_name: &str,
    partition_index: i32,
    fallback: i64,
    voter_count: usize,
) -> i64 {
    if voter_count <= 1 {
        return 0;
    }
    image
        .topics
        .iter()
        .find(|topic| topic.name == topic_name)
        .and_then(|topic| {
            topic
                .partitions
                .iter()
                .find(|partition| partition.partition == partition_index)
        })
        .map(|partition| {
            if partition.replicas.len() <= 1 {
                0
            } else {
                partition.high_watermark
            }
        })
        .unwrap_or(fallback)
}
