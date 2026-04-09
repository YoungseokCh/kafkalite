use std::net::SocketAddr;

use anyhow::Result;
use kafka_protocol::messages::ApiKey;
use tokio::net::TcpStream;

use crate::protocol;

use super::handlers::{bootstrap, groups, produce_fetch};
use super::KafkaBroker;

pub async fn serve_connection(
    mut stream: TcpStream,
    peer: SocketAddr,
    broker: KafkaBroker,
) -> Result<()> {
    loop {
        let frame = protocol::read_frame(&mut stream).await?;
        let header = protocol::decode_header(&frame)?;
        let _ = peer;
        let api_key = ApiKey::try_from(header.request_api_key)
            .map_err(|_| anyhow::anyhow!("unknown api key {}", header.request_api_key))?;

        match api_key {
            ApiKey::ApiVersions => {
                let response = bootstrap::handle_api_versions();
                protocol::write_response(
                    &mut stream,
                    api_key,
                    header.correlation_id,
                    header.request_api_version,
                    &response,
                )
                .await?;
            }
            ApiKey::Metadata => {
                let request = protocol::decode_body::<kafka_protocol::messages::MetadataRequest>(
                    &frame,
                    api_key,
                    header.request_api_version,
                )?;
                let response = bootstrap::handle_metadata(&broker, request).await?;
                protocol::write_response(
                    &mut stream,
                    api_key,
                    header.correlation_id,
                    header.request_api_version,
                    &response,
                )
                .await?;
            }
            ApiKey::InitProducerId => {
                let _request = protocol::decode_body::<
                    kafka_protocol::messages::InitProducerIdRequest,
                >(&frame, api_key, header.request_api_version)?;
                let response = bootstrap::handle_init_producer_id(&broker).await?;
                protocol::write_response(
                    &mut stream,
                    api_key,
                    header.correlation_id,
                    header.request_api_version,
                    &response,
                )
                .await?;
            }
            ApiKey::ListOffsets => {
                let request = protocol::decode_body::<kafka_protocol::messages::ListOffsetsRequest>(
                    &frame,
                    api_key,
                    header.request_api_version,
                )?;
                let response = produce_fetch::handle_list_offsets(&broker, request).await?;
                protocol::write_response(
                    &mut stream,
                    api_key,
                    header.correlation_id,
                    header.request_api_version,
                    &response,
                )
                .await?;
            }
            ApiKey::Produce => {
                let request = protocol::decode_body::<kafka_protocol::messages::ProduceRequest>(
                    &frame,
                    api_key,
                    header.request_api_version,
                )?;
                let response = produce_fetch::handle_produce(&broker, request).await?;
                protocol::write_response(
                    &mut stream,
                    api_key,
                    header.correlation_id,
                    header.request_api_version,
                    &response,
                )
                .await?;
            }
            ApiKey::Fetch => {
                let request = protocol::decode_body::<kafka_protocol::messages::FetchRequest>(
                    &frame,
                    api_key,
                    header.request_api_version,
                )?;
                let response = produce_fetch::handle_fetch(&broker, request).await?;
                protocol::write_response(
                    &mut stream,
                    api_key,
                    header.correlation_id,
                    header.request_api_version,
                    &response,
                )
                .await?;
            }
            ApiKey::FindCoordinator => {
                let request = protocol::decode_body::<
                    kafka_protocol::messages::FindCoordinatorRequest,
                >(&frame, api_key, header.request_api_version)?;
                let response = groups::handle_find_coordinator(
                    &broker,
                    request,
                    header.request_api_version,
                );
                protocol::write_response(
                    &mut stream,
                    api_key,
                    header.correlation_id,
                    header.request_api_version,
                    &response,
                )
                .await?;
            }
            ApiKey::JoinGroup => {
                let request = protocol::decode_body::<kafka_protocol::messages::JoinGroupRequest>(
                    &frame,
                    api_key,
                    header.request_api_version,
                )?;
                let response = groups::handle_join_group(&broker, request).await?;
                protocol::write_response(
                    &mut stream,
                    api_key,
                    header.correlation_id,
                    header.request_api_version,
                    &response,
                )
                .await?;
            }
            ApiKey::SyncGroup => {
                let request = protocol::decode_body::<kafka_protocol::messages::SyncGroupRequest>(
                    &frame,
                    api_key,
                    header.request_api_version,
                )?;
                let response = groups::handle_sync_group(&broker, request).await?;
                protocol::write_response(
                    &mut stream,
                    api_key,
                    header.correlation_id,
                    header.request_api_version,
                    &response,
                )
                .await?;
            }
            ApiKey::Heartbeat => {
                let request = protocol::decode_body::<kafka_protocol::messages::HeartbeatRequest>(
                    &frame,
                    api_key,
                    header.request_api_version,
                )?;
                let response = groups::handle_heartbeat(&broker, request).await;
                protocol::write_response(
                    &mut stream,
                    api_key,
                    header.correlation_id,
                    header.request_api_version,
                    &response,
                )
                .await?;
            }
            ApiKey::LeaveGroup => {
                let request = protocol::decode_body::<kafka_protocol::messages::LeaveGroupRequest>(
                    &frame,
                    api_key,
                    header.request_api_version,
                )?;
                let response = groups::handle_leave_group(&broker, request).await;
                protocol::write_response(
                    &mut stream,
                    api_key,
                    header.correlation_id,
                    header.request_api_version,
                    &response,
                )
                .await?;
            }
            ApiKey::OffsetCommit => {
                let request = protocol::decode_body::<
                    kafka_protocol::messages::OffsetCommitRequest,
                >(&frame, api_key, header.request_api_version)?;
                let response = groups::handle_offset_commit(&broker, request).await?;
                protocol::write_response(
                    &mut stream,
                    api_key,
                    header.correlation_id,
                    header.request_api_version,
                    &response,
                )
                .await?;
            }
            ApiKey::OffsetFetch => {
                let request = protocol::decode_body::<kafka_protocol::messages::OffsetFetchRequest>(
                    &frame,
                    api_key,
                    header.request_api_version,
                )?;
                let response = groups::handle_offset_fetch(&broker, request).await?;
                protocol::write_response(
                    &mut stream,
                    api_key,
                    header.correlation_id,
                    header.request_api_version,
                    &response,
                )
                .await?;
            }
            other => anyhow::bail!("unsupported api key: {other:?}"),
        }
    }
}
