use anyhow::{Context, Result};
use bytes::{Buf, Bytes, BytesMut};
use kafka_protocol::messages::{ApiKey, RequestHeader, ResponseHeader};
use kafka_protocol::protocol::{Decodable, Encodable, HeaderVersion};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

pub const API_VERSIONS_VERSION: i16 = 3;
pub const METADATA_VERSION: i16 = 12;
pub const PRODUCE_VERSION: i16 = 9;
pub const FETCH_VERSION: i16 = 12;
pub const LIST_OFFSETS_VERSION: i16 = 7;
pub const FIND_COORDINATOR_VERSION: i16 = 4;
pub const JOIN_GROUP_VERSION: i16 = 5;
pub const SYNC_GROUP_VERSION: i16 = 5;
pub const HEARTBEAT_VERSION: i16 = 4;
pub const LEAVE_GROUP_VERSION: i16 = 4;
pub const OFFSET_COMMIT_VERSION: i16 = 7;
pub const OFFSET_FETCH_VERSION: i16 = 7;
pub const INIT_PRODUCER_ID_VERSION: i16 = 4;

pub async fn read_frame(stream: &mut TcpStream) -> Result<BytesMut> {
    let size = stream.read_i32().await.context("read frame size")?;
    let size = usize::try_from(size).context("negative frame size")?;
    let mut payload = vec![0_u8; size];
    stream.read_exact(&mut payload).await.context("read frame body")?;
    Ok(BytesMut::from(payload.as_slice()))
}

pub fn decode_header(frame: &BytesMut) -> Result<RequestHeader> {
    let mut buf = Bytes::copy_from_slice(frame.as_ref());
    let (api_key, api_version) = peek_key_and_version(&mut buf)?;
    let header_version = request_header_version(api_key, api_version);
    let mut buf = Bytes::copy_from_slice(frame.as_ref());
    RequestHeader::decode(&mut buf, header_version).context("decode request header")
}

pub fn decode_body<T: Decodable + HeaderVersion>(frame: &BytesMut, api_version: i16) -> Result<T> {
    let header_version = T::header_version(api_version);
    let mut buf = Bytes::copy_from_slice(frame.as_ref());
    let _ = RequestHeader::decode(&mut buf, header_version)?;
    T::decode(&mut buf, api_version).context("decode request body")
}

pub async fn write_response<T: Encodable + HeaderVersion>(
    stream: &mut TcpStream,
    correlation_id: i32,
    api_version: i16,
    response: &T,
) -> Result<()> {
    let mut payload = BytesMut::new();
    let header = ResponseHeader::default().with_correlation_id(correlation_id);
    header.encode(&mut payload, T::header_version(api_version))?;
    response.encode(&mut payload, api_version)?;
    stream.write_i32(payload.len() as i32).await?;
    stream.write_all(payload.as_ref()).await?;
    Ok(())
}

pub fn peek_key_and_version(buf: &mut Bytes) -> Result<(ApiKey, i16)> {
    if buf.remaining() < 4 {
        anyhow::bail!("short request header")
    }
    let raw_api_key = buf.get_i16();
    let api_key = ApiKey::try_from(raw_api_key)
        .map_err(|_| anyhow::anyhow!("unknown api key {raw_api_key}"))?;
    let api_version = buf.get_i16();
    Ok((api_key, api_version))
}

fn request_header_version(api_key: ApiKey, api_version: i16) -> i16 {
    match api_key {
        ApiKey::ApiVersions => {
            if api_version >= 3 { 2 } else { 1 }
        }
        ApiKey::Metadata => {
            if api_version >= 9 { 2 } else { 1 }
        }
        ApiKey::Produce => {
            if api_version >= 9 { 2 } else { 1 }
        }
        ApiKey::Fetch => {
            if api_version >= 12 { 2 } else { 1 }
        }
        _ => 1,
    }
}
