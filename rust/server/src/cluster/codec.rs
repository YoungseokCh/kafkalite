use anyhow::{Context, Result, bail};

use crate::cluster::{ClusterRpcRequest, ClusterRpcResponse};

pub fn encode_request(request: &ClusterRpcRequest) -> Result<Vec<u8>> {
    encode_frame(request)
}

pub fn decode_request(bytes: &[u8]) -> Result<ClusterRpcRequest> {
    decode_frame(bytes)
}

pub fn encode_response(response: &ClusterRpcResponse) -> Result<Vec<u8>> {
    encode_frame(response)
}

pub fn decode_response(bytes: &[u8]) -> Result<ClusterRpcResponse> {
    decode_frame(bytes)
}

fn encode_frame<T: serde::Serialize>(value: &T) -> Result<Vec<u8>> {
    let payload = serde_json::to_vec(value)?;
    let len = u32::try_from(payload.len()).context("cluster rpc frame too large")?;
    let mut frame = Vec::with_capacity(4 + payload.len());
    frame.extend_from_slice(&len.to_be_bytes());
    frame.extend_from_slice(&payload);
    Ok(frame)
}

fn decode_frame<T: for<'de> serde::Deserialize<'de>>(bytes: &[u8]) -> Result<T> {
    if bytes.len() < 4 {
        bail!("cluster rpc frame too short")
    }
    let declared = u32::from_be_bytes(bytes[0..4].try_into().unwrap()) as usize;
    let payload = &bytes[4..];
    if payload.len() != declared {
        bail!("cluster rpc frame length mismatch")
    }
    Ok(serde_json::from_slice(payload)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::{
        ClusterRpcRequest, ClusterRpcResponse, UpdatePartitionLeaderRequest,
        UpdatePartitionLeaderResponse,
    };

    #[test]
    fn request_codec_round_trip() {
        let request = ClusterRpcRequest::UpdatePartitionLeader(UpdatePartitionLeaderRequest {
            topic_name: "codec.topic".to_string(),
            partition_index: 0,
            leader_id: 2,
            leader_epoch: 3,
        });

        let encoded = encode_request(&request).unwrap();
        let decoded = decode_request(&encoded).unwrap();

        assert_eq!(decoded, request);
    }

    #[test]
    fn response_codec_round_trip() {
        let response = ClusterRpcResponse::UpdatePartitionLeader(UpdatePartitionLeaderResponse {
            accepted: true,
            metadata_offset: 7,
        });

        let encoded = encode_response(&response).unwrap();
        let decoded = decode_response(&encoded).unwrap();

        assert_eq!(decoded, response);
    }

    #[test]
    fn decode_rejects_length_mismatch() {
        let request = ClusterRpcRequest::UpdatePartitionLeader(UpdatePartitionLeaderRequest {
            topic_name: "codec.topic".to_string(),
            partition_index: 0,
            leader_id: 2,
            leader_epoch: 3,
        });
        let mut encoded = encode_request(&request).unwrap();
        encoded[3] = encoded[3].wrapping_add(1);

        let err = decode_request(&encoded).unwrap_err().to_string();
        assert!(err.contains("length mismatch"));
    }
}
