use bytes::{Buf, BufMut, BytesMut};

use crate::store::Result;

use super::{
    get_bytes, get_nullable_string, get_string, put_bytes, put_nullable_string, put_string,
};
use crate::store::file::state::{GroupMemberState, GroupState};

pub(super) const KEY_VERSION: i16 = 2;

const VALUE_VERSION: i16 = 2;
const EMPTY_CLIENT_ID: &str = "";
const EMPTY_CLIENT_HOST: &str = "";

pub(super) fn encode_key(group_id: &str) -> Vec<u8> {
    let mut bytes = BytesMut::new();
    bytes.put_i16(KEY_VERSION);
    put_string(&mut bytes, group_id);
    bytes.to_vec()
}

pub(super) fn decode_key_payload(bytes: &mut &[u8]) -> Result<Option<String>> {
    get_string(bytes)
}

pub(super) fn encode_value(group: &GroupState) -> Vec<u8> {
    let mut bytes = BytesMut::new();
    bytes.put_i16(VALUE_VERSION);
    put_string(&mut bytes, &group.protocol_type);
    bytes.put_i32(group.generation_id);
    put_nullable_string(&mut bytes, Some(&group.protocol_name));
    put_nullable_string(&mut bytes, group.leader_member_id.as_deref());
    bytes.put_i64(group.updated_at_unix_ms);
    bytes.put_i32(group.members.len() as i32);
    for member in group.members.values() {
        encode_member(&mut bytes, member);
    }
    bytes.to_vec()
}

pub(super) fn decode_value(bytes: &[u8]) -> Result<Option<GroupState>> {
    let mut bytes = bytes;
    if bytes.remaining() < 2 || bytes.get_i16() != VALUE_VERSION {
        return Ok(None);
    }
    let Some(protocol_type) = get_string(&mut bytes)? else {
        return Ok(None);
    };
    if bytes.remaining() < 4 {
        return Ok(None);
    }
    let generation_id = bytes.get_i32();
    let protocol_name = get_nullable_string(&mut bytes)?.unwrap_or_default();
    let leader_member_id = get_nullable_string(&mut bytes)?;
    if bytes.remaining() < 12 {
        return Ok(None);
    }
    let updated_at_unix_ms = bytes.get_i64();
    let member_count = bytes.get_i32();
    if member_count < 0 {
        return Ok(None);
    }
    let mut group = GroupState {
        generation_id,
        protocol_type,
        protocol_name,
        leader_member_id,
        assignments_ready: false,
        assignments_failed: false,
        members: Default::default(),
        updated_at_unix_ms,
    };
    for _ in 0..member_count {
        let Some(member) = decode_member(&mut bytes, &group)? else {
            return Ok(None);
        };
        group.members.insert(member.member_id.clone(), member);
    }
    group.assignments_ready = !group.members.is_empty()
        && group
            .members
            .values()
            .all(|member| !member.assignment.is_empty());
    Ok(Some(group))
}

fn encode_member(bytes: &mut BytesMut, member: &GroupMemberState) {
    put_string(bytes, &member.member_id);
    put_string(bytes, EMPTY_CLIENT_ID);
    put_string(bytes, EMPTY_CLIENT_HOST);
    bytes.put_i32(member.rebalance_timeout_ms);
    bytes.put_i32(member.session_timeout_ms);
    put_bytes(bytes, &member.subscription_metadata);
    put_bytes(bytes, &member.assignment);
}

fn decode_member(bytes: &mut &[u8], group: &GroupState) -> Result<Option<GroupMemberState>> {
    let Some(member_id) = get_string(bytes)? else {
        return Ok(None);
    };
    let _ = get_string(bytes)?;
    let _ = get_string(bytes)?;
    if bytes.remaining() < 8 {
        return Ok(None);
    }
    let rebalance_timeout_ms = bytes.get_i32();
    let session_timeout_ms = bytes.get_i32();
    let Some(subscription_metadata) = get_bytes(bytes)? else {
        return Ok(None);
    };
    let Some(assignment) = get_bytes(bytes)? else {
        return Ok(None);
    };
    Ok(Some(GroupMemberState {
        member_id,
        generation_id: group.generation_id,
        protocol_type: group.protocol_type.clone(),
        protocol_name: group.protocol_name.clone(),
        subscription_metadata,
        assignment,
        session_timeout_ms,
        rebalance_timeout_ms,
        last_heartbeat_unix_ms: group.updated_at_unix_ms,
        updated_at_unix_ms: group.updated_at_unix_ms,
    }))
}
