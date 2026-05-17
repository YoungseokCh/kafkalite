use crate::store::{GroupJoinRequest, Result, StoreError};

use super::{GroupMemberState, GroupState};

pub(super) struct MemberRegistration<'a> {
    protocol_type: &'a str,
    protocol_name: &'a str,
    metadata: &'a [u8],
    session_timeout_ms: i32,
    rebalance_timeout_ms: i32,
    now_ms: i64,
}

impl<'a> MemberRegistration<'a> {
    pub(super) fn from_request(request: GroupJoinRequest<'a>) -> Self {
        Self {
            protocol_type: request.protocol_type,
            protocol_name: request.protocol_name,
            metadata: request.metadata,
            session_timeout_ms: request.session_timeout_ms,
            rebalance_timeout_ms: request.rebalance_timeout_ms,
            now_ms: request.now_ms,
        }
    }
}

pub(super) fn prune_expired_members(group: &mut GroupState, now_ms: i64) -> bool {
    let before = group.members.len();
    group.members.retain(|_, member| {
        now_ms - member.last_heartbeat_unix_ms <= i64::from(member.session_timeout_ms)
    });
    before != group.members.len()
}

pub(super) fn upsert_group_member(
    group: &mut GroupState,
    member_id: &str,
    registration: MemberRegistration<'_>,
) -> bool {
    let next = GroupMemberState {
        member_id: member_id.to_string(),
        generation_id: group.generation_id,
        protocol_type: registration.protocol_type.to_string(),
        protocol_name: registration.protocol_name.to_string(),
        subscription_metadata: registration.metadata.to_vec(),
        assignment: Vec::new(),
        session_timeout_ms: registration.session_timeout_ms,
        rebalance_timeout_ms: registration.rebalance_timeout_ms,
        last_heartbeat_unix_ms: registration.now_ms,
        updated_at_unix_ms: registration.now_ms,
    };
    match group.members.insert(member_id.to_string(), next.clone()) {
        None => true,
        Some(previous) => {
            previous.protocol_type != next.protocol_type
                || previous.protocol_name != next.protocol_name
                || previous.subscription_metadata != next.subscription_metadata
                || previous.session_timeout_ms != next.session_timeout_ms
                || previous.rebalance_timeout_ms != next.rebalance_timeout_ms
        }
    }
}

pub(super) fn ensure_generation(group: &GroupState, generation_id: i32) -> Result<()> {
    if group.generation_id != generation_id {
        return Err(StoreError::StaleGeneration {
            expected: group.generation_id,
            actual: generation_id,
        });
    }
    Ok(())
}
