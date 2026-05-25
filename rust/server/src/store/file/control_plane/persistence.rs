use crate::store::{OffsetCommitRequest, Result};

use super::ControlPlaneState;
use super::offset_key::OffsetKey;
use crate::store::file::consumer_offsets::{self, GroupStateRecord, OffsetCommitRecord};
use crate::store::file::state::GroupState;

impl ControlPlaneState {
    pub(super) fn persist_offset_commit(
        &mut self,
        request: OffsetCommitRequest<'_>,
        offset_key: OffsetKey,
    ) -> Result<()> {
        let offset_topic_partition = consumer_offsets::partition_for_group_id(request.group_id);
        let record_offset = self.next_record_offset(offset_topic_partition);
        consumer_offsets::append_commit(
            &self.logs,
            record_offset,
            OffsetCommitRecord {
                group_id: request.group_id,
                offset_topic_partition,
                topic: request.topic,
                partition: request.partition,
                next_offset: request.next_offset,
                now_ms: request.now_ms,
            },
        )?;
        self.advance_record_offset(offset_topic_partition, record_offset);
        self.offsets.insert(offset_key, request.next_offset);
        Ok(())
    }

    pub(super) fn persist_group_state_snapshot(
        &mut self,
        group_id: &str,
        group: GroupState,
        now_ms: i64,
    ) -> Result<()> {
        let offset_topic_partition = consumer_offsets::partition_for_group_id(group_id);
        let record_offset = self.next_record_offset(offset_topic_partition);
        consumer_offsets::append_group_state(
            &self.logs,
            record_offset,
            GroupStateRecord {
                group_id,
                offset_topic_partition,
                group: &group,
                now_ms,
            },
        )?;
        self.advance_record_offset(offset_topic_partition, record_offset);
        Ok(())
    }

    fn next_record_offset(&self, offset_topic_partition: i32) -> i64 {
        self.next_consumer_offsets_records
            .get(&offset_topic_partition)
            .copied()
            .unwrap_or(0)
    }

    fn advance_record_offset(&mut self, offset_topic_partition: i32, record_offset: i64) {
        self.next_consumer_offsets_records
            .insert(offset_topic_partition, record_offset + 1);
    }
}
