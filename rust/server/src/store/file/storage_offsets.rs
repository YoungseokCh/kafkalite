use super::FileStore;
use crate::store::{ListOffsetResult, Result};

impl FileStore {
    pub(super) fn partition_offsets(
        &self,
        topic: &str,
        partition: i32,
    ) -> Result<(ListOffsetResult, ListOffsetResult)> {
        let latest_offset = self
            .data
            .lock()
            .expect("file store mutex poisoned")
            .latest_offset(topic, partition)?;
        let earliest = self
            .logs
            .earliest_offset(topic, partition)?
            .unwrap_or((0, 0));
        Ok((
            ListOffsetResult {
                offset: earliest.0,
                timestamp_ms: earliest.1,
            },
            ListOffsetResult {
                offset: latest_offset,
                timestamp_ms: 0,
            },
        ))
    }
}
