use crate::store::{BrokerRecord, Result};

use super::data_plane::PreparedAppend;

pub(super) fn strict_replica_prepare(
    topic: &str,
    partition: i32,
    records: &[BrokerRecord],
    expected_offset: i64,
) -> Result<Option<PreparedAppend>> {
    if records.is_empty() {
        return Ok(None);
    }

    let mut expected = expected_offset;
    for record in records {
        if record.offset != expected {
            return Err(crate::store::StoreError::ReplicaOffsetMismatch {
                expected,
                actual: record.offset,
            });
        }
        expected += 1;
    }

    Ok(Some(PreparedAppend {
        topic: topic.to_string(),
        partition,
        base_offset: records[0].offset,
        last_offset: records
            .last()
            .map(|record| record.offset)
            .unwrap_or(expected),
        records: records.to_vec(),
    }))
}
