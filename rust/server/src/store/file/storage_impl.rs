use super::FileStore;
use crate::store::{
    BrokerRecord, FetchResult, GroupJoinRequest, GroupJoinResult, ListOffsetResult,
    OffsetCommitRequest, ProducerSession, ReplicaFetchResult, Result, Storage, SyncGroupResult,
    TopicMetadata,
};

use super::control_plane::SyncGroupStateRequest;
use super::data_plane::AppendDecision;
use super::log::StoredBatch;
use super::replica_prepare::strict_replica_prepare;

impl Storage for FileStore {
    fn topic_metadata(
        &self,
        topics: Option<&[String]>,
        _now_ms: i64,
    ) -> Result<Vec<TopicMetadata>> {
        let data = self.data.lock().expect("file store mutex poisoned");
        Ok(data.topic_metadata(topics))
    }

    fn ensure_topic(&self, topic: &str, partition_count: i32, now_ms: i64) -> Result<()> {
        self.logs.ensure_topic(topic, partition_count)?;
        let mut data = self.data.lock().expect("file store mutex poisoned");
        data.ensure_topic(topic, partition_count, now_ms)
    }

    fn init_producer(&self) -> Result<ProducerSession> {
        let mut data = self.data.lock().expect("file store mutex poisoned");
        data.init_producer()
    }

    fn append_records(
        &self,
        topic: &str,
        partition: i32,
        records: &[BrokerRecord],
        now_ms: i64,
    ) -> Result<(i64, i64)> {
        let mut data = self.data.lock().expect("file store mutex poisoned");
        let decision = match data.prepare_append(topic, partition, records, now_ms) {
            Ok(decision) => decision,
            Err(crate::store::StoreError::UnknownTopicOrPartition { .. }) if partition == 0 => {
                drop(data);
                self.ensure_topic(topic, 1, now_ms)?;
                data = self.data.lock().expect("file store mutex poisoned");
                data.prepare_append(topic, partition, records, now_ms)?
            }
            Err(err) => return Err(err),
        };
        match decision {
            AppendDecision::Duplicate {
                base_offset,
                last_offset,
            } => Ok((base_offset, last_offset)),
            AppendDecision::Append(prepared) => {
                self.logs.ensure_partition(topic, partition)?;
                self.logs.append_batch(
                    topic,
                    partition,
                    &StoredBatch::from_records(&prepared.records),
                )?;
                let result = (prepared.base_offset, prepared.last_offset);
                data.finish_append(&prepared, now_ms)?;
                Ok(result)
            }
        }
    }

    fn fetch_records(
        &self,
        topic: &str,
        partition: i32,
        start_offset: i64,
        limit: usize,
    ) -> Result<FetchResult> {
        let high_watermark = self
            .data
            .lock()
            .expect("file store mutex poisoned")
            .high_watermark(topic, partition)?;
        let records = self
            .logs
            .read_records(topic, partition, start_offset, limit)?;
        Ok(FetchResult {
            high_watermark,
            records,
        })
    }

    fn fetch_records_for_client(
        &self,
        topic: &str,
        partition: i32,
        start_offset: i64,
        limit: usize,
    ) -> Result<FetchResult> {
        let high_watermark = self
            .data
            .lock()
            .expect("file store mutex poisoned")
            .high_watermark(topic, partition)?;
        let records = self
            .logs
            .read_records_for_client(topic, partition, start_offset, limit)?;
        Ok(FetchResult {
            high_watermark,
            records,
        })
    }

    fn replica_fetch_records(
        &self,
        topic: &str,
        partition: i32,
        start_offset: i64,
        limit: usize,
    ) -> Result<ReplicaFetchResult> {
        let (high_watermark, log_end_offset) = self
            .data
            .lock()
            .expect("file store mutex poisoned")
            .replica_progress(topic, partition)?;
        let records = self
            .logs
            .read_records(topic, partition, start_offset, limit)?;
        Ok(ReplicaFetchResult {
            high_watermark,
            log_end_offset,
            records,
        })
    }

    fn append_replica_records(
        &self,
        topic: &str,
        partition: i32,
        records: &[BrokerRecord],
        now_ms: i64,
    ) -> Result<i64> {
        let mut data = self.data.lock().expect("file store mutex poisoned");
        let prepared = data.prepare_replica_append(topic, partition, records)?;
        let Some(prepared) = prepared else {
            return data.latest_offset(topic, partition);
        };
        self.logs.ensure_partition(topic, partition)?;
        self.logs.append_batch(
            topic,
            partition,
            &StoredBatch::from_records(&prepared.records),
        )?;
        data.finish_append(&prepared, now_ms)?;
        data.latest_offset(topic, partition)
    }

    fn apply_replica_records(
        &self,
        topic: &str,
        partition: i32,
        records: &[BrokerRecord],
        leader_high_watermark: i64,
        now_ms: i64,
    ) -> Result<crate::store::ReplicaApplyResult> {
        let mut data = self.data.lock().expect("file store mutex poisoned");
        let expected = data.latest_offset(topic, partition)?;
        let prepared = strict_replica_prepare(topic, partition, records, expected)?;
        if let Some(prepared) = prepared.as_ref() {
            self.logs.ensure_partition(topic, partition)?;
            self.logs.append_batch(
                topic,
                partition,
                &StoredBatch::from_records(&prepared.records),
            )?;
        }
        data.finish_replica_append(
            prepared.as_ref(),
            topic,
            partition,
            leader_high_watermark,
            now_ms,
        )
    }

    fn truncate_partition(&self, topic: &str, partition: i32, next_offset: i64) -> Result<()> {
        self.logs
            .truncate_to_offset(topic, partition, next_offset)?;
        let mut data = self.data.lock().expect("file store mutex poisoned");
        data.reconcile_partition_offset(topic, partition, next_offset)
    }

    fn list_offsets(
        &self,
        topic: &str,
        partition: i32,
    ) -> Result<(ListOffsetResult, ListOffsetResult)> {
        self.partition_offsets(topic, partition)
    }

    fn join_group(&self, request: GroupJoinRequest<'_>) -> Result<GroupJoinResult> {
        let mut control = self.control.lock().expect("file store mutex poisoned");
        control.join_group(request)
    }

    fn sync_group(
        &self,
        group_id: &str,
        member_id: &str,
        generation_id: i32,
        protocol_name: &str,
        assignments: &[(String, Vec<u8>)],
        now_ms: i64,
    ) -> Result<SyncGroupResult> {
        let topics = {
            let data = self.data.lock().expect("file store mutex poisoned");
            data.topic_metadata(None)
        };
        let mut control = self.control.lock().expect("file store mutex poisoned");
        control.sync_group(SyncGroupStateRequest {
            group_id,
            member_id,
            generation_id,
            protocol_name,
            assignments,
            topics: &topics,
            now_ms,
        })
    }

    fn heartbeat(
        &self,
        group_id: &str,
        member_id: &str,
        generation_id: i32,
        now_ms: i64,
    ) -> Result<()> {
        let mut control = self.control.lock().expect("file store mutex poisoned");
        control.heartbeat(group_id, member_id, generation_id, now_ms)
    }

    fn leave_group(&self, group_id: &str, member_id: &str, now_ms: i64) -> Result<()> {
        let mut control = self.control.lock().expect("file store mutex poisoned");
        control.leave_group(group_id, member_id, now_ms)
    }

    fn commit_offset(&self, request: OffsetCommitRequest<'_>) -> Result<()> {
        let known_partition = self
            .data
            .lock()
            .expect("file store mutex poisoned")
            .has_partition(request.topic, request.partition);
        if !known_partition {
            return Err(crate::store::StoreError::UnknownTopicOrPartition {
                topic: request.topic.to_string(),
                partition: request.partition,
            });
        }
        let mut control = self.control.lock().expect("file store mutex poisoned");
        control.commit_offset(request)
    }

    fn fetch_offset(&self, group_id: &str, topic: &str, partition: i32) -> Result<Option<i64>> {
        let known_partition = self
            .data
            .lock()
            .expect("file store mutex poisoned")
            .has_partition(topic, partition);
        if !known_partition {
            return Err(crate::store::StoreError::UnknownTopicOrPartition {
                topic: topic.to_string(),
                partition,
            });
        }
        let control = self.control.lock().expect("file store mutex poisoned");
        Ok(control.fetch_offset(group_id, topic, partition))
    }
}
