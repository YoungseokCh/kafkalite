use anyhow::Result;

use crate::cluster::{
    AdvancePartitionReassignmentRequest, BeginPartitionReassignmentRequest,
    GetPartitionStateRequest, GetPartitionStateResponse, PartitionReassignmentResponse,
    ReplicaFetchRequest, ReplicaFetchResponse, UpdatePartitionLeaderRequest,
    UpdatePartitionLeaderResponse, UpdatePartitionReplicationRequest,
    UpdatePartitionReplicationResponse, UpdateReplicaProgressRequest,
    UpdateReplicaProgressResponse,
};

use super::ClusterRuntime;
use super::partition_helpers::{
    partition_leader_matches, partition_replication_matches,
    rejected_replica_progress_high_watermark,
};

impl ClusterRuntime {
    pub fn handle_update_partition_leader(
        &self,
        request: UpdatePartitionLeaderRequest,
    ) -> Result<UpdatePartitionLeaderResponse> {
        if !self.can_write_metadata_locally() {
            return Ok(UpdatePartitionLeaderResponse {
                accepted: false,
                metadata_offset: self.metadata_image().metadata_offset,
            });
        }
        let mut preview = self.metadata_image();
        if !preview.update_partition_leader(
            &request.topic_name,
            request.partition_index,
            request.leader_id,
            request.leader_epoch,
        ) {
            let current = self.metadata_image();
            return Ok(UpdatePartitionLeaderResponse {
                accepted: partition_leader_matches(&current, &request),
                metadata_offset: current.metadata_offset,
            });
        }
        let response = self.append_with_retry(|prev_metadata_offset, term, leader_id| {
            crate::cluster::AppendMetadataRequest {
                term,
                leader_id,
                prev_metadata_offset,
                records: vec![crate::cluster::MetadataRecord::UpdatePartitionLeader {
                    topic_name: request.topic_name.clone(),
                    partition_index: request.partition_index,
                    leader_id: request.leader_id,
                    leader_epoch: request.leader_epoch,
                }],
            }
        })?;
        Ok(UpdatePartitionLeaderResponse {
            accepted: response.accepted,
            metadata_offset: response.last_metadata_offset,
        })
    }

    pub fn handle_update_partition_replication(
        &self,
        request: UpdatePartitionReplicationRequest,
    ) -> Result<UpdatePartitionReplicationResponse> {
        if !self.can_write_metadata_locally() {
            return Ok(UpdatePartitionReplicationResponse {
                accepted: false,
                metadata_offset: self.metadata_image().metadata_offset,
            });
        }
        let mut preview = self.metadata_image();
        if !preview.update_partition_replication(
            &request.topic_name,
            request.partition_index,
            request.replicas.clone(),
            request.isr.clone(),
            request.leader_epoch,
        ) {
            let current = self.metadata_image();
            return Ok(UpdatePartitionReplicationResponse {
                accepted: partition_replication_matches(&current, &request),
                metadata_offset: current.metadata_offset,
            });
        }
        let response = self.append_with_retry(|prev_metadata_offset, term, leader_id| {
            crate::cluster::AppendMetadataRequest {
                term,
                leader_id,
                prev_metadata_offset,
                records: vec![crate::cluster::MetadataRecord::UpdatePartitionReplication {
                    topic_name: request.topic_name.clone(),
                    partition_index: request.partition_index,
                    replicas: request.replicas.clone(),
                    isr: request.isr.clone(),
                    leader_epoch: request.leader_epoch,
                }],
            }
        })?;
        Ok(UpdatePartitionReplicationResponse {
            accepted: response.accepted,
            metadata_offset: response.last_metadata_offset,
        })
    }

    pub fn handle_update_replica_progress(
        &self,
        request: UpdateReplicaProgressRequest,
    ) -> Result<UpdateReplicaProgressResponse> {
        if !self.can_write_metadata_locally() {
            return Ok(UpdateReplicaProgressResponse {
                accepted: false,
                metadata_offset: self.metadata_image().metadata_offset,
                high_watermark: self
                    .metadata_image()
                    .partition_high_watermark(&request.topic_name, request.partition_index)
                    .unwrap_or(0),
            });
        }
        let topic_name = request.topic_name.clone();
        let partition_index = request.partition_index;
        let image_before = self.metadata_image();
        let Some((_, current_epoch, _, _)) =
            image_before.partition_state_view(&topic_name, partition_index)
        else {
            return Ok(UpdateReplicaProgressResponse {
                accepted: false,
                metadata_offset: image_before.metadata_offset,
                high_watermark: 0,
            });
        };
        if request.leader_epoch != current_epoch {
            return Ok(UpdateReplicaProgressResponse {
                accepted: false,
                metadata_offset: image_before.metadata_offset,
                high_watermark: rejected_replica_progress_high_watermark(
                    &image_before,
                    &topic_name,
                    partition_index,
                ),
            });
        }
        let response = self.append_with_retry(|prev_metadata_offset, term, leader_id| {
            crate::cluster::AppendMetadataRequest {
                term,
                leader_id,
                prev_metadata_offset,
                records: vec![crate::cluster::MetadataRecord::UpdateReplicaProgress {
                    topic_name: request.topic_name.clone(),
                    partition_index: request.partition_index,
                    leader_epoch: request.leader_epoch,
                    progress: crate::cluster::ReplicaProgress {
                        broker_id: request.broker_id,
                        log_end_offset: request.log_end_offset,
                        last_caught_up_ms: request.last_caught_up_ms,
                    },
                }],
            }
        })?;
        let image = self.metadata_image();
        let high_watermark = image
            .partition_high_watermark(&topic_name, partition_index)
            .unwrap_or(0);
        Ok(UpdateReplicaProgressResponse {
            accepted: response.accepted,
            metadata_offset: response.last_metadata_offset,
            high_watermark,
        })
    }

    pub fn handle_get_partition_state(
        &self,
        request: GetPartitionStateRequest,
    ) -> Result<GetPartitionStateResponse> {
        let Some((leader_id, leader_epoch, high_watermark, leader_log_end_offset)) = self
            .metadata_image()
            .partition_state_view(&request.topic_name, request.partition_index)
        else {
            return Ok(GetPartitionStateResponse {
                found: false,
                leader_id: -1,
                leader_epoch: -1,
                high_watermark: -1,
                leader_log_end_offset: -1,
            });
        };
        Ok(GetPartitionStateResponse {
            found: true,
            leader_id,
            leader_epoch,
            high_watermark,
            leader_log_end_offset,
        })
    }

    pub fn handle_replica_fetch(
        &self,
        _request: ReplicaFetchRequest,
    ) -> Result<ReplicaFetchResponse> {
        anyhow::bail!("replica fetch requires broker data-plane transport")
    }

    pub fn handle_begin_partition_reassignment(
        &self,
        request: BeginPartitionReassignmentRequest,
    ) -> Result<PartitionReassignmentResponse> {
        if !self.can_write_metadata_locally() {
            return Ok(PartitionReassignmentResponse {
                accepted: false,
                metadata_offset: self.metadata_image().metadata_offset,
            });
        }
        let mut preview = self.metadata_image();
        if !preview.begin_partition_reassignment(
            &request.topic_name,
            request.partition_index,
            request.target_replicas.clone(),
        ) {
            return Ok(PartitionReassignmentResponse {
                accepted: false,
                metadata_offset: preview.metadata_offset,
            });
        }
        let response = self.append_with_retry(|prev_metadata_offset, term, leader_id| {
            crate::cluster::AppendMetadataRequest {
                term,
                leader_id,
                prev_metadata_offset,
                records: vec![crate::cluster::MetadataRecord::BeginPartitionReassignment {
                    topic_name: request.topic_name.clone(),
                    partition_index: request.partition_index,
                    target_replicas: request.target_replicas.clone(),
                }],
            }
        })?;
        Ok(PartitionReassignmentResponse {
            accepted: response.accepted,
            metadata_offset: response.last_metadata_offset,
        })
    }

    pub fn handle_advance_partition_reassignment(
        &self,
        request: AdvancePartitionReassignmentRequest,
    ) -> Result<PartitionReassignmentResponse> {
        if !self.can_write_metadata_locally() {
            return Ok(PartitionReassignmentResponse {
                accepted: false,
                metadata_offset: self.metadata_image().metadata_offset,
            });
        }
        let mut preview = self.metadata_image();
        let preview_accepted = if request.step == crate::cluster::ReassignmentStep::Complete {
            preview.complete_partition_reassignment(&request.topic_name, request.partition_index)
        } else {
            preview.advance_partition_reassignment(
                &request.topic_name,
                request.partition_index,
                request.step.clone(),
            )
        };
        if !preview_accepted {
            return Ok(PartitionReassignmentResponse {
                accepted: false,
                metadata_offset: preview.metadata_offset,
            });
        }
        let record = if request.step == crate::cluster::ReassignmentStep::Complete {
            crate::cluster::MetadataRecord::CompletePartitionReassignment {
                topic_name: request.topic_name.clone(),
                partition_index: request.partition_index,
            }
        } else {
            crate::cluster::MetadataRecord::AdvancePartitionReassignment {
                topic_name: request.topic_name.clone(),
                partition_index: request.partition_index,
                step: request.step.clone(),
            }
        };
        let response = self.append_with_retry(|prev_metadata_offset, term, leader_id| {
            crate::cluster::AppendMetadataRequest {
                term,
                leader_id,
                prev_metadata_offset,
                records: vec![record.clone()],
            }
        })?;
        Ok(PartitionReassignmentResponse {
            accepted: response.accepted,
            metadata_offset: response.last_metadata_offset,
        })
    }
}
