use anyhow::Result;

use crate::cluster::ClusterRpcTransport;
use crate::cluster::rpc::{
    AppendMetadataRequest, AppendMetadataResponse, VoteRequest, VoteResponse,
};
use crate::cluster::{ClusterMetadataImage, MetadataRecord};

use super::ClusterRuntime;

impl ClusterRuntime {
    pub fn handle_append_metadata(
        &self,
        request: AppendMetadataRequest,
    ) -> Result<AppendMetadataResponse> {
        let snapshot = {
            let mut quorum = self.quorum.lock().expect("quorum state mutex poisoned");
            if !quorum.is_voter(request.leader_id) {
                let snapshot = quorum.snapshot();
                return Ok(AppendMetadataResponse {
                    term: snapshot.current_term,
                    accepted: false,
                    last_metadata_offset: self.metadata_image().metadata_offset,
                });
            }
            if request.term < quorum.current_term() {
                let snapshot = quorum.snapshot();
                return Ok(AppendMetadataResponse {
                    term: snapshot.current_term,
                    accepted: false,
                    last_metadata_offset: self.metadata_image().metadata_offset,
                });
            }
            let current = quorum.snapshot();
            if request.term == current.current_term
                && current.leader_id.is_some()
                && current.leader_id != Some(request.leader_id)
            {
                return Ok(AppendMetadataResponse {
                    term: current.current_term,
                    accepted: false,
                    last_metadata_offset: self.metadata_image().metadata_offset,
                });
            }
            quorum.follow_leader(request.leader_id, request.term);
            quorum.snapshot()
        };
        let mut metadata = self
            .metadata
            .lock()
            .expect("cluster metadata mutex poisoned");
        let accepted = metadata
            .append_remote_records(request.prev_metadata_offset, &request.records)?
            || metadata_records_match_current(metadata.image(), &request.records);
        Ok(AppendMetadataResponse {
            term: snapshot.current_term,
            accepted,
            last_metadata_offset: metadata.metadata_offset(),
        })
    }

    pub fn handle_vote(&self, request: VoteRequest) -> Result<VoteResponse> {
        let current_offset = self.metadata_image().metadata_offset;
        let mut quorum = self.quorum.lock().expect("quorum state mutex poisoned");
        if !quorum.is_voter(request.candidate_id) {
            let term = if request.term <= quorum.current_term() {
                quorum.current_term()
            } else {
                quorum.current_term().saturating_sub(1)
            };
            return Ok(VoteResponse {
                term,
                vote_granted: false,
            });
        }
        let vote_granted = candidate_log_is_fresh(request.last_metadata_offset, current_offset)
            && quorum.record_vote(request.candidate_id, request.term);
        Ok(VoteResponse {
            term: quorum.current_term(),
            vote_granted,
        })
    }

    pub fn run_election<T: ClusterRpcTransport>(
        &self,
        transport: &T,
        targets: &[crate::cluster::ClusterRpcTarget],
    ) -> Result<bool> {
        let (term, candidate_id, last_metadata_offset, majority_with_self) = {
            let mut quorum = self.quorum.lock().expect("quorum state mutex poisoned");
            let term = quorum.become_candidate();
            (
                term,
                quorum.local_node_id(),
                self.metadata_image().metadata_offset,
                quorum.has_majority(1),
            )
        };
        if majority_with_self {
            let mut quorum = self.quorum.lock().expect("quorum state mutex poisoned");
            quorum.become_leader();
            if let Some(leader_id) = quorum.snapshot().leader_id {
                self.metadata
                    .lock()
                    .expect("cluster metadata mutex poisoned")
                    .sync_controller(leader_id)?;
            }
            return Ok(true);
        }

        let mut votes = 1_usize;
        for target in targets {
            let response = transport.vote_to(
                target,
                VoteRequest {
                    term,
                    candidate_id,
                    last_metadata_offset,
                },
            )?;
            if response.term > term {
                let mut quorum = self.quorum.lock().expect("quorum state mutex poisoned");
                quorum.step_down(response.term);
                return Ok(false);
            }
            if response.vote_granted {
                votes += 1;
            }
        }
        let mut quorum = self.quorum.lock().expect("quorum state mutex poisoned");
        if quorum.has_majority(votes) {
            quorum.become_leader();
            if let Some(leader_id) = quorum.snapshot().leader_id {
                self.metadata
                    .lock()
                    .expect("cluster metadata mutex poisoned")
                    .sync_controller(leader_id)?;
            }
            Ok(true)
        } else {
            Ok(false)
        }
    }

    pub(super) fn append_with_retry(
        &self,
        build: impl Fn(i64, i64, i32) -> AppendMetadataRequest,
    ) -> Result<AppendMetadataResponse> {
        const MAX_ATTEMPTS: usize = 3;
        for _ in 0..MAX_ATTEMPTS {
            let snapshot = self.quorum_snapshot();
            let leader_id = snapshot.leader_id.unwrap_or(self.config.node_id);
            let request = build(
                self.metadata_image().metadata_offset,
                snapshot.current_term,
                leader_id,
            );
            let response = self.handle_append_metadata(request)?;
            if response.accepted {
                return Ok(response);
            }
        }
        anyhow::bail!("metadata append rejected after retry budget")
    }
}

fn candidate_log_is_fresh(candidate_offset: i64, current_offset: i64) -> bool {
    candidate_offset >= current_offset
}

fn metadata_records_match_current(
    image: &ClusterMetadataImage,
    records: &[MetadataRecord],
) -> bool {
    records.iter().all(|record| match record {
        MetadataRecord::SetController { controller_id } => image.controller_id == *controller_id,
        MetadataRecord::RegisterBroker(broker) => image.brokers.iter().any(|entry| entry == broker),
        MetadataRecord::UpdatePartitionLeader {
            topic_name,
            partition_index,
            leader_id,
            leader_epoch,
        } => image
            .partition_state_view(topic_name, *partition_index)
            .is_some_and(|(current_leader, current_epoch, _, _)| {
                current_leader == *leader_id && current_epoch == *leader_epoch
            }),
        MetadataRecord::UpdatePartitionReplication {
            topic_name,
            partition_index,
            replicas,
            isr,
            leader_epoch,
        } => image
            .topics
            .iter()
            .find(|topic| topic.name == *topic_name)
            .and_then(|topic| {
                topic
                    .partitions
                    .iter()
                    .find(|partition| partition.partition == *partition_index)
            })
            .is_some_and(|partition| {
                partition.replicas == *replicas
                    && partition.isr == *isr
                    && partition.leader_epoch == *leader_epoch
            }),
        MetadataRecord::UpsertTopic(topic) => image.topics.iter().any(|entry| entry == topic),
        MetadataRecord::UpdateReplicaProgress { .. }
        | MetadataRecord::BeginPartitionReassignment { .. }
        | MetadataRecord::AdvancePartitionReassignment { .. }
        | MetadataRecord::CompletePartitionReassignment { .. } => false,
    })
}
