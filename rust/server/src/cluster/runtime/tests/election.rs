use anyhow::anyhow;

use crate::cluster::transport::ClusterRpcTransport;
use crate::cluster::{ClusterRpcRequest, VoteRequest, test_support::ThreeNodeClusterHarness};

#[derive(Clone, Copy)]
struct FailingTransport;

impl ClusterRpcTransport for FailingTransport {
    fn send(
        &self,
        _request: crate::cluster::ClusterRpcRequest,
    ) -> anyhow::Result<crate::cluster::ClusterRpcResponse> {
        Err(anyhow!("scripted transport failure"))
    }

    fn send_to(
        &self,
        _target: &crate::cluster::ClusterRpcTarget,
        _request: crate::cluster::ClusterRpcRequest,
    ) -> anyhow::Result<crate::cluster::ClusterRpcResponse> {
        Err(anyhow!("scripted transport failure"))
    }
}

#[test]
fn election_steps_down_on_higher_term_vote_response() {
    let harness = ThreeNodeClusterHarness::new_controller_triplet();
    let transport = harness.transport_from_node(1);
    let target = transport.resolve_target(2).unwrap();
    let _ = harness
        .node2
        .runtime
        .handle_vote(VoteRequest {
            term: 5,
            candidate_id: 2,
            last_metadata_offset: harness.node2.runtime.metadata_image().metadata_offset,
        })
        .unwrap();

    let elected = harness
        .node1
        .runtime
        .run_election(&transport, &[target])
        .unwrap();

    assert!(!elected);
    assert_eq!(harness.node1.runtime.quorum_snapshot().current_term, 5);
    assert_eq!(harness.node1.runtime.quorum_snapshot().leader_id, None);
}

#[test]
fn run_election_propagates_transport_errors() {
    let harness = ThreeNodeClusterHarness::new_controller_triplet();
    let target = harness.transport_from_node(1).resolve_target(2).unwrap();
    assert!(
        FailingTransport
            .send(ClusterRpcRequest::Vote(VoteRequest {
                term: 1,
                candidate_id: 1,
                last_metadata_offset: 0,
            }))
            .is_err()
    );

    let err = harness
        .node1
        .runtime
        .run_election(&FailingTransport, &[target])
        .unwrap_err()
        .to_string();

    assert!(err.contains("scripted transport failure"));
}
