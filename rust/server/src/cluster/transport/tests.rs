use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::Mutex;

use anyhow::Result;
use tempfile::tempdir;
use tokio::net::TcpListener;

use crate::cluster::{
    ClusterConfig, ClusterRuntime, ControllerQuorumVoter, ProcessRole,
    test_support::TwoNodeClusterHarness,
};
use crate::config::Config;
use crate::store::{FileStore, Storage};

use super::*;

#[derive(Clone)]
struct ScriptedTransport {
    responses: Arc<Mutex<VecDeque<ClusterRpcResponse>>>,
}

impl ScriptedTransport {
    fn new(responses: impl IntoIterator<Item = ClusterRpcResponse>) -> Self {
        Self {
            responses: Arc::new(Mutex::new(responses.into_iter().collect())),
        }
    }
}

impl ClusterRpcTransport for ScriptedTransport {
    fn send(&self, _request: ClusterRpcRequest) -> Result<ClusterRpcResponse> {
        self.responses
            .lock()
            .unwrap()
            .pop_front()
            .ok_or_else(|| anyhow::anyhow!("missing scripted response"))
    }
}

mod local;
mod remote_basic;
mod remote_failover;
mod scripted_errors;
mod scripted_success;
mod tcp_broker;
mod tcp_core;
mod tcp_wrappers;
