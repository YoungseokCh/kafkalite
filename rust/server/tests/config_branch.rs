use std::path::{Path, PathBuf};

use kafkalite_server::Config;
use tempfile::TempDir;

fn write_config(dir: &Path, content: &str) -> PathBuf {
    let path = dir.join("server.properties");
    std::fs::write(&path, content).unwrap();
    path
}

fn load_err(content: &str) -> String {
    let dir = TempDir::new().unwrap();
    let path = write_config(dir.path(), content);
    Config::load(path.to_str()).unwrap_err().to_string()
}

#[test]
fn rejects_invalid_properties_line_without_equals() {
    let err = load_err("not-a-key-value-line\n");
    assert!(err.contains("expected key=value"));
}

#[test]
fn rejects_empty_properties_key() {
    let err = load_err("=value\n");
    assert!(err.contains("empty key"));
}

#[test]
fn rejects_invalid_process_role() {
    let err = load_err("process.roles=broker,invalid-role\nlisteners=PLAINTEXT://:19092\n");
    assert!(err.contains("Unsupported process role"));
}

#[test]
fn rejects_invalid_listener_shape() {
    let err = load_err("listeners=PLAINTEXT:19092\n");
    assert!(err.contains("Invalid listener"));
}

#[test]
fn rejects_invalid_listener_port() {
    let err = load_err("listeners=PLAINTEXT://127.0.0.1:not-a-port\n");
    assert!(err.contains("Invalid port"));
}

#[test]
fn rejects_controller_role_without_node_id() {
    let err = load_err(
        "process.roles=broker,controller\nlisteners=PLAINTEXT://:19092,CONTROLLER://:19093\ncontroller.listener.names=CONTROLLER\ncontroller.quorum.voters=1@node1:19093\n",
    );
    assert!(err.contains("Controller role requires node.id"));
}

#[test]
fn rejects_controller_role_when_node_is_missing_from_voters() {
    let err = load_err(
        "process.roles=broker,controller\nnode.id=2\nlisteners=PLAINTEXT://:19092,CONTROLLER://:19093\ncontroller.listener.names=CONTROLLER\ncontroller.quorum.voters=1@node1:19093,3@node3:19093\n",
    );
    assert!(err.contains("requires node.id to appear"));
}

#[test]
fn rejects_invalid_controller_voter_entry() {
    let err = load_err(
        "process.roles=broker,controller\nnode.id=1\nlisteners=PLAINTEXT://:19092,CONTROLLER://:19093\ncontroller.listener.names=CONTROLLER\ncontroller.quorum.voters=not-a-voter-entry\n",
    );
    assert!(err.contains("Invalid voter entry"));
}

#[test]
fn rejects_num_partitions_less_than_one() {
    let err = load_err("listeners=PLAINTEXT://:19092\nnum.partitions=0\n");
    assert!(err.contains("expected >= 1"));
}

#[test]
fn ignores_comment_and_blank_lines_while_loading_defaults() {
    let dir = TempDir::new().unwrap();
    let path = write_config(
        dir.path(),
        "# comment line\n\nlisteners=PLAINTEXT://127.0.0.1:19092\n",
    );

    let config = Config::load(path.to_str()).unwrap();

    assert_eq!(config.broker.host, "127.0.0.1");
    assert_eq!(config.broker.port, 19092);
    assert_eq!(config.broker.advertised_host, "127.0.0.1");
    assert_eq!(config.broker.advertised_port, 19092);
    assert_eq!(config.broker.cluster_id, "kafkalite-single-broker");
}

#[test]
fn rejects_empty_process_roles_list() {
    let err = load_err("process.roles=, ,\nlisteners=PLAINTEXT://:19092\n");
    assert!(err.contains("at least one role"));
}

#[test]
fn rejects_empty_listeners_value() {
    let err = load_err("listeners=, ,\n");
    assert!(err.contains("expected at least one listener"));
}

#[test]
fn controller_only_role_does_not_require_plaintext_listener() {
    let dir = TempDir::new().unwrap();
    let path = write_config(
        dir.path(),
        "process.roles=controller\nnode.id=1\nlisteners=CONTROLLER://:19093\ncontroller.listener.names=CONTROLLER\ncontroller.quorum.voters=1@node1:19093\n",
    );

    let config = Config::load(path.to_str()).unwrap();

    assert_eq!(config.cluster.process_roles.len(), 1);
    assert_eq!(config.cluster.node_id, 1);
    assert!(config.cluster.listeners.contains_key("CONTROLLER"));
}
