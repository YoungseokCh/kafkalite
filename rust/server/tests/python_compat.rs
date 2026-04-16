use std::ffi::{OsStr, OsString};
use std::fs;
use std::net::TcpListener;
#[cfg(unix)]
use std::os::unix::ffi::OsStringExt;
use std::path::Path;
use std::process::Command;
use std::sync::Arc;
use std::time::Duration;

use kafkalite_server::{Config, FileStore, KafkaBroker};
use tempfile::tempdir;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn aiokafka_compatibility_matrix() {
    let _ = env_logger::builder().is_test(true).try_init();
    let _ = tracing_subscriber::fmt()
        .with_test_writer()
        .with_env_filter("kafkalite=debug")
        .try_init();
    let Some(python) = std::env::var_os("AIOKAFKA_PYTHON") else {
        eprintln!(
            "skipping aiokafka compatibility matrix: set AIOKAFKA_PYTHON to a python interpreter with aiokafka installed"
        );
        return;
    };

    if let Some(real_kafka_bootstrap) = std::env::var_os("REAL_KAFKA_BOOTSTRAP") {
        run_aiokafka_matrix(&python, &real_kafka_bootstrap);
        return;
    }

    let tempdir = tempdir().unwrap();
    let port = free_port();
    let config = Config::single_node(tempdir.path().join("kafkalite-data"), port, 3);
    let store = Arc::new(FileStore::open(&config.storage.data_dir).unwrap());
    let broker = KafkaBroker::new(config, store).unwrap();
    let handle = tokio::spawn(async move { broker.run().await });
    tokio::time::sleep(Duration::from_millis(150)).await;

    let bootstrap = format!("127.0.0.1:{port}");
    run_aiokafka_matrix_str(&python, &bootstrap);

    handle.abort();
    let _ = handle.await;
}

fn run_aiokafka_matrix(python: &OsStr, bootstrap: &OsStr) {
    for script in aiokafka_scripts() {
        let output = Command::new(python)
            .arg(&script)
            .arg(bootstrap)
            .current_dir(env!("CARGO_MANIFEST_DIR"))
            .output()
            .unwrap();

        if !output.status.success() {
            let script_name = script.to_string_lossy();
            panic!(
                "aiokafka compatibility matrix failed for {script_name}:\nstdout:\n{}\nstderr:\n{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
        }
    }
}

fn aiokafka_scripts() -> Vec<OsString> {
    let tests_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests");
    let scripts = fs::read_dir(&tests_dir)
        .unwrap()
        .filter_map(Result::ok)
        .filter_map(|entry| {
            let is_file = entry.file_type().ok()?.is_file();
            script_path_from_name(entry.file_name(), is_file)
        })
        .collect::<Vec<_>>();
    order_aiokafka_scripts(scripts)
}

fn script_path_from_name(file_name: OsString, is_file: bool) -> Option<OsString> {
    if !is_file {
        return None;
    }
    let name = file_name.to_str()?;
    if name.starts_with("aiokafka") && name.ends_with(".py") {
        Some(OsString::from(format!("tests/{name}")))
    } else {
        None
    }
}

fn order_aiokafka_scripts(mut scripts: Vec<OsString>) -> Vec<OsString> {
    let smoke_script = OsString::from("tests/aiokafka_smoke.py");
    scripts.sort();
    if let Some(smoke_index) = scripts.iter().position(|script| script == &smoke_script) {
        let smoke = scripts.remove(smoke_index);
        scripts.insert(0, smoke);
    }
    scripts
}

fn run_aiokafka_matrix_str(python: &OsStr, bootstrap: &str) {
    run_aiokafka_matrix(python, OsStr::new(bootstrap));
}

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

#[test]
fn aiokafka_scripts_are_smoke_first_sorted_and_filtered() {
    let scripts = aiokafka_scripts();

    assert!(!scripts.is_empty());
    assert!(
        scripts
            .iter()
            .all(|script| script.to_string_lossy().starts_with("tests/aiokafka"))
    );
    assert!(
        scripts
            .iter()
            .all(|script| script.to_string_lossy().ends_with(".py"))
    );
    assert_eq!(
        scripts.first(),
        Some(&OsString::from("tests/aiokafka_smoke.py"))
    );

    let mut sorted = scripts[1..].to_vec();
    sorted.sort();
    assert_eq!(scripts[1..], sorted);
    assert!(scripts.contains(&OsString::from("tests/aiokafka_smoke.py")));
}

#[test]
fn script_helpers_filter_and_order_synthetic_names() {
    let discovered = [
        OsString::from("notes.txt"),
        OsString::from("aiokafka_z.py"),
        OsString::from("aiokafka_smoke.py"),
        OsString::from("aiokafka_a.py"),
        OsString::from("not_aiokafka.py"),
        OsString::from("aiokafka_helper.rs"),
    ]
    .into_iter()
    .filter_map(|file_name| script_path_from_name(file_name, true))
    .collect::<Vec<_>>();

    let ordered = order_aiokafka_scripts(discovered);

    assert_eq!(
        ordered,
        vec![
            OsString::from("tests/aiokafka_smoke.py"),
            OsString::from("tests/aiokafka_a.py"),
            OsString::from("tests/aiokafka_z.py"),
        ]
    );
}

#[test]
fn script_path_from_name_rejects_non_files() {
    assert_eq!(
        script_path_from_name(OsString::from("aiokafka_dir.py"), false),
        None
    );
}

#[test]
fn script_path_from_name_accepts_valid_python_file() {
    assert_eq!(
        script_path_from_name(OsString::from("aiokafka_sample.py"), true),
        Some(OsString::from("tests/aiokafka_sample.py"))
    );
}

#[cfg(unix)]
#[test]
fn script_path_from_name_rejects_non_utf8_names() {
    let non_utf8 = OsString::from_vec(vec![0x61, 0x69, 0x6f, 0xff, 0x2e, 0x70, 0x79]);
    assert_eq!(script_path_from_name(non_utf8, true), None);
}

#[test]
fn order_aiokafka_scripts_sorts_normally_without_smoke_script() {
    let ordered = order_aiokafka_scripts(vec![
        OsString::from("tests/aiokafka_z.py"),
        OsString::from("tests/aiokafka_a.py"),
        OsString::from("tests/aiokafka_m.py"),
    ]);

    assert_eq!(
        ordered,
        vec![
            OsString::from("tests/aiokafka_a.py"),
            OsString::from("tests/aiokafka_m.py"),
            OsString::from("tests/aiokafka_z.py"),
        ]
    );
}
