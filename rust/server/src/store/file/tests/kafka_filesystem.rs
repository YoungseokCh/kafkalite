use super::*;
use crate::store::file::log::StoredBatch;

#[tokio::test]
#[ignore]
async fn create_real_kafka_filesystem_fixture() {
    use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
    use rdkafka::client::DefaultClientContext;
    use rdkafka::config::ClientConfig;
    use rdkafka::producer::{FutureProducer, FutureRecord};

    let bootstrap = std::env::var("REAL_KAFKA_BOOTSTRAP").unwrap();
    let topic = real_kafka_topic();
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", &bootstrap)
        .create()
        .unwrap();
    let spec = NewTopic::new(&topic, 1, TopicReplication::Fixed(1));
    let _ = admin.create_topics(&[spec], &AdminOptions::new()).await;

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &bootstrap)
        .set("message.timeout.ms", "10000")
        .create()
        .unwrap();
    producer
        .send(
            FutureRecord::to(&topic)
                .partition(0)
                .key("kafka-key")
                .payload("kafka-value"),
            std::time::Duration::from_secs(10),
        )
        .await
        .unwrap();
}

#[test]
#[ignore]
fn real_kafka_log_dir_open_is_byte_exact_no_write() {
    let source = real_kafka_log_dir();
    let topic = real_kafka_topic();
    let dir = tempdir().unwrap();
    copy_dir_all(&source, dir.path());

    let before = filesystem_manifest(dir.path());
    let store = FileStore::open(dir.path()).unwrap();
    assert!(store.describe_topic(&topic).is_some());
    drop(store);

    assert_eq!(filesystem_manifest(dir.path()), before);
}

#[test]
#[ignore]
fn real_kafka_log_dir_append_changes_only_expected_user_log() {
    let source = real_kafka_log_dir();
    let topic = real_kafka_topic();
    let dir = tempdir().unwrap();
    copy_dir_all(&source, dir.path());

    let before = filesystem_manifest(dir.path());
    let store = FileStore::open(dir.path()).unwrap();
    let next_offset = store.list_offsets(&topic, 0).unwrap().1.offset;
    let record = BrokerRecord {
        offset: next_offset,
        timestamp_ms: 123_456,
        producer_id: -1,
        producer_epoch: -1,
        sequence: next_offset as i32,
        key: Some(Bytes::from_static(b"kafkalite-key")),
        value: Some(Bytes::from_static(b"kafkalite-value")),
        headers_json: b"[]".to_vec(),
    };
    store
        .append_records(&topic, 0, std::slice::from_ref(&record), 123_456)
        .unwrap();
    drop(store);

    let log_path = format!("{topic}-0/00000000000000000000.log");
    let mut expected = before.clone();
    let mut expected_log = before.get(&log_path).unwrap().bytes.clone();
    expected_log.extend_from_slice(
        &StoredBatch::from_records(&[record])
            .encode_binary()
            .unwrap(),
    );
    replace_manifest_file_bytes(&mut expected, &log_path, expected_log);

    assert_eq!(filesystem_manifest(dir.path()), expected);
}

fn real_kafka_log_dir() -> std::path::PathBuf {
    std::env::var_os("REAL_KAFKA_LOG_DIR")
        .map(std::path::PathBuf::from)
        .expect("REAL_KAFKA_LOG_DIR must point at a stopped Kafka log dir")
}

fn real_kafka_topic() -> String {
    std::env::var("REAL_KAFKA_TOPIC").expect("REAL_KAFKA_TOPIC must be set")
}

fn copy_dir_all(source: &std::path::Path, target: &std::path::Path) {
    std::fs::create_dir_all(target).unwrap();
    for entry in std::fs::read_dir(source).unwrap() {
        let entry = entry.unwrap();
        let source_path = entry.path();
        let target_path = target.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_dir_all(&source_path, &target_path);
        } else {
            std::fs::copy(&source_path, &target_path).unwrap();
        }
    }
}
