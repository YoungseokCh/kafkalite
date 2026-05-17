use anyhow::Result;

use crate::broker::KafkaBroker;

pub(super) fn maybe_auto_create_topic(
    broker: &KafkaBroker,
    topic: &str,
    partition: i32,
    now_ms: i64,
) -> Result<()> {
    let known = broker
        .store()
        .topic_metadata(Some(&[topic.to_string()]), now_ms)?;
    if !known.is_empty() {
        return Ok(());
    }
    if partition < 0 || partition >= broker.config().storage.default_partitions {
        return Ok(());
    }
    if !broker.cluster().can_auto_create_topics_locally() {
        return Ok(());
    }
    broker
        .store()
        .ensure_topic(topic, broker.config().storage.default_partitions, now_ms)?;
    let metadata = broker
        .store()
        .topic_metadata(Some(&[topic.to_string()]), now_ms)?;
    broker.sync_topic_metadata(&metadata)?;
    Ok(())
}
