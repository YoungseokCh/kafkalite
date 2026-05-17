use crate::store::DEFAULT_PARTITION;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct OffsetKey {
    group_id: String,
    topic: String,
    partition: i32,
}

impl OffsetKey {
    pub(super) fn new(group_id: &str, topic: &str, partition: i32) -> Self {
        Self {
            group_id: group_id.to_string(),
            topic: topic.to_string(),
            partition,
        }
    }

    pub(super) fn from_serialized(value: &str) -> Self {
        let mut parts = value.splitn(3, ':');
        let group_id = parts.next().unwrap_or_default().to_string();
        let topic = parts.next().unwrap_or_default().to_string();
        let partition = parts
            .next()
            .and_then(|value| value.parse().ok())
            .unwrap_or(DEFAULT_PARTITION);
        Self {
            group_id,
            topic,
            partition,
        }
    }

    pub(super) fn serialize(&self) -> String {
        format!("{}:{}:{}", self.group_id, self.topic, self.partition)
    }
}
