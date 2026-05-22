pub(super) fn is_internal_topic_name(topic: &str) -> bool {
    topic.starts_with("__")
}
