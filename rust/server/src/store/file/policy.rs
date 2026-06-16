#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FileStorePolicy {
    pub log_sync_interval: u64,
    pub index_stride: i64,
    pub segment_bytes: u64,
    pub segment_ms: u64,
    pub retention_bytes: Option<u64>,
    pub retention_ms: Option<u64>,
    pub sync_topic_journal: bool,
    pub sync_producer_journal: bool,
    pub persist_group_membership: bool,
    pub sync_group_journal: bool,
    pub sync_offset_journal: bool,
}

impl Default for FileStorePolicy {
    fn default() -> Self {
        Self {
            log_sync_interval: 64,
            index_stride: 16,
            segment_bytes: 1024 * 1024 * 1024,
            segment_ms: 24 * 7 * 60 * 60 * 1000,
            retention_bytes: None,
            retention_ms: Some(24 * 7 * 60 * 60 * 1000),
            sync_topic_journal: false,
            sync_producer_journal: false,
            persist_group_membership: false,
            sync_group_journal: false,
            sync_offset_journal: true,
        }
    }
}

pub const DEFAULT_POLICY: FileStorePolicy = FileStorePolicy {
    log_sync_interval: 64,
    index_stride: 16,
    segment_bytes: 1024 * 1024 * 1024,
    segment_ms: 24 * 7 * 60 * 60 * 1000,
    retention_bytes: None,
    retention_ms: Some(24 * 7 * 60 * 60 * 1000),
    sync_topic_journal: false,
    sync_producer_journal: false,
    persist_group_membership: false,
    sync_group_journal: false,
    sync_offset_journal: true,
};
