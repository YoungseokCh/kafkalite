#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FileStorePolicy {
    pub log_sync_interval: u64,
    pub index_stride: i64,
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
    sync_topic_journal: false,
    sync_producer_journal: false,
    persist_group_membership: false,
    sync_group_journal: false,
    sync_offset_journal: true,
};
