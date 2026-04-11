#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FileStorePolicy {
    pub log_sync_interval: u64,
    pub index_stride: i64,
    pub sync_offset_journal: bool,
}

impl Default for FileStorePolicy {
    fn default() -> Self {
        Self {
            log_sync_interval: 64,
            index_stride: 16,
            sync_offset_journal: true,
        }
    }
}

pub const DEFAULT_POLICY: FileStorePolicy = FileStorePolicy {
    log_sync_interval: 64,
    index_stride: 16,
    sync_offset_journal: true,
};
