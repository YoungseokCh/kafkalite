use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::SystemTime;

use memmap2::Mmap;

use crate::store::Result;

use super::super::policy::DEFAULT_POLICY;
use super::{SegmentPaths, StoredBatch};

#[derive(Debug, Clone, Copy)]
pub(super) struct IndexEntry {
    pub base_offset: i64,
    pub position: u64,
    pub length: u32,
    pub last_offset: i64,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct TimeIndexEntry {
    pub max_timestamp_ms: i64,
    pub offset: i64,
}

const INDEX_ENTRY_BYTES: usize = 28;
const TIME_INDEX_ENTRY_BYTES: usize = 16;
const KAFKA_INDEX_ENTRY_BYTES: usize = 8;
const KAFKA_TIME_INDEX_ENTRY_BYTES: usize = 12;

#[derive(Clone)]
struct CachedMappedFile {
    modified: Option<SystemTime>,
    len: u64,
    bytes: Arc<Mmap>,
}

pub(super) fn write_index_entry(writer: &mut File, entry: &IndexEntry) -> Result<()> {
    writer.write_all(&entry.base_offset.to_le_bytes())?;
    writer.write_all(&entry.position.to_le_bytes())?;
    writer.write_all(&entry.length.to_le_bytes())?;
    writer.write_all(&entry.last_offset.to_le_bytes())?;
    Ok(())
}

pub(super) fn should_index_batch(batch: &StoredBatch) -> bool {
    batch.base_offset == 0 || batch.base_offset % DEFAULT_POLICY.index_stride == 0
}

pub(super) fn write_time_index_entry(writer: &mut File, entry: &TimeIndexEntry) -> Result<()> {
    writer.write_all(&entry.max_timestamp_ms.to_le_bytes())?;
    writer.write_all(&entry.offset.to_le_bytes())?;
    Ok(())
}

pub(super) fn lookup_offset(
    segment: &SegmentPaths,
    target_offset: i64,
) -> Result<Option<IndexEntry>> {
    if !segment.index.exists() {
        return Ok(None);
    }
    let bytes = cached_file_bytes(&segment.index)?;
    let log_len = std::fs::metadata(&segment.log)
        .map(|metadata| metadata.len())
        .unwrap_or(0);
    match detect_offset_index_format(&bytes, segment.base_offset, log_len) {
        Some(IndexFormat::Native) => {
            let entry_count = bytes.len() / INDEX_ENTRY_BYTES;
            if entry_count == 0 {
                return Ok(None);
            }
            let mut low = 0_usize;
            let mut high = entry_count;
            while low < high {
                let mid = (low + high) / 2;
                let entry = decode_index_entry(
                    &bytes[mid * INDEX_ENTRY_BYTES..(mid + 1) * INDEX_ENTRY_BYTES],
                );
                if entry.base_offset <= target_offset {
                    low = mid + 1;
                } else {
                    high = mid;
                }
            }
            if low == 0 {
                return Ok(None);
            }
            let start = (low - 1) * INDEX_ENTRY_BYTES;
            Ok(Some(decode_index_entry(
                &bytes[start..start + INDEX_ENTRY_BYTES],
            )))
        }
        Some(IndexFormat::Kafka) => {
            lookup_kafka_offset_index(&bytes, segment.base_offset, target_offset)
        }
        None => Ok(None),
    }
}

fn decode_index_entry(bytes: &[u8]) -> IndexEntry {
    IndexEntry {
        base_offset: i64::from_le_bytes(bytes[0..8].try_into().expect("offset bytes")),
        position: u64::from_le_bytes(bytes[8..16].try_into().expect("position bytes")),
        length: u32::from_le_bytes(bytes[16..20].try_into().expect("length bytes")),
        last_offset: i64::from_le_bytes(bytes[20..28].try_into().expect("last offset bytes")),
    }
}

pub(super) fn lookup_timestamp(
    segment: &SegmentPaths,
    target_timestamp_ms: i64,
) -> Result<Option<TimeIndexEntry>> {
    if !segment.timeindex.exists() {
        return Ok(None);
    }
    let bytes = cached_file_bytes(&segment.timeindex)?;
    let log_len = std::fs::metadata(&segment.log)
        .map(|metadata| metadata.len())
        .unwrap_or(0);
    match detect_time_index_format(&bytes, segment.base_offset, log_len) {
        Some(IndexFormat::Native) => {
            let entry_count = bytes.len() / TIME_INDEX_ENTRY_BYTES;
            if entry_count == 0 {
                return Ok(None);
            }
            let mut low = 0_usize;
            let mut high = entry_count;
            while low < high {
                let mid = (low + high) / 2;
                let entry = decode_time_index_entry(
                    &bytes[mid * TIME_INDEX_ENTRY_BYTES..(mid + 1) * TIME_INDEX_ENTRY_BYTES],
                );
                if entry.max_timestamp_ms <= target_timestamp_ms {
                    low = mid + 1;
                } else {
                    high = mid;
                }
            }
            if low == 0 {
                return Ok(None);
            }
            let start = (low - 1) * TIME_INDEX_ENTRY_BYTES;
            Ok(Some(decode_time_index_entry(
                &bytes[start..start + TIME_INDEX_ENTRY_BYTES],
            )))
        }
        Some(IndexFormat::Kafka) => {
            lookup_kafka_time_index(&bytes, segment.base_offset, target_timestamp_ms)
        }
        None => Ok(None),
    }
}

pub(super) fn last_time_index_entry(segment: &SegmentPaths) -> Result<Option<TimeIndexEntry>> {
    if !segment.timeindex.exists() {
        return Ok(None);
    }
    let bytes = cached_file_bytes(&segment.timeindex)?;
    let log_len = std::fs::metadata(&segment.log)
        .map(|metadata| metadata.len())
        .unwrap_or(0);
    match detect_time_index_format(&bytes, segment.base_offset, log_len) {
        Some(IndexFormat::Native) => {
            let len = bytes.len();
            if len < TIME_INDEX_ENTRY_BYTES {
                return Ok(None);
            }
            let start = len - TIME_INDEX_ENTRY_BYTES;
            Ok(Some(decode_time_index_entry(
                &bytes[start..start + TIME_INDEX_ENTRY_BYTES],
            )))
        }
        Some(IndexFormat::Kafka) => {
            let len = bytes.len();
            if len < KAFKA_TIME_INDEX_ENTRY_BYTES {
                return Ok(None);
            }
            let start = len - KAFKA_TIME_INDEX_ENTRY_BYTES;
            Ok(Some(decode_kafka_time_index_entry(
                &bytes[start..start + KAFKA_TIME_INDEX_ENTRY_BYTES],
                segment.base_offset,
            )))
        }
        None => Ok(None),
    }
}

fn decode_time_index_entry(bytes: &[u8]) -> TimeIndexEntry {
    TimeIndexEntry {
        max_timestamp_ms: i64::from_le_bytes(bytes[0..8].try_into().expect("timestamp bytes")),
        offset: i64::from_le_bytes(bytes[8..16].try_into().expect("offset bytes")),
    }
}

#[derive(Clone, Copy)]
enum IndexFormat {
    Native,
    Kafka,
}

const KAFKA_INDEX_INTERVAL_BYTES: u64 = 4096;

fn lookup_kafka_offset_index(
    bytes: &[u8],
    segment_base_offset: i64,
    target_offset: i64,
) -> Result<Option<IndexEntry>> {
    let entry_count = bytes.len() / KAFKA_INDEX_ENTRY_BYTES;
    if entry_count == 0 {
        return Ok(None);
    }
    let mut low = 0_usize;
    let mut high = entry_count;
    while low < high {
        let mid = (low + high) / 2;
        let entry = decode_kafka_offset_index_entry(
            &bytes[mid * KAFKA_INDEX_ENTRY_BYTES..(mid + 1) * KAFKA_INDEX_ENTRY_BYTES],
            segment_base_offset,
        );
        if entry.base_offset <= target_offset {
            low = mid + 1;
        } else {
            high = mid;
        }
    }
    if low == 0 {
        return Ok(None);
    }
    let start = (low - 1) * KAFKA_INDEX_ENTRY_BYTES;
    Ok(Some(decode_kafka_offset_index_entry(
        &bytes[start..start + KAFKA_INDEX_ENTRY_BYTES],
        segment_base_offset,
    )))
}

pub(super) fn append_kafka_index_entries(
    segment: &SegmentPaths,
    batch: &StoredBatch,
    position: u64,
) -> Result<()> {
    let has_kafka_offset_index = segment_has_kafka_offset_index(segment)?;
    let has_kafka_time_index = segment_has_kafka_time_index(segment)?;
    if !has_kafka_offset_index && !has_kafka_time_index {
        return Ok(());
    }
    let appended_offset_index =
        has_kafka_offset_index && should_append_kafka_index_entry(segment, position)?;
    if appended_offset_index {
        let relative_offset = i32::try_from(batch.last_offset - segment.base_offset)
            .map_err(|_| std::io::Error::other("relative offset overflow"))?;
        let physical_position = i32::try_from(position)
            .map_err(|_| std::io::Error::other("index position overflow"))?;
        let mut index = std::fs::OpenOptions::new()
            .append(true)
            .open(&segment.index)?;
        index.write_all(&relative_offset.to_be_bytes())?;
        index.write_all(&physical_position.to_be_bytes())?;
    }
    if has_kafka_time_index {
        update_kafka_time_index_entry(
            segment,
            batch.last_offset,
            batch.max_timestamp_ms,
            appended_offset_index,
        )?;
    }
    Ok(())
}

fn segment_has_kafka_offset_index(segment: &SegmentPaths) -> Result<bool> {
    if !segment.index.exists() {
        return Ok(false);
    }
    let index_bytes = cached_file_bytes(&segment.index)?;
    let log_len = std::fs::metadata(&segment.log)
        .map(|metadata| metadata.len())
        .unwrap_or(0);
    Ok(matches!(
        detect_offset_index_format(&index_bytes, segment.base_offset, log_len),
        Some(IndexFormat::Kafka)
    ))
}

fn segment_has_kafka_time_index(segment: &SegmentPaths) -> Result<bool> {
    if !segment.timeindex.exists() {
        return Ok(false);
    }
    let timeindex_bytes = cached_file_bytes(&segment.timeindex)?;
    let log_len = std::fs::metadata(&segment.log)
        .map(|metadata| metadata.len())
        .unwrap_or(0);
    Ok(matches!(
        detect_time_index_format(&timeindex_bytes, segment.base_offset, log_len),
        Some(IndexFormat::Kafka)
    ))
}

fn should_append_kafka_index_entry(segment: &SegmentPaths, position: u64) -> Result<bool> {
    let last_position = last_kafka_index_position(segment)?.unwrap_or(0);
    Ok(position.saturating_sub(last_position) > KAFKA_INDEX_INTERVAL_BYTES)
}

fn last_kafka_index_position(segment: &SegmentPaths) -> Result<Option<u64>> {
    let bytes = cached_file_bytes(&segment.index)?;
    let len = bytes.len();
    if len < KAFKA_INDEX_ENTRY_BYTES {
        return Ok(None);
    }
    let start = len - KAFKA_INDEX_ENTRY_BYTES;
    Ok(Some(
        decode_kafka_offset_index_entry(
            &bytes[start..start + KAFKA_INDEX_ENTRY_BYTES],
            segment.base_offset,
        )
        .position,
    ))
}

fn update_kafka_time_index_entry(
    segment: &SegmentPaths,
    batch_last_offset: i64,
    batch_max_timestamp_ms: i64,
    appended_offset_index: bool,
) -> Result<()> {
    let last = last_time_index_entry(segment)?;
    if let Some(previous) = last
        && (batch_max_timestamp_ms <= previous.max_timestamp_ms
            || batch_last_offset <= previous.offset)
    {
        return Ok(());
    }

    let relative_offset = i32::try_from(batch_last_offset - segment.base_offset)
        .map_err(|_| std::io::Error::other("relative offset overflow"))?;
    let mut timeindex = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&segment.timeindex)?;
    let len = timeindex.metadata()?.len();
    let should_overwrite_last =
        !appended_offset_index && len >= KAFKA_TIME_INDEX_ENTRY_BYTES as u64;
    if should_overwrite_last {
        use std::io::Seek;
        timeindex.seek(std::io::SeekFrom::Start(
            len - KAFKA_TIME_INDEX_ENTRY_BYTES as u64,
        ))?;
    } else {
        use std::io::Seek;
        timeindex.seek(std::io::SeekFrom::End(0))?;
    }
    timeindex.write_all(&batch_max_timestamp_ms.to_be_bytes())?;
    timeindex.write_all(&relative_offset.to_be_bytes())?;
    Ok(())
}

fn decode_kafka_offset_index_entry(bytes: &[u8], segment_base_offset: i64) -> IndexEntry {
    let relative_offset = i32::from_be_bytes(bytes[0..4].try_into().expect("offset bytes"));
    let position = i32::from_be_bytes(bytes[4..8].try_into().expect("position bytes"));
    let offset = segment_base_offset + i64::from(relative_offset);
    IndexEntry {
        base_offset: offset,
        position: position as u64,
        length: 0,
        last_offset: offset,
    }
}

fn lookup_kafka_time_index(
    bytes: &[u8],
    segment_base_offset: i64,
    target_timestamp_ms: i64,
) -> Result<Option<TimeIndexEntry>> {
    let entry_count = bytes.len() / KAFKA_TIME_INDEX_ENTRY_BYTES;
    if entry_count == 0 {
        return Ok(None);
    }
    let mut low = 0_usize;
    let mut high = entry_count;
    while low < high {
        let mid = (low + high) / 2;
        let entry = decode_kafka_time_index_entry(
            &bytes[mid * KAFKA_TIME_INDEX_ENTRY_BYTES..(mid + 1) * KAFKA_TIME_INDEX_ENTRY_BYTES],
            segment_base_offset,
        );
        if entry.max_timestamp_ms <= target_timestamp_ms {
            low = mid + 1;
        } else {
            high = mid;
        }
    }
    if low == 0 {
        return Ok(None);
    }
    let start = (low - 1) * KAFKA_TIME_INDEX_ENTRY_BYTES;
    Ok(Some(decode_kafka_time_index_entry(
        &bytes[start..start + KAFKA_TIME_INDEX_ENTRY_BYTES],
        segment_base_offset,
    )))
}

fn decode_kafka_time_index_entry(bytes: &[u8], segment_base_offset: i64) -> TimeIndexEntry {
    let max_timestamp_ms = i64::from_be_bytes(bytes[0..8].try_into().expect("timestamp bytes"));
    let relative_offset = i32::from_be_bytes(bytes[8..12].try_into().expect("offset bytes"));
    TimeIndexEntry {
        max_timestamp_ms,
        offset: segment_base_offset + i64::from(relative_offset),
    }
}

fn detect_offset_index_format(
    bytes: &[u8],
    segment_base_offset: i64,
    log_len: u64,
) -> Option<IndexFormat> {
    if bytes.is_empty() {
        return None;
    }
    if bytes.len().is_multiple_of(INDEX_ENTRY_BYTES)
        && native_offset_entries_plausible(bytes, log_len)
    {
        return Some(IndexFormat::Native);
    }
    if bytes.len().is_multiple_of(KAFKA_INDEX_ENTRY_BYTES)
        && kafka_offset_entries_plausible(bytes, segment_base_offset, log_len)
    {
        return Some(IndexFormat::Kafka);
    }
    None
}

fn detect_time_index_format(
    bytes: &[u8],
    segment_base_offset: i64,
    log_len: u64,
) -> Option<IndexFormat> {
    if bytes.is_empty() {
        return None;
    }
    if bytes.len().is_multiple_of(TIME_INDEX_ENTRY_BYTES)
        && native_time_index_entries_plausible(bytes, segment_base_offset, log_len)
    {
        return Some(IndexFormat::Native);
    }
    if bytes.len().is_multiple_of(KAFKA_TIME_INDEX_ENTRY_BYTES)
        && kafka_time_index_entries_plausible(bytes, segment_base_offset)
    {
        return Some(IndexFormat::Kafka);
    }
    None
}

fn native_offset_entries_plausible(bytes: &[u8], log_len: u64) -> bool {
    let mut previous_offset = None;
    let mut previous_position = None;
    for chunk in bytes.chunks_exact(INDEX_ENTRY_BYTES) {
        let entry = decode_index_entry(chunk);
        if entry.position > log_len || entry.last_offset < entry.base_offset {
            return false;
        }
        if let Some(prev) = previous_offset
            && entry.base_offset < prev
        {
            return false;
        }
        if let Some(prev) = previous_position
            && entry.position < prev
        {
            return false;
        }
        previous_offset = Some(entry.base_offset);
        previous_position = Some(entry.position);
    }
    true
}

fn kafka_offset_entries_plausible(bytes: &[u8], segment_base_offset: i64, log_len: u64) -> bool {
    let mut previous_offset = None;
    let mut previous_position = None;
    for chunk in bytes.chunks_exact(KAFKA_INDEX_ENTRY_BYTES) {
        let entry = decode_kafka_offset_index_entry(chunk, segment_base_offset);
        if entry.position > log_len {
            return false;
        }
        if let Some(prev) = previous_offset
            && entry.base_offset < prev
        {
            return false;
        }
        if let Some(prev) = previous_position
            && entry.position < prev
        {
            return false;
        }
        previous_offset = Some(entry.base_offset);
        previous_position = Some(entry.position);
    }
    true
}

fn native_time_index_entries_plausible(
    bytes: &[u8],
    segment_base_offset: i64,
    _log_len: u64,
) -> bool {
    let mut previous_timestamp = None;
    let mut previous_offset = None;
    for chunk in bytes.chunks_exact(TIME_INDEX_ENTRY_BYTES) {
        let entry = decode_time_index_entry(chunk);
        if entry.offset < segment_base_offset {
            return false;
        }
        if let Some(prev) = previous_timestamp
            && entry.max_timestamp_ms < prev
        {
            return false;
        }
        if let Some(prev) = previous_offset
            && entry.offset < prev
        {
            return false;
        }
        previous_timestamp = Some(entry.max_timestamp_ms);
        previous_offset = Some(entry.offset);
    }
    true
}

fn kafka_time_index_entries_plausible(bytes: &[u8], segment_base_offset: i64) -> bool {
    let mut previous_timestamp = None;
    let mut previous_offset = None;
    for chunk in bytes.chunks_exact(KAFKA_TIME_INDEX_ENTRY_BYTES) {
        let entry = decode_kafka_time_index_entry(chunk, segment_base_offset);
        if entry.offset < segment_base_offset {
            return false;
        }
        if let Some(prev) = previous_timestamp
            && entry.max_timestamp_ms < prev
        {
            return false;
        }
        if let Some(prev) = previous_offset
            && entry.offset < prev
        {
            return false;
        }
        previous_timestamp = Some(entry.max_timestamp_ms);
        previous_offset = Some(entry.offset);
    }
    true
}

fn cached_file_bytes(path: &Path) -> Result<Arc<Mmap>> {
    static CACHE: OnceLock<Mutex<std::collections::HashMap<PathBuf, CachedMappedFile>>> =
        OnceLock::new();
    let cache = CACHE.get_or_init(|| Mutex::new(std::collections::HashMap::new()));
    let metadata = std::fs::metadata(path)?;
    let modified = metadata.modified().ok();
    let len = metadata.len();
    {
        let cache = cache.lock().expect("index cache mutex poisoned");
        if let Some(entry) = cache.get(path) {
            if entry.len == len && entry.modified == modified {
                return Ok(Arc::clone(&entry.bytes));
            }
        }
    }
    let file = File::open(path)?;
    // Index files are immutable between roll/recovery points, so a cached read-only mmap avoids
    // repeated file copies on every binary-search lookup.
    let bytes = Arc::new(unsafe { Mmap::map(&file)? });
    let cached = CachedMappedFile {
        modified,
        len,
        bytes: Arc::clone(&bytes),
    };
    cache
        .lock()
        .expect("index cache mutex poisoned")
        .insert(path.to_path_buf(), cached);
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use std::fs::File;
    use std::io::Write;

    use tempfile::tempdir;

    use super::*;

    #[test]
    fn lookup_offset_reads_kafka_offset_index_format() {
        let dir = tempdir().unwrap();
        let segment = sample_segment_paths(dir.path(), "kafka.lookup");
        File::create(&segment.log)
            .unwrap()
            .write_all(&vec![0_u8; 512])
            .unwrap();
        File::create(&segment.index)
            .unwrap()
            .write_all(&[
                0, 0, 0, 0, 0, 0, 0, 0, // 50 -> pos 0
                0, 0, 0, 5, 0, 0, 0, 64, // 55 -> pos 64
                0, 0, 0, 9, 0, 0, 0, 96, // 59 -> pos 96
            ])
            .unwrap();

        let entry = lookup_offset(&segment, 58).unwrap().unwrap();

        assert_eq!(entry.base_offset, 55);
        assert_eq!(entry.position, 64);
    }

    #[test]
    fn lookup_timestamp_reads_kafka_time_index_format() {
        let dir = tempdir().unwrap();
        let segment = sample_segment_paths(dir.path(), "kafka.time");
        File::create(&segment.log)
            .unwrap()
            .write_all(&vec![0_u8; 512])
            .unwrap();
        File::create(&segment.timeindex)
            .unwrap()
            .write_all(&[
                0, 0, 0, 0, 0, 0, 0, 10, 0, 0, 0, 0, // ts 10 -> 50
                0, 0, 0, 0, 0, 0, 0, 20, 0, 0, 0, 5, // ts 20 -> 55
                0, 0, 0, 0, 0, 0, 0, 30, 0, 0, 0, 9, // ts 30 -> 59
            ])
            .unwrap();

        let entry = lookup_timestamp(&segment, 25).unwrap().unwrap();
        let last = last_time_index_entry(&segment).unwrap().unwrap();

        assert_eq!(entry.max_timestamp_ms, 20);
        assert_eq!(entry.offset, 55);
        assert_eq!(last.max_timestamp_ms, 30);
        assert_eq!(last.offset, 59);
    }

    fn sample_segment_paths(root: &Path, topic: &str) -> SegmentPaths {
        let partition_dir = root.join(format!("{topic}-0"));
        std::fs::create_dir_all(&partition_dir).unwrap();
        SegmentPaths {
            base_offset: 50,
            log: partition_dir.join("00000000000000000050.log"),
            index: partition_dir.join("00000000000000000050.index"),
            timeindex: partition_dir.join("00000000000000000050.timeindex"),
        }
    }
}
