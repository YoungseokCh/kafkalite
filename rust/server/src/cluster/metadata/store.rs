use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};

use anyhow::Result;
use serde::{Deserialize, Serialize};

use crate::config::Config;
use crate::store::TopicMetadata;

use super::image::{BrokerMetadata, ClusterMetadataImage, TopicMetadataImage};
use super::record::MetadataRecord;

const SNAPSHOT_FILE: &str = "metadata.snapshot";
const SNAPSHOT_TMP_FILE: &str = "metadata.snapshot.tmp";
const LOG_FILE: &str = "metadata.log";

#[derive(Debug)]
pub struct MetadataStore {
    root: PathBuf,
    image: ClusterMetadataImage,
}

impl MetadataStore {
    pub fn open(root: &Path, config: &Config) -> Result<Self> {
        fs::create_dir_all(root)?;
        let mut image = load_snapshot(root)?.unwrap_or_else(|| {
            ClusterMetadataImage::new(config.broker.cluster_id.clone(), config.broker.broker_id)
        });
        replay_log(root, &mut image)?;
        let store = Self {
            root: root.to_path_buf(),
            image,
        };
        Ok(store)
    }

    pub fn image(&self) -> &ClusterMetadataImage {
        &self.image
    }

    pub fn metadata_offset(&self) -> i64 {
        self.image.metadata_offset
    }

    pub fn sync_topics(&mut self, topics: &[TopicMetadata], broker_id: i32) -> Result<bool> {
        let mut changed = false;
        for topic in topics {
            let next = TopicMetadataImage::from_store_topic(topic, broker_id);
            let maybe_topic = if self
                .image
                .topics
                .iter()
                .any(|existing| existing.name == topic.name)
            {
                let mut preview = self.image.clone();
                if preview.merge_store_topic(topic) {
                    preview
                        .topics
                        .iter()
                        .find(|existing| existing.name == topic.name)
                        .cloned()
                } else {
                    None
                }
            } else {
                let mut preview = self.image.clone();
                preview.upsert_topic(next.clone()).then_some(next)
            };
            if let Some(topic_image) = maybe_topic {
                self.append_records(&[MetadataRecord::UpsertTopic(topic_image)])?;
                changed = true;
            }
        }
        Ok(changed)
    }

    pub fn sync_broker(&mut self, broker: BrokerMetadata) -> Result<bool> {
        let mut preview = self.image.clone();
        if preview.upsert_broker(broker.clone()) {
            self.append_records(&[MetadataRecord::RegisterBroker(broker)])?;
            return Ok(true);
        }
        Ok(false)
    }

    pub fn sync_controller(&mut self, controller_id: i32) -> Result<bool> {
        if self.image.controller_id == controller_id {
            return Ok(false);
        }
        self.append_records(&[MetadataRecord::SetController { controller_id }])?;
        Ok(true)
    }

    pub fn append_remote_records(
        &mut self,
        prev_metadata_offset: i64,
        records: &[MetadataRecord],
    ) -> Result<bool> {
        if self.metadata_offset() != prev_metadata_offset {
            return Ok(false);
        }
        self.append_records(records)?;
        Ok(true)
    }

    fn append_records(&mut self, records: &[MetadataRecord]) -> Result<()> {
        if records.is_empty() {
            return Ok(());
        }
        let mut next_image = self.image.clone();
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.root.join(LOG_FILE))?;
        for record in records {
            serde_json::to_writer(
                &mut file,
                &MetadataLogEntry {
                    metadata_offset: Some(next_image.metadata_offset + 1),
                    record: record.clone(),
                },
            )?;
            file.write_all(b"\n")?;
            next_image.apply(record.clone());
        }
        file.sync_data()?;
        self.persist_snapshot(&next_image)?;
        self.truncate_log()?;
        self.image = next_image;
        Ok(())
    }

    fn persist_snapshot(&self, image: &ClusterMetadataImage) -> Result<()> {
        let path = self.root.join(SNAPSHOT_FILE);
        let tmp_path = self.root.join(SNAPSHOT_TMP_FILE);
        let mut file = File::create(&tmp_path)?;
        serde_json::to_writer_pretty(&mut file, image)?;
        file.sync_all()?;
        fs::rename(tmp_path, path)?;
        Ok(())
    }

    fn truncate_log(&self) -> Result<()> {
        File::create(self.root.join(LOG_FILE))?;
        Ok(())
    }
}

fn load_snapshot(root: &Path) -> Result<Option<ClusterMetadataImage>> {
    let path = root.join(SNAPSHOT_FILE);
    if !path.exists() {
        return Ok(None);
    }
    Ok(Some(serde_json::from_reader(File::open(path)?)?))
}

fn replay_log(root: &Path, image: &mut ClusterMetadataImage) -> Result<()> {
    let path = root.join(LOG_FILE);
    if !path.exists() {
        return Ok(());
    }
    let mut reader = BufReader::new(OpenOptions::new().read(true).write(true).open(&path)?);
    let file_len = reader.get_ref().metadata()?.len();
    let mut safe_len = 0_u64;
    let mut line = Vec::new();
    loop {
        line.clear();
        let read = reader.read_until(b'\n', &mut line)?;
        if read == 0 {
            break;
        }
        if !line.ends_with(b"\n") {
            break;
        }
        let line = trim_line_endings(&line);
        let Ok(line) = std::str::from_utf8(line) else {
            break;
        };
        if line.trim().is_empty() {
            safe_len += read as u64;
            continue;
        }
        let Ok(entry) = parse_log_entry(line) else {
            break;
        };
        if entry
            .metadata_offset
            .is_none_or(|offset| offset > image.metadata_offset)
        {
            image.apply(entry.record);
        }
        safe_len += read as u64;
    }
    let file = reader.into_inner();
    if safe_len < file_len {
        file.set_len(safe_len)?;
        file.sync_all()?;
    }
    Ok(())
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct MetadataLogEntry {
    metadata_offset: Option<i64>,
    record: MetadataRecord,
}

fn parse_log_entry(line: &str) -> Result<MetadataLogEntry> {
    if let Ok(entry) = serde_json::from_str::<MetadataLogEntry>(line) {
        return Ok(entry);
    }
    Ok(MetadataLogEntry {
        metadata_offset: None,
        record: serde_json::from_str(line)?,
    })
}

fn trim_line_endings(line: &[u8]) -> &[u8] {
    line.strip_suffix(b"\n")
        .unwrap_or(line)
        .strip_suffix(b"\r")
        .unwrap_or(line.strip_suffix(b"\n").unwrap_or(line))
}

#[cfg(test)]
mod store_tests;
