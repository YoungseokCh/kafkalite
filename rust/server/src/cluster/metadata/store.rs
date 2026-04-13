use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};

use anyhow::Result;

use crate::config::Config;
use crate::store::TopicMetadata;

use super::image::{BrokerMetadata, ClusterMetadataImage, TopicMetadataImage};
use super::record::MetadataRecord;

const SNAPSHOT_FILE: &str = "metadata.snapshot";
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

    pub fn sync_topics(&mut self, topics: &[TopicMetadata], broker_id: i32) -> Result<bool> {
        let mut changed = false;
        for topic in topics {
            let next = TopicMetadataImage::from_store_topic(topic, broker_id);
            if self.image.upsert_topic(next.clone()) {
                self.append_record(&MetadataRecord::UpsertTopic(next))?;
                changed = true;
            }
        }
        if changed {
            self.persist_snapshot()?;
        }
        Ok(changed)
    }

    pub fn sync_broker(&mut self, broker: BrokerMetadata) -> Result<bool> {
        if self.image.upsert_broker(broker.clone()) {
            self.append_record(&MetadataRecord::RegisterBroker(broker))?;
            self.persist_snapshot()?;
            return Ok(true);
        }
        Ok(false)
    }

    pub fn sync_controller(&mut self, controller_id: i32) -> Result<bool> {
        if self.image.controller_id == controller_id {
            return Ok(false);
        }
        self.image.controller_id = controller_id;
        self.append_record(&MetadataRecord::SetController { controller_id })?;
        self.persist_snapshot()?;
        Ok(true)
    }

    fn append_record(&self, record: &MetadataRecord) -> Result<()> {
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.root.join(LOG_FILE))?;
        serde_json::to_writer(&mut file, record)?;
        file.write_all(b"\n")?;
        file.sync_data()?;
        Ok(())
    }

    fn persist_snapshot(&self) -> Result<()> {
        let path = self.root.join(SNAPSHOT_FILE);
        serde_json::to_writer_pretty(File::create(path)?, &self.image)?;
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
    let reader = BufReader::new(File::open(path)?);
    for line in reader.lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        image.apply(serde_json::from_str(&line)?);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use crate::config::Config;
    use crate::store::{PartitionMetadata, TopicMetadata};

    use super::*;

    #[test]
    fn persists_broker_and_topic_metadata() {
        let dir = tempdir().unwrap();
        let mut config = Config::single_node(dir.path().join("data"), 29092, 1);
        config.broker.broker_id = 7;
        config.broker.advertised_host = "broker.local".to_string();
        let mut store = MetadataStore::open(dir.path(), &config).unwrap();
        store
            .sync_broker(BrokerMetadata {
                node_id: 7,
                host: "broker.local".to_string(),
                port: 29092,
            })
            .unwrap();
        store.sync_controller(7).unwrap();
        store
            .sync_topics(
                &[TopicMetadata {
                    name: "topic-a".to_string(),
                    partitions: vec![PartitionMetadata { partition: 0 }],
                }],
                7,
            )
            .unwrap();

        let reopened = MetadataStore::open(dir.path(), &config).unwrap();
        assert_eq!(reopened.image().controller_id, 7);
        assert_eq!(reopened.image().brokers.len(), 1);
        assert_eq!(reopened.image().topics.len(), 1);
        assert_eq!(reopened.image().topics[0].partitions[0].leader_id, 7);
    }

    #[test]
    fn replayed_log_updates_existing_topic_image() {
        let dir = tempdir().unwrap();
        let config = Config::single_node(dir.path().join("data"), 29092, 1);
        let mut store = MetadataStore::open(dir.path(), &config).unwrap();
        store
            .sync_topics(
                &[TopicMetadata {
                    name: "topic-a".to_string(),
                    partitions: vec![PartitionMetadata { partition: 0 }],
                }],
                1,
            )
            .unwrap();
        store
            .sync_topics(
                &[TopicMetadata {
                    name: "topic-a".to_string(),
                    partitions: vec![
                        PartitionMetadata { partition: 0 },
                        PartitionMetadata { partition: 1 },
                    ],
                }],
                1,
            )
            .unwrap();

        let reopened = MetadataStore::open(dir.path(), &config).unwrap();
        assert_eq!(reopened.image().topics.len(), 1);
        assert_eq!(reopened.image().topics[0].partitions.len(), 2);
    }
}
