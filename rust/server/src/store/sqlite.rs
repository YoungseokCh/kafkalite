use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use bytes::Bytes;
use rusqlite::{params, Connection, OptionalExtension, Transaction};

use super::{
    BrokerRecord, FetchResult, GroupJoinResult, GroupMember, ListOffsetResult, ProducerSession,
    Result, StoreError, SyncGroupResult, TopicMetadata, DEFAULT_PARTITION,
};

pub struct SqliteStore {
    db_path: PathBuf,
    connection: Mutex<Connection>,
}

impl SqliteStore {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let db_path = path.as_ref().to_path_buf();
        let connection = Connection::open(&db_path)?;
        bootstrap(&connection)?;
        Ok(Self {
            db_path,
            connection: Mutex::new(connection),
        })
    }

    pub fn db_path(&self) -> &Path {
        &self.db_path
    }

    pub fn topic_metadata(
        &self,
        topics: Option<&[String]>,
        now_ms: i64,
    ) -> Result<Vec<TopicMetadata>> {
        let connection = self.connection.lock().expect("sqlite mutex poisoned");
        if let Some(topics) = topics {
            for topic in topics {
                ensure_topic_row(&connection, topic, now_ms)?;
            }
            return Ok(topics
                .iter()
                .map(|name| TopicMetadata { name: name.clone() })
                .collect());
        }

        let mut statement = connection.prepare("SELECT topic FROM topics ORDER BY topic ASC")?;
        let rows = statement.query_map([], |row| row.get::<_, String>(0))?;
        let mut metadata = Vec::new();
        for row in rows {
            metadata.push(TopicMetadata { name: row? });
        }
        Ok(metadata)
    }

    pub fn init_producer(&self, now_ms: i64) -> Result<ProducerSession> {
        let mut connection = self.connection.lock().expect("sqlite mutex poisoned");
        let tx = connection.transaction()?;
        let next_id = tx.query_row(
            "SELECT COALESCE(MAX(producer_id) + 1, 1) FROM producer_sessions",
            [],
            |row| row.get::<_, i64>(0),
        )?;
        tx.execute(
            "INSERT INTO producer_sessions (producer_id, producer_epoch, updated_at_unix_ms)
             VALUES (?1, 0, ?2)",
            params![next_id, now_ms],
        )?;
        tx.commit()?;
        Ok(ProducerSession {
            producer_id: next_id,
            producer_epoch: 0,
        })
    }

    pub fn append_records(
        &self,
        topic: &str,
        records: &[BrokerRecord],
        now_ms: i64,
    ) -> Result<(i64, i64)> {
        let mut connection = self.connection.lock().expect("sqlite mutex poisoned");
        let tx = connection.transaction()?;
        ensure_topic_row_tx(&tx, topic, now_ms)?;
        let base_offset = next_offset_tx(&tx, topic)?;
        let mut last_offset = base_offset - 1;
        for (index, record) in records.iter().enumerate() {
            let next = base_offset + index as i64;
            validate_sequence(
                &tx,
                record.producer_id,
                record.producer_epoch,
                topic,
                record.sequence,
            )?;
            tx.execute(
                "INSERT INTO records (
                    topic, partition, offset, timestamp_ms, producer_id, producer_epoch,
                    sequence, key_bytes, value_bytes, headers_json
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
                params![
                    topic,
                    DEFAULT_PARTITION,
                    next,
                    record.timestamp_ms,
                    record.producer_id,
                    record.producer_epoch,
                    record.sequence,
                    record.key.as_ref().map(|value| value.as_ref()),
                    record.value.as_ref().map(|value| value.as_ref()),
                    &record.headers_json,
                ],
            )?;
            tx.execute(
                "INSERT INTO producer_sequences (producer_id, topic, partition, producer_epoch, last_sequence, updated_at_unix_ms)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)
                 ON CONFLICT(producer_id, topic, partition)
                 DO UPDATE SET producer_epoch = excluded.producer_epoch, last_sequence = excluded.last_sequence, updated_at_unix_ms = excluded.updated_at_unix_ms",
                params![
                    record.producer_id,
                    topic,
                    DEFAULT_PARTITION,
                    record.producer_epoch,
                    record.sequence,
                    now_ms,
                ],
            )?;
            last_offset = next;
        }
        tx.execute(
            "UPDATE topics SET next_offset = ?2, updated_at_unix_ms = ?3 WHERE topic = ?1",
            params![topic, last_offset + 1, now_ms],
        )?;
        tx.commit()?;
        Ok((base_offset, last_offset.max(base_offset)))
    }

    pub fn fetch_records(
        &self,
        topic: &str,
        start_offset: i64,
        limit: usize,
    ) -> Result<FetchResult> {
        let connection = self.connection.lock().expect("sqlite mutex poisoned");
        let high_watermark = next_offset(&connection, topic)?;
        let mut statement = connection.prepare(
            "SELECT offset, timestamp_ms, sequence, key_bytes, value_bytes, headers_json
             FROM records
             WHERE topic = ?1 AND partition = ?2 AND offset >= ?3
             ORDER BY offset ASC
             LIMIT ?4",
        )?;
        let rows = statement.query_map(
            params![topic, DEFAULT_PARTITION, start_offset, limit as i64],
            |row| {
                Ok(BrokerRecord {
                    offset: row.get(0)?,
                    timestamp_ms: row.get(1)?,
                    producer_id: -1,
                    producer_epoch: -1,
                    sequence: row.get(2)?,
                    key: row.get::<_, Option<Vec<u8>>>(3)?.map(Bytes::from),
                    value: row.get::<_, Option<Vec<u8>>>(4)?.map(Bytes::from),
                    headers_json: row.get(5)?,
                })
            },
        )?;
        let records = rows.collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(FetchResult {
            high_watermark,
            records,
        })
    }

    pub fn list_offsets(&self, topic: &str) -> Result<(ListOffsetResult, ListOffsetResult)> {
        let connection = self.connection.lock().expect("sqlite mutex poisoned");
        let earliest = connection
            .query_row(
                "SELECT offset, timestamp_ms FROM records
                 WHERE topic = ?1 AND partition = ?2 ORDER BY offset ASC LIMIT 1",
                params![topic, DEFAULT_PARTITION],
                |row| {
                    Ok(ListOffsetResult {
                        offset: row.get(0)?,
                        timestamp_ms: row.get(1)?,
                    })
                },
            )
            .optional()?
            .unwrap_or(ListOffsetResult {
                offset: 0,
                timestamp_ms: 0,
            });
        let latest_offset = next_offset(&connection, topic)?;
        Ok((
            earliest,
            ListOffsetResult {
                offset: latest_offset,
                timestamp_ms: 0,
            },
        ))
    }

    pub fn join_group(
        &self,
        group_id: &str,
        member_id: Option<&str>,
        protocol_type: &str,
        protocol_name: &str,
        metadata: &[u8],
        session_timeout_ms: i32,
        rebalance_timeout_ms: i32,
        now_ms: i64,
    ) -> Result<GroupJoinResult> {
        let mut connection = self.connection.lock().expect("sqlite mutex poisoned");
        let tx = connection.transaction()?;
        let member_id = member_id
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned)
            .unwrap_or_else(|| format!("{group_id}-member-{}", now_ms.abs()));
        upsert_group_member(
            &tx,
            group_id,
            &member_id,
            protocol_type,
            protocol_name,
            metadata,
            session_timeout_ms,
            rebalance_timeout_ms,
            now_ms,
        )?;
        let generation = bump_generation(&tx, group_id, protocol_type, protocol_name, now_ms)?;
        let leader = current_leader(&tx, group_id)?.unwrap_or_else(|| member_id.clone());
        let members = list_members(&tx, group_id)?;
        tx.commit()?;
        Ok(GroupJoinResult {
            generation_id: generation,
            protocol_name: protocol_name.to_string(),
            leader,
            member_id,
            members,
        })
    }

    pub fn sync_group(
        &self,
        group_id: &str,
        member_id: &str,
        generation_id: i32,
        protocol_name: &str,
        assignments: &[(String, Vec<u8>)],
        now_ms: i64,
    ) -> Result<SyncGroupResult> {
        let mut connection = self.connection.lock().expect("sqlite mutex poisoned");
        let tx = connection.transaction()?;
        ensure_generation(&tx, group_id, generation_id)?;
        if !assignments.is_empty() {
            for (assigned_member, assignment) in assignments {
                tx.execute(
                    "UPDATE group_members SET assignment_bytes = ?4, updated_at_unix_ms = ?5
                     WHERE group_id = ?1 AND member_id = ?2 AND generation = ?3",
                    params![group_id, assigned_member, generation_id, assignment, now_ms],
                )?;
            }
        } else {
            maybe_build_assignments(&tx, group_id, generation_id, now_ms)?;
        }
        let assignment = tx
            .query_row(
                "SELECT assignment_bytes FROM group_members WHERE group_id = ?1 AND member_id = ?2",
                params![group_id, member_id],
                |row| row.get::<_, Vec<u8>>(0),
            )
            .optional()?
            .unwrap_or_default();
        tx.commit()?;
        Ok(SyncGroupResult {
            protocol_name: protocol_name.to_string(),
            assignment,
        })
    }

    pub fn heartbeat(
        &self,
        group_id: &str,
        member_id: &str,
        generation_id: i32,
        now_ms: i64,
    ) -> Result<()> {
        let mut connection = self.connection.lock().expect("sqlite mutex poisoned");
        let tx = connection.transaction()?;
        ensure_generation(&tx, group_id, generation_id)?;
        let updated = tx.execute(
            "UPDATE group_members SET last_heartbeat_unix_ms = ?4, updated_at_unix_ms = ?4
             WHERE group_id = ?1 AND member_id = ?2 AND generation = ?3",
            params![group_id, member_id, generation_id, now_ms],
        )?;
        if updated == 0 {
            return Err(StoreError::UnknownMember {
                group_id: group_id.to_string(),
                member_id: member_id.to_string(),
            });
        }
        tx.commit()?;
        Ok(())
    }

    pub fn leave_group(&self, group_id: &str, member_id: &str, now_ms: i64) -> Result<()> {
        let mut connection = self.connection.lock().expect("sqlite mutex poisoned");
        let tx = connection.transaction()?;
        tx.execute(
            "DELETE FROM group_members WHERE group_id = ?1 AND member_id = ?2",
            params![group_id, member_id],
        )?;
        bump_generation_unknown_protocol(&tx, group_id, now_ms)?;
        tx.commit()?;
        Ok(())
    }

    pub fn commit_offset(
        &self,
        group_id: &str,
        topic: &str,
        next_offset: i64,
        now_ms: i64,
    ) -> Result<()> {
        let connection = self.connection.lock().expect("sqlite mutex poisoned");
        connection.execute(
            "INSERT INTO consumer_offsets (group_id, topic, partition, next_offset, updated_at_unix_ms)
             VALUES (?1, ?2, ?3, ?4, ?5)
             ON CONFLICT(group_id, topic, partition)
             DO UPDATE SET next_offset = excluded.next_offset, updated_at_unix_ms = excluded.updated_at_unix_ms",
            params![group_id, topic, DEFAULT_PARTITION, next_offset, now_ms],
        )?;
        Ok(())
    }

    pub fn fetch_offset(&self, group_id: &str, topic: &str) -> Result<Option<i64>> {
        let connection = self.connection.lock().expect("sqlite mutex poisoned");
        connection
            .query_row(
                "SELECT next_offset FROM consumer_offsets WHERE group_id = ?1 AND topic = ?2 AND partition = ?3",
                params![group_id, topic, DEFAULT_PARTITION],
                |row| row.get::<_, i64>(0),
            )
            .optional()
            .map_err(StoreError::from)
    }
}

fn bootstrap(connection: &Connection) -> Result<()> {
    connection.execute_batch(
        "PRAGMA journal_mode = WAL;
         PRAGMA synchronous = NORMAL;
         CREATE TABLE IF NOT EXISTS topics (
             topic TEXT PRIMARY KEY,
             next_offset INTEGER NOT NULL DEFAULT 0,
             created_at_unix_ms INTEGER NOT NULL,
             updated_at_unix_ms INTEGER NOT NULL
         );
         CREATE TABLE IF NOT EXISTS records (
             topic TEXT NOT NULL,
             partition INTEGER NOT NULL,
             offset INTEGER NOT NULL,
             timestamp_ms INTEGER NOT NULL,
             producer_id INTEGER NOT NULL,
             producer_epoch INTEGER NOT NULL,
             sequence INTEGER NOT NULL,
             key_bytes BLOB NULL,
             value_bytes BLOB NULL,
             headers_json BLOB NOT NULL,
             PRIMARY KEY(topic, partition, offset)
         );
         CREATE TABLE IF NOT EXISTS producer_sessions (
             producer_id INTEGER PRIMARY KEY,
             producer_epoch INTEGER NOT NULL,
             updated_at_unix_ms INTEGER NOT NULL
         );
         CREATE TABLE IF NOT EXISTS producer_sequences (
             producer_id INTEGER NOT NULL,
             topic TEXT NOT NULL,
             partition INTEGER NOT NULL,
             producer_epoch INTEGER NOT NULL,
             last_sequence INTEGER NOT NULL,
             updated_at_unix_ms INTEGER NOT NULL,
             PRIMARY KEY(producer_id, topic, partition)
         );
         CREATE TABLE IF NOT EXISTS consumer_groups (
             group_id TEXT PRIMARY KEY,
             generation INTEGER NOT NULL,
             protocol_type TEXT NOT NULL,
             protocol_name TEXT NOT NULL,
             leader_member_id TEXT NULL,
             updated_at_unix_ms INTEGER NOT NULL
         );
         CREATE TABLE IF NOT EXISTS group_members (
             group_id TEXT NOT NULL,
             member_id TEXT NOT NULL,
             generation INTEGER NOT NULL,
             protocol_type TEXT NOT NULL,
             protocol_name TEXT NOT NULL,
             subscription_metadata BLOB NOT NULL,
             assignment_bytes BLOB NOT NULL DEFAULT X'',
             session_timeout_ms INTEGER NOT NULL,
             rebalance_timeout_ms INTEGER NOT NULL,
             last_heartbeat_unix_ms INTEGER NOT NULL,
             updated_at_unix_ms INTEGER NOT NULL,
             PRIMARY KEY(group_id, member_id)
         );
         CREATE TABLE IF NOT EXISTS consumer_offsets (
             group_id TEXT NOT NULL,
             topic TEXT NOT NULL,
             partition INTEGER NOT NULL,
             next_offset INTEGER NOT NULL,
             updated_at_unix_ms INTEGER NOT NULL,
             PRIMARY KEY(group_id, topic, partition)
         );",
    )?;
    Ok(())
}

fn ensure_topic_row(connection: &Connection, topic: &str, now_ms: i64) -> Result<()> {
    connection.execute(
        "INSERT INTO topics (topic, next_offset, created_at_unix_ms, updated_at_unix_ms)
         VALUES (?1, 0, ?2, ?2)
         ON CONFLICT(topic)
         DO UPDATE SET updated_at_unix_ms = excluded.updated_at_unix_ms",
        params![topic, now_ms],
    )?;
    Ok(())
}

fn ensure_topic_row_tx(tx: &Transaction<'_>, topic: &str, now_ms: i64) -> Result<()> {
    tx.execute(
        "INSERT INTO topics (topic, next_offset, created_at_unix_ms, updated_at_unix_ms)
         VALUES (?1, 0, ?2, ?2)
         ON CONFLICT(topic)
         DO UPDATE SET updated_at_unix_ms = excluded.updated_at_unix_ms",
        params![topic, now_ms],
    )?;
    Ok(())
}

fn next_offset(connection: &Connection, topic: &str) -> Result<i64> {
    Ok(connection
        .query_row(
            "SELECT COALESCE(next_offset, 0) FROM topics WHERE topic = ?1",
            params![topic],
            |row| row.get::<_, i64>(0),
        )
        .optional()?
        .unwrap_or(0))
}

fn next_offset_tx(tx: &Transaction<'_>, topic: &str) -> Result<i64> {
    Ok(tx
        .query_row(
            "SELECT COALESCE(next_offset, 0) FROM topics WHERE topic = ?1",
            params![topic],
            |row| row.get::<_, i64>(0),
        )
        .optional()?
        .unwrap_or(0))
}

fn validate_sequence(
    tx: &Transaction<'_>,
    producer_id: i64,
    producer_epoch: i16,
    topic: &str,
    actual: i32,
) -> Result<()> {
    let expected = tx
        .query_row(
            "SELECT last_sequence + 1 FROM producer_sequences WHERE producer_id = ?1 AND topic = ?2 AND partition = ?3",
            params![producer_id, topic, DEFAULT_PARTITION],
            |row| row.get::<_, i32>(0),
        )
        .optional()?
        .unwrap_or(actual);
    if expected != actual {
        return Err(StoreError::InvalidProducerSequence {
            producer_id,
            expected,
            actual,
        });
    }
    let _ = producer_epoch;
    Ok(())
}

fn upsert_group_member(
    tx: &Transaction<'_>,
    group_id: &str,
    member_id: &str,
    protocol_type: &str,
    protocol_name: &str,
    metadata: &[u8],
    session_timeout_ms: i32,
    rebalance_timeout_ms: i32,
    now_ms: i64,
) -> Result<()> {
    tx.execute(
        "INSERT INTO group_members (
            group_id, member_id, generation, protocol_type, protocol_name, subscription_metadata,
            assignment_bytes, session_timeout_ms, rebalance_timeout_ms, last_heartbeat_unix_ms, updated_at_unix_ms
         ) VALUES (?1, ?2, 0, ?3, ?4, ?5, X'', ?6, ?7, ?8, ?8)
         ON CONFLICT(group_id, member_id)
         DO UPDATE SET protocol_type = excluded.protocol_type, protocol_name = excluded.protocol_name,
                       subscription_metadata = excluded.subscription_metadata,
                       session_timeout_ms = excluded.session_timeout_ms,
                       rebalance_timeout_ms = excluded.rebalance_timeout_ms,
                       last_heartbeat_unix_ms = excluded.last_heartbeat_unix_ms,
                       updated_at_unix_ms = excluded.updated_at_unix_ms",
        params![
            group_id,
            member_id,
            protocol_type,
            protocol_name,
            metadata,
            session_timeout_ms,
            rebalance_timeout_ms,
            now_ms,
        ],
    )?;
    Ok(())
}

fn bump_generation(
    tx: &Transaction<'_>,
    group_id: &str,
    protocol_type: &str,
    protocol_name: &str,
    now_ms: i64,
) -> Result<i32> {
    let next_generation = tx
        .query_row(
            "SELECT COALESCE(generation + 1, 1) FROM consumer_groups WHERE group_id = ?1",
            params![group_id],
            |row| row.get::<_, i32>(0),
        )
        .optional()?
        .unwrap_or(1);
    let leader = tx
        .query_row(
            "SELECT member_id FROM group_members WHERE group_id = ?1 ORDER BY member_id ASC LIMIT 1",
            params![group_id],
            |row| row.get::<_, String>(0),
        )
        .optional()?;
    tx.execute(
        "INSERT INTO consumer_groups (group_id, generation, protocol_type, protocol_name, leader_member_id, updated_at_unix_ms)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6)
         ON CONFLICT(group_id)
         DO UPDATE SET generation = excluded.generation, protocol_type = excluded.protocol_type,
                       protocol_name = excluded.protocol_name, leader_member_id = excluded.leader_member_id,
                       updated_at_unix_ms = excluded.updated_at_unix_ms",
        params![group_id, next_generation, protocol_type, protocol_name, leader, now_ms],
    )?;
    tx.execute(
        "UPDATE group_members SET generation = ?2, assignment_bytes = X'' WHERE group_id = ?1",
        params![group_id, next_generation],
    )?;
    Ok(next_generation)
}

fn bump_generation_unknown_protocol(
    tx: &Transaction<'_>,
    group_id: &str,
    now_ms: i64,
) -> Result<()> {
    let row = tx
        .query_row(
            "SELECT protocol_type, protocol_name, generation FROM consumer_groups WHERE group_id = ?1",
            params![group_id],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?, row.get::<_, i32>(2)?)),
        )
        .optional()?;
    if let Some((protocol_type, protocol_name, _)) = row {
        let _ = bump_generation(tx, group_id, &protocol_type, &protocol_name, now_ms)?;
    }
    Ok(())
}

fn current_leader(tx: &Transaction<'_>, group_id: &str) -> Result<Option<String>> {
    tx.query_row(
        "SELECT leader_member_id FROM consumer_groups WHERE group_id = ?1",
        params![group_id],
        |row| row.get::<_, Option<String>>(0),
    )
    .optional()
    .map(|value| value.flatten())
    .map_err(StoreError::from)
}

fn list_members(tx: &Transaction<'_>, group_id: &str) -> Result<Vec<GroupMember>> {
    let mut statement = tx.prepare(
        "SELECT member_id, subscription_metadata FROM group_members WHERE group_id = ?1 ORDER BY member_id ASC",
    )?;
    let rows = statement.query_map(params![group_id], |row| {
        Ok(GroupMember {
            member_id: row.get(0)?,
            metadata: row.get(1)?,
        })
    })?;
    rows.collect::<std::result::Result<Vec<_>, _>>()
        .map_err(StoreError::from)
}

fn ensure_generation(tx: &Transaction<'_>, group_id: &str, generation_id: i32) -> Result<()> {
    let current = tx
        .query_row(
            "SELECT generation FROM consumer_groups WHERE group_id = ?1",
            params![group_id],
            |row| row.get::<_, i32>(0),
        )
        .optional()?
        .unwrap_or_default();
    if current != generation_id {
        return Err(StoreError::StaleGeneration {
            expected: current,
            actual: generation_id,
        });
    }
    Ok(())
}

fn maybe_build_assignments(
    tx: &Transaction<'_>,
    group_id: &str,
    generation_id: i32,
    now_ms: i64,
) -> Result<()> {
    let members = list_members(tx, group_id)?;
    if members.is_empty() {
        return Ok(());
    }

    let subscriptions = members
        .iter()
        .filter_map(|member| {
            parse_topics(&member.metadata)
                .ok()
                .map(|topics| (member.member_id.clone(), topics))
        })
        .collect::<Vec<_>>();
    if subscriptions.is_empty() {
        return Ok(());
    }

    let mut assignments: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for (index, topic) in subscriptions
        .iter()
        .flat_map(|(_, topics)| topics.iter().cloned())
        .collect::<Vec<_>>()
        .into_iter()
        .enumerate()
    {
        let member_id = subscriptions[index % subscriptions.len()].0.clone();
        assignments.entry(member_id).or_default().push(topic);
    }

    for member in members {
        let topics = assignments.remove(&member.member_id).unwrap_or_default();
        let assignment = encode_assignment(&topics)?;
        tx.execute(
            "UPDATE group_members SET assignment_bytes = ?4, updated_at_unix_ms = ?5
             WHERE group_id = ?1 AND member_id = ?2 AND generation = ?3",
            params![
                group_id,
                member.member_id,
                generation_id,
                assignment,
                now_ms
            ],
        )?;
    }
    Ok(())
}

fn parse_topics(bytes: &[u8]) -> anyhow::Result<Vec<String>> {
    use bytes::Bytes as ByteBytes;
    use kafka_protocol::messages::ConsumerProtocolSubscription;
    use kafka_protocol::protocol::Decodable;

    let mut payload = ByteBytes::copy_from_slice(bytes);
    let subscription = ConsumerProtocolSubscription::decode(&mut payload, 3)?;
    Ok(subscription
        .topics
        .into_iter()
        .map(|topic| topic.to_string())
        .collect())
}

fn encode_assignment(topics: &[String]) -> Result<Vec<u8>> {
    use bytes::BytesMut;
    use kafka_protocol::messages::consumer_protocol_assignment::TopicPartition;
    use kafka_protocol::messages::{ConsumerProtocolAssignment, TopicName};
    use kafka_protocol::protocol::{Encodable, StrBytes};

    let partitions = topics
        .iter()
        .map(|topic| {
            TopicPartition::default()
                .with_topic(TopicName(StrBytes::from(topic.clone())))
                .with_partitions(vec![DEFAULT_PARTITION])
        })
        .collect();
    let assignment = ConsumerProtocolAssignment::default().with_assigned_partitions(partitions);
    let mut encoded = BytesMut::new();
    assignment
        .encode(&mut encoded, 3)
        .map_err(|err| StoreError::Protocol(err.to_string()))?;
    Ok(encoded.to_vec())
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::*;

    #[test]
    fn appends_and_fetches_records() {
        let dir = tempdir().unwrap();
        let store = SqliteStore::open(dir.path().join("messages.db")).unwrap();
        let producer = store.init_producer(10).unwrap();
        let records = vec![BrokerRecord {
            offset: 0,
            timestamp_ms: 10,
            producer_id: producer.producer_id,
            producer_epoch: producer.producer_epoch,
            sequence: 0,
            key: Some(Bytes::from_static(b"key")),
            value: Some(Bytes::from_static(b"value")),
            headers_json: b"[]".to_vec(),
        }];

        let (base, last) = store.append_records("test.events", &records, 10).unwrap();

        assert_eq!((base, last), (0, 0));
        let fetched = store.fetch_records("test.events", 0, 10).unwrap();
        assert_eq!(fetched.high_watermark, 1);
        assert_eq!(fetched.records.len(), 1);
        assert_eq!(fetched.records[0].offset, 0);
    }
}
