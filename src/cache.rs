use crate::indexer::{ImageAttachment, MessageRecord, Role, SourceKind, count_sessions};
use anyhow::Context as _;
use rusqlite::{Connection, Transaction, params};
use std::{
    collections::HashMap,
    env, fs,
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

const SCHEMA_VERSION: i64 = 4;

#[derive(Debug, Clone)]
pub struct RemoteSyncStatus {
    pub remote_name: String,
    pub host: String,
    pub machine_id: Option<String>,
    pub machine_name: Option<String>,
    pub last_attempted_ms: Option<i64>,
    pub last_success_ms: Option<i64>,
    pub last_duration_ms: Option<i64>,
    pub imported_records: i64,
    pub imported_sessions: i64,
    pub last_error: Option<String>,
}

pub struct CacheStore {
    conn: Connection,
}

impl CacheStore {
    pub fn open(path: &Path, rebuild: bool) -> anyhow::Result<Self> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).with_context(|| {
                format!("cache directory creation failed: {}", parent.display())
            })?;
        }

        let conn = Connection::open(path)
            .with_context(|| format!("cache open failed: {}", path.display()))?;
        let mut store = Self { conn };
        store.init_schema()?;
        if rebuild {
            store.clear()?;
        }
        Ok(store)
    }

    pub fn prune_missing_units(&mut self) -> anyhow::Result<usize> {
        let mut stmt = self
            .conn
            .prepare("SELECT unit_key, path FROM source_units")
            .context("cache source unit query failed")?;
        let rows = stmt.query_map([], |row| {
            let unit_key: String = row.get(0)?;
            let path: String = row.get(1)?;
            Ok((unit_key, path))
        })?;

        let mut to_remove: Vec<String> = Vec::new();
        for row in rows {
            let (unit_key, path) = row?;
            if path.starts_with("ssh://") {
                continue;
            }
            if Path::new(&path).exists() {
                continue;
            }
            to_remove.push(unit_key);
        }
        drop(stmt);

        let mut removed = 0usize;
        for unit_key in to_remove {
            self.delete_unit(&unit_key)?;
            removed = removed.saturating_add(1);
        }

        Ok(removed)
    }

    pub fn fingerprints_for_keys(
        &self,
        unit_keys: &[String],
    ) -> anyhow::Result<HashMap<String, String>> {
        let mut out = HashMap::new();
        for chunk in unit_keys.chunks(400) {
            let placeholders = std::iter::repeat_n("?", chunk.len())
                .collect::<Vec<_>>()
                .join(", ");
            let sql = format!(
                "SELECT unit_key, fingerprint FROM source_units WHERE unit_key IN ({placeholders})"
            );
            let mut stmt = self
                .conn
                .prepare(&sql)
                .context("cache fingerprint query failed")?;
            let rows = stmt.query_map(rusqlite::params_from_iter(chunk.iter()), |row| {
                let key: String = row.get(0)?;
                let fingerprint: String = row.get(1)?;
                Ok((key, fingerprint))
            })?;
            for row in rows {
                let (key, fingerprint) = row?;
                out.insert(key, fingerprint);
            }
        }
        Ok(out)
    }

    pub fn load_records(&self, unit_keys: &[String]) -> anyhow::Result<Vec<MessageRecord>> {
        let mut out = Vec::new();
        for chunk in unit_keys.chunks(400) {
            let placeholders = std::iter::repeat_n("?", chunk.len())
                .collect::<Vec<_>>()
                .join(", ");
            let sql = format!(
                "SELECT timestamp, role, text, file, line, session_id, account, cwd, phase, images_json, source, machine_id, machine_name, project_slug, origin \
                 FROM message_records WHERE unit_key IN ({placeholders}) ORDER BY unit_key, ord"
            );
            let mut stmt = self
                .conn
                .prepare(&sql)
                .context("cache record query failed")?;
            let rows = stmt.query_map(rusqlite::params_from_iter(chunk.iter()), |row| {
                Ok(MessageRecord {
                    timestamp: row.get(0)?,
                    role: role_from_db(row.get::<_, i64>(1)?),
                    text: row.get(2)?,
                    file: PathBuf::from(row.get::<_, String>(3)?),
                    line: row.get::<_, i64>(4)? as u32,
                    session_id: row.get(5)?,
                    account: row.get(6)?,
                    cwd: row.get(7)?,
                    phase: row.get(8)?,
                    images: serde_json::from_str::<Vec<ImageAttachment>>(&row.get::<_, String>(9)?)
                        .unwrap_or_default(),
                    source: source_from_db(row.get::<_, i64>(10)?),
                    machine_id: row.get(11)?,
                    machine_name: row.get(12)?,
                    project_slug: row.get(13)?,
                    origin: row.get(14)?,
                })
            })?;
            for row in rows {
                out.push(row?);
            }
        }
        Ok(out)
    }

    pub fn load_all_records(&self) -> anyhow::Result<Vec<MessageRecord>> {
        let mut stmt = self
            .conn
            .prepare(
                "SELECT timestamp, role, text, file, line, session_id, account, cwd, phase, images_json, source, machine_id, machine_name, project_slug, origin
                 FROM message_records ORDER BY origin, unit_key, ord",
            )
            .context("cache all-record query failed")?;
        let rows = stmt.query_map([], |row| {
            Ok(MessageRecord {
                timestamp: row.get(0)?,
                role: role_from_db(row.get::<_, i64>(1)?),
                text: row.get(2)?,
                file: PathBuf::from(row.get::<_, String>(3)?),
                line: row.get::<_, i64>(4)? as u32,
                session_id: row.get(5)?,
                account: row.get(6)?,
                cwd: row.get(7)?,
                phase: row.get(8)?,
                images: serde_json::from_str::<Vec<ImageAttachment>>(&row.get::<_, String>(9)?)
                    .unwrap_or_default(),
                source: source_from_db(row.get::<_, i64>(10)?),
                machine_id: row.get(11)?,
                machine_name: row.get(12)?,
                project_slug: row.get(13)?,
                origin: row.get(14)?,
            })
        })?;
        let mut out = Vec::new();
        for row in rows {
            out.push(row?);
        }
        Ok(out)
    }

    pub fn load_local_records(&self) -> anyhow::Result<Vec<MessageRecord>> {
        self.load_records_for_origin("local")
    }

    pub fn load_records_for_origin(&self, origin: &str) -> anyhow::Result<Vec<MessageRecord>> {
        let mut stmt = self
            .conn
            .prepare(
                "SELECT timestamp, role, text, file, line, session_id, account, cwd, phase, images_json, source, machine_id, machine_name, project_slug, origin
                 FROM message_records WHERE origin = ?1 ORDER BY unit_key, ord",
            )
            .context("cache origin record query failed")?;
        let rows = stmt.query_map(params![origin], |row| {
            Ok(MessageRecord {
                timestamp: row.get(0)?,
                role: role_from_db(row.get::<_, i64>(1)?),
                text: row.get(2)?,
                file: PathBuf::from(row.get::<_, String>(3)?),
                line: row.get::<_, i64>(4)? as u32,
                session_id: row.get(5)?,
                account: row.get(6)?,
                cwd: row.get(7)?,
                phase: row.get(8)?,
                images: serde_json::from_str::<Vec<ImageAttachment>>(&row.get::<_, String>(9)?)
                    .unwrap_or_default(),
                source: source_from_db(row.get::<_, i64>(10)?),
                machine_id: row.get(11)?,
                machine_name: row.get(12)?,
                project_slug: row.get(13)?,
                origin: row.get(14)?,
            })
        })?;
        let mut out = Vec::new();
        for row in rows {
            out.push(row?);
        }
        Ok(out)
    }

    pub fn replace_unit(
        &mut self,
        unit_key: &str,
        path: &Path,
        fingerprint: &str,
        records: &[MessageRecord],
    ) -> anyhow::Result<()> {
        let tx = self
            .conn
            .transaction()
            .context("cache transaction failed")?;
        Self::delete_unit_tx(&tx, unit_key)?;
        tx.execute(
            "INSERT INTO source_units (unit_key, path, fingerprint, updated_at) VALUES (?1, ?2, ?3, ?4)",
            params![unit_key, path.to_string_lossy().to_string(), fingerprint, unix_now()],
        )
        .context("cache source unit insert failed")?;

        {
            let mut stmt = tx
                .prepare(
                    "INSERT INTO message_records (
                        unit_key, ord, timestamp, role, text, file, line, session_id, account, cwd, phase, images_json, source, machine_id, machine_name, project_slug, origin
                    ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17)",
                )
                .context("cache record statement failed")?;

            for (ord, rec) in records.iter().enumerate() {
                stmt.execute(params![
                    unit_key,
                    ord as i64,
                    rec.timestamp,
                    role_to_db(rec.role),
                    rec.text,
                    rec.file.to_string_lossy().to_string(),
                    i64::from(rec.line),
                    rec.session_id,
                    rec.account,
                    rec.cwd,
                    rec.phase,
                    serde_json::to_string(&rec.images).unwrap_or_else(|_| "[]".to_string()),
                    source_to_db(rec.source),
                    rec.machine_id,
                    rec.machine_name,
                    rec.project_slug,
                    rec.origin,
                ])
                .context("cache record insert failed")?;
            }
        }

        tx.commit().context("cache commit failed")?;
        Ok(())
    }

    pub fn delete_unit(&mut self, unit_key: &str) -> anyhow::Result<()> {
        let tx = self
            .conn
            .transaction()
            .context("cache delete transaction failed")?;
        Self::delete_unit_tx(&tx, unit_key)?;
        tx.commit().context("cache delete commit failed")?;
        Ok(())
    }

    pub fn replace_remote_snapshot(
        &mut self,
        remote_name: &str,
        host: &str,
        records: &[MessageRecord],
        machine_id: Option<&str>,
        machine_name: Option<&str>,
        duration_ms: i64,
    ) -> anyhow::Result<()> {
        let unit_key = format!("remote::{remote_name}");
        let tx = self
            .conn
            .transaction()
            .context("remote snapshot transaction failed")?;
        Self::delete_unit_tx(&tx, &unit_key)?;
        tx.execute(
            "INSERT INTO source_units (unit_key, path, fingerprint, updated_at) VALUES (?1, ?2, ?3, ?4)",
            params![unit_key, format!("ssh://{host}/{remote_name}"), unix_now().to_string(), unix_now()],
        )
        .context("remote source unit insert failed")?;
        {
            let mut stmt = tx
                .prepare(
                    "INSERT INTO message_records (
                        unit_key, ord, timestamp, role, text, file, line, session_id, account, cwd, phase, images_json, source, machine_id, machine_name, project_slug, origin
                    ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17)",
                )
                .context("remote record statement failed")?;
            for (ord, rec) in records.iter().enumerate() {
                stmt.execute(params![
                    unit_key,
                    ord as i64,
                    rec.timestamp,
                    role_to_db(rec.role),
                    rec.text,
                    rec.file.to_string_lossy().to_string(),
                    i64::from(rec.line),
                    rec.session_id,
                    rec.account,
                    rec.cwd,
                    rec.phase,
                    serde_json::to_string(&rec.images).unwrap_or_else(|_| "[]".to_string()),
                    source_to_db(rec.source),
                    rec.machine_id,
                    rec.machine_name,
                    rec.project_slug,
                    rec.origin,
                ])?;
            }
        }
        tx.execute(
            "INSERT INTO remote_sync_state (
                remote_name, host, machine_id, machine_name, last_attempted_ms, last_success_ms, last_duration_ms, imported_records, imported_sessions, last_error
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?5, ?6, ?7, ?8, NULL)
            ON CONFLICT(remote_name) DO UPDATE SET
                host=excluded.host,
                machine_id=excluded.machine_id,
                machine_name=excluded.machine_name,
                last_attempted_ms=excluded.last_attempted_ms,
                last_success_ms=excluded.last_success_ms,
                last_duration_ms=excluded.last_duration_ms,
                imported_records=excluded.imported_records,
                imported_sessions=excluded.imported_sessions,
                last_error=NULL",
            params![
                remote_name,
                host,
                machine_id,
                machine_name,
                unix_now() as i64,
                duration_ms,
                records.len() as i64,
                count_sessions(records) as i64,
            ],
        )
        .context("remote sync status upsert failed")?;
        tx.commit().context("remote snapshot commit failed")?;
        Ok(())
    }

    pub fn mark_remote_sync_failed(
        &mut self,
        remote_name: &str,
        host: &str,
        error: &str,
        duration_ms: i64,
    ) -> anyhow::Result<()> {
        self.conn.execute(
            "INSERT INTO remote_sync_state (
                remote_name, host, last_attempted_ms, last_duration_ms, last_error
            ) VALUES (?1, ?2, ?3, ?4, ?5)
            ON CONFLICT(remote_name) DO UPDATE SET
                host=excluded.host,
                last_attempted_ms=excluded.last_attempted_ms,
                last_duration_ms=excluded.last_duration_ms,
                last_error=excluded.last_error",
            params![remote_name, host, unix_now() as i64, duration_ms, error],
        ).context("remote sync failure upsert failed")?;
        Ok(())
    }

    pub fn load_remote_sync_states(&self) -> anyhow::Result<Vec<RemoteSyncStatus>> {
        let mut stmt = self.conn.prepare(
            "SELECT remote_name, host, machine_id, machine_name, last_attempted_ms, last_success_ms, last_duration_ms, imported_records, imported_sessions, last_error
             FROM remote_sync_state ORDER BY remote_name"
        ).context("remote sync state query failed")?;
        let rows = stmt.query_map([], |row| {
            Ok(RemoteSyncStatus {
                remote_name: row.get(0)?,
                host: row.get(1)?,
                machine_id: row.get(2)?,
                machine_name: row.get(3)?,
                last_attempted_ms: row.get(4)?,
                last_success_ms: row.get(5)?,
                last_duration_ms: row.get(6)?,
                imported_records: row.get(7)?,
                imported_sessions: row.get(8)?,
                last_error: row.get(9)?,
            })
        })?;
        let mut out = Vec::new();
        for row in rows {
            out.push(row?);
        }
        Ok(out)
    }

    fn clear(&mut self) -> anyhow::Result<()> {
        self.conn
            .execute_batch(
                "DELETE FROM message_records;
                 DELETE FROM source_units;
                 DELETE FROM remote_sync_state;",
            )
            .context("cache clear failed")?;
        Ok(())
    }

    fn init_schema(&mut self) -> anyhow::Result<()> {
        let version: i64 = self
            .conn
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .context("cache schema version query failed")?;

        if version > SCHEMA_VERSION {
            anyhow::bail!("cache schema version is newer than supported");
        }

        if version != SCHEMA_VERSION {
            self.conn
                .execute_batch(
                    "DROP TABLE IF EXISTS message_records;
                     DROP TABLE IF EXISTS source_units;",
                )
                .context("cache schema reset failed")?;
        }

        self.conn
            .execute_batch(
                "CREATE TABLE IF NOT EXISTS source_units (
                    unit_key TEXT PRIMARY KEY,
                    path TEXT NOT NULL,
                    fingerprint TEXT NOT NULL,
                    updated_at INTEGER NOT NULL
                 );
                 CREATE TABLE IF NOT EXISTS message_records (
                    unit_key TEXT NOT NULL,
                    ord INTEGER NOT NULL,
                    timestamp TEXT,
                    role INTEGER NOT NULL,
                    text TEXT NOT NULL,
                    file TEXT NOT NULL,
                    line INTEGER NOT NULL,
                    session_id TEXT,
                    account TEXT,
                    cwd TEXT,
                    phase TEXT,
                    images_json TEXT NOT NULL DEFAULT '[]',
                    source INTEGER NOT NULL,
                    machine_id TEXT NOT NULL DEFAULT '',
                    machine_name TEXT NOT NULL DEFAULT '',
                    project_slug TEXT,
                    origin TEXT NOT NULL DEFAULT 'local',
                    PRIMARY KEY (unit_key, ord),
                    FOREIGN KEY (unit_key) REFERENCES source_units(unit_key) ON DELETE CASCADE
                 );
                 CREATE TABLE IF NOT EXISTS remote_sync_state (
                    remote_name TEXT PRIMARY KEY,
                    host TEXT NOT NULL,
                    machine_id TEXT,
                    machine_name TEXT,
                    last_attempted_ms INTEGER,
                    last_success_ms INTEGER,
                    last_duration_ms INTEGER,
                    imported_records INTEGER NOT NULL DEFAULT 0,
                    imported_sessions INTEGER NOT NULL DEFAULT 0,
                    last_error TEXT
                 );
                 CREATE INDEX IF NOT EXISTS idx_message_records_session_id ON message_records(session_id);
                 CREATE INDEX IF NOT EXISTS idx_message_records_origin ON message_records(origin);
                 PRAGMA user_version = 4;",
            )
            .context("cache schema create failed")?;

        Ok(())
    }

    fn delete_unit_tx(tx: &Transaction<'_>, unit_key: &str) -> anyhow::Result<()> {
        tx.execute(
            "DELETE FROM message_records WHERE unit_key = ?1",
            params![unit_key],
        )
        .context("cache record delete failed")?;
        tx.execute(
            "DELETE FROM source_units WHERE unit_key = ?1",
            params![unit_key],
        )
        .context("cache source unit delete failed")?;
        Ok(())
    }
}

pub fn default_db_path() -> PathBuf {
    if let Some(path) = env::var_os("AGENT_HISTORY_CACHE_DB") {
        return PathBuf::from(path);
    }

    let home = env::var_os("HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("."));
    home.join(".local/state/agent-history/index.sqlite")
}

pub fn unit_key(path: &Path) -> String {
    path.to_string_lossy().to_string()
}

fn unix_now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs() as i64)
        .unwrap_or_default()
}

fn role_to_db(role: Role) -> i64 {
    match role {
        Role::User => 0,
        Role::Assistant => 1,
        Role::System => 2,
        Role::Tool => 3,
        Role::Unknown => 4,
    }
}

fn role_from_db(value: i64) -> Role {
    match value {
        0 => Role::User,
        1 => Role::Assistant,
        2 => Role::System,
        3 => Role::Tool,
        _ => Role::Unknown,
    }
}

fn source_to_db(source: SourceKind) -> i64 {
    match source {
        SourceKind::CodexSessionJsonl => 0,
        SourceKind::CodexHistoryJsonl => 1,
        SourceKind::ClaudeProjectJsonl => 2,
        SourceKind::OpenCodeSession => 3,
    }
}

fn source_from_db(value: i64) -> SourceKind {
    match value {
        0 => SourceKind::CodexSessionJsonl,
        1 => SourceKind::CodexHistoryJsonl,
        2 => SourceKind::ClaudeProjectJsonl,
        _ => SourceKind::OpenCodeSession,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn temp_root(prefix: &str) -> PathBuf {
        std::env::temp_dir().join(format!("{prefix}-{}-{}", std::process::id(), unix_now()))
    }

    #[test]
    fn unit_key_uses_path_string() {
        assert_eq!(unit_key(Path::new("/tmp/demo")), "/tmp/demo");
    }

    #[test]
    fn prune_missing_units_removes_deleted_entries() {
        let root = temp_root("agent-history-cache-test");
        fs::create_dir_all(&root).unwrap();
        let db_path = root.join("index.sqlite");
        let source_path = root.join("source.jsonl");
        fs::write(&source_path, "{}\n").unwrap();

        let mut store = CacheStore::open(&db_path, false).unwrap();
        let record = MessageRecord {
            timestamp: None,
            role: Role::User,
            text: "hello".to_string(),
            file: source_path.clone(),
            line: 1,
            session_id: Some("s1".to_string()),
            account: None,
            cwd: None,
            phase: None,
            images: Vec::new(),
            machine_id: "local".to_string(),
            machine_name: "local".to_string(),
            project_slug: None,
            origin: "local".to_string(),
            source: SourceKind::CodexSessionJsonl,
        };
        let key = unit_key(&source_path);
        store
            .replace_unit(&key, &source_path, "fingerprint", &[record])
            .unwrap();

        fs::remove_file(&source_path).unwrap();
        assert_eq!(store.prune_missing_units().unwrap(), 1);
        assert!(store.load_records(&[key]).unwrap().is_empty());

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn open_rejects_newer_schema_versions() {
        let root = temp_root("agent-history-cache-newer");
        fs::create_dir_all(&root).unwrap();
        let db_path = root.join("index.sqlite");

        let conn = Connection::open(&db_path).unwrap();
        conn.pragma_update(None, "user_version", SCHEMA_VERSION + 1)
            .unwrap();
        drop(conn);

        let err = match CacheStore::open(&db_path, false) {
            Ok(_) => panic!("expected newer schema version to fail"),
            Err(err) => err.to_string(),
        };
        assert!(err.contains("newer than supported"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn open_resets_older_schema_versions() {
        let root = temp_root("agent-history-cache-reset");
        fs::create_dir_all(&root).unwrap();
        let db_path = root.join("index.sqlite");

        let conn = Connection::open(&db_path).unwrap();
        conn.execute("CREATE TABLE stale_table (id INTEGER)", [])
            .unwrap();
        conn.pragma_update(None, "user_version", 0).unwrap();
        drop(conn);

        let store = CacheStore::open(&db_path, false).unwrap();
        let version: i64 = store
            .conn
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .unwrap();
        assert_eq!(version, SCHEMA_VERSION);
        let source_units_count: i64 = store
            .conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='source_units'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        let message_records_count: i64 = store
            .conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='message_records'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(source_units_count, 1);
        assert_eq!(message_records_count, 1);

        let _ = fs::remove_dir_all(root);
    }
}
