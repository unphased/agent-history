use crate::indexer::{MessageRecord, Role, SourceKind};
use anyhow::Context as _;
use rusqlite::{Connection, Transaction, params};
use std::{
    collections::HashMap,
    env, fs,
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

const SCHEMA_VERSION: i64 = 1;

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
                "SELECT timestamp, role, text, file, line, session_id, account, cwd, phase, source \
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
                    source: source_from_db(row.get::<_, i64>(9)?),
                })
            })?;
            for row in rows {
                out.push(row?);
            }
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
                        unit_key, ord, timestamp, role, text, file, line, session_id, account, cwd, phase, source
                    ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
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
                    source_to_db(rec.source),
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

    fn clear(&mut self) -> anyhow::Result<()> {
        self.conn
            .execute_batch(
                "DELETE FROM message_records;
                 DELETE FROM source_units;",
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
                    source INTEGER NOT NULL,
                    PRIMARY KEY (unit_key, ord),
                    FOREIGN KEY (unit_key) REFERENCES source_units(unit_key) ON DELETE CASCADE
                 );
                 CREATE INDEX IF NOT EXISTS idx_message_records_session_id ON message_records(session_id);
                 PRAGMA user_version = 1;",
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

    #[test]
    fn unit_key_uses_path_string() {
        assert_eq!(unit_key(Path::new("/tmp/demo")), "/tmp/demo");
    }
}
