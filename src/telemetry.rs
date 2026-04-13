use anyhow::Context as _;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    collections::VecDeque,
    env, fs,
    fs::OpenOptions,
    io::{BufRead as _, BufReader, Write as _},
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventRecord {
    pub ts_ms: u128,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub group: Option<String>,
    pub kind: String,
    pub data: Value,
}

impl EventRecord {
    pub fn new(group: Option<&str>, kind: &str, data: Value) -> Self {
        Self {
            ts_ms: unix_now_ms(),
            group: group.map(str::to_string),
            kind: kind.to_string(),
            data,
        }
    }

    pub fn encoded_len(&self) -> usize {
        serde_json::to_vec(self)
            .map(|bytes| bytes.len().saturating_add(1))
            .unwrap_or_default()
    }
}

#[derive(Debug)]
pub struct TelemetrySink {
    file: fs::File,
}

impl TelemetrySink {
    pub fn open(path: &Path) -> anyhow::Result<Self> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).with_context(|| {
                format!("telemetry directory creation failed: {}", parent.display())
            })?;
        }

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .with_context(|| format!("telemetry log open failed: {}", path.display()))?;

        Ok(Self { file })
    }

    pub fn emit_record(&mut self, rec: &EventRecord) -> anyhow::Result<()> {
        serde_json::to_writer(&mut self.file, rec).context("telemetry record encode failed")?;
        self.file
            .write_all(b"\n")
            .context("telemetry newline write failed")?;
        self.file.flush().context("telemetry flush failed")?;
        Ok(())
    }

}

pub fn default_log_path() -> PathBuf {
    if let Some(path) = env::var_os("AGENT_HISTORY_TELEMETRY_LOG") {
        return PathBuf::from(path);
    }

    let home = env::var_os("HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("."));
    home.join(".local/state/agent-history/events.jsonl")
}

fn unix_now_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or_default()
}

pub fn read_recent_records(path: &Path, max_bytes: usize) -> Vec<EventRecord> {
    let Ok(file) = fs::File::open(path) else {
        return Vec::new();
    };
    let reader = BufReader::new(file);
    let mut records = VecDeque::new();
    let mut total_bytes = 0usize;

    for line in reader.lines().map_while(Result::ok) {
        let line_bytes = line.len().saturating_add(1);
        let Ok(record) = serde_json::from_str::<EventRecord>(&line) else {
            continue;
        };
        records.push_back(record);
        total_bytes = total_bytes.saturating_add(line_bytes);
        while total_bytes > max_bytes {
            if let Some(oldest) = records.pop_front() {
                total_bytes = total_bytes.saturating_sub(oldest.encoded_len());
            } else {
                break;
            }
        }
    }

    records.into_iter().collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn default_log_path_uses_events_jsonl() {
        assert!(default_log_path().ends_with("agent-history/events.jsonl"));
    }

    #[test]
    fn event_record_serializes_optional_group() {
        let grouped = EventRecord::new(Some("perf"), "preview_build_summary", json!({"ms": 1}));
        let plain = EventRecord::new(None, "indexer_started", json!({}));
        let grouped_json = serde_json::to_value(grouped).unwrap();
        let plain_json = serde_json::to_value(plain).unwrap();

        assert_eq!(grouped_json.get("group").and_then(|value| value.as_str()), Some("perf"));
        assert!(plain_json.get("group").is_none());
    }
}
