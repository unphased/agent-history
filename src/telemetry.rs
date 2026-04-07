use anyhow::Context as _;
use serde::Serialize;
use serde_json::Value;
use std::{
    env, fs,
    fs::OpenOptions,
    io::Write as _,
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

#[derive(Debug, Serialize)]
struct TelemetryRecord {
    ts_ms: u128,
    kind: String,
    data: Value,
}

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

    pub fn emit(&mut self, kind: &str, data: Value) -> anyhow::Result<()> {
        let rec = TelemetryRecord {
            ts_ms: unix_now_ms(),
            kind: kind.to_string(),
            data,
        };
        serde_json::to_writer(&mut self.file, &rec).context("telemetry record encode failed")?;
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_log_path_uses_events_jsonl() {
        assert!(default_log_path().ends_with("agent-history/events.jsonl"));
    }
}
