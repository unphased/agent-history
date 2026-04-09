use anyhow::Context as _;
use serde::Deserialize;
use std::{
    env, fs,
    path::{Path, PathBuf},
};

#[derive(Debug, Clone, Deserialize, Default)]
pub struct AppConfig {
    #[serde(default)]
    pub machine: MachineConfig,
    #[serde(default)]
    pub ui: UiConfig,
    #[serde(default)]
    pub remotes: Vec<RemoteConfig>,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct MachineConfig {
    pub id: Option<String>,
    pub name: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct UiConfig {
    #[serde(default)]
    pub tags: UiTagConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct UiTagConfig {
    #[serde(default = "default_true")]
    pub show_provider: bool,
    #[serde(default = "default_true")]
    pub show_account: bool,
    #[serde(default = "default_true")]
    pub show_host: bool,
    #[serde(default = "default_true")]
    pub show_project: bool,
}

impl Default for UiTagConfig {
    fn default() -> Self {
        Self {
            show_provider: true,
            show_account: true,
            show_host: true,
            show_project: true,
        }
    }
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct RemoteConfig {
    pub name: String,
    pub host: String,
    pub user: Option<String>,
    pub command: Option<String>,
    /// Path to the cache DB on the remote machine (default: ~/.local/state/agent-history/index.sqlite)
    pub cache_path: Option<String>,
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default = "default_true")]
    pub refresh_on_start: bool,
}

#[derive(Debug, Clone)]
pub struct MachineIdentity {
    pub id: String,
    pub name: String,
}

impl AppConfig {
    pub fn machine_identity(&self) -> MachineIdentity {
        let fallback = detected_hostname();
        MachineIdentity {
            id: self.machine.id.clone().unwrap_or_else(|| fallback.clone()),
            name: self.machine.name.clone().unwrap_or(fallback),
        }
    }
}

pub fn load_config(path: Option<&Path>) -> anyhow::Result<AppConfig> {
    let path = path
        .map(PathBuf::from)
        .unwrap_or_else(default_config_path);
    if !path.exists() {
        return Ok(AppConfig::default());
    }
    let body = fs::read_to_string(&path)
        .with_context(|| format!("config read failed: {}", path.display()))?;
    let cfg: AppConfig = toml::from_str(&body)
        .with_context(|| format!("config parse failed: {}", path.display()))?;
    Ok(cfg)
}

pub fn default_config_path() -> PathBuf {
    if let Some(path) = env::var_os("AGENT_HISTORY_CONFIG") {
        return PathBuf::from(path);
    }
    let home = env::var_os("HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("."));
    home.join(".config/agent-history/config.toml")
}

fn detected_hostname() -> String {
    hostname::get()
        .ok()
        .and_then(|name| name.into_string().ok())
        .filter(|value| !value.trim().is_empty())
        .or_else(|| env::var("HOSTNAME").ok())
        .unwrap_or_else(|| "localhost".to_string())
}

fn default_true() -> bool {
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn missing_config_defaults_cleanly() {
        let tmp = PathBuf::from("/tmp/agent-history-no-such-config.toml");
        let cfg = load_config(Some(&tmp)).unwrap();
        assert!(cfg.remotes.is_empty());
        assert!(cfg.ui.tags.show_provider);
    }

    #[test]
    fn machine_identity_uses_config_values() {
        let cfg = AppConfig {
            machine: MachineConfig {
                id: Some("mbp".to_string()),
                name: Some("MacBook Pro".to_string()),
            },
            ..AppConfig::default()
        };
        let id = cfg.machine_identity();
        assert_eq!(id.id, "mbp");
        assert_eq!(id.name, "MacBook Pro");
    }
}
