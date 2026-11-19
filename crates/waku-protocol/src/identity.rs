//! Shared application identity and isolated directory resolution used by the
//! daemon and desktop client.

use std::path::{Path, PathBuf};

use serde::Deserialize;

#[cfg(debug_assertions)]
pub const APP_NAME: &str = "AgentOS Waku Debug";
#[cfg(not(debug_assertions))]
pub const APP_NAME: &str = "AgentOS Waku";

#[cfg(debug_assertions)]
pub const APP_ID: &str = "app.agentos.waku.dev";
#[cfg(not(debug_assertions))]
pub const APP_ID: &str = "app.agentos.waku";

#[cfg(debug_assertions)]
pub const DATA_DIRECTORY_NAME: &str = "AgentOS Waku Debug";
#[cfg(not(debug_assertions))]
pub const DATA_DIRECTORY_NAME: &str = "AgentOS Waku";

pub const CONFIG_DIRECTORY_NAME: &str = ".agentos-waku";
pub const LEGACY_CONFIG_DIRECTORY_NAME: &str = ".waku";

/// Optional environment override for the data directory holding `app.db`,
/// `state.json`, and `blobs/` (useful for external-volume storage and isolated
/// acceptance harnesses).
pub const DATA_DIR_ENV: &str = "AGENTOS_WAKU_DATA_DIR";

/// Optional environment override for the user configuration directory holding
/// `app.json`, `settings.json`, `projects/`, and `worktrees/`.
pub const CONFIG_DIR_ENV: &str = "AGENTOS_WAKU_CONFIG_DIR";

/// File inside `default_config_dir()` that lets a directly double-clicked
/// desktop app resolve persistent external-volume data and isolated profile
/// roots without requiring wrapper launch scripts.
pub const PATHS_BOOTSTRAP_FILE: &str = "paths.json";

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default)]
struct BootstrapPaths {
    #[serde(alias = "dataDir")]
    data_dir: Option<String>,
    #[serde(alias = "agentProfilesDir")]
    agent_profiles_dir: Option<String>,
}

fn expand_tilde_path(raw: &str, home: Option<&Path>) -> PathBuf {
    let trimmed = raw.trim();
    if trimmed == "~"
        && let Some(home) = home
    {
        return home.to_path_buf();
    }
    if let Some(rest) = trimmed
        .strip_prefix("~/")
        .or_else(|| trimmed.strip_prefix("~\\"))
        && let Some(home) = home
    {
        return home.join(rest);
    }
    PathBuf::from(trimmed)
}

fn load_bootstrap_paths() -> BootstrapPaths {
    let path = default_config_dir().join(PATHS_BOOTSTRAP_FILE);
    let Ok(contents) = std::fs::read_to_string(path) else {
        return BootstrapPaths::default();
    };
    serde_json::from_str::<BootstrapPaths>(&contents).unwrap_or_default()
}

pub fn default_config_dir() -> PathBuf {
    if let Some(custom) = std::env::var_os(CONFIG_DIR_ENV).filter(|val| !val.is_empty()) {
        return PathBuf::from(custom);
    }
    dirs::home_dir()
        .unwrap_or_else(std::env::temp_dir)
        .join(CONFIG_DIRECTORY_NAME)
}

pub fn configured_agent_profiles_dir() -> Option<PathBuf> {
    let raw = load_bootstrap_paths().agent_profiles_dir?;
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return None;
    }
    Some(expand_tilde_path(trimmed, dirs::home_dir().as_deref()))
}

pub fn default_release_data_dir() -> PathBuf {
    if let Some(custom) = std::env::var_os(DATA_DIR_ENV).filter(|val| !val.is_empty()) {
        return PathBuf::from(custom);
    }
    if let Some(configured) = load_bootstrap_paths()
        .data_dir
        .filter(|val| !val.trim().is_empty())
    {
        return expand_tilde_path(&configured, dirs::home_dir().as_deref());
    }
    dirs::data_local_dir()
        .unwrap_or_else(std::env::temp_dir)
        .join(DATA_DIRECTORY_NAME)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bootstrap_paths_parses_snake_and_camel_case_and_expands_tilde() {
        let parsed: BootstrapPaths = serde_json::from_str(
            r#"{"data_dir": "/Volumes/data/AgentOS/waku", "agentProfilesDir": "~/profiles"}"#,
        )
        .unwrap();
        assert_eq!(parsed.data_dir.as_deref(), Some("/Volumes/data/AgentOS/waku"));
        let expanded = expand_tilde_path(
            parsed.agent_profiles_dir.as_deref().unwrap(),
            Some(Path::new("/Users/tester")),
        );
        assert_eq!(expanded, PathBuf::from("/Users/tester/profiles"));
    }
}
