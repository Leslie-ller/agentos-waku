//! Shared application identity and isolated directory resolution used by the
//! daemon and desktop client.

use std::path::PathBuf;

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

pub fn default_config_dir() -> PathBuf {
    if let Some(custom) = std::env::var_os(CONFIG_DIR_ENV).filter(|val| !val.is_empty()) {
        return PathBuf::from(custom);
    }
    dirs::home_dir()
        .unwrap_or_else(std::env::temp_dir)
        .join(CONFIG_DIRECTORY_NAME)
}

pub fn default_release_data_dir() -> PathBuf {
    if let Some(custom) = std::env::var_os(DATA_DIR_ENV).filter(|val| !val.is_empty()) {
        return PathBuf::from(custom);
    }
    dirs::data_local_dir()
        .unwrap_or_else(std::env::temp_dir)
        .join(DATA_DIRECTORY_NAME)
}
