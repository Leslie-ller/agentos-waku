use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use ts_rs::TS;

use crate::computer_use::ComputerAppGrant;
use crate::model::ProviderKind;

fn default_true() -> bool {
    true
}

fn default_advisor_provider() -> String {
    "auto".to_owned()
}

/// Feature toggles and routing preferences for the built-in `agentos-mcp`
/// companion server. Stored in `DaemonSettings` so the desktop UI, Web SPA,
/// and daemon share one authoritative configuration.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize, TS)]
#[serde(default, rename_all = "camelCase")]
pub struct AgentOsMcpSettings {
    #[serde(default = "default_true")]
    pub memory_enabled: bool,
    #[serde(default = "default_true")]
    pub goal_enabled: bool,
    #[serde(default = "default_true")]
    pub advisor_enabled: bool,
    /// Target advisor provider: `"auto"` (heterogeneous opposite family),
    /// `"codex"`, or `"antigravity"`.
    #[serde(default = "default_advisor_provider")]
    pub advisor_provider: String,
    #[serde(default = "default_true")]
    pub ask_user_enabled: bool,
    #[serde(default)]
    pub delegation_enabled: bool,
    #[serde(default = "default_true")]
    pub session_info_enabled: bool,
}

impl Default for AgentOsMcpSettings {
    fn default() -> Self {
        Self {
            memory_enabled: true,
            goal_enabled: true,
            advisor_enabled: true,
            advisor_provider: default_advisor_provider(),
            ask_user_enabled: true,
            delegation_enabled: false,
            session_info_enabled: true,
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize, TS)]
#[serde(default)]
pub struct DaemonSettings {
    pub computer_use_enabled: bool,
    pub computer_use_allowed_apps: Vec<ComputerAppGrant>,
    pub disabled_providers: Vec<ProviderKind>,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    pub provider_binary_overrides: HashMap<ProviderKind, String>,
    #[serde(default)]
    pub agentos_mcp: AgentOsMcpSettings,
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}

impl Default for DaemonSettings {
    fn default() -> Self {
        Self {
            computer_use_enabled: false,
            computer_use_allowed_apps: Vec::new(),
            disabled_providers: Vec::new(),
            provider_binary_overrides: HashMap::new(),
            agentos_mcp: AgentOsMcpSettings::default(),
            extra: BTreeMap::new(),
        }
    }
}

impl DaemonSettings {
    pub fn default_path() -> PathBuf {
        crate::identity::default_config_dir().join("settings.json")
    }

    pub fn discard_legacy_app_keys(&mut self) {
        for key in ["analytics_enabled", "favorite_models", "theme", "language"] {
            self.extra.remove(key);
        }
    }
}
