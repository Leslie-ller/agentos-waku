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
    "both".to_owned()
}

fn default_advisor_codex_model() -> String {
    "gpt-6.2-astra".to_owned()
}

fn default_advisor_codex_effort() -> String {
    "medium".to_owned()
}

fn default_advisor_antigravity_model() -> String {
    "claude-opus-5-5".to_owned()
}

fn default_advisor_timeout_seconds() -> u32 {
    120
}

fn default_advisor_auto_codex_model() -> String {
    "gpt-6.2-sol".to_owned()
}

fn default_advisor_auto_antigravity_effort() -> String {
    "medium".to_owned()
}

fn default_advisor_auto_antigravity_max_files() -> u32 {
    3
}

fn default_advisor_auto_review_cooldown_minutes() -> u32 {
    30
}

fn default_advisor_auto_review_daily_cap() -> u32 {
    0
}

fn default_advisor_auto_min_edited_files() -> u32 {
    3
}

fn default_advisor_auto_min_diff_lines() -> u32 {
    150
}

fn default_advisor_auto_min_research_ops() -> u32 {
    10
}

fn default_advisor_auto_min_human_turns() -> u32 {
    4
}

fn default_advisor_auto_min_tool_ops() -> u32 {
    12
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
    /// Target advisor provider: `"both"` (Codex + Antigravity in parallel),
    /// `"auto"` (heterogeneous opposite family), `"codex"`, or `"antigravity"`.
    #[serde(default = "default_advisor_provider")]
    pub advisor_provider: String,
    // Hot-reloadable advisor model/effort/timeout overrides (read live on every
    // consult_advisor call and background auto-review without recompiling).
    #[serde(default = "default_advisor_codex_model")]
    pub advisor_codex_model: String,
    #[serde(default = "default_advisor_codex_effort")]
    pub advisor_codex_effort: String,
    #[serde(default = "default_advisor_antigravity_model")]
    pub advisor_antigravity_model: String,
    #[serde(default)]
    pub advisor_antigravity_effort: String,
    #[serde(default = "default_advisor_timeout_seconds")]
    pub advisor_timeout_seconds: u32,
    // Automatically run background dual-advisor review once a human-rooted task
    // crosses scale thresholds, injecting the result into the next prompt.
    #[serde(default = "default_true")]
    pub advisor_auto_review_enabled: bool,
    // Optional provider override for background auto-reviews (empty = inherit advisorProvider).
    #[serde(default)]
    pub advisor_auto_provider: String,
    #[serde(default = "default_advisor_auto_codex_model")]
    pub advisor_auto_codex_model: String,
    #[serde(default)]
    pub advisor_auto_codex_effort: String,
    // Optional Antigravity model override for background auto-reviews (empty = inherit advisorAntigravityModel).
    #[serde(default)]
    pub advisor_auto_antigravity_model: String,
    #[serde(default = "default_advisor_auto_antigravity_effort")]
    pub advisor_auto_antigravity_effort: String,
    // Max workspace files passed to background auto-review (0 = unrestricted).
    #[serde(default)]
    pub advisor_auto_codex_max_files: u32,
    #[serde(default = "default_advisor_auto_antigravity_max_files")]
    pub advisor_auto_antigravity_max_files: u32,
    #[serde(default = "default_advisor_auto_review_cooldown_minutes")]
    pub advisor_auto_review_cooldown_minutes: u32,
    // Global 24h rolling cap for auto-reviews (0 = unlimited; disable via advisorAutoReviewEnabled).
    #[serde(default = "default_advisor_auto_review_daily_cap")]
    pub advisor_auto_review_daily_cap: u32,
    // Per-session 24h rolling cap for auto-reviews (0 = unlimited).
    #[serde(default)]
    pub advisor_auto_review_per_session_daily_cap: u32,
    // Scale-trigger thresholds for background auto-review (0 disables that individual rule).
    #[serde(default = "default_advisor_auto_min_edited_files")]
    pub advisor_auto_min_edited_files: u32,
    #[serde(default = "default_advisor_auto_min_diff_lines")]
    pub advisor_auto_min_diff_lines: u32,
    #[serde(default = "default_advisor_auto_min_research_ops")]
    pub advisor_auto_min_research_ops: u32,
    #[serde(default = "default_advisor_auto_min_human_turns")]
    pub advisor_auto_min_human_turns: u32,
    #[serde(default = "default_advisor_auto_min_tool_ops")]
    pub advisor_auto_min_tool_ops: u32,
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
            advisor_codex_model: default_advisor_codex_model(),
            advisor_codex_effort: default_advisor_codex_effort(),
            advisor_antigravity_model: default_advisor_antigravity_model(),
            advisor_antigravity_effort: String::new(),
            advisor_timeout_seconds: default_advisor_timeout_seconds(),
            advisor_auto_review_enabled: true,
            advisor_auto_provider: String::new(),
            advisor_auto_codex_model: default_advisor_auto_codex_model(),
            advisor_auto_codex_effort: String::new(),
            advisor_auto_antigravity_model: String::new(),
            advisor_auto_antigravity_effort: default_advisor_auto_antigravity_effort(),
            advisor_auto_codex_max_files: 0,
            advisor_auto_antigravity_max_files: default_advisor_auto_antigravity_max_files(),
            advisor_auto_review_cooldown_minutes: default_advisor_auto_review_cooldown_minutes(),
            advisor_auto_review_daily_cap: default_advisor_auto_review_daily_cap(),
            advisor_auto_review_per_session_daily_cap: 0,
            advisor_auto_min_edited_files: default_advisor_auto_min_edited_files(),
            advisor_auto_min_diff_lines: default_advisor_auto_min_diff_lines(),
            advisor_auto_min_research_ops: default_advisor_auto_min_research_ops(),
            advisor_auto_min_human_turns: default_advisor_auto_min_human_turns(),
            advisor_auto_min_tool_ops: default_advisor_auto_min_tool_ops(),
            ask_user_enabled: true,
            delegation_enabled: false,
            session_info_enabled: true,
        }
    }
}

/// Configuration for the daemon's Remote Streamable HTTP MCP endpoint (`/mcp`).
///
/// Uses a dedicated, revocable bearer token separate from the master daemon
/// WebSocket token so remote MCP clients can inspect sessions and send messages
/// without gaining shell/terminal or full daemon control.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize, TS)]
#[serde(default, rename_all = "camelCase")]
pub struct RemoteMcpSettings {
    /// Explicit opt-in toggle for exposing `/mcp` on the daemon HTTP listener.
    #[serde(default)]
    pub enabled: bool,
    /// Dedicated bearer token required via `Authorization: Bearer <token>`.
    #[serde(default)]
    pub token: String,
    /// Allow remote MCP clients to create sessions, send messages, and cancel turns.
    #[serde(default = "default_true")]
    pub allow_send: bool,
    /// Allow remote MCP clients to approve permission prompts and answer
    /// interactive user-input questions (disabled by default for safety).
    #[serde(default)]
    pub allow_approvals: bool,
}

impl Default for RemoteMcpSettings {
    fn default() -> Self {
        Self {
            enabled: false,
            token: String::new(),
            allow_send: true,
            allow_approvals: false,
        }
    }
}

impl RemoteMcpSettings {
    pub fn new_token() -> String {
        format!(
            "agm_{}{}",
            uuid::Uuid::new_v4().simple(),
            uuid::Uuid::new_v4().simple()
        )
    }

    pub fn ensure_token(&mut self) -> bool {
        if !self.token.trim().is_empty() {
            return false;
        }
        self.token = Self::new_token();
        true
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
    #[serde(default)]
    pub remote_mcp: RemoteMcpSettings,
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
            remote_mcp: RemoteMcpSettings::default(),
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
