//! Isolated AgentOS provider profiles and managed binary discovery.
//!
//! Each supported provider runs against an AgentOS-owned profile directory
//! (`<data_dir>/agent-profiles/<provider>`, overridable via
//! `AGENTOS_AGENT_PROFILES_DIR`) rather than the user's personal `~/.gemini`,
//! `~/.codex`, `~/.claude`, or `~/.dsh` trees or global OS Keychain entries.

use std::cmp::Ordering;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Duration;

use agent_client_protocol::schema::ProtocolVersion;
use agent_client_protocol::schema::v1::{ClientCapabilities, Implementation, InitializeRequest};
use agent_client_protocol::{Agent, Client, ConnectionTo};
use anyhow::{Context as _, anyhow, bail};
use serde_json::{Value, json};

use crate::model::ProviderKind;

pub const AGENT_PROFILES_DIR_ENV: &str = "AGENTOS_AGENT_PROFILES_DIR";
pub const ANTIGRAVITY_ACP_BIN_ENV: &str = "AGENTOS_ANTIGRAVITY_ACP_BIN";
pub const ALLOW_AMBIENT_API_KEYS_ENV: &str = "AGENTOS_ALLOW_AMBIENT_API_KEYS";

/// Ambient shell API keys that belong to the user's personal CLI environment
/// rather than the isolated AgentOS profile.
pub const AMBIENT_CREDENTIAL_KEYS: &[&str] = &[
    "OPENAI_API_KEY",
    "CODEX_API_KEY",
    "CODEX_ACCESS_TOKEN",
    "CHATGPT_ACCESS_TOKEN",
    "ANTHROPIC_API_KEY",
    "ANTHROPIC_AUTH_TOKEN",
    "CLAUDE_CODE_OAUTH_TOKEN",
    "GEMINI_API_KEY",
    "GOOGLE_API_KEY",
    "GOOGLE_APPLICATION_CREDENTIALS",
    "GOOGLE_GENAI_USE_VERTEXAI",
];

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IsolatedProfile {
    pub env_key: &'static str,
    pub directory: PathBuf,
    pub extra_env: Vec<(String, String)>,
}

pub fn default_runtime_data_dir() -> PathBuf {
    if cfg!(debug_assertions) {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../..")
            .join("temp")
    } else {
        crate::identity::default_release_data_dir()
    }
}

pub fn profiles_root() -> PathBuf {
    if let Some(configured) = std::env::var_os(AGENT_PROFILES_DIR_ENV)
        .map(|value| value.to_string_lossy().into_owned())
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
    {
        return expand_tilde(&configured, dirs::home_dir().as_deref());
    }
    default_runtime_data_dir().join("agent-profiles")
}

fn expand_tilde(raw: &str, home: Option<&Path>) -> PathBuf {
    if raw == "~"
        && let Some(home) = home
    {
        return home.to_path_buf();
    }
    if let Some(rest) = raw.strip_prefix("~/").or_else(|| raw.strip_prefix("~\\"))
        && let Some(home) = home
    {
        return home.join(rest);
    }
    PathBuf::from(raw)
}

fn profile_spec(provider: ProviderKind) -> Option<(&'static str, &'static str, &'static str)> {
    match provider {
        ProviderKind::Antigravity => Some(("GEMINI_HOME", "antigravity", ".gemini")),
        ProviderKind::Codex => Some(("CODEX_HOME", "codex", ".codex")),
        ProviderKind::DeepSeek => Some(("DSH_HOME", "deepseek", ".dsh")),
        ProviderKind::Claude => Some(("CLAUDE_CONFIG_DIR", "claude", ".claude")),
        _ => None,
    }
}

pub fn resolve_isolated_profile(provider: ProviderKind) -> anyhow::Result<Option<IsolatedProfile>> {
    resolve_isolated_profile_under(provider, &profiles_root(), dirs::home_dir().as_deref())
}

pub fn resolve_isolated_profile_under(
    provider: ProviderKind,
    root: &Path,
    user_home: Option<&Path>,
) -> anyhow::Result<Option<IsolatedProfile>> {
    let Some((env_key, leaf, personal_rel)) = profile_spec(provider) else {
        return Ok(None);
    };
    if !root.is_absolute() {
        bail!(
            "isolated agent profiles root must be an absolute path: {}",
            root.display()
        );
    }
    if let Ok(meta) = fs::symlink_metadata(root)
        && meta.file_type().is_symlink()
    {
        bail!(
            "isolated agent profiles root must not be a symlink: {}",
            root.display()
        );
    }

    let candidate = root.join(leaf);
    if let Ok(meta) = fs::symlink_metadata(&candidate)
        && meta.file_type().is_symlink()
    {
        bail!(
            "isolated {} profile directory must not be a symlink: {}",
            provider.display_name(),
            candidate.display()
        );
    }

    if let Some(home) = user_home {
        let personal = home.join(personal_rel);
        reject_overlap(&candidate, &personal, provider)?;
    }

    fs::create_dir_all(&candidate).with_context(|| {
        format!(
            "could not create isolated {} profile directory at {}",
            provider.display_name(),
            candidate.display()
        )
    })?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        let _ = fs::set_permissions(&candidate, fs::Permissions::from_mode(0o700));
    }

    let resolved = candidate.canonicalize().unwrap_or(candidate);
    if let Some(home) = user_home {
        let personal = home.join(personal_rel);
        let personal_resolved = personal.canonicalize().unwrap_or(personal);
        reject_overlap(&resolved, &personal_resolved, provider)?;
    }

    let mut extra_env = Vec::new();
    match provider {
        ProviderKind::Antigravity => {
            // Prevent agy_acp_server from reading or mutating the personal
            // macOS/OS Keychain entry even when GEMINI_HOME is relocated.
            extra_env.push(("AGY_ACP_FORCE_FILE_STORAGE".into(), "1".into()));
            ensure_antigravity_settings(&resolved)?;
        }
        ProviderKind::Codex => {
            ensure_codex_file_auth_store(&resolved)?;
        }
        ProviderKind::DeepSeek => {
            bridge_deepseek_default_model(&resolved, &mut extra_env);
        }
        _ => {}
    }

    Ok(Some(IsolatedProfile {
        env_key,
        directory: resolved,
        extra_env,
    }))
}

fn reject_overlap(candidate: &Path, personal: &Path, provider: ProviderKind) -> anyhow::Result<()> {
    if candidate.starts_with(personal) || personal.starts_with(candidate) {
        bail!(
            "isolated {} profile ({}) overlaps personal directory ({})",
            provider.display_name(),
            candidate.display(),
            personal.display()
        );
    }
    Ok(())
}

/// Ensures `<GEMINI_HOME>/antigravity-acp/settings.json` declares an
/// `auth.type` (`oauth-personal` by default) so `session/new` never fails with
/// `-32000 Authentication required` on a fresh isolated profile. Existing
/// `auth.type` values are always preserved.
pub fn ensure_antigravity_settings(gemini_home: &Path) -> anyhow::Result<()> {
    let acp_dir = gemini_home.join("antigravity-acp");
    fs::create_dir_all(&acp_dir).with_context(|| {
        format!(
            "could not create Antigravity ACP directory at {}",
            acp_dir.display()
        )
    })?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        let _ = fs::set_permissions(&acp_dir, fs::Permissions::from_mode(0o700));
    }

    let settings_path = acp_dir.join("settings.json");
    let mut doc = match fs::read_to_string(&settings_path) {
        Ok(raw) if !raw.trim().is_empty() => serde_json::from_str::<Value>(&raw)
            .with_context(|| format!("invalid JSON in {}", settings_path.display()))?,
        Ok(_) => json!({}),
        Err(error) if error.kind() == io::ErrorKind::NotFound => json!({}),
        Err(error) => {
            return Err(error)
                .with_context(|| format!("could not read {}", settings_path.display()));
        }
    };

    let has_auth_type = doc
        .get("auth")
        .and_then(|auth| auth.get("type"))
        .and_then(Value::as_str)
        .is_some_and(|value| !value.trim().is_empty());
    if has_auth_type {
        return Ok(());
    }

    let root = doc
        .as_object_mut()
        .ok_or_else(|| anyhow!("{} root must be a JSON object", settings_path.display()))?;
    let auth_entry = root.entry("auth").or_insert_with(|| json!({}));
    if !auth_entry.is_object() {
        *auth_entry = json!({});
    }
    if let Some(auth_obj) = auth_entry.as_object_mut() {
        auth_obj.insert("type".into(), Value::String("oauth-personal".into()));
    }

    let serialized = serde_json::to_string_pretty(&doc)?;
    fs::write(&settings_path, format!("{serialized}\n"))
        .with_context(|| format!("could not write {}", settings_path.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        let _ = fs::set_permissions(&settings_path, fs::Permissions::from_mode(0o600));
    }
    Ok(())
}

/// Returns `true` when the isolated Antigravity profile already contains stored
/// OAuth/token credentials on disk. Background model discovery uses this guard
/// so it never spawns `session/new` on an unauthenticated `oauth-personal`
/// profile (which would otherwise repeatedly pop open a browser OAuth login).
pub fn antigravity_has_stored_credentials() -> bool {
    let Ok(Some(profile)) = resolve_isolated_profile(ProviderKind::Antigravity) else {
        return false;
    };
    antigravity_profile_has_credentials(&profile.directory)
}

pub fn antigravity_profile_has_credentials(gemini_home: &Path) -> bool {
    let acp_dir = gemini_home.join("antigravity-acp");
    ["acp_token.json", "oauth_creds.json"].iter().any(|name| {
        let path = acp_dir.join(name);
        fs::metadata(&path).is_ok_and(|meta| meta.is_file() && meta.len() > 2)
    })
}

/// Ensures `<CODEX_HOME>/config.toml` stores credentials in its isolated file
/// rather than the user's shared OS Keychain.
pub fn ensure_codex_file_auth_store(codex_home: &Path) -> anyhow::Result<()> {
    let config_path = codex_home.join("config.toml");
    let raw = match fs::read_to_string(&config_path) {
        Ok(raw) if !raw.trim().is_empty() => raw,
        Ok(_) => String::new(),
        Err(error) if error.kind() == io::ErrorKind::NotFound => String::new(),
        Err(error) => {
            return Err(error)
                .with_context(|| format!("could not read {}", config_path.display()));
        }
    };
    let mut table: toml::Table = if raw.is_empty() {
        toml::Table::new()
    } else {
        toml::from_str(&raw)
            .with_context(|| format!("invalid TOML in {}", config_path.display()))?
    };
    if table
        .get("cli_auth_credentials_store")
        .and_then(toml::Value::as_str)
        != Some("file")
    {
        table.insert(
            "cli_auth_credentials_store".into(),
            toml::Value::String("file".into()),
        );
        let serialized = toml::to_string_pretty(&table)?;
        fs::write(&config_path, serialized)
            .with_context(|| format!("could not write {}", config_path.display()))?;
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        if config_path.exists() {
            let _ = fs::set_permissions(&config_path, fs::Permissions::from_mode(0o600));
        }
    }
    Ok(())
}

fn bridge_deepseek_default_model(dsh_home: &Path, extra_env: &mut Vec<(String, String)>) {
    let Ok(raw) = fs::read_to_string(dsh_home.join("settings.yaml")) else {
        return;
    };
    let Ok(doc) = serde_saphyr::from_str::<Value>(&raw) else {
        return;
    };
    let pick = |key: &str| {
        doc.get("agent-default-model")?
            .get(key)?
            .as_str()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_owned)
    };
    if let (Some(provider), Some(model)) = (pick("provider"), pick("model")) {
        extra_env.push(("DEEPSEEK_ACP_PROVIDER".into(), provider));
        extra_env.push(("DEEPSEEK_ACP_MODEL".into(), model));
    }
}

fn allow_ambient_api_keys() -> bool {
    std::env::var(ALLOW_AMBIENT_API_KEYS_ENV)
        .ok()
        .is_some_and(|value| value.trim() == "1" || value.trim().eq_ignore_ascii_case("true"))
}

pub fn apply_to_command(provider: ProviderKind, command: &mut Command) -> anyhow::Result<()> {
    let Some(profile) = resolve_isolated_profile(provider)? else {
        return Ok(());
    };
    if !allow_ambient_api_keys() {
        for key in AMBIENT_CREDENTIAL_KEYS {
            command.env_remove(key);
        }
    }
    command.env(profile.env_key, &profile.directory);
    for (key, value) in profile.extra_env {
        command.env(key, value);
    }
    Ok(())
}

pub fn apply_to_env_vec(
    provider: ProviderKind,
    environment: &mut Vec<(String, String)>,
) -> anyhow::Result<Option<IsolatedProfile>> {
    apply_to_env_vec_under(
        provider,
        environment,
        &profiles_root(),
        dirs::home_dir().as_deref(),
        allow_ambient_api_keys(),
    )
}

pub fn apply_to_env_vec_under(
    provider: ProviderKind,
    environment: &mut Vec<(String, String)>,
    root: &Path,
    user_home: Option<&Path>,
    keep_ambient_keys: bool,
) -> anyhow::Result<Option<IsolatedProfile>> {
    let Some(profile) = resolve_isolated_profile_under(provider, root, user_home)? else {
        return Ok(None);
    };
    environment.retain(|(key, _)| {
        if key == profile.env_key {
            return false;
        }
        if profile.extra_env.iter().any(|(extra, _)| extra == key) {
            return false;
        }
        if !keep_ambient_keys && AMBIENT_CREDENTIAL_KEYS.contains(&key.as_str()) {
            return false;
        }
        true
    });
    environment.push((
        profile.env_key.to_owned(),
        profile.directory.to_string_lossy().into_owned(),
    ));
    for (key, value) in &profile.extra_env {
        environment.push((key.clone(), value.clone()));
    }
    Ok(Some(profile))
}

/// Platform directory key used inside managed `acp-binaries/antigravity-acp/<version>/<platform>`.
pub fn antigravity_platform_slug() -> &'static str {
    #[cfg(all(target_os = "macos", target_arch = "aarch64"))]
    {
        "darwin-aarch64"
    }
    #[cfg(all(target_os = "macos", target_arch = "x86_64"))]
    {
        "darwin-x86_64"
    }
    #[cfg(all(target_os = "linux", target_arch = "aarch64"))]
    {
        "linux-aarch64"
    }
    #[cfg(all(target_os = "linux", target_arch = "x86_64"))]
    {
        "linux-x86_64"
    }
    #[cfg(all(target_os = "windows", target_arch = "aarch64"))]
    {
        "win32-arm64"
    }
    #[cfg(all(target_os = "windows", target_arch = "x86_64"))]
    {
        "win32-x64"
    }
}

fn antigravity_entry_and_sibling() -> (&'static str, &'static str) {
    if cfg!(windows) {
        ("agy_acp_server.exe", "localharness_external.exe")
    } else {
        ("agy_acp_server.par", "localharness_external")
    }
}

/// Locates Google Antigravity's first-party ACP server (`agy_acp_server.par` /
/// `agy_acp_server.exe`), checking explicit env overrides, `PATH`, and managed
/// `acp-binaries/antigravity-acp/<version>/<platform>` caches.
pub fn find_antigravity_binary() -> Option<PathBuf> {
    if let Some(explicit) = std::env::var_os(ANTIGRAVITY_ACP_BIN_ENV)
        .map(|value| value.to_string_lossy().into_owned())
        .filter(|value| !value.trim().is_empty())
        && let Some(resolved) = crate::command_env::resolve_binary_override(&explicit)
    {
        return Some(resolved);
    }

    for cmd in [ProviderKind::Antigravity.command(), "antigravity-acp"] {
        if let Some(found) = crate::command_env::find_executable(cmd) {
            return Some(found);
        }
    }

    let mut search_roots = Vec::new();
    search_roots.push(
        default_runtime_data_dir()
            .join("acp-binaries")
            .join("antigravity-acp"),
    );
    if let Some(parent) = profiles_root().parent() {
        search_roots.push(parent.join("acp-binaries").join("antigravity-acp"));
    }
    search_roots.push(PathBuf::from(
        "/Volumes/data/Codex/AgentOS/app-data/app.agentos.sessionlab/acp-binaries/antigravity-acp",
    ));
    if let Some(data_dir) = dirs::data_dir() {
        search_roots.push(
            data_dir
                .join("app.codeg")
                .join("acp-binaries")
                .join("antigravity-acp"),
        );
    }

    find_antigravity_in_cache_roots(&search_roots, antigravity_platform_slug())
}

pub fn find_antigravity_in_cache_roots(roots: &[PathBuf], platform: &str) -> Option<PathBuf> {
    let (entry_name, sibling_name) = antigravity_entry_and_sibling();
    for root in roots {
        let Ok(entries) = fs::read_dir(root) else {
            continue;
        };
        let mut versions: Vec<(String, PathBuf)> = entries
            .filter_map(Result::ok)
            .filter_map(|dir_entry| {
                let version = dir_entry.file_name().to_string_lossy().into_owned();
                let platform_dir = dir_entry.path().join(platform);
                let binary = platform_dir.join(entry_name);
                let sibling = platform_dir.join(sibling_name);
                (binary.is_file() && sibling.is_file()).then_some((version, binary))
            })
            .collect();
        versions.sort_by(|(a, _), (b, _)| compare_version_tags(b, a));
        if let Some((_, binary)) = versions.into_iter().next() {
            return Some(binary);
        }
    }
    None
}

fn compare_version_tags(a: &str, b: &str) -> Ordering {
    let parse_parts = |tag: &str| -> Vec<u64> {
        tag.trim_start_matches('v')
            .split(['.', '-', '_'])
            .map(|part| part.parse::<u64>().unwrap_or(0))
            .collect()
    };
    parse_parts(a).cmp(&parse_parts(b)).then_with(|| a.cmp(b))
}

/// Probes Antigravity's version without invoking `--version` (which hangs
/// because `agy_acp_server.par` speaks ACP JSON-RPC over stdio rather than CLI
/// flags). Extracts the version from managed cache paths first and falls back
/// to a bounded ACP `initialize` handshake.
pub fn probe_antigravity_version(binary: &Path) -> Option<String> {
    if let Some(version_os) = binary.parent().and_then(Path::parent).and_then(Path::file_name) {
        let candidate = version_os.to_string_lossy();
        if let Some(parsed) = waku_protocol::model::parse_cli_version(&candidate) {
            return Some(parsed);
        }
    }

    let cwd = crate::acp_session::catalog_working_directory().ok()?;
    let agent = crate::driver::catalog_agent(ProviderKind::Antigravity, binary, &cwd).ok()?;
    let request = Client.builder().name("waku-version-probe").connect_with(
        agent,
        async move |connection: ConnectionTo<Agent>| {
            let init = connection
                .send_request(
                    InitializeRequest::new(ProtocolVersion::V1)
                        .client_capabilities(ClientCapabilities::new().terminal(false))
                        .client_info(Implementation::new("waku", env!("CARGO_PKG_VERSION"))),
                )
                .block_task()
                .await?;
            Ok(init.agent_info.map(|info| info.version))
        },
    );
    smol::block_on(smol::future::race(
        async move { request.await.ok().flatten() },
        async move {
            smol::Timer::after(Duration::from_secs(3)).await;
            None
        },
    ))
    .and_then(|raw| waku_protocol::model::parse_cli_version(&raw).or(Some(raw)))
}

#[cfg(test)]
mod tests {
    use serde_json::Map;

    use super::*;

    fn unique_temp_dir(label: &str) -> PathBuf {
        let base = std::env::temp_dir().canonicalize().unwrap_or_else(|_| std::env::temp_dir());
        let dir = base.join(format!("waku-agent-profiles-{label}-{}", uuid::Uuid::new_v4()));
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn antigravity_profile_sets_gemini_home_file_storage_and_default_oauth_settings() {
        let sandbox = unique_temp_dir("agy");
        let fake_home = sandbox.join("user-home");
        let personal_gemini = fake_home.join(".gemini");
        fs::create_dir_all(&personal_gemini).unwrap();
        fs::write(personal_gemini.join("personal_marker.txt"), "untouched").unwrap();

        let profiles_root = sandbox.join("isolated-profiles");
        let mut env = vec![
            ("GEMINI_HOME".into(), personal_gemini.display().to_string()),
            ("GEMINI_API_KEY".into(), "personal-secret".into()),
            ("OPENAI_API_KEY".into(), "personal-openai".into()),
            ("PATH".into(), "/usr/bin".into()),
        ];

        let profile = apply_to_env_vec_under(
            ProviderKind::Antigravity,
            &mut env,
            &profiles_root,
            Some(&fake_home),
            false,
        )
        .unwrap()
        .expect("Antigravity has an isolated profile");

        let map: Map<String, Value> = env
            .into_iter()
            .map(|(k, v)| (k, Value::String(v)))
            .collect();
        assert_eq!(
            map.get("GEMINI_HOME").and_then(Value::as_str),
            Some(profile.directory.to_string_lossy().as_ref())
        );
        assert_eq!(
            map.get("AGY_ACP_FORCE_FILE_STORAGE").and_then(Value::as_str),
            Some("1")
        );
        assert!(!map.contains_key("GEMINI_API_KEY"));
        assert!(!map.contains_key("OPENAI_API_KEY"));

        let settings_raw =
            fs::read_to_string(profile.directory.join("antigravity-acp/settings.json")).unwrap();
        let settings: Value = serde_json::from_str(&settings_raw).unwrap();
        assert_eq!(settings["auth"]["type"], "oauth-personal");

        // Personal ~/.gemini was never mutated with antigravity-acp/settings.json.
        assert!(!personal_gemini.join("antigravity-acp").exists());
        let _ = fs::remove_dir_all(sandbox);
    }

    #[test]
    fn rejects_profile_overlapping_personal_directory_or_symlink() {
        let sandbox = unique_temp_dir("overlap");
        let fake_home = sandbox.join("home");
        fs::create_dir_all(fake_home.join(".gemini")).unwrap();

        // Setting root so `<root>/antigravity` overlaps personal directory fails.
        let err = resolve_isolated_profile_under(
            ProviderKind::Antigravity,
            &fake_home.join(".gemini"),
            Some(&fake_home),
        )
        .unwrap_err();
        assert!(err.to_string().contains("overlaps personal directory"));

        #[cfg(unix)]
        {
            let root = sandbox.join("profiles");
            fs::create_dir_all(&root).unwrap();
            std::os::unix::fs::symlink(fake_home.join(".gemini"), root.join("antigravity"))
                .unwrap();
            let symlink_err = resolve_isolated_profile_under(
                ProviderKind::Antigravity,
                &root,
                Some(&fake_home),
            )
            .unwrap_err();
            assert!(symlink_err.to_string().contains("symlink"));
        }

        let _ = fs::remove_dir_all(sandbox);
    }

    #[test]
    fn codex_profile_enforces_file_credentials_store() {
        let sandbox = unique_temp_dir("codex");
        let fake_home = sandbox.join("home");
        let root = sandbox.join("profiles");
        let profile = resolve_isolated_profile_under(
            ProviderKind::Codex,
            &root,
            Some(&fake_home),
        )
        .unwrap()
        .unwrap();

        let config = fs::read_to_string(profile.directory.join("config.toml")).unwrap();
        assert!(config.contains("cli_auth_credentials_store = \"file\""));
        let _ = fs::remove_dir_all(sandbox);
    }

    #[test]
    fn discovers_highest_version_antigravity_binary_with_required_localharness_sibling() {
        let sandbox = unique_temp_dir("bins");
        let cache = sandbox.join("antigravity-acp");
        let (entry, sibling) = antigravity_entry_and_sibling();

        // 2.0.0 is missing localharness_external -> must be skipped!
        let broken = cache.join("2.0.0/darwin-aarch64");
        fs::create_dir_all(&broken).unwrap();
        fs::write(broken.join(entry), "bin").unwrap();

        // 1.2.1 and 1.4.5 both have required sibling -> 1.4.5 wins!
        for ver in ["1.2.1", "1.4.5"] {
            let dir = cache.join(format!("{ver}/darwin-aarch64"));
            fs::create_dir_all(&dir).unwrap();
            fs::write(dir.join(entry), "bin").unwrap();
            fs::write(dir.join(sibling), "harness").unwrap();
        }

        let found =
            find_antigravity_in_cache_roots(&[cache.clone()], "darwin-aarch64").unwrap();
        assert_eq!(found, cache.join("1.4.5/darwin-aarch64").join(entry));
        assert_eq!(probe_antigravity_version(&found).as_deref(), Some("1.4.5"));
        let _ = fs::remove_dir_all(sandbox);
    }
}
