//! Daemon-only provider discovery layered over shared protocol models.

use std::path::Path;

pub use waku_protocol::model::*;

pub fn provider_probe(provider: ProviderKind, binary_override: Option<&str>) -> ProviderProbe {
    let path = match binary_override {
        Some(binary) => crate::command_env::resolve_binary_override(binary),
        None if provider == ProviderKind::Antigravity => {
            crate::agent_profiles::find_antigravity_binary()
        }
        None if provider == ProviderKind::Codex => find_codex_binary(),
        None => crate::command_env::find_executable(provider.command()),
    };
    ProviderProbe {
        provider,
        installed: path.is_some(),
        path,
        models: crate::model_catalog::fallback_models(provider),
        agent_presets: crate::model_catalog::fallback_agent_presets(provider),
    }
}

/// Resolve the newest installed Codex CLI binary. On macOS, ChatGPT.app bundles
/// a newer `codex-cli` binary than older standalone `~/.local/bin/codex`
/// installs; preferring the higher version ensures newly released models (such
/// as GPT-6.2) are visible out of the box without manual override.
pub(crate) fn find_codex_binary() -> Option<std::path::PathBuf> {
    let mut candidates = Vec::new();
    if let Some(path) = crate::command_env::find_executable(ProviderKind::Codex.command()) {
        candidates.push(path);
    }
    #[cfg(target_os = "macos")]
    {
        let mut mac_candidates = vec![std::path::PathBuf::from(
            "/Applications/ChatGPT.app/Contents/Resources/codex-cli/bin/codex",
        )];
        if let Some(home) = dirs::home_dir() {
            mac_candidates
                .push(home.join("Applications/ChatGPT.app/Contents/Resources/codex-cli/bin/codex"));
        }
        for candidate in mac_candidates {
            if candidate.is_file() && !candidates.iter().any(|existing| existing == &candidate) {
                candidates.push(candidate);
            }
        }
    }
    if candidates.len() <= 1 {
        return candidates.into_iter().next();
    }
    candidates
        .into_iter()
        .max_by_key(|candidate| {
            probe_provider_version(candidate)
                .map(|v| parse_version_key(&v))
                .unwrap_or_default()
        })
}

fn parse_version_key(version: &str) -> (u64, u64, u64, u8) {
    let clean = version.trim().trim_start_matches('v');
    let (core, is_stable) = match clean.split_once('-') {
        Some((c, _)) => (c, 0_u8),
        None => (clean, 1_u8),
    };
    let mut parts = core.split('.').map(|p| p.parse::<u64>().unwrap_or(0));
    let major = parts.next().unwrap_or(0);
    let minor = parts.next().unwrap_or(0);
    let patch = parts.next().unwrap_or(0);
    (major, minor, patch, is_stable)
}

/// Detect a provider and hydrate its catalog from the daemon-owned cache.
///
/// This is the fast half of stale-while-revalidate: clients can render the
/// last successful catalog immediately, then request live discovery to replace
/// it. Cache I/O stays in the daemon instead of leaking host filesystem access
/// into desktop or Web clients.
pub fn cached_provider_probe(
    provider: ProviderKind,
    binary_override: Option<&str>,
) -> ProviderProbe {
    let cached = crate::model_catalog::cached_models(provider);
    apply_cached_models(provider_probe(provider, binary_override), cached)
}

fn apply_cached_models(
    mut probe: ProviderProbe,
    cached_models: Option<Vec<ProviderModel>>,
) -> ProviderProbe {
    if probe.provider.supports_model_discovery()
        && let Some(models) = cached_models
    {
        probe.models = models;
    }
    probe
}

pub fn discover_provider_models(mut probe: ProviderProbe) -> ProviderProbe {
    if probe.provider.supports_model_discovery()
        && let Some(path) = probe.path.as_deref()
    {
        let (models, agent_presets) = crate::model_catalog::discover_catalog(probe.provider, path);
        probe.models = models;
        probe.agent_presets = agent_presets;
    }
    probe
}

/// Run `<cli> --version` on the daemon host and extract its first version-like
/// token. Provider CLIs decorate this output differently, so clients receive a
/// normalized value rather than subprocess output.
pub fn probe_provider_version_for(provider: ProviderKind, binary: &Path) -> Option<String> {
    if provider == ProviderKind::Antigravity {
        return crate::agent_profiles::probe_antigravity_version(binary);
    }
    probe_provider_version(binary)
}

pub fn probe_provider_version(binary: &Path) -> Option<String> {
    let mut command = crate::command_env::command(binary);
    let command = command.arg("--version").stdin(std::process::Stdio::null());
    let output = crate::command_env::output(command).ok()?;
    let combined = format!(
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    parse_cli_version(&combined)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cached_catalog_replaces_fallback_before_live_discovery() {
        let probe = ProviderProbe {
            provider: ProviderKind::Codex,
            installed: true,
            path: Some("/usr/bin/codex".into()),
            models: crate::model_catalog::fallback_models(ProviderKind::Codex),
            agent_presets: Vec::new(),
        };
        let cached = vec![ProviderModel::new("cached-model", "Cached model").default()];

        let probe = apply_cached_models(probe, Some(cached));

        assert_eq!(probe.models.len(), 1);
        assert_eq!(probe.models[0].id, "cached-model");
    }
}
