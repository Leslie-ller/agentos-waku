//! Provider backend and driver-event wire translation for `waku-daemon`.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};

use crate::{
    Backend, Command, EventSink, Request, ResponsePayload, WireDriverEvent, WorkspaceOperation,
    WorkspaceResult,
};
use anyhow::{Context as _, anyhow, bail};
use parking_lot::Mutex;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use uuid::Uuid;

use crate::attachments::AttachmentStore;
use crate::computer_use::{ComputerTarget, ComputerUsePhase, ComputerUseState};
use crate::driver::{self, DriverHandle, DriverStartOptions, SessionOptions};
use crate::model::{
    ActivityKind, AgentSession, Checkpoint, CheckpointStatus, DriverEvent, PermissionOption,
    Project, ProviderKind, ProviderResumeCursor, SessionStatus,
};
use crate::persistence::{ComposerDraftStore, PersistedState, StateStore};
use crate::settings::DaemonSettingsStore;
use waku_protocol::provider_session::{ProviderSessionFork, ProviderSessionForkRequest};

/// How many fully hydrated transcripts the daemon keeps resident.
///
/// Hydration is a cache: consumers reload a released session from the store on
/// demand. Without a cap, a daemon that lives for days adopts the transcript
/// of every session its clients have touched — SaveTaskState pushes, hydrate
/// requests, forks, checkpoints — and resident memory grows without bound.
const RESIDENT_TRANSCRIPT_WINDOW: usize = 24;

/// Releases resident transcripts beyond the recency window after a save.
/// `pinned` names sessions with live runtimes; dirty sessions are skipped
/// inside [`PersistedState::trim_idle_transcripts`] because they hold unsaved
/// work.
fn trim_resident_transcripts(state: &mut PersistedState, pinned: &HashSet<Uuid>) {
    state.trim_idle_transcripts(pinned, RESIDENT_TRANSCRIPT_WINDOW);
}

pub struct WakuBackend {
    sessions: Mutex<HashMap<Uuid, (Uuid, ProviderKind, DriverHandle, Arc<AtomicBool>)>>,
    shutting_down: AtomicBool,
    scale_trackers: Mutex<HashMap<Uuid, Arc<Mutex<crate::mcp_companion::SessionScaleTracker>>>>,
    auto_advisor: Arc<crate::mcp_companion::AutoAdvisorPool>,
    terminals: Mutex<HashMap<Uuid, (Uuid, crate::terminal::DaemonTerminal)>>,
    #[cfg(all(test, unix))]
    terminal_shell: Option<alacritty_terminal::tty::Shell>,
    settings: DaemonSettingsStore,
    task_store: StateStore,
    task_state: Mutex<PersistedState>,
    removed_session_ids: Mutex<HashSet<Uuid>>,
    composer_drafts: ComposerDraftStore,
    attachments: AttachmentStore,
    usage_scan_cache: Mutex<crate::usage_history::ScanCache>,
    checkpoint_capture_locks: Mutex<HashMap<(PathBuf, Uuid, usize), Arc<Mutex<()>>>>,
    prompt_idempotency: crate::remote_mcp::PromptIdempotencyCache,
    usage_rates_dir: std::path::PathBuf,
    default_cwd: std::path::PathBuf,
}

impl WakuBackend {
    pub fn new(settings: DaemonSettingsStore, task_store: StateStore) -> anyhow::Result<Self> {
        let mut task_state = task_store
            .load()
            .context("could not load Waku task database")?;
        migrate_projectless_state(&task_store, &mut task_state)?;
        let composer_drafts = ComposerDraftStore::for_state_path(task_store.path());
        let attachments = AttachmentStore::new(
            task_store
                .path()
                .parent()
                .unwrap_or_else(|| std::path::Path::new("."))
                .join("attachments"),
        );
        let usage_rates_dir = task_store
            .path()
            .parent()
            .unwrap_or_else(|| std::path::Path::new("."))
            .to_owned();
        Ok(Self {
            sessions: Mutex::new(HashMap::new()),
            shutting_down: AtomicBool::new(false),
            scale_trackers: Mutex::new(HashMap::new()),
            auto_advisor: crate::mcp_companion::AutoAdvisorPool::new(),
            terminals: Mutex::new(HashMap::new()),
            #[cfg(all(test, unix))]
            terminal_shell: None,
            settings,
            task_store,
            task_state: Mutex::new(task_state),
            removed_session_ids: Mutex::new(HashSet::new()),
            composer_drafts,
            attachments,
            usage_scan_cache: Mutex::new(HashMap::new()),
            checkpoint_capture_locks: Mutex::new(HashMap::new()),
            prompt_idempotency: crate::remote_mcp::PromptIdempotencyCache::default(),
            usage_rates_dir,
            default_cwd: std::env::current_dir().unwrap_or_else(|_| std::path::PathBuf::from(".")),
        })
    }

    #[cfg(all(test, unix))]
    pub(crate) fn with_terminal_shell(mut self, shell: alacritty_terminal::tty::Shell) -> Self {
        self.terminal_shell = Some(shell);
        self
    }

    fn open_terminal(
        &self,
        cwd: &Path,
        cols: u16,
        rows: u16,
        events: EventSink,
    ) -> anyhow::Result<crate::terminal::DaemonTerminal> {
        #[cfg(all(test, unix))]
        if let Some(shell) = &self.terminal_shell {
            return crate::terminal::DaemonTerminal::open_with_shell(
                cwd,
                cols,
                rows,
                events,
                shell.clone(),
            );
        }
        ensure_shell_environment();
        crate::terminal::DaemonTerminal::open(cwd, cols, rows, events)
    }

    /// Capture and persist one ending checkpoint exactly once per daemon.
    /// Desktop and Web may observe the same turn completion concurrently; a
    /// per-turn lock prevents both clients from running the expensive Git
    /// snapshot while leaving unrelated tasks independent.
    fn capture_turn_checkpoint(
        &self,
        cwd: PathBuf,
        session_id: Uuid,
        turn_count: usize,
    ) -> anyhow::Result<Checkpoint> {
        let key = (cwd.clone(), session_id, turn_count);
        let capture_lock = self
            .checkpoint_capture_locks
            .lock()
            .entry(key)
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone();
        let _capture = capture_lock.lock();

        {
            let mut state = self.task_state.lock();
            if let Some(index) = state
                .sessions
                .iter()
                .position(|session| session.id == session_id)
            {
                self.task_store.hydrate(&mut state.sessions[index])?;
                if let Some(checkpoint) = state.sessions[index]
                    .turns
                    .iter()
                    .find(|turn| turn.turn_count == turn_count)
                    .and_then(|turn| turn.checkpoint.as_ref())
                    .filter(|checkpoint| {
                        matches!(
                            checkpoint.status,
                            CheckpointStatus::Ready | CheckpointStatus::Unavailable
                        )
                    })
                {
                    return Ok(checkpoint.clone());
                }
            }
        }

        let checkpoint = crate::checkpoint::capture_turn(&cwd, session_id, turn_count)?;
        let mut state = self.task_state.lock();
        if let Some(index) = state
            .sessions
            .iter()
            .position(|session| session.id == session_id)
        {
            self.task_store.hydrate(&mut state.sessions[index])?;
            if let Some(turn) = state.sessions[index]
                .turns
                .iter_mut()
                .find(|turn| turn.turn_count == turn_count)
            {
                turn.checkpoint = Some(checkpoint.clone());
                state.mark_session_dirty(session_id);
                self.task_store.save(&mut state)?;
            }
        }
        Ok(checkpoint)
    }
}

/// Storage-layout migrations belong to the daemon because both the database
/// rows and the directories name paths on its host. Persist after each move
/// so a later failure cannot leave an earlier project pointing at its old
/// location in SQLite.
fn migrate_projectless_state(
    task_store: &StateStore,
    task_state: &mut PersistedState,
) -> anyhow::Result<()> {
    let indices = task_state
        .projects
        .iter()
        .enumerate()
        .filter_map(|(index, project)| {
            crate::projectless::needs_migration(&project.path).then_some(index)
        })
        .collect::<Vec<_>>();
    for index in indices {
        let old_path = task_state.projects[index].path.clone();
        let workspace = crate::projectless::migrate_workspace(&old_path).with_context(|| {
            format!(
                "could not move projectless workspace {} under ~/.waku/projects",
                old_path.display()
            )
        })?;
        task_state.projects[index].name = crate::model::Project::PROJECTLESS_NAME.to_owned();
        task_state.projects[index].path = workspace.cwd;
        task_store
            .save(task_state)
            .context("could not persist migrated projectless workspace")?;
    }
    Ok(())
}

impl Backend for WakuBackend {
    fn handle(&self, request: Request, events: EventSink) -> anyhow::Result<ResponsePayload> {
        let session_id = request.session_id;
        let runtime_id = request.runtime_id;
        match request.command {
            Command::AttachSession => {
                let sessions = self.sessions.lock();
                let Some((runtime_id, _, driver, _)) = sessions.get(&session_id) else {
                    return Ok(ResponsePayload::SessionRuntime {
                        runtime_id: None,
                        supports_steer: false,
                    });
                };
                Ok(ResponsePayload::SessionRuntime {
                    runtime_id: Some(*runtime_id),
                    supports_steer: driver.supports_steer(),
                })
            }
            Command::GetSettings => Ok(ResponsePayload::Settings {
                settings: self.settings.get(),
            }),
            Command::UpdateSettings { settings } => {
                self.settings.replace(settings)?;
                Ok(ResponsePayload::Ack)
            }
            Command::ProbeProvider {
                provider,
                binary_override,
                discover_models,
                probe_version,
            } => {
                ensure_shell_environment();
                let mut probe = match binary_override.as_deref() {
                    override_value if discover_models || probe_version => {
                        crate::model::provider_probe(provider, override_value)
                    }
                    override_value => crate::model::cached_provider_probe(provider, override_value),
                };
                let version = probe_version
                    .then(|| {
                        probe
                            .path
                            .as_deref()
                            .and_then(|path| crate::model::probe_provider_version_for(provider, path))
                    })
                    .flatten();
                if discover_models {
                    probe = crate::model::discover_provider_models(probe);
                }
                Ok(ResponsePayload::ProviderProbe { probe, version })
            }
            Command::FetchPlanUsage {
                provider,
                binary_override,
                cli_version,
            } => {
                let usage = match provider {
                    crate::model::ProviderKind::Claude => Some(
                        crate::usage::fetch_claude_plan_usage(cli_version.as_deref())?,
                    ),
                    crate::model::ProviderKind::Codex => {
                        Some(crate::usage::fetch_codex_plan_usage()?)
                    }
                    crate::model::ProviderKind::OpenCode => {
                        crate::usage::fetch_opencode_go_plan_usage()?
                    }
                    crate::model::ProviderKind::Grok => {
                        ensure_shell_environment();
                        let probe = match binary_override.as_deref() {
                            override_value => {
                                crate::model::provider_probe(provider, override_value)
                            }
                        };
                        let binary = probe.path.ok_or_else(|| anyhow!("grok is not installed"))?;
                        Some(crate::usage::fetch_grok_plan_usage(&binary)?)
                    }
                    _ => bail!("provider has no plan usage fetcher"),
                };
                Ok(ResponsePayload::PlanUsage { usage })
            }
            Command::ProbeComputerPermissions { prompt } => {
                Ok(ResponsePayload::ComputerPermissions {
                    permissions: crate::computer_use::probe_permissions(prompt)?,
                })
            }
            Command::LoadUsageHistory {
                window,
                project_roots,
            } => {
                let rates = crate::usage_history::load_rate_table(&self.usage_rates_dir);
                let history = crate::usage_history::scan(
                    &mut self.usage_scan_cache.lock(),
                    &rates,
                    window,
                    &project_roots,
                );
                Ok(ResponsePayload::UsageHistory { history })
            }
            Command::LoadSkills { projects } => {
                let locations = crate::skills::skill_locations(&projects);
                Ok(ResponsePayload::SkillsCatalog {
                    catalog: crate::skills::scan_skills(&locations),
                })
            }
            Command::SetSkillsEnabled { dirs, enabled } => {
                for dir in dirs {
                    crate::skills::set_skill_enabled(&dir, enabled)
                        .map_err(|error| anyhow!(error))?;
                }
                Ok(ResponsePayload::Ack)
            }
            Command::TrashSkills { dirs } => {
                crate::skills::trash_skills(&dirs).map_err(|error| anyhow!(error))?;
                Ok(ResponsePayload::Ack)
            }
            Command::LoadMemoryLibrary {
                project_path,
                query,
            } => {
                let library = crate::mcp_companion::load_memory_library(
                    self.task_store.path(),
                    project_path.as_deref(),
                    query.as_deref(),
                )?;
                Ok(ResponsePayload::MemoryLibrary { library })
            }
            Command::UpdateMemoryKnowledgeStatus {
                knowledge_id,
                status,
            } => {
                crate::mcp_companion::update_memory_knowledge_status(
                    self.task_store.path(),
                    &knowledge_id,
                    &status,
                )?;
                Ok(ResponsePayload::Ack)
            }
            Command::DeleteMemoryEntry {
                kind,
                id,
                note_id,
                scope,
            } => {
                crate::mcp_companion::delete_memory_entry(
                    self.task_store.path(),
                    kind,
                    &id,
                    note_id.as_deref(),
                    scope.as_deref(),
                )?;
                Ok(ResponsePayload::Ack)
            }
            Command::LoadTaskState => {
                let state = self.task_state.lock();
                Ok(ResponsePayload::TaskState {
                    projects: state.projects.clone(),
                    sessions: state
                        .sessions
                        .iter()
                        .map(AgentSession::list_projection)
                        .collect(),
                    default_cwd: self.default_cwd.clone(),
                    projectless_root: crate::projectless::workspace_root(),
                })
            }
            Command::SaveTaskState {
                projects,
                live_session_ids: _,
                sessions,
            } => {
                let active_runtimes = self
                    .sessions
                    .lock()
                    .iter()
                    .map(|(session_id, (runtime_id, _, _, _))| (*session_id, *runtime_id))
                    .collect::<HashMap<_, _>>();
                let mut state = self.task_state.lock();
                let removed_session_ids = self.removed_session_ids.lock();
                for project in projects {
                    if let Some(existing) = state
                        .projects
                        .iter_mut()
                        .find(|existing| existing.id == project.id)
                    {
                        *existing = project;
                    } else {
                        state.projects.push(project);
                    }
                }
                let sessions = sessions
                    .into_iter()
                    .filter(|session| !removed_session_ids.contains(&session.id))
                    .collect::<Vec<_>>();
                drop(removed_session_ids);
                let saved_ids = sessions
                    .iter()
                    .map(|session| session.id)
                    .collect::<Vec<_>>();
                for mut session in sessions {
                    if let Some(existing) = state
                        .sessions
                        .iter_mut()
                        .find(|existing| existing.id == session.id)
                    {
                        let incoming_lacks_transcript = session.messages.is_empty()
                            && session.transcript_blocks.is_empty()
                            && session.turns.is_empty();
                        let existing_owns_transcript = !existing.detail_loaded
                            || !existing.messages.is_empty()
                            || !existing.transcript_blocks.is_empty()
                            || !existing.turns.is_empty();
                        if (incoming_lacks_transcript && existing_owns_transcript)
                            || session_projection_precedes(
                                existing,
                                &session,
                                active_runtimes.get(&session.id).copied(),
                            )
                        {
                            merge_stale_session_metadata(existing, session);
                        } else {
                            preserve_daemon_checkpoints(existing, &mut session);
                            preserve_daemon_handoff_consumption(existing, &mut session);
                            *existing = session;
                        }
                    } else {
                        state.sessions.push(session);
                    }
                }
                let used_project_ids = state
                    .sessions
                    .iter()
                    .map(|session| session.project_id)
                    .collect::<std::collections::HashSet<_>>();
                state.projects.retain(|project| {
                    !project.is_projectless() || used_project_ids.contains(&project.id)
                });
                for session_id in &saved_ids {
                    state.mark_session_dirty(*session_id);
                }
                self.task_store.save(&mut state)?;
                let sessions = saved_ids
                    .into_iter()
                    .filter_map(|session_id| {
                        state
                            .sessions
                            .iter()
                            .find(|session| session.id == session_id)
                            .cloned()
                    })
                    .collect();
                // The save above can adopt full transcripts for every session
                // the client touched. Keep only the recent window resident;
                // the echoed clones above still carry the saved detail.
                trim_resident_transcripts(&mut state, &active_runtimes.keys().copied().collect());
                Ok(ResponsePayload::TaskStateSaved { sessions })
            }
            Command::RemoveSession => {
                {
                    let mut state = self.task_state.lock();
                    self.removed_session_ids.lock().insert(session_id);
                    let project_id = state
                        .sessions
                        .iter()
                        .find(|session| session.id == session_id)
                        .map(|session| session.project_id);
                    state.sessions.retain(|session| session.id != session_id);
                    if let Some(project_id) = project_id {
                        let remove_project = state
                            .projects
                            .iter()
                            .find(|project| project.id == project_id)
                            .is_some_and(Project::is_projectless)
                            && !state
                                .sessions
                                .iter()
                                .any(|session| session.project_id == project_id);
                        if remove_project {
                            state.projects.retain(|project| project.id != project_id);
                        }
                    }
                    self.task_store.save(&mut state)?;
                }
                let removed = self.sessions.lock().remove(&session_id);
                drop(removed);
                self.scale_trackers.lock().remove(&session_id);
                self.auto_advisor.cancel_session(session_id);
                let _ = crate::mcp_companion::dismiss_pending_advisor_reviews(
                    self.task_store.path(),
                    session_id,
                );
                Ok(ResponsePayload::Ack)
            }
            Command::HydrateSession { session_id } => {
                // Live runtimes stay resident; everything else is trimmed to
                // the recency window once the response is built.
                let pinned = self.sessions.lock().keys().copied().collect();
                let mut state = self.task_state.lock();
                let session = if let Some(session) = state
                    .sessions
                    .iter_mut()
                    .find(|session| session.id == session_id)
                {
                    self.task_store.hydrate(session)?;
                    let mut outbound = session.clone();
                    outbound.compact_for_wire_budget(
                        waku_protocol::HYDRATE_WIRE_SOFT_BUDGET_BYTES,
                    );
                    Some(outbound)
                } else {
                    None
                };
                trim_resident_transcripts(&mut state, &pinned);
                Ok(ResponsePayload::Session { session })
            }
            Command::SearchSessionMessages { query, limit } => {
                let matches = self.task_store.session_message_search(query, limit)()?;
                Ok(ResponsePayload::SessionMessageMatches { matches })
            }
            Command::ListProviderSessions { provider, limit } => {
                const MAX_PROVIDER_SESSIONS: usize = 500;
                let limit = limit.min(MAX_PROVIDER_SESSIONS);
                if limit == 0 {
                    return Ok(ResponsePayload::ProviderSessions {
                        sessions: Vec::new(),
                    });
                }
                ensure_shell_environment();
                let settings = self.settings.get();
                if settings.disabled_providers.contains(&provider) {
                    return Ok(ResponsePayload::ProviderSessions {
                        sessions: Vec::new(),
                    });
                }
                let binary_override = settings
                    .provider_binary_overrides
                    .get(&provider)
                    .map(String::as_str);
                let Some(binary) = crate::model::provider_probe(provider, binary_override).path
                else {
                    return Ok(ResponsePayload::ProviderSessions {
                        sessions: Vec::new(),
                    });
                };
                // Discovery is deliberately provider-scoped. Opening Resume
                // must not start every installed agent CLI, and another
                // provider is queried only after the user explicitly picks it.
                let mut sessions = match provider {
                    ProviderKind::Amp => {
                        crate::amp_session::list_provider_sessions(&binary, limit)?
                    }
                    ProviderKind::Claude => crate::claude_session::list_provider_sessions(limit)?,
                    ProviderKind::Codex => {
                        crate::codex_session::list_provider_sessions(&binary, limit)?
                    }
                    ProviderKind::Antigravity | ProviderKind::Cursor | ProviderKind::Fx => {
                        crate::acp_session::list_provider_sessions(provider, &binary, &[], limit)?
                    }
                    ProviderKind::OpenCode => {
                        crate::opencode_session::list_provider_sessions(&binary, limit)?
                    }
                    ProviderKind::DeepSeek => {
                        crate::deepseek_session::list_provider_sessions(&binary, limit)?
                    }
                    ProviderKind::Grok => crate::grok_session::list_provider_sessions(limit)?,
                    ProviderKind::Kimi => crate::kimi_session::list_provider_sessions(limit)?,
                    ProviderKind::OhMyPi | ProviderKind::Pi => {
                        crate::pi_session::list_provider_sessions(provider, limit)?
                    }
                };
                sessions.sort_by(|a, b| {
                    b.updated_at
                        .cmp(&a.updated_at)
                        .then_with(|| a.title.cmp(&b.title))
                });
                let imported = {
                    let state = self.task_state.lock();
                    state
                        .sessions
                        .iter()
                        .filter_map(|session| session.provider_cursor.as_ref())
                        .map(|cursor| (cursor.provider(), cursor.native_id().to_owned()))
                        .collect::<HashSet<_>>()
                };
                sessions.retain(|session| {
                    !imported.contains(&(session.provider(), session.cursor.native_id().to_owned()))
                });
                sessions.truncate(limit);
                Ok(ResponsePayload::ProviderSessions { sessions })
            }
            Command::LoadProviderSession { cursor, cwd } => {
                // Preserve every native turn shell for exact provider turn
                // numbering, but bound imported display text to recent turns.
                const VISIBLE_TURN_LIMIT: usize = 100;
                let history = match &cursor {
                    ProviderResumeCursor::Amp { thread_id, .. } => {
                        let binary = self.provider_binary(ProviderKind::Amp)?;
                        crate::amp_session::provider_session_history(
                            &binary,
                            &cwd,
                            thread_id,
                            VISIBLE_TURN_LIMIT,
                        )?
                    }
                    ProviderResumeCursor::Claude { session_id, .. } => {
                        self.provider_binary(ProviderKind::Claude)?;
                        crate::claude_session::provider_session_history(
                            session_id,
                            VISIBLE_TURN_LIMIT,
                        )?
                    }
                    ProviderResumeCursor::Codex { thread_id } => {
                        let binary = self.provider_binary(ProviderKind::Codex)?;
                        crate::codex_session::provider_session_history(
                            &binary,
                            thread_id,
                            VISIBLE_TURN_LIMIT,
                        )?
                    }
                    // OpenCode is not an ACP provider: its history comes from
                    // the adopted background service's own export route.
                    ProviderResumeCursor::OpenCode { session_id, .. } => {
                        let binary = self.provider_binary(ProviderKind::OpenCode)?;
                        crate::opencode_session::provider_session_history(
                            &binary,
                            session_id,
                            VISIBLE_TURN_LIMIT,
                        )?
                    }
                    ProviderResumeCursor::Antigravity { session_id }
                    | ProviderResumeCursor::Cursor { session_id, .. }
                    | ProviderResumeCursor::Fx { session_id }
                    | ProviderResumeCursor::Grok { session_id }
                    | ProviderResumeCursor::Kimi { session_id } => {
                        let provider = cursor.provider();
                        let binary = self.provider_binary(provider)?;
                        crate::acp_session::provider_session_history(
                            provider,
                            &binary,
                            &cwd,
                            session_id,
                            VISIBLE_TURN_LIMIT,
                        )?
                    }
                    ProviderResumeCursor::DeepSeek { session_id } => {
                        let binary = self.provider_binary(ProviderKind::DeepSeek)?;
                        crate::deepseek_session::provider_session_history(
                            &binary,
                            session_id,
                            VISIBLE_TURN_LIMIT,
                        )?
                    }
                    ProviderResumeCursor::OhMyPi {
                        session_id,
                        session_file,
                    }
                    | ProviderResumeCursor::Pi {
                        session_id,
                        session_file,
                    } => {
                        self.provider_binary(cursor.provider())?;
                        let session_file = session_file.as_deref().ok_or_else(|| {
                            anyhow!(
                                "{} did not report its native session file",
                                cursor.provider().display_name()
                            )
                        })?;
                        crate::pi_session::provider_session_history(
                            cursor.provider(),
                            session_id,
                            session_file,
                            VISIBLE_TURN_LIMIT,
                        )?
                    }
                };
                Ok(ResponsePayload::ProviderSessionHistory { history })
            }
            Command::LoadComposerDrafts => Ok(ResponsePayload::ComposerDrafts {
                drafts: self.composer_drafts.load()?,
            }),
            Command::SaveComposerDrafts { drafts, generation } => {
                self.composer_drafts.save(drafts, generation)?;
                Ok(ResponsePayload::Ack)
            }
            Command::ApplyComposerDraftChanges { changes } => {
                self.composer_drafts.apply_changes(changes)?;
                Ok(ResponsePayload::Ack)
            }
            Command::StoreBlob { mime_type, bytes } => {
                let reference = self
                    .task_store
                    .blobs()
                    .store_image_bytes(&mime_type, &bytes)?;
                let path = self
                    .task_store
                    .blobs()
                    .path_for(&reference)
                    .ok_or_else(|| anyhow!("stored blob has no daemon path"))?;
                Ok(ResponsePayload::BlobStored { reference, path })
            }
            Command::ImportAttachment { name, upload } => Ok(ResponsePayload::AttachmentStored {
                attachment: self.attachments.import(&name, upload)?,
            }),
            Command::ImportPathAttachment { path } => Ok(ResponsePayload::AttachmentStored {
                attachment: self.attachments.import_path(&path)?,
            }),
            Command::ReadBlob { reference } => {
                let path = self
                    .task_store
                    .blobs()
                    .path_for(&reference)
                    .ok_or_else(|| anyhow!("invalid blob reference"))?;
                Ok(ResponsePayload::BlobData {
                    bytes: std::fs::read(path)?,
                })
            }
            Command::ReadAttachment { reference, path } => Ok(ResponsePayload::BlobData {
                bytes: self.attachments.read_file(&reference, &path)?,
            }),
            Command::SweepBlobs => {
                self.task_store.blob_sweep()();
                Ok(ResponsePayload::Ack)
            }
            Command::ForkSessionFromResponse { turn_count } => {
                let (session, checkpoint_warning) =
                    self.fork_session_from_response(session_id, turn_count)?;
                Ok(ResponsePayload::SessionForked {
                    session,
                    checkpoint_warning,
                })
            }
            Command::RewindSessionToMessage { turn_count } => {
                let (session, cleanup_warning) =
                    self.rewind_session_to_message(session_id, turn_count)?;
                Ok(ResponsePayload::SessionRewound {
                    session,
                    cleanup_warning,
                })
            }
            Command::ForkProviderSession { request } => {
                Ok(ResponsePayload::ProviderSessionForked {
                    result: fork_provider_session(request)?,
                })
            }
            Command::Workspace {
                operation:
                    WorkspaceOperation::CaptureTurn {
                        cwd,
                        session_id,
                        turn_count,
                    },
            } => Ok(ResponsePayload::Workspace {
                result: WorkspaceResult::Checkpoint {
                    checkpoint: self.capture_turn_checkpoint(cwd, session_id, turn_count)?,
                },
            }),
            Command::Workspace { operation } => Ok(ResponsePayload::Workspace {
                result: crate::workspace::execute(operation)?,
            }),
            Command::OpenTerminal { cwd, cols, rows } => {
                let terminal = self.open_terminal(&cwd, cols, rows, events)?;
                let previous = self
                    .terminals
                    .lock()
                    .insert(session_id, (runtime_id, terminal));
                drop(previous);
                Ok(ResponsePayload::Ack)
            }
            Command::WriteTerminal { data } => {
                let terminals = self.terminals.lock();
                let (active_runtime_id, terminal) = terminals
                    .get(&session_id)
                    .ok_or_else(|| anyhow!("daemon terminal {session_id} is not running"))?;
                if *active_runtime_id != runtime_id {
                    bail!(
                        "daemon terminal {session_id} belongs to runtime {active_runtime_id}, not {runtime_id}"
                    );
                }
                terminal.write(data)?;
                Ok(ResponsePayload::Ack)
            }
            Command::ResizeTerminal { cols, rows } => {
                let terminals = self.terminals.lock();
                let (active_runtime_id, terminal) = terminals
                    .get(&session_id)
                    .ok_or_else(|| anyhow!("daemon terminal {session_id} is not running"))?;
                if *active_runtime_id != runtime_id {
                    bail!(
                        "daemon terminal {session_id} belongs to runtime {active_runtime_id}, not {runtime_id}"
                    );
                }
                terminal.resize(cols, rows);
                Ok(ResponsePayload::Ack)
            }
            Command::CloseTerminal => {
                let removed = {
                    let mut terminals = self.terminals.lock();
                    if let Some((active_runtime_id, _)) = terminals.get(&session_id) {
                        if *active_runtime_id != runtime_id {
                            bail!(
                                "daemon terminal {session_id} belongs to runtime {active_runtime_id}, not {runtime_id}"
                            );
                        }
                    }
                    terminals.remove(&session_id)
                };
                drop(removed);
                Ok(ResponsePayload::Ack)
            }
            Command::Start { options } => {
                let supports_steer =
                    self.start_session_driver(session_id, runtime_id, options, events)?;
                Ok(ResponsePayload::Started { supports_steer })
            }
            Command::CloseSession => {
                let _ =
                    crate::mcp_companion::cancel_pending_questions(self.task_store.path(), session_id);
                let _ = crate::mcp_companion::pause_active_goal_on_cancel(
                    self.task_store.path(),
                    session_id,
                );
                self.auto_advisor.cancel_session(session_id);
                let _ = crate::mcp_companion::dismiss_pending_advisor_reviews(
                    self.task_store.path(),
                    session_id,
                );
                let removed = {
                    let mut sessions = self.sessions.lock();
                    sessions
                        .get(&session_id)
                        .is_some_and(|(active_runtime_id, _, _, _)| {
                            *active_runtime_id == runtime_id
                        })
                        .then(|| sessions.remove(&session_id))
                        .flatten()
                };
                drop(removed);
                Ok(ResponsePayload::Ack)
            }
            command => {
                let (provider, driver, turn_active) = {
                    let sessions = self.sessions.lock();
                    let (active_runtime_id, active_provider, driver, turn_active) = sessions
                        .get(&session_id)
                        .ok_or_else(|| anyhow!("daemon session {session_id} is not running"))?;
                    if *active_runtime_id != runtime_id {
                        bail!(
                            "daemon session {session_id} belongs to runtime {active_runtime_id}, not {runtime_id}"
                        );
                    }
                    (*active_provider, driver.clone(), turn_active.clone())
                };
                match &command {
                    Command::Cancel => {
                        turn_active.store(false, Ordering::Release);
                        let _ = crate::mcp_companion::cancel_pending_questions(
                            self.task_store.path(),
                            session_id,
                        );
                        if let Ok(Some(paused_goal)) =
                            crate::mcp_companion::pause_active_goal_on_cancel(
                                self.task_store.path(),
                                session_id,
                            )
                            && provider != ProviderKind::Codex
                        {
                            let _ = events
                                .send(event_to_wire(DriverEvent::GoalUpdated(Some(paused_goal)))?);
                        }
                    }
                    Command::RespondUserInput {
                        request_id,
                        answers,
                    } if request_id.starts_with(crate::mcp_companion::AGENTOS_QUESTION_PREFIX) => {
                        if let Some(qid) =
                            request_id.strip_prefix(crate::mcp_companion::AGENTOS_QUESTION_PREFIX)
                        {
                            crate::mcp_companion::resolve_pending_question(
                                self.task_store.path(),
                                qid,
                                answers,
                            )?;
                        }
                        return Ok(ResponsePayload::Ack);
                    }
                    Command::Goal { operation } if provider != ProviderKind::Codex => {
                        let (updated_goal, pursuit_prompt) =
                            crate::mcp_companion::apply_host_goal_operation_with_prompt(
                                self.task_store.path(),
                                session_id,
                                operation.clone(),
                            )?;
                        events.send(event_to_wire(DriverEvent::GoalUpdated(updated_goal))?)?;
                        if let Some(prompt_text) = pursuit_prompt
                            && !turn_active.swap(true, Ordering::AcqRel)
                        {
                            driver.prompt(prompt_text);
                        }
                        return Ok(ResponsePayload::Ack);
                    }
                    _ => {}
                }
                if let Command::Prompt {
                    prompt,
                    turn_id,
                    message_id,
                } = command
                {
                    self.prepare_and_send_prompt(
                        session_id,
                        &driver,
                        &turn_active,
                        &events,
                        prompt,
                        turn_id.unwrap_or_else(Uuid::new_v4),
                        message_id.unwrap_or_else(Uuid::new_v4),
                    )?;
                    return Ok(ResponsePayload::Ack);
                }
                handle_driver_command(&driver, command)
            }
        }
    }

    fn shutdown(&self) {
        self.auto_advisor.shutdown();
        let sessions = {
            let mut sessions = self.sessions.lock();
            self.shutting_down.store(true, Ordering::Release);
            std::mem::take(&mut *sessions)
        };
        drop(sessions);
        let terminals = std::mem::take(&mut *self.terminals.lock());
        drop(terminals);
    }

    fn remote_mcp(&self) -> Option<&dyn crate::remote_mcp::RemoteMcpBackend> {
        Some(self)
    }
}

fn session_projection_precedes(
    existing: &AgentSession,
    incoming: &AgentSession,
    active_runtime_id: Option<Uuid>,
) -> bool {
    let existing_cursor = existing.runtime_event_cursor;
    let incoming_cursor = incoming.runtime_event_cursor;
    if let Some(active_runtime_id) = active_runtime_id {
        let existing_is_active =
            existing_cursor.is_some_and(|cursor| cursor.runtime_id == active_runtime_id);
        let incoming_is_active =
            incoming_cursor.is_some_and(|cursor| cursor.runtime_id == active_runtime_id);
        if existing_is_active != incoming_is_active {
            return existing_is_active;
        }
    }
    match (existing_cursor, incoming_cursor) {
        (Some(existing), Some(incoming))
            if existing.runtime_id == incoming.runtime_id && existing.epoch == incoming.epoch =>
        {
            incoming.sequence < existing.sequence
        }
        (Some(_), None)
            if existing.status.is_busy()
                || incoming.turns.len() < existing.turns.len()
                || (incoming.turns.len() == existing.turns.len()
                    && incoming.messages.len() < existing.messages.len()) =>
        {
            true
        }
        (Some(_), Some(_))
            if incoming.turns.len() < existing.turns.len()
                || (incoming.turns.len() == existing.turns.len()
                    && incoming.messages.len() < existing.messages.len()) =>
        {
            true
        }
        _ => incoming.updated_at < existing.updated_at,
    }
}

fn merge_stale_session_metadata(existing: &mut AgentSession, incoming: AgentSession) {
    if incoming.updated_at >= existing.updated_at {
        existing.title = incoming.title;
        existing.project_id = incoming.project_id;
        existing.workspace = incoming.workspace;
        let provider_changed = existing.provider != incoming.provider;
        existing.provider = incoming.provider;
        existing.model = incoming.model;
        existing.runtime_mode = incoming.runtime_mode;
        existing.reasoning_effort = incoming.reasoning_effort;
        existing.service_tier = incoming.service_tier;
        existing.context_window = incoming.context_window;
        existing.agent_preset = incoming.agent_preset;
        if !incoming.parked_cursors.is_empty() {
            existing.parked_cursors = incoming.parked_cursors;
        }
        if provider_changed && incoming.pending_handoff_seed.is_some() {
            existing.pending_handoff_seed = incoming.pending_handoff_seed;
        }
        existing.updated_at = incoming.updated_at;
        existing.last_reply_at = incoming.last_reply_at.or(existing.last_reply_at);
    }
    for queued in incoming.queued_messages {
        if !existing
            .queued_messages
            .iter()
            .any(|candidate| candidate.id == queued.id)
        {
            existing.queued_messages.push(queued);
        }
    }
}

/// Ending checkpoints are produced and stored by the daemon. A second client
/// may still save a projection created just before capture completed; never
/// let that stale projection erase the canonical Git snapshot.
fn preserve_daemon_checkpoints(existing: &AgentSession, incoming: &mut AgentSession) {
    for turn in &mut incoming.turns {
        let Some(checkpoint) = existing
            .turns
            .iter()
            .find(|candidate| candidate.turn_count == turn.turn_count)
            .and_then(|candidate| candidate.checkpoint.as_ref())
            .filter(|checkpoint| {
                matches!(
                    checkpoint.status,
                    CheckpointStatus::Ready | CheckpointStatus::Unavailable
                )
            })
        else {
            continue;
        };
        turn.checkpoint = Some(checkpoint.clone());
    }
}

/// `Command::Prompt` consumes `pending_handoff_seed` on the daemon, while
/// connected clients may still hold the pre-prompt snapshot and save it back
/// milliseconds later. Once the daemon has cleared the seed for the current
/// provider, never let a same-provider client save resurrect it.
fn preserve_daemon_handoff_consumption(existing: &AgentSession, incoming: &mut AgentSession) {
    if existing.pending_handoff_seed.is_none()
        && existing.provider == incoming.provider
        && incoming.pending_handoff_seed.is_some()
    {
        incoming.pending_handoff_seed = None;
    }
}

impl WakuBackend {
    /// Fork a response using only daemon-host state.
    ///
    /// A browser must never reconstruct or persist this operation itself:
    /// provider-native sessions, checkpoint refs, and the task database all
    /// belong to the daemon and may be on another machine.
    fn fork_session_from_response(
        &self,
        session_id: Uuid,
        turn_count: usize,
    ) -> anyhow::Result<(AgentSession, Option<String>)> {
        let (source, cwd, fork_title) = {
            let mut state = self.task_state.lock();
            let source_index = state
                .sessions
                .iter()
                .position(|session| session.id == session_id)
                .ok_or_else(|| anyhow!("the source task is unavailable"))?;
            self.task_store
                .hydrate(&mut state.sessions[source_index])
                .context("could not load the source task")?;
            let source = state.sessions[source_index].clone();
            let project = state
                .projects
                .iter()
                .find(|project| project.id == source.project_id)
                .ok_or_else(|| anyhow!("the source task project is unavailable"))?;
            let cwd = source.workspace.path().unwrap_or(&project.path).to_owned();
            let fork_title = next_response_fork_title(
                source.display_title(),
                state
                    .sessions
                    .iter()
                    .filter(|session| session.project_id == source.project_id)
                    .map(AgentSession::display_title),
            );
            (source, cwd, fork_title)
        };

        validate_response_fork(&source, turn_count)?;
        let provider_turn_count = source
            .turns
            .iter()
            .take(turn_count)
            .filter(|turn| turn.provider_turn_started)
            .count();
        let turns_to_remove = source.provider_turns_after(turn_count);
        let (provider_cursor, message_ids) = self.fork_provider_response(
            &source,
            &cwd,
            &fork_title,
            turn_count,
            provider_turn_count,
            turns_to_remove,
        )?;
        let mut forked = source
            .fork_through_turn(turn_count, provider_cursor, &fork_title)
            .ok_or_else(|| anyhow!("the selected response cannot be copied"))?;
        if !message_ids.is_empty() {
            for turn in &mut forked.turns {
                if let Some(message_id) = turn.provider_resume_at.as_mut()
                    && let Some(remapped) = message_ids.get(message_id)
                {
                    *message_id = remapped.clone();
                }
            }
        }

        let fork_id = forked.id;
        for turn in &mut forked.turns {
            if let Some(checkpoint) = turn.checkpoint.as_mut() {
                checkpoint.git_ref =
                    crate::checkpoint::checkpoint_ref(fork_id, checkpoint.turn_count);
            }
        }
        let checkpoint_warning =
            crate::checkpoint::copy_session_refs(&cwd, source.id, fork_id, turn_count)
                .err()
                .map(|error| error.to_string());

        let pinned = self.sessions.lock().keys().copied().collect();
        let mut state = self.task_state.lock();
        state.push_session(forked.clone());
        if let Err(error) = self.task_store.save(&mut state) {
            state.sessions.retain(|session| session.id != fork_id);
            let _ = crate::checkpoint::delete_all_session_refs(&cwd, fork_id);
            return Err(error).context("could not save the forked task");
        }
        trim_resident_transcripts(&mut state, &pinned);
        Ok((forked, checkpoint_warning))
    }

    /// Restore the daemon-host worktree, provider conversation, and stored
    /// transcript to immediately before one user turn.
    fn rewind_session_to_message(
        &self,
        session_id: Uuid,
        turn_count: usize,
    ) -> anyhow::Result<(AgentSession, Option<String>)> {
        let (source, cwd) = {
            let mut state = self.task_state.lock();
            let source_index = state
                .sessions
                .iter()
                .position(|session| session.id == session_id)
                .ok_or_else(|| anyhow!("the task is unavailable"))?;
            self.task_store
                .hydrate(&mut state.sessions[source_index])
                .context("could not load the task")?;
            let source = state.sessions[source_index].clone();
            let project = state
                .projects
                .iter()
                .find(|project| project.id == source.project_id)
                .ok_or_else(|| anyhow!("the task project is unavailable"))?;
            let cwd = source.workspace.path().unwrap_or(&project.path).to_owned();
            (source, cwd)
        };
        validate_message_rewind(&source, turn_count)?;

        // Resolve the executable before touching the worktree. Even native
        // transcript operations are immediately followed by a replacement
        // prompt, so accepting a rewind that cannot resume would strand the
        // user at a provider state the UI cannot continue.
        let binary = self.provider_binary(source.provider)?;
        let retained_turn_count = turn_count.saturating_sub(1);
        let previous_turn_count = source.turns.len();
        let rollback_turns = source.provider_turns_after(retained_turn_count);
        let provider_turn_count = source
            .turns
            .iter()
            .take(retained_turn_count)
            .filter(|turn| turn.provider_turn_started)
            .count();
        let provider_resume_at = retained_turn_count
            .checked_sub(1)
            .and_then(|index| source.turns.get(index))
            .and_then(|turn| turn.provider_resume_at.clone());

        let turn_start_ref = crate::checkpoint::turn_start_ref(session_id, turn_count);
        let retained_ref = crate::checkpoint::checkpoint_ref(session_id, retained_turn_count);
        let restore_ref = if crate::checkpoint::has_ref(&cwd, &turn_start_ref) {
            turn_start_ref
        } else {
            retained_ref
        };
        if !crate::checkpoint::has_ref(&cwd, &restore_ref) {
            bail!("the checkpoint before this message is unavailable");
        }

        let safety_ref = format!("refs/waku/revert-backup-{session_id}-{}", Uuid::new_v4());
        crate::checkpoint::capture_ref(&cwd, &safety_ref)
            .context("could not create a rewind safety snapshot")?;
        if let Err(error) = crate::checkpoint::restore_ref(&cwd, &restore_ref) {
            return Err(restore_rewind_safety(
                &cwd,
                &safety_ref,
                "could not restore the selected checkpoint",
                error,
            ));
        }

        let provider_rewind = self.rewind_provider_response(
            &source,
            &cwd,
            &binary,
            retained_turn_count,
            rollback_turns,
            provider_turn_count,
            provider_resume_at,
        );
        let (provider_cursor, message_ids, reset_native_session) = match provider_rewind {
            Ok(result) => result,
            Err(error) => {
                return Err(restore_rewind_safety(
                    &cwd,
                    &safety_ref,
                    "the provider rejected the rewind",
                    error,
                ));
            }
        };

        let _ = crate::checkpoint::delete_ref(&cwd, &safety_ref);
        let cleanup_warning = crate::checkpoint::delete_turn_refs_after(
            &cwd,
            session_id,
            retained_turn_count,
            previous_turn_count,
        )
        .err()
        .map(|error| error.to_string());

        // Every provider resumes from the newly stored cursor on the next
        // prompt. Dropping a resident source driver also prevents its late
        // events from racing the rewound transcript.
        let removed = self.sessions.lock().remove(&session_id);
        drop(removed);

        let mut rewound = source.clone();
        if !message_ids.is_empty() {
            for turn in rewound.turns.iter_mut().take(retained_turn_count) {
                if let Some(remapped) = turn
                    .provider_resume_at
                    .as_ref()
                    .and_then(|message_id| message_ids.get(message_id))
                    .cloned()
                {
                    turn.provider_resume_at = Some(remapped);
                }
            }
        }
        if reset_native_session {
            rewound.provider_cursor = None;
        } else if let Some(cursor) = provider_cursor {
            rewound.provider_cursor = Some(cursor);
        }
        rewound.truncate_after_turn(retained_turn_count);
        rewound.status = SessionStatus::Idle;

        let pinned = self.sessions.lock().keys().copied().collect();
        let mut state = self.task_state.lock();
        let existing = state
            .sessions
            .iter_mut()
            .find(|session| session.id == session_id)
            .ok_or_else(|| anyhow!("the task was removed while it was being rewound"))?;
        *existing = rewound.clone();
        state.mark_session_dirty(session_id);
        self.task_store
            .save(&mut state)
            .context("could not save the rewound task")?;
        trim_resident_transcripts(&mut state, &pinned);
        Ok((rewound, cleanup_warning))
    }

    fn fork_provider_response(
        &self,
        source: &AgentSession,
        cwd: &Path,
        fork_title: &str,
        turn_count: usize,
        provider_turn_count: usize,
        turns_to_remove: usize,
    ) -> anyhow::Result<(ProviderResumeCursor, HashMap<String, String>)> {
        match source.provider {
            ProviderKind::Claude => {
                let Some(ProviderResumeCursor::Claude { session_id, .. }) =
                    source.provider_cursor.as_ref()
                else {
                    bail!("Claude's native session is unavailable");
                };
                let resume_at = source
                    .turns
                    .get(turn_count.saturating_sub(1))
                    .and_then(|turn| turn.provider_resume_at.clone());
                let fork = fork_provider_session(ProviderSessionForkRequest::Claude {
                    session_id: session_id.clone(),
                    resume_at,
                    turn_count: provider_turn_count,
                    title: fork_title.to_owned(),
                })?;
                Ok((fork.cursor, fork.message_ids))
            }
            ProviderKind::Codex
            | ProviderKind::DeepSeek
            | ProviderKind::OhMyPi
            | ProviderKind::Pi => Ok((
                self.fork_response_with_driver(source, cwd, turns_to_remove)?,
                HashMap::new(),
            )),
            ProviderKind::Cursor => {
                let fork = fork_provider_session(ProviderSessionForkRequest::Cursor {
                    source: source.clone(),
                    turn_count,
                })?;
                Ok((fork.cursor, HashMap::new()))
            }
            ProviderKind::Amp => {
                let Some(ProviderResumeCursor::Amp {
                    thread_id,
                    fork_context,
                }) = source.provider_cursor.as_ref()
                else {
                    bail!("Amp's native thread is unavailable");
                };
                let fork = fork_provider_session(ProviderSessionForkRequest::Amp {
                    binary: self.provider_binary(ProviderKind::Amp)?,
                    cwd: cwd.to_owned(),
                    thread_id: thread_id.clone(),
                    fork_context: fork_context.clone(),
                    turn_count: provider_turn_count,
                })?;
                Ok((fork.cursor, HashMap::new()))
            }
            ProviderKind::OpenCode => {
                let Some(ProviderResumeCursor::OpenCode { session_id, .. }) =
                    source.provider_cursor.as_ref()
                else {
                    bail!("OpenCode's native session is unavailable");
                };
                // No cwd: a session carries its own `location`, so there is no
                // server working directory to fork against.
                let fork = fork_provider_session(ProviderSessionForkRequest::OpenCode {
                    binary: self.provider_binary(ProviderKind::OpenCode)?,
                    session_id: session_id.clone(),
                    turn_count: provider_turn_count,
                })?;
                Ok((fork.cursor, HashMap::new()))
            }
            ProviderKind::Grok => {
                let Some(ProviderResumeCursor::Grok { session_id }) =
                    source.provider_cursor.as_ref()
                else {
                    bail!("Grok Build's native session is unavailable");
                };
                let fork = fork_provider_session(ProviderSessionForkRequest::Grok {
                    binary: self.provider_binary(ProviderKind::Grok)?,
                    cwd: cwd.to_owned(),
                    session_id: session_id.clone(),
                    turn_count: provider_turn_count,
                })?;
                Ok((fork.cursor, HashMap::new()))
            }
            // Unreachable through the UI, which hides branching for providers
            // that answer `supports_conversation_fork` with false.
            ProviderKind::Antigravity | ProviderKind::Fx | ProviderKind::Kimi => {
                bail!(
                    "{} cannot branch a conversation at a turn",
                    source.provider.display_name()
                )
            }
        }
    }

    fn fork_response_with_driver(
        &self,
        source: &AgentSession,
        cwd: &Path,
        turns_to_remove: usize,
    ) -> anyhow::Result<ProviderResumeCursor> {
        if let Some(driver) = self
            .sessions
            .lock()
            .get(&source.id)
            .map(|(_, _, driver, _)| driver.clone())
        {
            return driver.fork(turns_to_remove);
        }

        match source.provider {
            ProviderKind::Codex
                if !matches!(
                    source.provider_cursor.as_ref(),
                    Some(ProviderResumeCursor::Codex { .. })
                ) =>
            {
                bail!("Codex's native thread is unavailable");
            }
            ProviderKind::DeepSeek
                if !matches!(
                    source.provider_cursor.as_ref(),
                    Some(ProviderResumeCursor::DeepSeek { .. })
                ) =>
            {
                bail!("DeepSeek Harness's native session is unavailable");
            }
            ProviderKind::Pi
                if !matches!(
                    source.provider_cursor.as_ref(),
                    Some(ProviderResumeCursor::Pi {
                        session_file: Some(_),
                        ..
                    })
                ) =>
            {
                bail!("Pi's native session file is unavailable");
            }
            ProviderKind::OhMyPi
                if !matches!(
                    source.provider_cursor.as_ref(),
                    Some(ProviderResumeCursor::OhMyPi {
                        session_file: Some(_),
                        ..
                    })
                ) =>
            {
                bail!("Oh My Pi's native session file is unavailable");
            }
            _ => {}
        }

        let (wake, _wake_events) = smol::channel::bounded(1);
        let (event_sender, _event_receiver) = driver::event_channel(wake);
        let driver = driver::start_local(
            source.provider,
            DriverStartOptions {
                binary: self.provider_binary(source.provider)?,
                cwd: cwd.to_owned(),
                mode: source.runtime_mode,
                model: source.model.clone(),
                reasoning_effort: source.reasoning_effort.clone(),
                service_tier: source.service_tier.clone(),
                context_window: source.context_window.clone(),
                agent_preset: source.agent_preset.clone(),
                computer_use_enabled: false,
                provider_cursor: source.provider_cursor.clone(),
            },
            event_sender,
        )?;
        driver.fork(turns_to_remove)
    }

    #[allow(clippy::too_many_arguments)]
    fn rewind_provider_response(
        &self,
        source: &AgentSession,
        cwd: &Path,
        binary: &Path,
        retained_turn_count: usize,
        rollback_turns: usize,
        provider_turn_count: usize,
        provider_resume_at: Option<String>,
    ) -> anyhow::Result<(Option<ProviderResumeCursor>, HashMap<String, String>, bool)> {
        if rollback_turns == 0 {
            return Ok((None, HashMap::new(), false));
        }
        let reset_native_session = retained_turn_count == 0
            && matches!(
                source.provider,
                ProviderKind::Claude | ProviderKind::Cursor | ProviderKind::Grok
            );
        if reset_native_session {
            return Ok((None, HashMap::new(), true));
        }

        match source.provider {
            ProviderKind::Claude => {
                let Some(ProviderResumeCursor::Claude { session_id, .. }) =
                    source.provider_cursor.as_ref()
                else {
                    bail!("Claude's native session is unavailable");
                };
                let fork = fork_provider_session(ProviderSessionForkRequest::Claude {
                    session_id: session_id.clone(),
                    resume_at: provider_resume_at,
                    turn_count: provider_turn_count,
                    title: format!("{} (rewind)", source.display_title()),
                })?;
                Ok((Some(fork.cursor), fork.message_ids, false))
            }
            ProviderKind::OpenCode => {
                let cursor = if let Some(driver) = self
                    .sessions
                    .lock()
                    .get(&source.id)
                    .map(|(_, _, driver, _)| driver.clone())
                {
                    driver
                        .rollback(rollback_turns)?
                        .ok_or_else(|| anyhow!("OpenCode returned no rewound-session cursor"))?
                } else {
                    let Some(ProviderResumeCursor::OpenCode { session_id, .. }) =
                        source.provider_cursor.as_ref()
                    else {
                        bail!("OpenCode's native session is unavailable");
                    };
                    fork_provider_session(ProviderSessionForkRequest::OpenCode {
                        binary: binary.to_owned(),
                        session_id: session_id.clone(),
                        turn_count: provider_turn_count,
                    })?
                    .cursor
                };
                Ok((Some(cursor), HashMap::new(), false))
            }
            ProviderKind::Amp => {
                let Some(ProviderResumeCursor::Amp {
                    thread_id,
                    fork_context,
                }) = source.provider_cursor.as_ref()
                else {
                    bail!("Amp's native thread is unavailable");
                };
                let cursor = fork_provider_session(ProviderSessionForkRequest::Amp {
                    binary: binary.to_owned(),
                    cwd: cwd.to_owned(),
                    thread_id: thread_id.clone(),
                    fork_context: fork_context.clone(),
                    turn_count: provider_turn_count,
                })?
                .cursor;
                Ok((Some(cursor), HashMap::new(), false))
            }
            ProviderKind::Cursor => {
                let cursor = fork_provider_session(ProviderSessionForkRequest::Cursor {
                    source: source.clone(),
                    turn_count: retained_turn_count,
                })?
                .cursor;
                Ok((Some(cursor), HashMap::new(), false))
            }
            ProviderKind::Grok => {
                let Some(ProviderResumeCursor::Grok { session_id }) =
                    source.provider_cursor.as_ref()
                else {
                    bail!("Grok Build's native session is unavailable");
                };
                let cursor = fork_provider_session(ProviderSessionForkRequest::Grok {
                    binary: binary.to_owned(),
                    cwd: cwd.to_owned(),
                    session_id: session_id.clone(),
                    turn_count: provider_turn_count,
                })?
                .cursor;
                Ok((Some(cursor), HashMap::new(), false))
            }
            ProviderKind::Codex
            | ProviderKind::DeepSeek
            | ProviderKind::OhMyPi
            | ProviderKind::Pi => Ok((
                self.rollback_response_with_driver(source, cwd, binary, rollback_turns)?,
                HashMap::new(),
                false,
            )),
            // Unreachable through the UI, which hides rewinding for providers
            // that answer `supports_conversation_rollback` with false.
            ProviderKind::Antigravity | ProviderKind::Fx | ProviderKind::Kimi => {
                bail!(
                    "{} cannot rewind a conversation to a turn",
                    source.provider.display_name()
                )
            }
        }
    }

    fn rollback_response_with_driver(
        &self,
        source: &AgentSession,
        cwd: &Path,
        binary: &Path,
        rollback_turns: usize,
    ) -> anyhow::Result<Option<ProviderResumeCursor>> {
        if let Some(driver) = self
            .sessions
            .lock()
            .get(&source.id)
            .map(|(_, _, driver, _)| driver.clone())
        {
            return driver.rollback(rollback_turns);
        }

        let (wake, _wake_events) = smol::channel::bounded(1);
        let (event_sender, _event_receiver) = driver::event_channel(wake);
        let driver = driver::start_local(
            source.provider,
            DriverStartOptions {
                binary: binary.to_owned(),
                cwd: cwd.to_owned(),
                mode: source.runtime_mode,
                model: source.model.clone(),
                reasoning_effort: source.reasoning_effort.clone(),
                service_tier: source.service_tier.clone(),
                context_window: source.context_window.clone(),
                agent_preset: source.agent_preset.clone(),
                computer_use_enabled: false,
                provider_cursor: source.provider_cursor.clone(),
            },
            event_sender,
        )?;
        driver.rollback(rollback_turns)
    }

    fn provider_binary(&self, provider: ProviderKind) -> anyhow::Result<PathBuf> {
        ensure_shell_environment();
        let settings = self.settings.get();
        let binary_override = settings
            .provider_binary_overrides
            .get(&provider)
            .map(String::as_str);
        crate::model::provider_probe(provider, binary_override)
            .path
            .ok_or_else(|| anyhow!("{} is not installed on the daemon", provider.display_name()))
    }

    fn start_session_driver(
        &self,
        session_id: Uuid,
        runtime_id: Uuid,
        options: crate::WireDriverStartOptions,
        events: EventSink,
    ) -> anyhow::Result<bool> {
        let previous = self.sessions.lock().remove(&session_id);
        drop(previous);
        let provider = decode_enum(&options.provider)?;
        let mut requested_cursor: Option<ProviderResumeCursor> = options
            .provider_cursor
            .map(serde_json::from_value)
            .transpose()
            .context("daemon received an invalid provider cursor")?;

        // Reconcile cross-agent in-place handoff in daemon task state so
        // Desktop, Web, and Mobile clients all park & restore native
        // cursors consistently without cross-provider cursor errors.
        {
            let mut state = self.task_state.lock();
            let mut switched = false;
            if let Some(stored_session) =
                state.sessions.iter_mut().find(|s| s.id == session_id)
            {
                let _ = self.task_store.hydrate(stored_session);
                if let Some(ref cur) = requested_cursor
                    && cur.provider() != provider
                    && stored_session.provider_cursor.is_none()
                {
                    stored_session.provider_cursor = Some(cur.clone());
                }
                if stored_session.provider != provider {
                    stored_session.switch_provider(provider);
                    switched = true;
                }
                if requested_cursor
                    .as_ref()
                    .is_some_and(|cur| cur.provider() != provider)
                {
                    requested_cursor = stored_session.provider_cursor.clone();
                } else if requested_cursor.is_none()
                    && stored_session
                        .provider_cursor
                        .as_ref()
                        .is_some_and(|cur| cur.provider() == provider)
                {
                    requested_cursor = stored_session.provider_cursor.clone();
                }
            } else if requested_cursor
                .as_ref()
                .is_some_and(|cur| cur.provider() != provider)
            {
                requested_cursor = None;
            }
            if switched {
                state.mark_session_dirty(session_id);
                let _ = self.task_store.save(&mut state);
            }
        }

        let mcp_companion = crate::mcp_companion::companion_launch_for_session_with_provider(
            session_id,
            self.task_store.path(),
            &options.cwd,
            Some(provider),
            Some(self.settings.path()),
        );
        let cwd_for_loop = options.cwd.clone();
        let settings_path_for_loop = self.settings.path().to_path_buf();
        let auto_advisor_for_loop = self.auto_advisor.clone();
        let scale_tracker_for_loop = self
            .scale_trackers
            .lock()
            .entry(session_id)
            .or_insert_with(|| {
                Arc::new(Mutex::new(
                    crate::mcp_companion::SessionScaleTracker::new(),
                ))
            })
            .clone();
        let options = DriverStartOptions {
            binary: options.binary,
            cwd: options.cwd,
            mode: decode_enum(&options.mode)?,
            model: options.model,
            reasoning_effort: options.reasoning_effort,
            service_tier: options.service_tier,
            context_window: options.context_window,
            agent_preset: options.agent_preset,
            computer_use_enabled: options.computer_use_enabled,
            provider_cursor: requested_cursor,
        };
        let (wake, _wake_events) = smol::channel::bounded(1);
        let (event_sender, event_receiver) = driver::event_channel(wake);
        let handle =
            driver::start_local_with_mcp(provider, options, mcp_companion, event_sender)?;
        let supports_steer = handle.supports_steer();
        let db_path_for_loop = self.task_store.path().to_path_buf();
        // Keep a weak handle here so the event-forwarding thread cannot
        // form an Arc cycle with the driver's event sender and keep
        // child processes alive after CloseSession / Start / RemoveSession.
        let driver_for_loop = handle.downgrade();
        let turn_active = Arc::new(AtomicBool::new(false));
        let turn_active_for_loop = turn_active.clone();
        std::thread::Builder::new()
            .name(format!("waku-daemon-events-{session_id}"))
            .spawn(move || {
                let unix_now_secs = || {
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs() as i64
                };
                loop {
                    if let Ok(claimed) = crate::mcp_companion::claim_pending_questions(
                        &db_path_for_loop,
                        session_id,
                    ) {
                        for (qid, questions) in claimed {
                            if let Ok(wire) = event_to_wire(DriverEvent::UserInputRequested {
                                request_id: format!(
                                    "{}{qid}",
                                    crate::mcp_companion::AGENTOS_QUESTION_PREFIX
                                ),
                                questions,
                            }) && events.send(wire).is_err()
                            {
                                return;
                            }
                        }
                    }
                    match event_receiver.recv_timeout(std::time::Duration::from_millis(250)) {
                        Ok(event) => {
                            match &event {
                                DriverEvent::TurnStarted => {
                                    turn_active_for_loop.store(true, Ordering::Release);
                                    scale_tracker_for_loop.lock().observe_turn_started();
                                }
                                DriverEvent::TurnParked => {
                                    scale_tracker_for_loop.lock().observe_turn_parked();
                                }
                                DriverEvent::TextDelta(delta) => {
                                    scale_tracker_for_loop.lock().observe_text_delta(delta);
                                }
                                DriverEvent::Activity {
                                    kind,
                                    title,
                                    complete: true,
                                    ..
                                } => {
                                    scale_tracker_for_loop.lock().observe_activity(
                                        *kind,
                                        None,
                                        None,
                                        title,
                                        None,
                                        false,
                                        &[],
                                        unix_now_secs(),
                                    );
                                }
                                DriverEvent::RichActivity(item) if item.complete => {
                                    let changes: Vec<(String, u64)> = item
                                        .file_changes
                                        .iter()
                                        .map(|fc| {
                                            (
                                                fc.path.clone(),
                                                fc.additions
                                                    .unwrap_or(0)
                                                    .saturating_add(fc.deletions.unwrap_or(0)),
                                            )
                                        })
                                        .collect();
                                    scale_tracker_for_loop.lock().observe_activity(
                                        item.kind,
                                        item.tool_name.as_deref(),
                                        item.mcp_server.as_deref(),
                                        &item.title,
                                        item.display_target.as_deref(),
                                        item.failed,
                                        &changes,
                                        unix_now_secs(),
                                    );
                                }
                                DriverEvent::TurnFinished { .. } | DriverEvent::ProcessExited => {
                                    turn_active_for_loop.store(false, Ordering::Release);
                                }
                                _ => {}
                            }
                            let turn_finished =
                                matches!(&event, DriverEvent::TurnFinished { .. });
                            let turn_succeeded = matches!(
                                &event,
                                DriverEvent::TurnFinished { success: true, .. }
                            );
                            let wire = event_to_wire(event).unwrap_or_else(|error| {
                                WireDriverEvent::new(
                                    "error",
                                    Value::String(format!(
                                        "could not encode daemon event: {error}"
                                    )),
                                )
                            });
                            if events.send(wire).is_err() {
                                break;
                            }
                            if turn_finished {
                                let mcp_settings = std::fs::read(&settings_path_for_loop)
                                    .ok()
                                    .and_then(|bytes| {
                                        serde_json::from_slice::<
                                            waku_protocol::settings::DaemonSettings,
                                        >(&bytes)
                                        .ok()
                                    })
                                    .map(|s| s.agentos_mcp)
                                    .unwrap_or_default();
                                if let Some(snapshot) = scale_tracker_for_loop
                                    .lock()
                                    .evaluate_on_turn_finished_with_settings(
                                        unix_now_secs(),
                                        &mcp_settings,
                                        turn_succeeded,
                                    )
                                    && mcp_settings.advisor_enabled
                                    && mcp_settings.advisor_auto_review_enabled
                                {
                                    auto_advisor_for_loop.enqueue(
                                        crate::mcp_companion::AutoAdvisorReviewJob {
                                            session_id,
                                            db_path: db_path_for_loop.clone(),
                                            settings_path: settings_path_for_loop.clone(),
                                            cwd: cwd_for_loop.clone(),
                                            caller_provider: provider,
                                            snapshot,
                                        },
                                    );
                                }
                            }
                            if turn_succeeded
                                && let Some(driver) = driver_for_loop.upgrade()
                                && let Ok(Some((goal, next_prompt))) =
                                    crate::mcp_companion::take_pending_goal_continuation(
                                        &db_path_for_loop,
                                        session_id,
                                    )
                            {
                                if let Ok(goal_wire) =
                                    event_to_wire(DriverEvent::GoalUpdated(Some(goal)))
                                {
                                    let _ = events.send(goal_wire);
                                }
                                let mut continuation_prefixes = Vec::new();
                                if let Ok(Some(rot_pkg)) =
                                    crate::mcp_companion::take_pending_context_rotation(
                                        &db_path_for_loop,
                                        session_id,
                                    )
                                {
                                    continuation_prefixes.push(rot_pkg);
                                }
                                if let Ok(Some(review_pkg)) =
                                    crate::mcp_companion::take_pending_advisor_review(
                                        &db_path_for_loop,
                                        session_id,
                                    )
                                {
                                    continuation_prefixes.push(review_pkg);
                                }
                                let prompt_to_send = if continuation_prefixes.is_empty() {
                                    next_prompt.clone()
                                } else {
                                    format!(
                                        "{}\n\n{next_prompt}",
                                        continuation_prefixes.join("\n\n")
                                    )
                                };
                                if let Ok(submit_wire) =
                                    event_to_wire(DriverEvent::PromptSubmitted {
                                        message: next_prompt,
                                        turn_id: Uuid::new_v4(),
                                        message_id: Uuid::new_v4(),
                                    })
                                {
                                    let _ = events.send(submit_wire);
                                }
                                scale_tracker_for_loop.lock().mark_goal_continuation();
                                turn_active_for_loop.store(true, Ordering::Release);
                                driver.prompt(prompt_to_send);
                            } else if turn_finished && provider != ProviderKind::Codex {
                                if let Ok(Some(goal)) =
                                    crate::mcp_companion::current_session_goal(
                                        &db_path_for_loop,
                                        session_id,
                                    )
                                    && let Ok(goal_wire) =
                                        event_to_wire(DriverEvent::GoalUpdated(Some(goal)))
                                {
                                    let _ = events.send(goal_wire);
                                }
                            }
                        }
                        Err(crossbeam_channel::RecvTimeoutError::Timeout) => {}
                        Err(crossbeam_channel::RecvTimeoutError::Disconnected) => break,
                    }
                }
            })
            .context("could not start daemon event forwarding thread")?;
        {
            let mut sessions = self.sessions.lock();
            if self.shutting_down.load(Ordering::Acquire) {
                drop(sessions);
                drop(handle);
                bail!("daemon is shutting down");
            }
            sessions.insert(session_id, (runtime_id, provider, handle, turn_active));
        }
        Ok(supports_steer)
    }

    #[allow(clippy::too_many_arguments)]
    fn prepare_and_send_prompt(
        &self,
        session_id: Uuid,
        driver: &DriverHandle,
        turn_active: &Arc<AtomicBool>,
        events: &EventSink,
        mut prompt: String,
        turn_id: Uuid,
        message_id: Uuid,
    ) -> anyhow::Result<()> {
        turn_active.store(true, Ordering::Release);
        self.scale_trackers
            .lock()
            .entry(session_id)
            .or_insert_with(|| {
                Arc::new(Mutex::new(
                    crate::mcp_companion::SessionScaleTracker::new(),
                ))
            })
            .lock()
            .mark_human_prompt(&prompt);
        // Publish the user's clean prompt into the runtime event stream before
        // prepending any one-shot handoff digest, context-rotation package, or
        // background advisor review.
        events.send(event_to_wire(DriverEvent::PromptSubmitted {
            message: prompt.clone(),
            turn_id,
            message_id,
        })?)?;

        let mut prefix_blocks = Vec::new();
        if let Ok(Some(rot_pkg)) = crate::mcp_companion::take_pending_context_rotation(
            self.task_store.path(),
            session_id,
        ) {
            prefix_blocks.push(rot_pkg);
        }
        if let Ok(Some(review_pkg)) = crate::mcp_companion::take_pending_advisor_review(
            self.task_store.path(),
            session_id,
        ) {
            prefix_blocks.push(review_pkg);
        }

        let handoff_seed = {
            let mut state = self.task_state.lock();
            if let Some(stored) = state.sessions.iter_mut().find(|s| s.id == session_id) {
                let _ = self.task_store.hydrate(stored);
                let taken = stored.take_pending_handoff_seed();
                if taken.is_some() {
                    state.mark_session_dirty(session_id);
                    let _ = self.task_store.save(&mut state);
                }
                taken
            } else {
                None
            }
        };
        if let Some(seed) = handoff_seed {
            prefix_blocks.push(seed);
        }
        if !prefix_blocks.is_empty() {
            prompt = format!("{}\n\n{prompt}", prefix_blocks.join("\n\n"));
        }
        driver.prompt(prompt);
        Ok(())
    }
}

impl crate::remote_mcp::RemoteMcpBackend for WakuBackend {
    fn remote_mcp_settings(&self) -> waku_protocol::settings::RemoteMcpSettings {
        self.settings.get().remote_mcp
    }

    fn list_projects_and_sessions(&self) -> anyhow::Result<(Vec<Project>, Vec<AgentSession>, PathBuf)> {
        let state = self.task_state.lock();
        Ok((
            state.projects.clone(),
            state
                .sessions
                .iter()
                .map(AgentSession::list_projection)
                .collect(),
            self.default_cwd.clone(),
        ))
    }

    fn hydrate_session_by_id(&self, session_id: Uuid) -> anyhow::Result<Option<AgentSession>> {
        let pinned = self.sessions.lock().keys().copied().collect();
        let mut state = self.task_state.lock();
        let session = if let Some(session) = state
            .sessions
            .iter_mut()
            .find(|session| session.id == session_id)
        {
            self.task_store.hydrate(session)?;
            Some(session.clone())
        } else {
            None
        };
        trim_resident_transcripts(&mut state, &pinned);
        Ok(session)
    }

    fn resolve_legacy_session_id(&self, legacy_id: i64) -> anyhow::Result<Option<Uuid>> {
        use rusqlite::OptionalExtension as _;
        let Ok(conn) = crate::mcp_companion::open_mcp_connection(self.task_store.path()) else {
            return Ok(None);
        };
        let mapped: Option<String> = conn
            .query_row(
                "SELECT session_id FROM agentos_legacy_session_map WHERE legacy_id = ?1",
                rusqlite::params![legacy_id],
                |row| row.get(0),
            )
            .optional()?;
        Ok(mapped.and_then(|s| Uuid::parse_str(&s).ok()))
    }

    fn search_session_messages(
        &self,
        query: String,
        limit: usize,
    ) -> anyhow::Result<Vec<waku_protocol::persistence::SessionMessageMatch>> {
        self.task_store
            .session_message_search(query, limit)()
            .map_err(Into::into)
    }

    fn create_session(
        &self,
        params: crate::remote_mcp::CreateSessionParams,
    ) -> anyhow::Result<(Project, AgentSession)> {
        let pinned = self.sessions.lock().keys().copied().collect();
        let mut state = self.task_state.lock();
        let project = if let Some(pid) = params.project_id {
            state
                .projects
                .iter()
                .find(|p| p.id == pid)
                .cloned()
                .ok_or_else(|| anyhow!("project {pid} not found"))?
        } else if let Some(path) = params.project_path {
            if let Some(existing) = state.projects.iter().find(|p| p.path == path).cloned() {
                existing
            } else {
                if !path.is_dir() {
                    bail!("project_path {} does not exist or is not a directory", path.display());
                }
                let created = Project::from_path(path);
                state.projects.push(created.clone());
                created
            }
        } else if let Some(first) = state.projects.first().cloned() {
            first
        } else {
            let created = Project::from_path(self.default_cwd.clone());
            state.projects.push(created.clone());
            created
        };

        let provider = params.provider.unwrap_or(ProviderKind::Codex);
        let mut session = AgentSession::new(project.id, provider);
        if let Some(model) = params.model {
            session.model = Some(model);
        }
        if let Some(mode) = params.runtime_mode {
            session.runtime_mode = mode;
        }
        if let Some(title) = params.title {
            session.title = title;
        }
        state.push_session(session.clone());
        self.task_store
            .save(&mut state)
            .context("could not save newly created session")?;
        trim_resident_transcripts(&mut state, &pinned);
        Ok((project, session))
    }

    fn submit_prompt_in_mailbox(
        &self,
        session_id: Uuid,
        active_runtime_id: &mut Option<Uuid>,
        hub: &Arc<crate::server::Hub>,
        params: crate::remote_mcp::SubmitPromptParams,
    ) -> anyhow::Result<crate::remote_mcp::SubmitPromptOutcome> {
        if let Some(ref key) = params.idempotency_key
            && let Some(cached) = self.prompt_idempotency.get(session_id, key)
        {
            return Ok(cached);
        }

        let existing = {
            let sessions = self.sessions.lock();
            sessions
                .get(&session_id)
                .map(|(rid, _, driver, turn_active)| (*rid, driver.clone(), turn_active.clone()))
        };

        let (runtime_id, driver, turn_active) = if let Some((rid, driver, turn_active)) = existing {
            *active_runtime_id = Some(rid);
            if turn_active.load(Ordering::Acquire) {
                if params.steer {
                    if !driver.supports_steer() {
                        return Ok(crate::remote_mcp::SubmitPromptOutcome {
                            session_id,
                            runtime_id: rid,
                            turn_id: None,
                            message_id: None,
                            status: "busy".into(),
                            accepted: false,
                            deduplicated: false,
                            detail: Some(
                                "session is busy and its provider does not support steering".into(),
                            ),
                        });
                    }
                    driver.steer(params.prompt);
                    let outcome = crate::remote_mcp::SubmitPromptOutcome {
                        session_id,
                        runtime_id: rid,
                        turn_id: None,
                        message_id: None,
                        status: "steered".into(),
                        accepted: true,
                        deduplicated: false,
                        detail: None,
                    };
                    if let Some(key) = params.idempotency_key {
                        self.prompt_idempotency
                            .insert(session_id, key, outcome.clone());
                    }
                    return Ok(outcome);
                }
                return Ok(crate::remote_mcp::SubmitPromptOutcome {
                    session_id,
                    runtime_id: rid,
                    turn_id: None,
                    message_id: None,
                    status: "busy".into(),
                    accepted: false,
                    deduplicated: false,
                    detail: Some(
                        "session is currently running a turn; pass steer=true, call wait_for_turn, or cancel_session"
                            .into(),
                    ),
                });
            }
            (rid, driver, turn_active)
        } else {
            let (session, cwd) = {
                let mut state = self.task_state.lock();
                let Some(idx) = state.sessions.iter().position(|s| s.id == session_id) else {
                    bail!("session {session_id} not found");
                };
                self.task_store.hydrate(&mut state.sessions[idx])?;
                let session = state.sessions[idx].clone();
                let project = state
                    .projects
                    .iter()
                    .find(|p| p.id == session.project_id)
                    .ok_or_else(|| anyhow!("project for session {session_id} not found"))?;
                let cwd = session.workspace.path().unwrap_or(&project.path).to_owned();
                (session, cwd)
            };

            let binary = self.provider_binary(session.provider)?;
            let new_runtime_id = Uuid::new_v4();
            let wire_options = crate::WireDriverStartOptions {
                provider: encode_enum(session.provider)?,
                binary,
                cwd,
                mode: encode_enum(session.runtime_mode)?,
                model: session.model.clone(),
                reasoning_effort: session.reasoning_effort.clone(),
                service_tier: session.service_tier.clone(),
                context_window: session.context_window.clone(),
                agent_preset: session.agent_preset.clone(),
                computer_use_enabled: false,
                provider_cursor: session
                    .provider_cursor
                    .as_ref()
                    .map(serde_json::to_value)
                    .transpose()?,
            };
            hub.begin_runtime(session_id, new_runtime_id);
            if let Err(error) = self.start_session_driver(
                session_id,
                new_runtime_id,
                wire_options,
                hub.event_sink(session_id, new_runtime_id),
            ) {
                hub.end_runtime(session_id, Some(new_runtime_id));
                return Err(error);
            }
            *active_runtime_id = Some(new_runtime_id);
            let sessions = self.sessions.lock();
            let (rid, _, driver, turn_active) = sessions
                .get(&session_id)
                .ok_or_else(|| anyhow!("session runtime disappeared immediately after start"))?;
            (*rid, driver.clone(), turn_active.clone())
        };

        let turn_id = Uuid::new_v4();
        let message_id = Uuid::new_v4();
        // Make the accepted user prompt durable in SQLite before acknowledging.
        {
            let pinned = self.sessions.lock().keys().copied().collect();
            let mut state = self.task_state.lock();
            if let Some(stored) = state.sessions.iter_mut().find(|s| s.id == session_id) {
                let _ = self.task_store.hydrate(stored);
                if stored.adopt_submitted_prompt(&params.prompt, turn_id, message_id) {
                    state.mark_session_dirty(session_id);
                    let _ = self.task_store.save(&mut state);
                }
            }
            trim_resident_transcripts(&mut state, &pinned);
        }
        hub.broadcast_task_state_changed_all();

        let events = hub.event_sink(session_id, runtime_id);
        self.prepare_and_send_prompt(
            session_id,
            &driver,
            &turn_active,
            &events,
            params.prompt,
            turn_id,
            message_id,
        )?;

        let outcome = crate::remote_mcp::SubmitPromptOutcome {
            session_id,
            runtime_id,
            turn_id: Some(turn_id),
            message_id: Some(message_id),
            status: "running".into(),
            accepted: true,
            deduplicated: false,
            detail: None,
        };
        if let Some(key) = params.idempotency_key {
            self.prompt_idempotency
                .insert(session_id, key, outcome.clone());
        }
        Ok(outcome)
    }

    fn cancel_session_in_mailbox(
        &self,
        session_id: Uuid,
        _active_runtime_id: Option<Uuid>,
        hub: &Arc<crate::server::Hub>,
    ) -> anyhow::Result<bool> {
        let found = {
            let sessions = self.sessions.lock();
            sessions
                .get(&session_id)
                .map(|(rid, provider, driver, turn_active)| {
                    (*rid, *provider, driver.clone(), turn_active.clone())
                })
        };
        let Some((runtime_id, provider, driver, turn_active)) = found else {
            return Ok(false);
        };
        turn_active.store(false, Ordering::Release);
        let _ = crate::mcp_companion::cancel_pending_questions(self.task_store.path(), session_id);
        if let Ok(Some(paused_goal)) =
            crate::mcp_companion::pause_active_goal_on_cancel(self.task_store.path(), session_id)
            && provider != ProviderKind::Codex
        {
            let events = hub.event_sink(session_id, runtime_id);
            let _ = events.send(event_to_wire(DriverEvent::GoalUpdated(Some(paused_goal)))?);
        }
        driver.cancel();
        Ok(true)
    }

    fn respond_permission_in_mailbox(
        &self,
        session_id: Uuid,
        runtime_id: Uuid,
        request_id: String,
        option_id: String,
        _hub: &Arc<crate::server::Hub>,
    ) -> anyhow::Result<()> {
        let driver = {
            let sessions = self.sessions.lock();
            let (active_runtime_id, _, driver, _) = sessions
                .get(&session_id)
                .ok_or_else(|| anyhow!("daemon session {session_id} is not running"))?;
            if *active_runtime_id != runtime_id {
                bail!(
                    "daemon session {session_id} belongs to runtime {active_runtime_id}, not {runtime_id}"
                );
            }
            driver.clone()
        };
        driver.respond(request_id, option_id);
        Ok(())
    }

    fn respond_user_input_in_mailbox(
        &self,
        session_id: Uuid,
        runtime_id: Uuid,
        request_id: String,
        answers: Vec<waku_protocol::model::UserInputAnswer>,
        _hub: &Arc<crate::server::Hub>,
    ) -> anyhow::Result<()> {
        let driver = {
            let sessions = self.sessions.lock();
            let (active_runtime_id, _, driver, _) = sessions
                .get(&session_id)
                .ok_or_else(|| anyhow!("daemon session {session_id} is not running"))?;
            if *active_runtime_id != runtime_id {
                bail!(
                    "daemon session {session_id} belongs to runtime {active_runtime_id}, not {runtime_id}"
                );
            }
            driver.clone()
        };
        if let Some(qid) = request_id.strip_prefix(crate::mcp_companion::AGENTOS_QUESTION_PREFIX) {
            crate::mcp_companion::resolve_pending_question(self.task_store.path(), qid, &answers)?;
            return Ok(());
        }
        driver.respond_user_input(request_id, answers);
        Ok(())
    }

    fn persist_scribe_event(
        &self,
        event: &crate::SequencedEvent,
        driver_event: &DriverEvent,
        stream_phase: &mut Option<crate::remote_mcp::ScribeStreamPhase>,
        flush_now: bool,
    ) -> anyhow::Result<bool> {
        let pinned = self.sessions.lock().keys().copied().collect();
        let mut state = self.task_state.lock();
        let Some(idx) = state
            .sessions
            .iter()
            .position(|session| session.id == event.session_id)
        else {
            return Ok(false);
        };
        let _ = self.task_store.hydrate(&mut state.sessions[idx]);
        let session = &mut state.sessions[idx];

        let catalog_changed = apply_scribe_driver_event(session, driver_event, stream_phase);
        session.runtime_event_cursor = Some(waku_protocol::model::RuntimeEventCursor {
            runtime_id: event.runtime_id,
            epoch: event.epoch,
            sequence: event.sequence,
        });
        state.mark_session_dirty(event.session_id);
        if flush_now {
            self.task_store.save(&mut state)?;
            trim_resident_transcripts(&mut state, &pinned);
        }
        Ok(catalog_changed)
    }

    fn flush_scribe_session(&self, _session_id: Uuid) -> anyhow::Result<()> {
        let pinned = self.sessions.lock().keys().copied().collect();
        let mut state = self.task_state.lock();
        self.task_store.save(&mut state)?;
        trim_resident_transcripts(&mut state, &pinned);
        Ok(())
    }

    fn active_runtime_for(&self, session_id: Uuid) -> Option<(Uuid, bool)> {
        self.sessions
            .lock()
            .get(&session_id)
            .map(|(rid, _, _, turn_active)| (*rid, turn_active.load(Ordering::Acquire)))
    }
}

fn scribe_unix_time() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn scribe_unix_time_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn scribe_finish_streaming_assistant(session: &mut AgentSession) {
    for message in &mut session.messages {
        if message.role == crate::model::MessageRole::Assistant && message.streaming {
            message.streaming = false;
        }
    }
}

fn scribe_complete_reasoning_activity(session: &mut AgentSession) {
    if let Some(reasoning) = session
        .transcript_blocks
        .iter_mut()
        .rev()
        .flat_map(|block| block.activities.iter_mut().rev())
        .find(|activity| activity.reasoning.is_some() && !activity.complete)
    {
        reasoning.complete = true;
        session.updated_at = scribe_unix_time();
    }
}

fn scribe_complete_turn_blocks(session: &mut AgentSession) {
    for block in &mut session.transcript_blocks {
        for activity in &mut block.activities {
            activity.complete = true;
        }
    }
}

fn scribe_push_activity(
    session: &mut AgentSession,
    item: crate::model::ActivityItem,
    continuing_work: bool,
) {
    let after_message = session.messages.len();
    let turn_id = session.active_turn_id();
    if continuing_work
        && let Some(block) = session.transcript_blocks.last_mut()
        && block.after_message == after_message
        && block.turn_id == turn_id
    {
        block.activities.push(item);
    } else {
        session.transcript_blocks.push(crate::model::TranscriptBlock {
            after_message,
            turn_id,
            activities: vec![item],
        });
    }
}

fn scribe_session_accepts_output(session: &mut AgentSession) -> bool {
    if session.active_turn_id().is_none() || !session.status.is_busy() {
        return false;
    }
    session.mark_active_turn_provider_started();
    if session.status == SessionStatus::Connecting {
        session.status = SessionStatus::Working;
    }
    true
}

fn scribe_turn_has_assistant_message(session: &AgentSession) -> bool {
    let Some(turn_id) = session.active_turn_id() else {
        return false;
    };
    session.messages.iter().any(|message| {
        message.role == crate::model::MessageRole::Assistant && message.turn_id == Some(turn_id)
    })
}

/// Mirror Desktop's `handle_driver_event` session-state reducer on the daemon
/// so headless turns started or continued without an attached UI persist the
/// exact same transcript structure, cursors, and status transitions.
fn apply_scribe_driver_event(
    session: &mut AgentSession,
    event: &DriverEvent,
    stream_phase: &mut Option<crate::remote_mcp::ScribeStreamPhase>,
) -> bool {
    use crate::model::{ActivityItem, ContextUsage, Message, MessageRole, ReasoningBlock, TurnStatus};
    use crate::remote_mcp::ScribeStreamPhase;

    let mut catalog_changed = false;
    match event {
        DriverEvent::Connected { provider_cursor } => {
            if let Some(ProviderResumeCursor::Claude {
                resume_at: Some(message_id),
                ..
            }) = provider_cursor
            {
                session.mark_active_turn_provider_resume_at(message_id.clone());
            }
            session.provider_cursor = provider_cursor.clone();
            if session.status == SessionStatus::Connecting {
                session.status = SessionStatus::Working;
                catalog_changed = true;
            }
        }
        DriverEvent::AgentPresetSelected(preset) => {
            session.agent_preset = preset.clone();
        }
        DriverEvent::AutoTitleUpdated(title) => {
            session.set_auto_title(title.clone());
            catalog_changed = true;
        }
        DriverEvent::AvailableCommands(names) => {
            session.available_commands = names.clone();
        }
        DriverEvent::PromptSubmitted {
            message,
            turn_id,
            message_id,
        } => {
            if session.adopt_submitted_prompt(message, *turn_id, *message_id) {
                catalog_changed = true;
            }
        }
        DriverEvent::TurnStarted => {
            if session.active_turn_id().is_some() {
                session.mark_active_turn_provider_started();
                if session.status != SessionStatus::Working {
                    session.status = SessionStatus::Working;
                    catalog_changed = true;
                }
            } else if matches!(
                session.provider,
                ProviderKind::Codex
                    | ProviderKind::Claude
                    | ProviderKind::OpenCode
                    | ProviderKind::Antigravity
            ) {
                session.begin_provider_turn();
                session.mark_active_turn_provider_started();
                session.status = SessionStatus::Working;
                catalog_changed = true;
            }
        }
        DriverEvent::TurnParked => {
            if session.active_turn_id().is_some() {
                scribe_finish_streaming_assistant(session);
                scribe_complete_turn_blocks(session);
                *stream_phase = None;
                session.status = SessionStatus::Background;
                session.updated_at = scribe_unix_time();
                catalog_changed = true;
            }
        }
        DriverEvent::TextDelta(delta) => {
            if scribe_session_accepts_output(session) {
                let prev = *stream_phase;
                if prev == Some(ScribeStreamPhase::Reasoning) {
                    scribe_complete_reasoning_activity(session);
                }
                let continuing = prev == Some(ScribeStreamPhase::Text);
                if !continuing {
                    scribe_finish_streaming_assistant(session);
                }
                let existing = continuing.then(|| {
                    session
                        .messages
                        .iter_mut()
                        .rev()
                        .find(|m| m.role == MessageRole::Assistant && m.streaming)
                });
                if let Some(Some(message)) = existing {
                    message.content.push_str(delta);
                } else {
                    let mut message = session
                        .active_turn_id()
                        .map(|tid| Message::new_for_turn(MessageRole::Assistant, delta.clone(), tid))
                        .unwrap_or_else(|| Message::new(MessageRole::Assistant, delta.clone()))
                        .with_provider(Some(session.provider));
                    message.streaming = true;
                    session.messages.push(message);
                }
                session.updated_at = scribe_unix_time();
                *stream_phase = Some(ScribeStreamPhase::Text);
            }
        }
        DriverEvent::ReasoningDelta(delta) => {
            if scribe_session_accepts_output(session) {
                let prev = *stream_phase;
                let continuing = prev == Some(ScribeStreamPhase::Reasoning);
                if continuing || !delta.trim().is_empty() {
                    let now = scribe_unix_time_millis();
                    if !continuing {
                        scribe_finish_streaming_assistant(session);
                    }
                    if continuing
                        && let Some(reasoning) = session
                            .transcript_blocks
                            .last_mut()
                            .and_then(|b| b.activities.last_mut())
                            .and_then(|a| a.reasoning.as_mut())
                    {
                        reasoning.content.push_str(delta);
                        reasoning.finished_at_ms = now;
                    } else {
                        scribe_push_activity(
                            session,
                            ActivityItem::from_reasoning(
                                ReasoningBlock {
                                    content: delta.clone(),
                                    started_at_ms: now,
                                    finished_at_ms: now,
                                },
                                false,
                            ),
                            matches!(
                                prev,
                                Some(ScribeStreamPhase::Reasoning | ScribeStreamPhase::Activity)
                            ),
                        );
                    }
                    session.updated_at = scribe_unix_time();
                    *stream_phase = Some(ScribeStreamPhase::Reasoning);
                }
            }
        }
        DriverEvent::Activity {
            id,
            kind,
            title,
            detail,
            complete,
        } => {
            if scribe_session_accepts_output(session) {
                let item = ActivityItem::new(
                    id.clone(),
                    *kind,
                    title.clone(),
                    detail.clone(),
                    *complete,
                );
                scribe_upsert_activity(session, stream_phase, item);
            }
        }
        DriverEvent::RichActivity(item) => {
            if scribe_session_accepts_output(session) {
                scribe_upsert_activity(session, stream_phase, item.clone());
            }
        }
        DriverEvent::Permission { .. } => {
            if scribe_session_accepts_output(session) && session.status != SessionStatus::Waiting {
                session.status = SessionStatus::Waiting;
                catalog_changed = true;
            }
        }
        DriverEvent::UserInputRequested { questions, .. } => {
            if scribe_session_accepts_output(session)
                && !questions.is_empty()
                && session.status != SessionStatus::Waiting
            {
                session.status = SessionStatus::Waiting;
                catalog_changed = true;
            }
        }
        DriverEvent::SteerAccepted { message } => {
            session.push_user_message_with_presentation(message.clone(), None, Vec::new());
            session.updated_at = scribe_unix_time();
        }
        DriverEvent::GoalUpdated(goal) => {
            if let Some(goal) = goal
                && session.messages.is_empty()
            {
                session.set_title_from_prompt(&goal.objective);
                catalog_changed = true;
            }
            session.thread_goal = goal.clone();
        }
        DriverEvent::UsageUpdated {
            context_tokens,
            context_window,
        } => {
            let usage = session.context_usage.get_or_insert(ContextUsage::default());
            if let Some(tokens) = context_tokens {
                usage.tokens = *tokens;
            }
            if let Some(window) = context_window {
                usage.window = Some(*window);
            }
        }
        DriverEvent::TurnFinished { success, summary } => {
            if session.active_turn_id().is_some() {
                scribe_finish_streaming_assistant(session);
                scribe_complete_turn_blocks(session);
                *stream_phase = None;
                let needs_fallback = !scribe_turn_has_assistant_message(session);
                session.status = if *success {
                    SessionStatus::Idle
                } else {
                    SessionStatus::Failed
                };
                if needs_fallback {
                    session.push_message(
                        MessageRole::Assistant,
                        summary.clone().unwrap_or_else(|| {
                            if *success {
                                "Turn completed.".into()
                            } else {
                                "Stopped before response.".into()
                            }
                        }),
                    );
                }
                session.finish_active_turn(if *success {
                    TurnStatus::Completed
                } else {
                    TurnStatus::Failed
                });
                session.updated_at = scribe_unix_time();
                catalog_changed = true;
            }
        }
        DriverEvent::Error(error) => {
            if session.active_turn_id().is_some() && session.status != SessionStatus::Working {
                let needs_fallback = !scribe_turn_has_assistant_message(session);
                session.status = SessionStatus::Failed;
                if needs_fallback {
                    session.push_message(MessageRole::Assistant, error.clone());
                }
                session.updated_at = scribe_unix_time();
                catalog_changed = true;
            }
        }
        DriverEvent::ProcessExited => {
            scribe_finish_streaming_assistant(session);
            scribe_complete_turn_blocks(session);
            *stream_phase = None;
            if session.status.is_busy() {
                let needs_fallback = !scribe_turn_has_assistant_message(session);
                session.status = SessionStatus::Failed;
                session.updated_at = scribe_unix_time();
                if needs_fallback {
                    session.push_message(
                        MessageRole::Assistant,
                        "Provider process exited before completing the response.",
                    );
                }
                session.finish_active_turn(TurnStatus::Failed);
                catalog_changed = true;
            }
        }
        DriverEvent::RuntimeEventCursorAdvanced(_)
        | DriverEvent::BackgroundWork(_)
        | DriverEvent::ComputerUseUpdated(_)
        | DriverEvent::SteerRejected { .. }
        | DriverEvent::PlanUsageUpdated(_) => {}
    }
    catalog_changed
}

fn scribe_upsert_activity(
    session: &mut AgentSession,
    stream_phase: &mut Option<crate::remote_mcp::ScribeStreamPhase>,
    item: crate::model::ActivityItem,
) {
    use crate::remote_mcp::ScribeStreamPhase;

    let prev = *stream_phase;
    if prev == Some(ScribeStreamPhase::Text) {
        scribe_finish_streaming_assistant(session);
    }
    if prev == Some(ScribeStreamPhase::Reasoning) {
        scribe_complete_reasoning_activity(session);
    }
    let continuing_work = matches!(
        prev,
        Some(ScribeStreamPhase::Reasoning | ScribeStreamPhase::Activity)
    );
    for block in session.transcript_blocks.iter_mut().rev() {
        let matching = block.activities.iter_mut().rev().find(|activity| {
            item.source_id
                .as_ref()
                .is_some_and(|id| activity.source_id.as_ref() == Some(id))
                || (item.source_id.is_none()
                    && activity.title == item.title
                    && !activity.complete)
        });
        if let Some(activity) = matching {
            let has_arguments = item.arguments.is_some();
            activity.kind = item.kind;
            activity.title = item.title;
            if item.tool_name.is_some() {
                activity.tool_name = item.tool_name;
            }
            if item.mcp_server.is_some() {
                activity.mcp_server = item.mcp_server;
            }
            activity.complete = item.complete;
            activity.failed = item.failed;
            if item.detail.is_some() {
                activity.detail = item.detail;
            }
            if item.arguments.is_some() {
                activity.arguments = item.arguments;
            }
            if item.output.is_some() {
                activity.output = item.output;
            }
            if !item.image_urls.is_empty() {
                activity.image_urls = item.image_urls;
            }
            if !item.file_changes.is_empty() {
                activity.file_changes = item.file_changes;
            }
            if item.display_target.is_some()
                && (activity.display_target.is_none() || has_arguments)
            {
                activity.display_target = item.display_target;
            }
            if item.display_description.is_some()
                && (activity.display_description.is_none() || has_arguments)
            {
                activity.display_description = item.display_description;
            }
            if item.reasoning.is_some() {
                activity.reasoning = item.reasoning;
            }
            session.updated_at = scribe_unix_time();
            *stream_phase = Some(ScribeStreamPhase::Activity);
            return;
        }
    }
    scribe_push_activity(session, item, continuing_work);
    session.updated_at = scribe_unix_time();
    *stream_phase = Some(ScribeStreamPhase::Activity);
}

fn validate_message_rewind(source: &AgentSession, turn_count: usize) -> anyhow::Result<()> {
    if !matches!(source.status, SessionStatus::Idle | SessionStatus::Failed) {
        bail!("stop the task before editing a prior message");
    }
    let Some(turn) = source
        .turns
        .iter()
        .find(|turn| turn.turn_count == turn_count)
    else {
        bail!("the selected message is unavailable");
    };
    if !source.messages.iter().any(|message| {
        message.turn_id == Some(turn.id) && message.role == crate::model::MessageRole::User
    }) {
        bail!("the selected user message is unavailable");
    }
    let rollback_turns = source.provider_turns_after(turn_count.saturating_sub(1));
    if rollback_turns > 0 && source.provider_cursor.is_none() {
        bail!("the provider conversation is unavailable");
    }
    Ok(())
}

fn restore_rewind_safety(
    cwd: &Path,
    safety_ref: &str,
    context: &str,
    error: anyhow::Error,
) -> anyhow::Error {
    match crate::checkpoint::restore_ref(cwd, safety_ref) {
        Ok(()) => {
            let _ = crate::checkpoint::delete_ref(cwd, safety_ref);
            anyhow!("{context}: {error}; the original worktree was restored")
        }
        Err(restore_error) => anyhow!(
            "{context}: {error}; restoring the safety snapshot also failed: {restore_error}; snapshot: {safety_ref}"
        ),
    }
}

fn validate_response_fork(source: &AgentSession, turn_count: usize) -> anyhow::Result<()> {
    if !matches!(source.status, SessionStatus::Idle | SessionStatus::Failed) {
        bail!("stop the task before forking a response");
    }
    let cursor = source
        .provider_cursor
        .as_ref()
        .ok_or_else(|| anyhow!("the provider conversation is unavailable"))?;
    if cursor.provider() != source.provider {
        bail!("the provider conversation does not match this task");
    }
    if source
        .turns
        .get(turn_count.saturating_sub(1))
        .is_none_or(|turn| turn.turn_count != turn_count || !turn.provider_turn_started)
    {
        bail!("the selected response cannot be forked");
    }
    Ok(())
}

fn numbered_title_suffix(title: &str) -> Option<(&str, usize)> {
    let (base, suffix) = title.rsplit_once(" (")?;
    let number = suffix.strip_suffix(')')?.parse().ok()?;
    (!base.is_empty() && number >= 2).then_some((base, number))
}

fn next_response_fork_title<'a>(
    source_title: &str,
    existing_titles: impl IntoIterator<Item = &'a str>,
) -> String {
    let existing_titles = existing_titles.into_iter().collect::<Vec<_>>();
    let base = numbered_title_suffix(source_title)
        .filter(|(base, _)| existing_titles.iter().any(|title| title == base))
        .map_or(source_title, |(base, _)| base);
    let highest_number = existing_titles
        .iter()
        .filter_map(|title| {
            if *title == base {
                Some(1)
            } else {
                numbered_title_suffix(title)
                    .filter(|(candidate_base, _)| *candidate_base == base)
                    .map(|(_, number)| number)
            }
        })
        .max()
        .unwrap_or(1);
    format!("{base} ({})", highest_number.saturating_add(1).max(2))
}

fn fork_provider_session(
    request: ProviderSessionForkRequest,
) -> anyhow::Result<ProviderSessionFork> {
    use crate::model::ProviderResumeCursor;

    let (cursor, message_ids, source_resume_at) = match request {
        ProviderSessionForkRequest::Claude {
            session_id,
            resume_at,
            turn_count,
            title,
        } => {
            let source_resume_at = resume_at.map(Ok).unwrap_or_else(|| {
                crate::claude_session::message_id_for_turn(&session_id, turn_count)
            })?;
            let fork =
                crate::claude_session::fork_session_at(&session_id, &source_resume_at, &title)?;
            let fork_resume_at = fork
                .message_ids
                .get(&source_resume_at)
                .cloned()
                .ok_or_else(|| anyhow!("Claude fork did not include its target message"))?;
            (
                ProviderResumeCursor::Claude {
                    session_id: fork.session_id,
                    resume_at: Some(fork_resume_at),
                },
                fork.message_ids,
                Some(source_resume_at),
            )
        }
        ProviderSessionForkRequest::Amp {
            binary,
            cwd,
            thread_id,
            fork_context,
            turn_count,
        } => (
            crate::amp_session::fork_session_at_turn(
                &binary,
                &cwd,
                &thread_id,
                fork_context.as_deref(),
                turn_count,
            )?,
            HashMap::new(),
            None,
        ),
        ProviderSessionForkRequest::Cursor { source, turn_count } => (
            crate::cursor_session::fork_session_at_turn(&source, turn_count)?,
            HashMap::new(),
            None,
        ),
        ProviderSessionForkRequest::OpenCode {
            binary,
            session_id,
            turn_count,
        } => (
            crate::opencode_session::fork_session_at_turn(&binary, &session_id, turn_count)?,
            HashMap::new(),
            None,
        ),
        ProviderSessionForkRequest::Grok {
            binary,
            cwd,
            session_id,
            turn_count,
        } => (
            crate::grok_session::fork_session_at_turn(&binary, &cwd, &session_id, turn_count)?,
            HashMap::new(),
            None,
        ),
    };
    Ok(ProviderSessionFork {
        cursor,
        message_ids,
        source_resume_at,
    })
}

fn handle_driver_command(
    driver: &DriverHandle,
    command: Command,
) -> anyhow::Result<ResponsePayload> {
    match command {
        Command::Prompt { prompt, .. } => driver.prompt(prompt),
        Command::Steer { prompt } => driver.steer(prompt),
        Command::Cancel => driver.cancel(),
        Command::CancelComputerUse => driver.cancel_computer_use(),
        Command::RefreshBackgroundWork => driver.refresh_background_work(),
        Command::StopBackgroundWork { key, control_id } => {
            driver.stop_background_work(
                serde_json::from_value(key).context("invalid background-work key")?,
                control_id,
            );
        }
        Command::Respond {
            request_id,
            option_id,
        } => driver.respond(request_id, option_id),
        Command::RespondUserInput {
            request_id,
            answers,
        } => driver.respond_user_input(request_id, answers),
        Command::Goal { operation } => driver.goal(operation),
        Command::RunComputerTool { request } => {
            driver.run_computer_tool(crate::computer_use::ComputerToolRequest {
                call_id: request.call_id,
                tool: request.tool,
                arguments: request.arguments,
            });
        }
        Command::RejectComputerTool { request, reason } => {
            driver.reject_computer_tool(
                crate::computer_use::ComputerToolRequest {
                    call_id: request.call_id,
                    tool: request.tool,
                    arguments: request.arguments,
                },
                reason,
            );
        }
        Command::ApplyOptions { options } => {
            return Ok(ResponsePayload::OptionsApplied {
                applied: driver.apply_options(SessionOptions {
                    mode: decode_enum(&options.mode)?,
                    model: options.model,
                    reasoning_effort: options.reasoning_effort,
                    service_tier: options.service_tier,
                    context_window: options.context_window,
                }),
            });
        }
        Command::Rollback { turns } => {
            let cursor = driver
                .rollback(turns)?
                .map(serde_json::to_value)
                .transpose()?;
            return Ok(ResponsePayload::Cursor { cursor });
        }
        Command::Fork { turns_to_remove } => {
            let cursor = Some(serde_json::to_value(driver.fork(turns_to_remove)?)?);
            return Ok(ResponsePayload::Cursor { cursor });
        }
        Command::AttachSession
        | Command::Start { .. }
        | Command::GetSettings
        | Command::UpdateSettings { .. }
        | Command::ProbeProvider { .. }
        | Command::FetchPlanUsage { .. }
        | Command::ProbeComputerPermissions { .. }
        | Command::LoadUsageHistory { .. }
        | Command::LoadSkills { .. }
        | Command::SetSkillsEnabled { .. }
        | Command::TrashSkills { .. }
        | Command::LoadMemoryLibrary { .. }
        | Command::UpdateMemoryKnowledgeStatus { .. }
        | Command::DeleteMemoryEntry { .. }
        | Command::LoadTaskState
        | Command::SaveTaskState { .. }
        | Command::RemoveSession
        | Command::HydrateSession { .. }
        | Command::SearchSessionMessages { .. }
        | Command::ListProviderSessions { .. }
        | Command::LoadProviderSession { .. }
        | Command::LoadComposerDrafts
        | Command::SaveComposerDrafts { .. }
        | Command::ApplyComposerDraftChanges { .. }
        | Command::StoreBlob { .. }
        | Command::ImportAttachment { .. }
        | Command::ImportPathAttachment { .. }
        | Command::ReadBlob { .. }
        | Command::ReadAttachment { .. }
        | Command::SweepBlobs
        | Command::ForkSessionFromResponse { .. }
        | Command::RewindSessionToMessage { .. }
        | Command::ForkProviderSession { .. }
        | Command::Workspace { .. }
        | Command::OpenTerminal { .. }
        | Command::WriteTerminal { .. }
        | Command::ResizeTerminal { .. }
        | Command::CloseTerminal
        | Command::CloseSession => {
            bail!("daemon received a command in the wrong dispatch path")
        }
    }
    Ok(ResponsePayload::Ack)
}

fn ensure_shell_environment() {
    static REFRESHED: OnceLock<()> = OnceLock::new();
    REFRESHED.get_or_init(|| {
        crate::command_env::refresh_from_default_shell();
    });
}

fn decode_enum<T: DeserializeOwned>(value: &str) -> anyhow::Result<T> {
    serde_json::from_value(Value::String(value.to_owned()))
        .with_context(|| format!("invalid protocol enum value {value:?}"))
}

pub fn encode_enum<T: Serialize>(value: T) -> anyhow::Result<String> {
    serde_json::to_value(value)?
        .as_str()
        .map(str::to_owned)
        .ok_or_else(|| anyhow!("protocol enum did not serialize as a string"))
}

fn event_to_wire(event: DriverEvent) -> anyhow::Result<WireDriverEvent> {
    let (kind, payload) = match event {
        DriverEvent::RuntimeEventCursorAdvanced(_) => {
            bail!("client-only runtime cursors cannot be sent by the daemon")
        }
        DriverEvent::Connected { provider_cursor } => {
            ("connected", serde_json::to_value(provider_cursor)?)
        }
        DriverEvent::AgentPresetSelected(preset) => {
            ("agentPresetSelected", serde_json::to_value(preset)?)
        }
        DriverEvent::AutoTitleUpdated(title) => ("autoTitleUpdated", serde_json::to_value(title)?),
        DriverEvent::AvailableCommands(commands) => {
            ("availableCommands", serde_json::to_value(commands)?)
        }
        DriverEvent::TurnStarted => ("turnStarted", Value::Null),
        DriverEvent::TurnParked => ("turnParked", Value::Null),
        DriverEvent::TextDelta(text) => ("textDelta", Value::String(text)),
        DriverEvent::ReasoningDelta(text) => ("reasoningDelta", Value::String(text)),
        DriverEvent::Activity {
            id,
            kind,
            title,
            detail,
            complete,
        } => (
            "activity",
            json!({
                "id": id,
                "kind": kind,
                "title": title,
                "detail": detail,
                "complete": complete,
            }),
        ),
        DriverEvent::RichActivity(activity) => ("richActivity", serde_json::to_value(activity)?),
        DriverEvent::BackgroundWork(work) => ("backgroundWork", serde_json::to_value(work)?),
        DriverEvent::Permission {
            request_id,
            title,
            detail,
            options,
        } => (
            "permission",
            json!({
                "requestId": request_id,
                "title": title,
                "detail": detail,
                "options": options,
            }),
        ),
        DriverEvent::UserInputRequested {
            request_id,
            questions,
        } => (
            "userInputRequested",
            json!({
                "requestId": request_id,
                "questions": questions,
            }),
        ),
        DriverEvent::ComputerUseUpdated(state) => (
            "computerUseUpdated",
            serde_json::to_value(ComputerUseWire {
                target: state.target,
                phase: state.phase,
                visible: state.visible,
                image_url: state.image_url,
            })?,
        ),
        DriverEvent::PromptSubmitted {
            message,
            turn_id,
            message_id,
        } => (
            "promptSubmitted",
            json!({ "message": message, "turnId": turn_id, "messageId": message_id }),
        ),
        DriverEvent::SteerAccepted { message } => ("steerAccepted", json!({ "message": message })),
        DriverEvent::SteerRejected { message, reason } => (
            "steerRejected",
            json!({ "message": message, "reason": reason }),
        ),
        DriverEvent::UsageUpdated {
            context_tokens,
            context_window,
        } => (
            "usageUpdated",
            json!({
                "contextTokens": context_tokens,
                "contextWindow": context_window,
            }),
        ),
        DriverEvent::PlanUsageUpdated(usage) => ("planUsageUpdated", serde_json::to_value(usage)?),
        DriverEvent::GoalUpdated(goal) => ("goalUpdated", serde_json::to_value(goal)?),
        DriverEvent::TurnFinished { success, summary } => (
            "turnFinished",
            json!({ "success": success, "summary": summary }),
        ),
        DriverEvent::Error(error) => ("error", Value::String(error)),
        DriverEvent::ProcessExited => ("processExited", Value::Null),
    };
    Ok(WireDriverEvent::new(kind, payload))
}

pub fn event_from_wire(event: WireDriverEvent) -> anyhow::Result<DriverEvent> {
    let payload = event.payload;
    Ok(match event.kind.as_str() {
        "connected" => DriverEvent::Connected {
            provider_cursor: serde_json::from_value(payload)?,
        },
        "agentPresetSelected" => DriverEvent::AgentPresetSelected(serde_json::from_value(payload)?),
        "autoTitleUpdated" => DriverEvent::AutoTitleUpdated(serde_json::from_value(payload)?),
        "availableCommands" => DriverEvent::AvailableCommands(serde_json::from_value(payload)?),
        "turnStarted" => DriverEvent::TurnStarted,
        "turnParked" => DriverEvent::TurnParked,
        "textDelta" => DriverEvent::TextDelta(serde_json::from_value(payload)?),
        "reasoningDelta" => DriverEvent::ReasoningDelta(serde_json::from_value(payload)?),
        "activity" => {
            let activity: ActivityWire = serde_json::from_value(payload)?;
            DriverEvent::Activity {
                id: activity.id,
                kind: activity.kind,
                title: activity.title,
                detail: activity.detail,
                complete: activity.complete,
            }
        }
        "richActivity" => DriverEvent::RichActivity(serde_json::from_value(payload)?),
        "backgroundWork" => DriverEvent::BackgroundWork(serde_json::from_value(payload)?),
        "permission" => {
            let permission: PermissionWire = serde_json::from_value(payload)?;
            DriverEvent::Permission {
                request_id: permission.request_id,
                title: permission.title,
                detail: permission.detail,
                options: permission.options,
            }
        }
        "userInputRequested" => {
            let request: UserInputWire = serde_json::from_value(payload)?;
            DriverEvent::UserInputRequested {
                request_id: request.request_id,
                questions: request.questions,
            }
        }
        "computerUseUpdated" => {
            let state: ComputerUseWire = serde_json::from_value(payload)?;
            DriverEvent::ComputerUseUpdated(ComputerUseState {
                target: state.target,
                phase: state.phase,
                visible: state.visible,
                image_url: state.image_url,
            })
        }
        "promptSubmitted" => {
            let submitted: SubmittedPromptWire = serde_json::from_value(payload)?;
            DriverEvent::PromptSubmitted {
                message: submitted.message,
                turn_id: submitted.turn_id,
                message_id: submitted.message_id,
            }
        }
        "steerAccepted" => {
            let steer: AcceptedSteerWire = serde_json::from_value(payload)?;
            DriverEvent::SteerAccepted {
                message: steer.message,
            }
        }
        "steerRejected" => {
            let steer: RejectedSteerWire = serde_json::from_value(payload)?;
            DriverEvent::SteerRejected {
                message: steer.message,
                reason: steer.reason,
            }
        }
        "usageUpdated" => {
            let usage: UsageWire = serde_json::from_value(payload)?;
            DriverEvent::UsageUpdated {
                context_tokens: usage.context_tokens,
                context_window: usage.context_window,
            }
        }
        "planUsageUpdated" => DriverEvent::PlanUsageUpdated(serde_json::from_value(payload)?),
        "goalUpdated" => DriverEvent::GoalUpdated(serde_json::from_value(payload)?),
        "turnFinished" => {
            let finished: TurnFinishedWire = serde_json::from_value(payload)?;
            DriverEvent::TurnFinished {
                success: finished.success,
                summary: finished.summary,
            }
        }
        "error" => DriverEvent::Error(serde_json::from_value(payload)?),
        "processExited" => DriverEvent::ProcessExited,
        kind => bail!("daemon sent an unsupported driver event {kind:?}"),
    })
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct SubmittedPromptWire {
    message: String,
    turn_id: Uuid,
    message_id: Uuid,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ActivityWire {
    id: Option<String>,
    kind: ActivityKind,
    title: String,
    detail: Option<String>,
    complete: bool,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct PermissionWire {
    request_id: String,
    title: String,
    detail: String,
    options: Vec<PermissionOption>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct UserInputWire {
    request_id: String,
    questions: Vec<crate::model::UserInputQuestion>,
}

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct ComputerUseWire {
    target: Option<ComputerTarget>,
    phase: ComputerUsePhase,
    visible: bool,
    image_url: Option<String>,
}

#[derive(Deserialize)]
struct AcceptedSteerWire {
    message: String,
}

#[derive(Deserialize)]
struct RejectedSteerWire {
    message: String,
    reason: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct UsageWire {
    context_tokens: Option<u64>,
    context_window: Option<u64>,
}

#[derive(Deserialize)]
struct TurnFinishedWire {
    success: bool,
    summary: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stale_runtime_projection_keeps_newer_transcript_cursor() {
        let runtime_id = Uuid::new_v4();
        let epoch = Uuid::new_v4();
        let mut existing = AgentSession::new(Uuid::new_v4(), ProviderKind::Codex);
        existing.status = SessionStatus::Working;
        existing.runtime_event_cursor = Some(crate::model::RuntimeEventCursor {
            runtime_id,
            epoch,
            sequence: 10,
        });
        existing.push_message(crate::model::MessageRole::Assistant, "complete so far");

        let mut stale = existing.clone();
        stale.title = "Renamed elsewhere".into();
        stale.messages.clear();
        stale.runtime_event_cursor = Some(crate::model::RuntimeEventCursor {
            runtime_id,
            epoch,
            sequence: 7,
        });

        assert!(session_projection_precedes(
            &existing,
            &stale,
            Some(runtime_id)
        ));
        merge_stale_session_metadata(&mut existing, stale);
        assert_eq!(existing.title, "Renamed elsewhere");
        assert_eq!(existing.messages.len(), 1);
        assert_eq!(existing.runtime_event_cursor.unwrap().sequence, 10);
    }

    #[test]
    fn stale_unattached_client_cannot_erase_completed_headless_turn() {
        let mut existing = AgentSession::new(Uuid::new_v4(), ProviderKind::Codex);
        existing.begin_turn("first");
        existing.push_message(crate::model::MessageRole::Assistant, "first reply");
        existing.finish_active_turn(crate::model::TurnStatus::Completed);

        let mut stale_desktop = existing.clone();
        stale_desktop.runtime_event_cursor = None;
        stale_desktop.title = "Edited title on desktop".into();

        // Now a headless Remote MCP turn runs and completes on the daemon.
        existing.begin_turn("remote follow-up");
        existing.push_message(crate::model::MessageRole::Assistant, "remote reply");
        existing.finish_active_turn(crate::model::TurnStatus::Completed);
        existing.status = SessionStatus::Idle;
        existing.runtime_event_cursor = Some(crate::model::RuntimeEventCursor {
            runtime_id: Uuid::new_v4(),
            epoch: Uuid::new_v4(),
            sequence: 8,
        });
        stale_desktop.updated_at = existing.updated_at.saturating_add(10);

        assert!(session_projection_precedes(&existing, &stale_desktop, None));
        merge_stale_session_metadata(&mut existing, stale_desktop);
        assert_eq!(existing.title, "Edited title on desktop");
        assert_eq!(existing.turns.len(), 2);
        assert_eq!(existing.messages.len(), 4);
    }

    #[test]
    fn client_projection_cannot_replace_a_daemon_checkpoint() {
        let mut existing = AgentSession::new(Uuid::new_v4(), ProviderKind::Codex);
        existing.begin_turn("change it");
        existing.finish_active_turn(crate::model::TurnStatus::Completed);
        let checkpoint = Checkpoint {
            turn_count: 1,
            git_ref: "refs/waku/canonical".into(),
            status: CheckpointStatus::Ready,
            files: Vec::new(),
            additions: 0,
            deletions: 0,
            created_at: 1,
        };
        existing.turns[0].checkpoint = Some(checkpoint.clone());

        let mut incoming = existing.clone();
        incoming.turns[0].checkpoint = Some(Checkpoint {
            git_ref: "refs/waku/stale-client".into(),
            ..checkpoint.clone()
        });
        preserve_daemon_checkpoints(&existing, &mut incoming);

        assert_eq!(incoming.turns[0].checkpoint.as_ref(), Some(&checkpoint));
    }

    #[test]
    fn stale_client_save_cannot_resurrect_consumed_handoff_seed() {
        let mut existing = AgentSession::new(Uuid::new_v4(), ProviderKind::Antigravity);
        existing.pending_handoff_seed = None;

        // Same provider stale snapshot carrying an already-consumed seed must keep None.
        let mut stale = existing.clone();
        stale.pending_handoff_seed = Some("<agent_handoff_context>old</agent_handoff_context>".into());
        preserve_daemon_handoff_consumption(&existing, &mut stale);
        assert!(stale.pending_handoff_seed.is_none());

        // Same provider stale metadata merge also refuses to resurrect the seed.
        let mut stale_meta = existing.clone();
        stale_meta.pending_handoff_seed =
            Some("<agent_handoff_context>old</agent_handoff_context>".into());
        merge_stale_session_metadata(&mut existing, stale_meta);
        assert!(existing.pending_handoff_seed.is_none());

        // A genuine provider switch IS allowed to attach a new handoff seed.
        let mut switched = existing.clone();
        switched.provider = ProviderKind::Codex;
        switched.pending_handoff_seed =
            Some("<agent_handoff_context>fresh</agent_handoff_context>".into());
        preserve_daemon_handoff_consumption(&existing, &mut switched);
        assert_eq!(
            switched.pending_handoff_seed.as_deref(),
            Some("<agent_handoff_context>fresh</agent_handoff_context>")
        );
    }

    #[test]
    fn response_fork_titles_follow_one_numbered_sequence() {
        assert_eq!(
            next_response_fork_title("Fix the bug", ["Fix the bug"]),
            "Fix the bug (2)"
        );
        assert_eq!(
            next_response_fork_title(
                "Fix the bug (2)",
                ["Fix the bug", "Fix the bug (2)", "Fix the bug (4)"]
            ),
            "Fix the bug (5)"
        );
        assert_eq!(
            next_response_fork_title("Plan (2026)", ["Plan (2026)"]),
            "Plan (2026) (2)"
        );
    }

    #[test]
    fn message_rewind_requires_a_settled_user_turn_and_provider_cursor() {
        let mut session = AgentSession::new(Uuid::new_v4(), ProviderKind::Codex);
        session.begin_turn("change it");
        session.mark_active_turn_provider_started();
        session.provider_cursor = Some(ProviderResumeCursor::Codex {
            thread_id: "thread".into(),
        });
        session.finish_active_turn(crate::model::TurnStatus::Completed);

        assert!(validate_message_rewind(&session, 1).is_ok());

        let mut busy = session.clone();
        busy.status = SessionStatus::Working;
        assert!(validate_message_rewind(&busy, 1).is_err());

        let mut missing_cursor = session.clone();
        missing_cursor.provider_cursor = None;
        assert!(validate_message_rewind(&missing_cursor, 1).is_err());

        let mut missing_message = session;
        missing_message.messages.clear();
        assert!(validate_message_rewind(&missing_message, 1).is_err());
    }

    #[test]
    fn wire_event_round_trip_preserves_ordered_delta_payload() {
        let wire = event_to_wire(DriverEvent::TextDelta("hello".into())).unwrap();
        assert_eq!(wire.kind, "textDelta");
        assert!(matches!(
            event_from_wire(wire).unwrap(),
            DriverEvent::TextDelta(text) if text == "hello"
        ));
    }

    #[test]
    fn wire_event_round_trip_preserves_prompt_submission_identity() {
        let turn_id = Uuid::new_v4();
        let message_id = Uuid::new_v4();
        let wire = event_to_wire(DriverEvent::PromptSubmitted {
            message: "ship it".into(),
            turn_id,
            message_id,
        })
        .unwrap();
        assert_eq!(wire.kind, "promptSubmitted");
        assert_eq!(wire.payload["message"], "ship it");
        assert_eq!(wire.payload["turnId"], turn_id.to_string());
        assert_eq!(wire.payload["messageId"], message_id.to_string());
        assert!(matches!(
            event_from_wire(wire).unwrap(),
            DriverEvent::PromptSubmitted { message, turn_id: decoded_turn, message_id: decoded_message }
                if message == "ship it" && decoded_turn == turn_id && decoded_message == message_id
        ));
    }

    #[test]
    fn oversized_monster_session_hydrates_within_wire_budget_without_mutating_sqlite() {
        let dir = std::env::temp_dir().join(format!("agentos-waku-monster-{}", Uuid::new_v4()));
        let settings = DaemonSettingsStore::open(dir.join("settings.json")).unwrap();
        let store = StateStore::daemon(dir.join("app.db"));

        let project = Project::from_path(dir.join("repo"));
        let mut monster = AgentSession::new(project.id, ProviderKind::Codex);
        monster.begin_turn("analyze 68MB monster history");
        monster.push_message(crate::model::MessageRole::Assistant, "completed massive audit");
        let big_arg = "a".repeat(16 * 1024);
        let big_out = "b".repeat(18 * 1024);
        let mut activities = Vec::with_capacity(2000);
        for i in 0..2000 {
            let mut item = crate::model::ActivityItem::new(
                Some(format!("call-{i}")),
                ActivityKind::Command,
                format!("cargo test case_{i}"),
                Some(big_arg.clone()),
                true,
            );
            item.arguments = Some(big_arg.clone());
            item.output = Some(big_out.clone());
            activities.push(item);
        }
        monster.transcript_blocks.push(crate::model::TranscriptBlock {
            after_message: 1,
            turn_id: monster.turns.first().map(|t| t.id),
            activities,
        });
        monster.finish_active_turn(crate::model::TurnStatus::Completed);
        assert!(monster.estimated_wire_bytes() > 64 * 1024 * 1024);

        let monster_id = monster.id;
        let mut initial = PersistedState::empty();
        initial.projects.push(project.clone());
        initial.push_session(monster);
        store.save(&mut initial).unwrap();

        let backend = WakuBackend::new(settings, store).unwrap();
        let sink = crate::server::EventSink::test_sink();
        let payload = backend
            .handle(
                Request {
                    request_id: Uuid::new_v4(),
                    session_id: Uuid::nil(),
                    runtime_id: Uuid::nil(),
                    command: Command::HydrateSession {
                        session_id: monster_id,
                    },
                },
                sink.clone(),
            )
            .unwrap();

        let ResponsePayload::Session {
            session: Some(hydrated),
        } = payload
        else {
            panic!("expected hydrated session payload");
        };
        let wire_json = serde_json::to_vec(&hydrated).unwrap();
        assert!(
            wire_json.len() <= waku_protocol::HYDRATE_WIRE_SOFT_BUDGET_BYTES,
            "hydrated wire payload ({}) exceeded soft budget",
            wire_json.len()
        );
        assert_eq!(hydrated.messages.len(), 2);
        assert_eq!(hydrated.transcript_blocks[0].activities.len(), 2000);
        assert!(
            hydrated.transcript_blocks[0].activities[0]
                .output
                .as_deref()
                .unwrap()
                .contains("[truncated")
        );

        // Verify skeleton metadata save never wipes stored monster transcript
        let mut skeleton = hydrated.list_projection();
        skeleton.title = "Renamed without hydration".into();
        skeleton.updated_at += 500;
        backend
            .handle(
                Request {
                    request_id: Uuid::new_v4(),
                    session_id: Uuid::nil(),
                    runtime_id: Uuid::nil(),
                    command: Command::SaveTaskState {
                        projects: vec![project],
                        live_session_ids: vec![monster_id],
                        sessions: vec![skeleton],
                    },
                },
                sink,
            )
            .unwrap();

        // Reload directly from SQLite and confirm all 2000 raw 18KB outputs remain lossless!
        let verify_store = StateStore::daemon(dir.join("app.db"));
        let mut reloaded = verify_store.load().unwrap();
        let saved = reloaded
            .sessions
            .iter_mut()
            .find(|s| s.id == monster_id)
            .unwrap();
        assert_eq!(saved.title, "Renamed without hydration");
        verify_store.hydrate(saved).unwrap();
        assert_eq!(saved.transcript_blocks[0].activities.len(), 2000);
        assert_eq!(
            saved.transcript_blocks[0].activities[0]
                .output
                .as_ref()
                .unwrap()
                .len(),
            18 * 1024
        );
        let _ = std::fs::remove_dir_all(dir);
    }

    #[cfg(unix)]
    #[test]
    fn cross_agent_handoff_codex_to_antigravity_and_back_preserves_parked_cursors_and_injects_codeg_mcp()
     {
        use crate::model::{MessageRole, RuntimeMode};
        use std::os::unix::fs::PermissionsExt as _;

        let dir = std::env::temp_dir()
            .canonicalize()
            .unwrap_or_else(|_| std::env::temp_dir())
            .join(format!("waku-handoff-e2e-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let workspace = dir.join("workspace");
        std::fs::create_dir_all(&workspace).unwrap();

        // Create a dummy waku-daemon MCP binary so companion_launch_for_session resolves it
        let fake_mcp_bin = dir.join("waku-daemon");
        std::fs::write(&fake_mcp_bin, "#!/bin/sh\nexit 0\n").unwrap();
        std::fs::set_permissions(&fake_mcp_bin, std::fs::Permissions::from_mode(0o755)).unwrap();

        let rpc_log = dir.join("agy_handoff_rpc.json");
        let fake_agy_py = dir.join("fake_agy.py");
        std::fs::write(
            &fake_agy_py,
            r#"#!/usr/bin/env python3
import json, os, sys
log_path = os.environ["AGY_HANDOFF_LOG"]
def record(item):
    arr = json.load(open(log_path)) if os.path.exists(log_path) else []
    arr.append(item)
    json.dump(arr, open(log_path, "w"))
def send(obj):
    sys.stdout.write(json.dumps(obj) + "\n")
    sys.stdout.flush()
for raw in sys.stdin:
    raw = raw.strip()
    if not raw:
        continue
    msg = json.loads(raw)
    method = msg.get("method")
    rid = msg.get("id")
    params = msg.get("params") or {}
    record({"method": method, "params": params})
    if method == "initialize":
        send({"jsonrpc": "2.0", "id": rid, "result": {"protocolVersion": 1, "agentCapabilities": {"loadSession": True, "sessionCapabilities": {"resume": {}}}, "agentInfo": {"name": "antigravity-acp", "version": "1.4.5"}}})
    elif method == "session/new":
        send({"jsonrpc": "2.0", "id": rid, "result": {"sessionId": "agy-mid-session", "modes": {"currentModeId": "yolo", "availableModes": [{"id": "yolo", "name": "YOLO"}]}, "configOptions": []}})
    elif method == "session/prompt":
        send({"jsonrpc": "2.0", "method": "session/update", "params": {"sessionId": params["sessionId"], "update": {"sessionUpdate": "agent_message_chunk", "content": {"type": "text", "text": "Antigravity completed step 2."}}}})
        send({"jsonrpc": "2.0", "id": rid, "result": {"stopReason": "end_turn"}})
"#,
        )
        .unwrap();
        std::fs::set_permissions(&fake_agy_py, std::fs::Permissions::from_mode(0o755)).unwrap();
        let fake_agy_sh = dir.join("run_agy.sh");
        std::fs::write(
            &fake_agy_sh,
            format!(
                "#!/bin/sh\nexport AGY_HANDOFF_LOG='{}'\nexec python3 '{}' \"$@\"\n",
                rpc_log.display(),
                fake_agy_py.display()
            ),
        )
        .unwrap();
        std::fs::set_permissions(&fake_agy_sh, std::fs::Permissions::from_mode(0o755)).unwrap();

        let project = Project::from_path(workspace.clone());
        let mut session = AgentSession::new(project.id, ProviderKind::Codex);
        let session_id = session.id;
        session.provider_cursor = Some(crate::model::ProviderResumeCursor::Codex {
            thread_id: "codex-orig-thread".into(),
        });
        session.push_message(MessageRole::User, "Step 1 on Codex");
        session.push_message(MessageRole::Assistant, "Codex finished step 1.");

        let store = StateStore::daemon(dir.join("app.db"));
        let mut state = store.load().unwrap();
        state.projects.push(project);
        state.mark_session_dirty(session_id);
        state.sessions.push(session);
        store.save(&mut state).unwrap();

        let settings = DaemonSettingsStore::open_with_legacy(
            dir.join("settings.json"),
            Vec::<std::path::PathBuf>::new(),
        )
        .unwrap();
        let backend = WakuBackend::new(settings, store).unwrap();

        unsafe {
            std::env::set_var(crate::mcp_companion::MCP_BIN_ENV, &fake_mcp_bin);
        }

        // Start Antigravity on the existing Codex session (auto-parks Codex cursor & injects agentos-mcp)
        let runtime_id = Uuid::new_v4();
        backend
            .handle(
                Request {
                    request_id: Uuid::new_v4(),
                    session_id,
                    runtime_id,
                    command: Command::Start {
                        options: waku_protocol::WireDriverStartOptions {
                            provider: waku_protocol::encode_enum(ProviderKind::Antigravity)
                                .unwrap(),
                            binary: fake_agy_sh,
                            cwd: workspace,
                            mode: waku_protocol::encode_enum(crate::model::RuntimeMode::FullAccess).unwrap(),
                            model: None,
                            reasoning_effort: None,
                            service_tier: None,
                            context_window: None,
                            agent_preset: None,
                            computer_use_enabled: false,
                            provider_cursor: None,
                        },
                    },
                },
                EventSink::test_sink(),
            )
            .unwrap();

        // Wait briefly for ACP handshake (`session/new`) to settle, then send Prompt
        std::thread::sleep(std::time::Duration::from_millis(400));
        backend
            .handle(
                Request {
                    request_id: Uuid::new_v4(),
                    session_id,
                    runtime_id,
                    command: Command::Prompt {
                        prompt: "Continue step 2 on Antigravity".into(),
                        turn_id: Some(Uuid::new_v4()),
                        message_id: Some(Uuid::new_v4()),
                    },
                },
                EventSink::test_sink(),
            )
            .unwrap();
        std::thread::sleep(std::time::Duration::from_millis(400));

        backend
            .handle(
                Request {
                    request_id: Uuid::new_v4(),
                    session_id,
                    runtime_id,
                    command: Command::CloseSession,
                },
                EventSink::test_sink(),
            )
            .unwrap();

        unsafe {
            std::env::remove_var(crate::mcp_companion::MCP_BIN_ENV);
        }

        // Verify ACP wire RPC log: agentos-mcp injected on session/new AND cold handoff seed prepended on session/prompt!
        let logged: Vec<Value> =
            serde_json::from_str(&std::fs::read_to_string(&rpc_log).unwrap()).unwrap();
        let new_call = logged
            .iter()
            .find(|item| item.get("method").and_then(Value::as_str) == Some("session/new"))
            .expect("session/new must be called");
        let mcp_servers = new_call
            .pointer("/params/mcpServers")
            .and_then(Value::as_array)
            .expect("mcpServers array present");
        assert!(
            mcp_servers
                .iter()
                .any(|srv| srv.get("name").and_then(Value::as_str) == Some("agentos-mcp"))
        );

        let prompt_call = logged
            .iter()
            .find(|item| item.get("method").and_then(Value::as_str) == Some("session/prompt"))
            .expect("session/prompt must be called");
        let prompt_text = prompt_call
            .pointer("/params/prompt/0/text")
            .and_then(Value::as_str)
            .unwrap();
        assert!(prompt_text.contains("<agent_handoff_context"));
        assert!(prompt_text.contains("Step 1 on Codex"));
        assert!(prompt_text.contains("Continue step 2 on Antigravity"));

        // Verify SQLite persisted parked Codex cursor, cleared pending_handoff_seed, and warm-returns to Codex!
        let verify_store = StateStore::daemon(dir.join("app.db"));
        let mut reloaded = verify_store.load().unwrap();
        let hydrated = reloaded
            .sessions
            .iter_mut()
            .find(|s| s.id == session_id)
            .unwrap();
        verify_store.hydrate(hydrated).unwrap();
        assert_eq!(hydrated.provider, ProviderKind::Antigravity);
        assert!(hydrated.pending_handoff_seed.is_none());
        assert_eq!(hydrated.parked_cursors.len(), 1);
        assert_eq!(
            hydrated.parked_cursors[0].cursor.native_id(),
            "codex-orig-thread"
        );

        // Simulate Antigravity's cursor + reply saved, then switch back to Codex:
        hydrated.provider_cursor = Some(crate::model::ProviderResumeCursor::Antigravity {
            session_id: "agy-mid-session".into(),
        });
        hydrated.push_message(MessageRole::User, "Continue step 2 on Antigravity");
        hydrated.push_message(MessageRole::Assistant, "Antigravity completed step 2.");
        assert_eq!(hydrated.messages[1].provider, Some(ProviderKind::Codex));
        assert_eq!(
            hydrated.messages[3].provider,
            Some(ProviderKind::Antigravity)
        );
        assert!(hydrated.switch_provider(ProviderKind::Codex));
        assert_eq!(
            hydrated.provider_cursor.as_ref().map(|c| c.native_id()),
            Some("codex-orig-thread")
        );
        assert_eq!(
            hydrated.parked_cursors[0].cursor.native_id(),
            "agy-mid-session"
        );
        let warm_seed = hydrated.take_pending_handoff_seed().unwrap();
        assert!(warm_seed.contains("mode=\"warm_delta\""));
        assert!(warm_seed.contains("Antigravity completed step 2."));
        assert!(!warm_seed.contains("Step 1 on Codex"));

        let _ = std::fs::remove_dir_all(dir);
    }

    #[cfg(unix)]
    #[test]
    fn close_session_and_restart_terminate_acp_child_process() {
        use std::os::unix::fs::PermissionsExt;

        struct Cleanup {
            dir: std::path::PathBuf,
            pid_file: std::path::PathBuf,
        }
        impl Drop for Cleanup {
            fn drop(&mut self) {
                for line in std::fs::read_to_string(&self.pid_file)
                    .unwrap_or_default()
                    .lines()
                {
                    for part in line.split_whitespace() {
                        if let Ok(pid) = part.parse::<i32>() {
                            let _ = std::process::Command::new("kill")
                                .args(["-9", &format!("-{pid}")])
                                .status();
                            let _ = std::process::Command::new("kill")
                                .args(["-9", &pid.to_string()])
                                .status();
                        }
                    }
                }
                let _ = std::fs::remove_dir_all(&self.dir);
            }
        }

        let dir = std::env::temp_dir().join(format!("waku-leak-regression-{}", Uuid::new_v4()));
        let workspace = dir.join("workspace");
        std::fs::create_dir_all(&workspace).unwrap();

        let pid_file = dir.join("pids.log");
        let _cleanup = Cleanup {
            dir: dir.clone(),
            pid_file: pid_file.clone(),
        };
        let fake_agy_sh = dir.join("agy");
        // Ignore SIGTERM, spawn a descendant child (like `agy --worker`), and
        // block on stdin so only real process-group termination (ChildGuard::drop)
        // can reap both the leader and descendant.
        let wrapper = format!(
            "#!/bin/sh\ntrap '' TERM\nsleep 300 &\nchild=$!\necho \"$$ $child\" >> \"{}\"\nwhile read -r _line; do :; done\nwait $child\n",
            pid_file.display()
        );
        std::fs::write(&fake_agy_sh, wrapper).unwrap();
        std::fs::set_permissions(&fake_agy_sh, std::fs::Permissions::from_mode(0o755)).unwrap();

        let store = StateStore::daemon(dir.join("app.db"));
        let settings = DaemonSettingsStore::open_with_legacy(
            dir.join("settings.json"),
            Vec::<std::path::PathBuf>::new(),
        )
        .unwrap();
        let backend = WakuBackend::new(settings, store).unwrap();

        let session_id = Uuid::new_v4();
        let start_opts = || waku_protocol::WireDriverStartOptions {
            provider: waku_protocol::encode_enum(ProviderKind::Antigravity).unwrap(),
            binary: fake_agy_sh.clone(),
            cwd: workspace.clone(),
            mode: waku_protocol::encode_enum(crate::model::RuntimeMode::FullAccess).unwrap(),
            model: None,
            reasoning_effort: None,
            service_tier: None,
            context_window: None,
            agent_preset: None,
            computer_use_enabled: false,
            provider_cursor: None,
        };

        let read_pid_pairs = || -> Vec<(i32, i32)> {
            std::fs::read_to_string(&pid_file)
                .unwrap_or_default()
                .lines()
                .filter_map(|line| {
                    let mut parts = line.split_whitespace();
                    Some((
                        parts.next()?.parse::<i32>().ok()?,
                        parts.next()?.parse::<i32>().ok()?,
                    ))
                })
                .collect()
        };
        let wait_for_pid_count = |expected: usize| -> Vec<(i32, i32)> {
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
            while std::time::Instant::now() < deadline {
                let pairs = read_pid_pairs();
                if pairs.len() >= expected {
                    return pairs;
                }
                std::thread::sleep(std::time::Duration::from_millis(25));
            }
            panic!(
                "timed out waiting for {expected} fake ACP PID pairs, got {:?}",
                read_pid_pairs()
            );
        };
        let wait_for_exit = |pid: i32| {
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
            while std::time::Instant::now() < deadline {
                let status = std::process::Command::new("/bin/kill")
                    .args(["-0", &pid.to_string()])
                    .status()
                    .expect("failed to invoke /bin/kill");
                if !status.success() {
                    return;
                }
                std::thread::sleep(std::time::Duration::from_millis(25));
            }
            panic!("leaked ACP process {pid} did not terminate");
        };

        // 1. Start runtime #1
        let runtime_1 = Uuid::new_v4();
        backend
            .handle(
                Request {
                    request_id: Uuid::new_v4(),
                    session_id,
                    runtime_id: runtime_1,
                    command: Command::Start {
                        options: start_opts(),
                    },
                },
                EventSink::test_sink(),
            )
            .unwrap();
        let (leader_1, child_1) = wait_for_pid_count(1)[0];

        // 2. Replacing the session with runtime #2 must drop & terminate runtime #1 + its descendant
        let runtime_2 = Uuid::new_v4();
        backend
            .handle(
                Request {
                    request_id: Uuid::new_v4(),
                    session_id,
                    runtime_id: runtime_2,
                    command: Command::Start {
                        options: start_opts(),
                    },
                },
                EventSink::test_sink(),
            )
            .unwrap();
        let (leader_2, child_2) = wait_for_pid_count(2)[1];
        wait_for_exit(leader_1);
        wait_for_exit(child_1);

        // 3. CloseSession must drop & terminate runtime #2 + its descendant even while stuck in startup
        backend
            .handle(
                Request {
                    request_id: Uuid::new_v4(),
                    session_id,
                    runtime_id: runtime_2,
                    command: Command::CloseSession,
                },
                EventSink::test_sink(),
            )
            .unwrap();
        wait_for_exit(leader_2);
        wait_for_exit(child_2);
    }
}
