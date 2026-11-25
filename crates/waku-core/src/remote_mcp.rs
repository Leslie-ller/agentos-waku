//! Streamable HTTP Remote MCP server (`/mcp`) and daemon-owned Event Scribe.
//!
//! Exposes a scoped, bearer-authenticated MCP 2025-03-26 / 2025-06-18 Streamable
//! HTTP endpoint directly on `waku-daemon`'s existing HTTP listener so remote
//! MCP clients (Claude, Cursor, Codex, OpenClaw, custom agents) can list and
//! inspect AgentOS sessions, send messages, wait for turns, and optionally
//! respond to pending approval/question requests.
//!
//! Every live provider runtime event flows through `Hub::emit` into
//! [`DaemonEventScribe`], which updates live status + pending requests and
//! persists transcripts to SQLite even when no Desktop or Web client is
//! attached.

use std::collections::{HashMap, HashSet, VecDeque};
use std::io::{self, Read as _, Write as _};
use std::net::TcpStream;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex as StdMutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context as _, anyhow, bail};
use crossbeam_channel::Receiver;
use parking_lot::Mutex;
use serde_json::{Value, json};
use uuid::Uuid;
use waku_protocol::model::{
    AgentSession, DriverEvent, MessageRole, PermissionOption, Project, ProviderKind, RuntimeMode,
    SessionStatus, TurnStatus, UserInputAnswer, UserInputQuestion,
};
use waku_protocol::persistence::SessionMessageMatch;
use waku_protocol::settings::RemoteMcpSettings;
use waku_protocol::{SequencedEvent, event_from_wire};

use crate::server::{
    Backend, Hub, HubObservation, RequestDispatcher, ServerOptions, origin_matches_host,
    token_matches,
};

pub const REMOTE_MCP_PATH: &str = "/mcp";
pub const REMOTE_MCP_SERVER_NAME: &str = "agentos-remote-mcp";
pub const MAX_MCP_CONNECTIONS: usize = 12;
pub const MAX_MCP_CONCURRENT_WAITS: usize = 6;
pub const MAX_MCP_ERROR_DRAINS: usize = 8;
const MAX_MCP_BODY_BYTES: usize = 2 * 1024 * 1024;
const MAX_FINISHED_TURNS: usize = 512;
const MAX_EARLY_ERROR_DRAIN_BYTES: usize = 4 * 1024 * 1024;
const UNKNOWN_LEN_ERROR_DRAIN_BYTES: usize = 512 * 1024;
const EARLY_ERROR_DRAIN_DEADLINE: Duration = Duration::from_millis(1500);
const EARLY_ERROR_DRAIN_SLICE: Duration = Duration::from_millis(50);
const HTTP_BODY_READ_DEADLINE: Duration = Duration::from_secs(15);
const STREAM_FLUSH_INTERVAL: Duration = Duration::from_secs(2);
const DEFAULT_WAIT_TIMEOUT_SECS: u64 = 30;
const MAX_WAIT_TIMEOUT_SECS: u64 = 60;
const SUPPORTED_PROTOCOL_VERSIONS: &[&str] = &["2025-06-18", "2025-03-26", "2024-11-05"];

const REMOTE_MCP_INSTRUCTIONS: &str = "\
AgentOS Remote MCP provides access to sessions managed by the local AgentOS Waku daemon. \
Use `list_projects` and `list_sessions` to discover workspaces and tasks, `get_session` or \
`get_messages` to read conversation history and check live status / pending approvals, \
`search_sessions` to search across transcripts, and (when enabled) `create_session`, \
`send_message`, `wait_for_turn`, and `cancel_session` to drive sessions. \
By default `send_message` returns immediately (`wait: false`) with `{ session_id, runtime_id, turn_id, message_id }`; \
pass `wait: true` or call `wait_for_turn` to wait up to 60s for completion or a pending approval.";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ScribeStreamPhase {
    Text,
    Reasoning,
    Activity,
}

#[derive(Clone, Debug)]
pub struct PendingPermissionRecord {
    pub runtime_id: Uuid,
    pub turn_id: Option<Uuid>,
    pub request_id: String,
    pub title: String,
    pub detail: String,
    pub options: Vec<PermissionOption>,
    pub created_at: u64,
}

#[derive(Clone, Debug)]
pub struct PendingUserInputRecord {
    pub runtime_id: Uuid,
    pub turn_id: Option<Uuid>,
    pub request_id: String,
    pub questions: Vec<UserInputQuestion>,
    pub created_at: u64,
}

#[derive(Clone, Debug)]
pub struct FinishedTurnRecord {
    pub session_id: Uuid,
    pub runtime_id: Uuid,
    pub turn_id: Uuid,
    pub success: bool,
    pub error: Option<String>,
}

#[derive(Clone, Debug)]
pub enum WaitOutcome {
    TurnFinished(FinishedTurnRecord),
    AwaitingPermission {
        snapshot: LiveSessionSnapshot,
        permissions: Vec<PendingPermissionRecord>,
    },
    AwaitingUserInput {
        snapshot: LiveSessionSnapshot,
        user_inputs: Vec<PendingUserInputRecord>,
    },
    Parked(LiveSessionSnapshot),
    SessionSettled(LiveSessionSnapshot),
    NoActiveRuntime,
    TimedOut {
        snapshot: Option<LiveSessionSnapshot>,
    },
}

#[derive(Clone, Debug)]
pub struct LiveSessionSnapshot {
    pub session_id: Uuid,
    pub runtime_id: Uuid,
    pub status: SessionStatus,
    pub active_turn_id: Option<Uuid>,
    pub last_finished_turn_id: Option<Uuid>,
    pub last_turn_success: Option<bool>,
    pub last_error: Option<String>,
    pub pending_permissions: Vec<PendingPermissionRecord>,
    pub pending_user_inputs: Vec<PendingUserInputRecord>,
    pub last_sequence: u64,
}

#[derive(Clone, Debug)]
struct LiveSessionEntry {
    runtime_id: Uuid,
    epoch: Uuid,
    last_sequence: u64,
    status: SessionStatus,
    active_turn_id: Option<Uuid>,
    last_finished_turn_id: Option<Uuid>,
    last_turn_success: Option<bool>,
    last_error: Option<String>,
    pending_permissions: Vec<PendingPermissionRecord>,
    pending_user_inputs: Vec<PendingUserInputRecord>,
    stream_phase: Option<ScribeStreamPhase>,
    last_flush: Instant,
    dirty_unflushed: bool,
}

impl LiveSessionEntry {
    fn snapshot(&self, session_id: Uuid) -> LiveSessionSnapshot {
        LiveSessionSnapshot {
            session_id,
            runtime_id: self.runtime_id,
            status: self.status,
            active_turn_id: self.active_turn_id,
            last_finished_turn_id: self.last_finished_turn_id,
            last_turn_success: self.last_turn_success,
            last_error: self.last_error.clone(),
            pending_permissions: self.pending_permissions.clone(),
            pending_user_inputs: self.pending_user_inputs.clone(),
            last_sequence: self.last_sequence,
        }
    }
}

#[derive(Clone, Debug)]
pub struct CreateSessionParams {
    pub project_id: Option<Uuid>,
    pub project_path: Option<PathBuf>,
    pub provider: Option<ProviderKind>,
    pub model: Option<String>,
    pub runtime_mode: Option<RuntimeMode>,
    pub title: Option<String>,
}

#[derive(Clone, Debug)]
pub struct SubmitPromptParams {
    pub prompt: String,
    pub steer: bool,
    pub idempotency_key: Option<String>,
}

#[derive(Clone, Debug)]
pub struct SubmitPromptOutcome {
    pub session_id: Uuid,
    pub runtime_id: Uuid,
    pub turn_id: Option<Uuid>,
    pub message_id: Option<Uuid>,
    pub status: String,
    pub accepted: bool,
    pub deduplicated: bool,
    pub detail: Option<String>,
}

/// Daemon capabilities needed by the Remote MCP endpoint and Event Scribe.
///
/// Implemented by `WakuBackend`; lightweight test backends inherit the default
/// `Backend::remote_mcp() -> None` unless they opt in.
pub trait RemoteMcpBackend: Send + Sync + 'static {
    fn remote_mcp_settings(&self) -> RemoteMcpSettings;

    fn list_projects_and_sessions(&self) -> anyhow::Result<(Vec<Project>, Vec<AgentSession>, PathBuf)>;

    fn hydrate_session_by_id(&self, session_id: Uuid) -> anyhow::Result<Option<AgentSession>>;

    fn resolve_legacy_session_id(&self, legacy_id: i64) -> anyhow::Result<Option<Uuid>>;

    fn search_session_messages(
        &self,
        query: String,
        limit: usize,
    ) -> anyhow::Result<Vec<SessionMessageMatch>>;

    fn create_session(&self, params: CreateSessionParams) -> anyhow::Result<(Project, AgentSession)>;

    fn submit_prompt_in_mailbox(
        &self,
        session_id: Uuid,
        active_runtime_id: &mut Option<Uuid>,
        hub: &Arc<Hub>,
        params: SubmitPromptParams,
    ) -> anyhow::Result<SubmitPromptOutcome>;

    fn cancel_session_in_mailbox(
        &self,
        session_id: Uuid,
        active_runtime_id: Option<Uuid>,
        hub: &Arc<Hub>,
    ) -> anyhow::Result<bool>;

    fn respond_permission_in_mailbox(
        &self,
        session_id: Uuid,
        runtime_id: Uuid,
        request_id: String,
        option_id: String,
        hub: &Arc<Hub>,
    ) -> anyhow::Result<()>;

    fn respond_user_input_in_mailbox(
        &self,
        session_id: Uuid,
        runtime_id: Uuid,
        request_id: String,
        answers: Vec<UserInputAnswer>,
        hub: &Arc<Hub>,
    ) -> anyhow::Result<()>;

    /// Apply one sequenced driver event to the daemon's authoritative
    /// `AgentSession`, flushing to SQLite when `flush_now` is true. Returns
    /// `Ok(true)` if sidebar/catalog metadata changed.
    fn persist_scribe_event(
        &self,
        event: &SequencedEvent,
        driver_event: &DriverEvent,
        stream_phase: &mut Option<ScribeStreamPhase>,
        flush_now: bool,
    ) -> anyhow::Result<bool>;

    fn flush_scribe_session(&self, session_id: Uuid) -> anyhow::Result<()>;

    fn active_runtime_for(&self, session_id: Uuid) -> Option<(Uuid, bool)>;
}

#[derive(Default)]
struct ScribeState {
    live: HashMap<Uuid, LiveSessionEntry>,
    finished_turns: VecDeque<FinishedTurnRecord>,
}

impl ScribeState {
    fn record_finished_turn(&mut self, record: FinishedTurnRecord) {
        self.finished_turns
            .retain(|existing| !(existing.session_id == record.session_id && existing.turn_id == record.turn_id));
        self.finished_turns.push_back(record);
        while self.finished_turns.len() > MAX_FINISHED_TURNS {
            self.finished_turns.pop_front();
        }
    }

    fn find_finished_turn(&self, session_id: Uuid, turn_id: Uuid) -> Option<FinishedTurnRecord> {
        self.finished_turns
            .iter()
            .rev()
            .find(|record| record.session_id == session_id && record.turn_id == turn_id)
            .cloned()
    }
}

/// Daemon-owned observer attached directly to `Hub::emit` / `Hub::end_runtime`.
///
/// Unlike WebSocket subscribers, the scribe receives every event through an
/// unbounded internal channel and is never dropped for backpressure.
pub struct DaemonEventScribe {
    state: StdMutex<ScribeState>,
    notify: Condvar,
}

impl Default for DaemonEventScribe {
    fn default() -> Self {
        Self {
            state: StdMutex::new(ScribeState::default()),
            notify: Condvar::new(),
        }
    }
}

impl DaemonEventScribe {
    pub fn spawn(
        observations: Receiver<HubObservation>,
        backend: Arc<dyn Backend>,
        hub: Arc<Hub>,
    ) -> Arc<Self> {
        let scribe = Arc::new(Self::default());
        let worker_scribe = scribe.clone();
        let _ = std::thread::Builder::new()
            .name("waku-daemon-scribe".into())
            .spawn(move || {
                worker_scribe.run_loop(observations, backend, hub);
            });
        scribe
    }

    fn run_loop(
        &self,
        observations: Receiver<HubObservation>,
        backend: Arc<dyn Backend>,
        hub: Arc<Hub>,
    ) {
        while let Ok(obs) = observations.recv() {
            match obs {
                HubObservation::Event(sequenced) => {
                    self.handle_event(&sequenced, &backend, &hub);
                }
                HubObservation::RuntimeEnded {
                    session_id,
                    runtime_id,
                } => {
                    let should_flush = {
                        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
                        let matches = state.live.get(&session_id).is_some_and(|entry| {
                            runtime_id.is_none_or(|rid| entry.runtime_id == rid)
                        });
                        if matches {
                            let removed = state.live.remove(&session_id);
                            if let Some(ref entry) = removed
                                && entry.status.is_busy()
                                && let Some(active_tid) = entry.active_turn_id
                            {
                                state.record_finished_turn(FinishedTurnRecord {
                                    session_id,
                                    runtime_id: entry.runtime_id,
                                    turn_id: active_tid,
                                    success: false,
                                    error: entry.last_error.clone().or_else(|| {
                                        Some("runtime ended before turn finished".into())
                                    }),
                                });
                            }
                            removed.is_some_and(|e| e.dirty_unflushed)
                        } else {
                            false
                        }
                    };
                    if should_flush
                        && let Some(mcp_backend) = backend.remote_mcp()
                    {
                        let _ = mcp_backend.flush_scribe_session(session_id);
                    }
                    self.notify.notify_all();
                }
            }
        }
    }

    fn handle_event(&self, sequenced: &SequencedEvent, backend: &Arc<dyn Backend>, hub: &Arc<Hub>) {
        let Ok(driver_event) = event_from_wire(sequenced.event.clone()) else {
            return;
        };
        let now = unix_now();
        let instant_now = Instant::now();

        let (mut stream_phase, flush_now, should_wake, deferred_terminal) = {
            let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
            let mut replaced_finished: Option<FinishedTurnRecord> = None;
            let mut deferred_terminal: Option<(SessionStatus, Option<FinishedTurnRecord>, Option<bool>)> =
                None;
            let mut wake = false;
            let entry = state
                .live
                .entry(sequenced.session_id)
                .and_modify(|existing| {
                    if existing.runtime_id != sequenced.runtime_id || existing.epoch != sequenced.epoch
                    {
                        if existing.status.is_busy()
                            && let Some(old_tid) = existing.active_turn_id.take()
                        {
                            replaced_finished = Some(FinishedTurnRecord {
                                session_id: sequenced.session_id,
                                runtime_id: existing.runtime_id,
                                turn_id: old_tid,
                                success: false,
                                error: existing.last_error.clone().or_else(|| {
                                    Some("runtime replaced before turn finished".into())
                                }),
                            });
                            wake = true;
                        }
                        existing.runtime_id = sequenced.runtime_id;
                        existing.epoch = sequenced.epoch;
                        existing.status = SessionStatus::Idle;
                        existing.active_turn_id = None;
                        existing.pending_permissions.clear();
                        existing.pending_user_inputs.clear();
                        existing.stream_phase = None;
                        existing.last_error = None;
                    }
                })
                .or_insert_with(|| LiveSessionEntry {
                    runtime_id: sequenced.runtime_id,
                    epoch: sequenced.epoch,
                    last_sequence: 0,
                    status: SessionStatus::Idle,
                    active_turn_id: None,
                    last_finished_turn_id: None,
                    last_turn_success: None,
                    last_error: None,
                    pending_permissions: Vec::new(),
                    pending_user_inputs: Vec::new(),
                    stream_phase: None,
                    last_flush: instant_now,
                    dirty_unflushed: false,
                });

            entry.last_sequence = sequenced.sequence;
            let mut immediate_flush = false;

            match &driver_event {
                DriverEvent::Connected { .. } => {
                    entry.last_error = None;
                    if entry.status == SessionStatus::Connecting {
                        entry.status = SessionStatus::Working;
                    }
                    immediate_flush = true;
                }
                DriverEvent::PromptSubmitted { turn_id, .. } => {
                    entry.active_turn_id = Some(*turn_id);
                    entry.status = SessionStatus::Connecting;
                    entry.last_error = None;
                    entry.pending_permissions.clear();
                    entry.pending_user_inputs.clear();
                    immediate_flush = true;
                    wake = true;
                }
                DriverEvent::TurnStarted => {
                    entry.status = SessionStatus::Working;
                    entry.last_error = None;
                    immediate_flush = true;
                    wake = true;
                }
                DriverEvent::AutoTitleUpdated(_) => {
                    immediate_flush = true;
                }
                DriverEvent::Permission {
                    request_id,
                    title,
                    detail,
                    options,
                } => {
                    entry.status = SessionStatus::Waiting;
                    entry
                        .pending_permissions
                        .retain(|item| item.request_id != *request_id);
                    entry.pending_permissions.push(PendingPermissionRecord {
                        runtime_id: sequenced.runtime_id,
                        turn_id: entry.active_turn_id,
                        request_id: request_id.clone(),
                        title: title.clone(),
                        detail: detail.clone(),
                        options: options.clone(),
                        created_at: now,
                    });
                    immediate_flush = true;
                    wake = true;
                }
                DriverEvent::UserInputRequested {
                    request_id,
                    questions,
                } => {
                    if !questions.is_empty() {
                        entry.status = SessionStatus::Waiting;
                        entry
                            .pending_user_inputs
                            .retain(|item| item.request_id != *request_id);
                        entry.pending_user_inputs.push(PendingUserInputRecord {
                            runtime_id: sequenced.runtime_id,
                            turn_id: entry.active_turn_id,
                            request_id: request_id.clone(),
                            questions: questions.clone(),
                            created_at: now,
                        });
                        immediate_flush = true;
                        wake = true;
                    }
                }
                DriverEvent::TurnParked => {
                    entry.status = SessionStatus::Background;
                    entry.stream_phase = None;
                    immediate_flush = true;
                    wake = true;
                }
                DriverEvent::TurnFinished { success, .. } => {
                    // Defer publishing terminal status + FinishedTurnRecord until
                    // after `persist_scribe_event` commits the final reply so no
                    // waiter can hydrate a partial/empty transcript.
                    let terminal_status = if *success {
                        SessionStatus::Idle
                    } else {
                        SessionStatus::Failed
                    };
                    let record = entry.active_turn_id.map(|tid| FinishedTurnRecord {
                        session_id: sequenced.session_id,
                        runtime_id: sequenced.runtime_id,
                        turn_id: tid,
                        success: *success,
                        error: if *success {
                            None
                        } else {
                            entry.last_error.clone()
                        },
                    });
                    deferred_terminal = Some((terminal_status, record, Some(*success)));
                    entry.pending_permissions.clear();
                    entry.pending_user_inputs.clear();
                    entry.stream_phase = None;
                    immediate_flush = true;
                    wake = true;
                }
                DriverEvent::Error(err) => {
                    entry.last_error = Some(err.clone());
                    immediate_flush = true;
                    wake = true;
                }
                DriverEvent::ProcessExited => {
                    if entry.status.is_busy() {
                        let record = entry.active_turn_id.map(|tid| FinishedTurnRecord {
                            session_id: sequenced.session_id,
                            runtime_id: sequenced.runtime_id,
                            turn_id: tid,
                            success: false,
                            error: entry.last_error.clone().or_else(|| {
                                Some("agent process exited before turn finished".into())
                            }),
                        });
                        deferred_terminal = Some((SessionStatus::Failed, record, Some(false)));
                    }
                    entry.pending_permissions.clear();
                    entry.pending_user_inputs.clear();
                    entry.stream_phase = None;
                    immediate_flush = true;
                    wake = true;
                }
                DriverEvent::SteerAccepted { .. } => {
                    immediate_flush = true;
                }
                _ => {}
            }

            let periodic_flush = !immediate_flush
                && entry.dirty_unflushed
                && instant_now.duration_since(entry.last_flush) >= STREAM_FLUSH_INTERVAL;
            let flush = immediate_flush || periodic_flush;
            if flush {
                entry.last_flush = instant_now;
                entry.dirty_unflushed = false;
            } else {
                entry.dirty_unflushed = true;
            }
            let phase = entry.stream_phase;
            if let Some(record) = replaced_finished {
                state.record_finished_turn(record);
            }
            (phase, flush, wake, deferred_terminal)
        };

        if let Some(mcp_backend) = backend.remote_mcp()
            && let Ok(catalog_changed) = mcp_backend.persist_scribe_event(
                sequenced,
                &driver_event,
                &mut stream_phase,
                flush_now,
            )
            && catalog_changed
        {
            hub.broadcast_task_state_changed_all();
        }

        {
            let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
            if let Some(entry) = state.live.get_mut(&sequenced.session_id)
                && entry.runtime_id == sequenced.runtime_id
                && entry.epoch == sequenced.epoch
            {
                entry.stream_phase = stream_phase;
                if let Some((terminal_status, ref record, last_success)) = deferred_terminal {
                    entry.status = terminal_status;
                    if let Some(rec) = record {
                        if entry.active_turn_id == Some(rec.turn_id) {
                            entry.active_turn_id = None;
                        }
                        entry.last_finished_turn_id = Some(rec.turn_id);
                    }
                    if let Some(ok) = last_success {
                        entry.last_turn_success = Some(ok);
                    }
                }
            }
            if let Some((_, Some(record), _)) = deferred_terminal {
                state.record_finished_turn(record);
            }
        }

        if should_wake {
            self.notify.notify_all();
        }
    }

    pub fn clear_pending_request(&self, session_id: Uuid, request_id: &str) {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(entry) = state.live.get_mut(&session_id) {
            let before = entry.pending_permissions.len() + entry.pending_user_inputs.len();
            entry
                .pending_permissions
                .retain(|item| item.request_id != request_id);
            entry
                .pending_user_inputs
                .retain(|item| item.request_id != request_id);
            let after = entry.pending_permissions.len() + entry.pending_user_inputs.len();
            if after < before && after == 0 && entry.status == SessionStatus::Waiting {
                entry.status = SessionStatus::Working;
            }
        }
        self.notify.notify_all();
    }

    pub fn snapshot(&self, session_id: Uuid) -> Option<LiveSessionSnapshot> {
        self.state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .live
            .get(&session_id)
            .map(|entry| entry.snapshot(session_id))
    }

    pub fn find_finished_turn(&self, session_id: Uuid, turn_id: Uuid) -> Option<FinishedTurnRecord> {
        self.state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .find_finished_turn(session_id, turn_id)
    }

    pub fn all_pending_requests(
        &self,
    ) -> Vec<(Uuid, Vec<PendingPermissionRecord>, Vec<PendingUserInputRecord>)> {
        self.state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .live
            .iter()
            .filter(|(_, entry)| {
                !entry.pending_permissions.is_empty() || !entry.pending_user_inputs.is_empty()
            })
            .map(|(sid, entry)| {
                (
                    *sid,
                    entry.pending_permissions.clone(),
                    entry.pending_user_inputs.clone(),
                )
            })
            .collect()
    }

    /// Block until the given session's turn reaches a terminal or blocked state
    /// (`Idle`, `Failed`, `Background`, `Waiting` with matching pending requests,
    /// or `turn_id` completed), or until `timeout` elapses.
    pub fn wait_for_turn(
        &self,
        session_id: Uuid,
        target_turn_id: Option<Uuid>,
        timeout: Duration,
    ) -> WaitOutcome {
        let deadline = Instant::now() + timeout;
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        loop {
            if let Some(outcome) = evaluate_wait_state(&state, session_id, target_turn_id) {
                return outcome;
            }
            let now = Instant::now();
            if now >= deadline {
                return WaitOutcome::TimedOut {
                    snapshot: state.live.get(&session_id).map(|e| e.snapshot(session_id)),
                };
            }
            let remaining = deadline.saturating_duration_since(now);
            let (guard, timeout_res) = self
                .notify
                .wait_timeout(state, remaining.min(Duration::from_millis(500)))
                .unwrap_or_else(|e| e.into_inner());
            state = guard;
            if timeout_res.timed_out() && Instant::now() >= deadline {
                if let Some(outcome) = evaluate_wait_state(&state, session_id, target_turn_id) {
                    return outcome;
                }
                return WaitOutcome::TimedOut {
                    snapshot: state.live.get(&session_id).map(|e| e.snapshot(session_id)),
                };
            }
        }
    }
}

fn evaluate_wait_state(
    state: &ScribeState,
    session_id: Uuid,
    target_turn_id: Option<Uuid>,
) -> Option<WaitOutcome> {
    if let Some(target) = target_turn_id {
        if let Some(record) = state.find_finished_turn(session_id, target) {
            return Some(WaitOutcome::TurnFinished(record));
        }
        if let Some(entry) = state.live.get(&session_id)
            && entry.active_turn_id == Some(target)
        {
            let matching_permissions: Vec<PendingPermissionRecord> = entry
                .pending_permissions
                .iter()
                .filter(|p| p.turn_id.is_none() || p.turn_id == Some(target))
                .cloned()
                .collect();
            if !matching_permissions.is_empty() {
                return Some(WaitOutcome::AwaitingPermission {
                    snapshot: entry.snapshot(session_id),
                    permissions: matching_permissions,
                });
            }
            let matching_inputs: Vec<PendingUserInputRecord> = entry
                .pending_user_inputs
                .iter()
                .filter(|q| q.turn_id.is_none() || q.turn_id == Some(target))
                .cloned()
                .collect();
            if !matching_inputs.is_empty() {
                return Some(WaitOutcome::AwaitingUserInput {
                    snapshot: entry.snapshot(session_id),
                    user_inputs: matching_inputs,
                });
            }
            if entry.status == SessionStatus::Background {
                return Some(WaitOutcome::Parked(entry.snapshot(session_id)));
            }
            if entry.status == SessionStatus::Failed {
                return Some(WaitOutcome::TurnFinished(FinishedTurnRecord {
                    session_id,
                    runtime_id: entry.runtime_id,
                    turn_id: target,
                    success: false,
                    error: entry.last_error.clone(),
                }));
            }
        }
        return None;
    }

    let Some(entry) = state.live.get(&session_id) else {
        return Some(WaitOutcome::NoActiveRuntime);
    };
    if !entry.pending_permissions.is_empty() {
        return Some(WaitOutcome::AwaitingPermission {
            snapshot: entry.snapshot(session_id),
            permissions: entry.pending_permissions.clone(),
        });
    }
    if !entry.pending_user_inputs.is_empty() {
        return Some(WaitOutcome::AwaitingUserInput {
            snapshot: entry.snapshot(session_id),
            user_inputs: entry.pending_user_inputs.clone(),
        });
    }
    match entry.status {
        SessionStatus::Background => Some(WaitOutcome::Parked(entry.snapshot(session_id))),
        SessionStatus::Idle | SessionStatus::Failed => {
            Some(WaitOutcome::SessionSettled(entry.snapshot(session_id)))
        }
        SessionStatus::Connecting | SessionStatus::Working | SessionStatus::Waiting => None,
    }
}

#[derive(Default)]
pub struct McpBudget {
    active_connections: AtomicUsize,
    active_waits: AtomicUsize,
    active_error_drains: AtomicUsize,
}

#[derive(Clone, Copy)]
enum BudgetKind {
    Connection,
    Wait,
    ErrorDrain,
}

impl McpBudget {
    fn try_acquire_connection(self: &Arc<Self>) -> Option<BudgetGuard> {
        self.active_connections
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| {
                (n < MAX_MCP_CONNECTIONS).then_some(n + 1)
            })
            .ok()
            .map(|_| BudgetGuard {
                counter: self.clone(),
                kind: BudgetKind::Connection,
            })
    }

    fn try_acquire_wait(self: &Arc<Self>) -> Option<BudgetGuard> {
        self.active_waits
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| {
                (n < MAX_MCP_CONCURRENT_WAITS).then_some(n + 1)
            })
            .ok()
            .map(|_| BudgetGuard {
                counter: self.clone(),
                kind: BudgetKind::Wait,
            })
    }

    fn try_acquire_error_drain(self: &Arc<Self>) -> Option<BudgetGuard> {
        self.active_error_drains
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| {
                (n < MAX_MCP_ERROR_DRAINS).then_some(n + 1)
            })
            .ok()
            .map(|_| BudgetGuard {
                counter: self.clone(),
                kind: BudgetKind::ErrorDrain,
            })
    }
}

struct BudgetGuard {
    counter: Arc<McpBudget>,
    kind: BudgetKind,
}

impl Drop for BudgetGuard {
    fn drop(&mut self) {
        match self.kind {
            BudgetKind::Connection => {
                self.counter
                    .active_connections
                    .fetch_sub(1, Ordering::AcqRel);
            }
            BudgetKind::Wait => {
                self.counter.active_waits.fetch_sub(1, Ordering::AcqRel);
            }
            BudgetKind::ErrorDrain => {
                self.counter
                    .active_error_drains
                    .fetch_sub(1, Ordering::AcqRel);
            }
        }
    }
}

pub(crate) struct RemoteMcpHttpContext<'a> {
    pub backend: Arc<dyn Backend>,
    pub dispatcher: Arc<RequestDispatcher>,
    pub hub: Arc<Hub>,
    pub scribe: Arc<DaemonEventScribe>,
    pub budget: Arc<McpBudget>,
    pub options: &'a ServerOptions,
}

struct ParsedHeaders {
    authorization: Option<String>,
    origin: Option<String>,
    host: Option<String>,
    mcp_session_id: Option<String>,
    expect_continue: bool,
    content_length: Option<usize>,
    content_length_count: usize,
    transfer_encoding_chunked: bool,
    transfer_encoding_present: bool,
}

impl ParsedHeaders {
    fn parse(header_text: &str) -> Result<Self, &'static str> {
        let mut parsed = Self {
            authorization: None,
            origin: None,
            host: None,
            mcp_session_id: None,
            expect_continue: false,
            content_length: None,
            content_length_count: 0,
            transfer_encoding_chunked: false,
            transfer_encoding_present: false,
        };
        for line in header_text.split("\r\n").skip(1) {
            if line.is_empty() {
                break;
            }
            let Some((raw_name, raw_value)) = line.split_once(':') else {
                continue;
            };
            let name = raw_name.trim();
            let value = raw_value.trim();
            if name.eq_ignore_ascii_case("authorization") {
                parsed.authorization = Some(value.to_owned());
            } else if name.eq_ignore_ascii_case("origin") {
                parsed.origin = Some(value.to_owned());
            } else if name.eq_ignore_ascii_case("host") {
                parsed.host = Some(value.to_owned());
            } else if name.eq_ignore_ascii_case("mcp-session-id") {
                if !value.is_empty() && value.bytes().all(|b| b.is_ascii_graphic()) {
                    parsed.mcp_session_id = Some(value.to_owned());
                }
            } else if name.eq_ignore_ascii_case("expect") {
                if value.eq_ignore_ascii_case("100-continue") {
                    parsed.expect_continue = true;
                }
            } else if name.eq_ignore_ascii_case("content-length") {
                parsed.content_length_count += 1;
                let len = value
                    .parse::<usize>()
                    .map_err(|_| "invalid Content-Length header")?;
                if let Some(prev) = parsed.content_length
                    && prev != len
                {
                    return Err("conflicting Content-Length headers");
                }
                parsed.content_length = Some(len);
            } else if name.eq_ignore_ascii_case("transfer-encoding") {
                parsed.transfer_encoding_present = true;
                if value
                    .split(',')
                    .any(|v| v.trim().eq_ignore_ascii_case("chunked"))
                {
                    parsed.transfer_encoding_chunked = true;
                }
            }
        }
        if parsed.transfer_encoding_present && parsed.content_length_count > 0 {
            return Err("ambiguous request framing: both Transfer-Encoding and Content-Length");
        }
        Ok(parsed)
    }
}

pub(crate) fn is_well_known_path(path: &str) -> bool {
    path == "/.well-known" || path.starts_with("/.well-known/")
}

pub(crate) fn is_remote_mcp_path(path: &str) -> bool {
    path == REMOTE_MCP_PATH || path == "/mcp/"
}

/// Handle a plain HTTP request targeting `/mcp` (or `/.well-known/*`).
/// The caller has already read and consumed the HTTP header block from `stream`.
pub(crate) fn handle_mcp_http_request(
    stream: &mut TcpStream,
    method: &str,
    raw_target: &str,
    header_text: &str,
    ctx: RemoteMcpHttpContext<'_>,
) -> anyhow::Result<()> {
    let raw_path = raw_target.split('?').next().unwrap_or("/");
    if is_well_known_path(raw_path) {
        let drain = if matches!(method, "GET" | "HEAD" | "OPTIONS" | "DELETE") {
            EarlyDrainHint::None
        } else {
            EarlyDrainHint::Unknown
        };
        write_mcp_error_and_drain(
            stream,
            "404 Not Found",
            "application/json; charset=utf-8",
            br#"{"error":"not_found"}"#,
            &[],
            method == "HEAD",
            drain,
            &ctx.budget,
        )?;
        return Ok(());
    }

    let headers = match ParsedHeaders::parse(header_text) {
        Ok(h) => h,
        Err(reason) => {
            let body = json!({ "error": reason }).to_string();
            let drain = if matches!(method, "GET" | "HEAD" | "OPTIONS" | "DELETE") {
                EarlyDrainHint::None
            } else {
                EarlyDrainHint::Unknown
            };
            write_mcp_error_and_drain(
                stream,
                "400 Bad Request",
                "application/json; charset=utf-8",
                body.as_bytes(),
                &[],
                false,
                drain,
                &ctx.budget,
            )?;
            return Ok(());
        }
    };
    let early_drain = EarlyDrainHint::from_headers(method, &headers);

    // Origin validation (DNS-rebinding protection per MCP Streamable HTTP spec).
    let mut cors_headers: Vec<(&str, String)> = Vec::new();
    if let Some(ref origin) = headers.origin {
        if !is_origin_allowed(origin, headers.host.as_deref(), ctx.options) {
            write_mcp_error_and_drain(
                stream,
                "403 Forbidden",
                "application/json; charset=utf-8",
                br#"{"error":"origin is not allowed"}"#,
                &[],
                false,
                early_drain,
                &ctx.budget,
            )?;
            return Ok(());
        }
        cors_headers.push(("Access-Control-Allow-Origin", origin.clone()));
        cors_headers.push(("Access-Control-Expose-Headers", "Mcp-Session-Id".into()));
        cors_headers.push(("Vary", "Origin".into()));
    }

    if method == "OPTIONS" {
        let mut extra: Vec<(&str, &str)> = vec![
            ("Allow", "POST, DELETE, OPTIONS"),
            ("Access-Control-Allow-Methods", "POST, DELETE, OPTIONS"),
            (
                "Access-Control-Allow-Headers",
                "Authorization, Content-Type, Accept, Mcp-Session-Id, MCP-Protocol-Version",
            ),
            ("Access-Control-Max-Age", "600"),
        ];
        for (k, v) in &cors_headers {
            extra.push((*k, v.as_str()));
        }
        write_mcp_response(stream, "204 No Content", "text/plain; charset=utf-8", b"", &extra, true)?;
        return Ok(());
    }

    let Some(mcp_backend) = ctx.backend.remote_mcp() else {
        write_mcp_error_and_drain(
            stream,
            "503 Service Unavailable",
            "application/json; charset=utf-8",
            br#"{"error":"remote MCP backend is unavailable"}"#,
            &[],
            false,
            early_drain,
            &ctx.budget,
        )?;
        return Ok(());
    };

    let settings = mcp_backend.remote_mcp_settings();
    if !settings.enabled || settings.token.trim().is_empty() {
        let extra: Vec<(&str, &str)> = cors_headers.iter().map(|(k, v)| (*k, v.as_str())).collect();
        write_mcp_error_and_drain(
            stream,
            "403 Forbidden",
            "application/json; charset=utf-8",
            br#"{"error":"remote MCP is disabled in AgentOS settings"}"#,
            &extra,
            method == "HEAD",
            early_drain,
            &ctx.budget,
        )?;
        return Ok(());
    }

    // Reject query-parameter tokens explicitly so secrets never end up in URLs/logs.
    if let Some((_, query)) = raw_target.split_once('?')
        && query.split('&').any(|pair| {
            let key = pair.split('=').next().unwrap_or_default();
            key.eq_ignore_ascii_case("token")
                || key.eq_ignore_ascii_case("access_token")
                || key.eq_ignore_ascii_case("api_key")
        })
    {
        let mut extra: Vec<(&str, &str)> =
            vec![("WWW-Authenticate", r#"Bearer realm="agentos-mcp""#)];
        for (k, v) in &cors_headers {
            extra.push((*k, v.as_str()));
        }
        write_mcp_error_and_drain(
            stream,
            "401 Unauthorized",
            "application/json; charset=utf-8",
            br#"{"error":"query-parameter tokens are not accepted; use Authorization: Bearer <token>"}"#,
            &extra,
            method == "HEAD",
            early_drain,
            &ctx.budget,
        )?;
        return Ok(());
    }

    let authorized = headers
        .authorization
        .as_deref()
        .and_then(extract_bearer_token)
        .is_some_and(|candidate| token_matches(settings.token.trim(), candidate));
    if !authorized {
        let mut extra: Vec<(&str, &str)> =
            vec![("WWW-Authenticate", r#"Bearer realm="agentos-mcp""#)];
        for (k, v) in &cors_headers {
            extra.push((*k, v.as_str()));
        }
        write_mcp_error_and_drain(
            stream,
            "401 Unauthorized",
            "application/json; charset=utf-8",
            br#"{"error":"unauthorized; valid Authorization: Bearer <token> header required"}"#,
            &extra,
            method == "HEAD",
            early_drain,
            &ctx.budget,
        )?;
        return Ok(());
    }

    // Acquire a concurrency slot only after auth/origin checks so unauthenticated
    // callers (and their early-error body drains) cannot exhaust MCP slots.
    let Some(conn_permit) = ctx.budget.try_acquire_connection() else {
        let mut extra: Vec<(&str, &str)> = vec![("Retry-After", "2")];
        for (k, v) in &cors_headers {
            extra.push((*k, v.as_str()));
        }
        write_mcp_error_and_drain(
            stream,
            "503 Service Unavailable",
            "application/json; charset=utf-8",
            br#"{"error":"remote MCP concurrency limit reached; retry shortly"}"#,
            &extra,
            method == "HEAD",
            early_drain,
            &ctx.budget,
        )?;
        return Ok(());
    };

    let session_header_value = headers
        .mcp_session_id
        .unwrap_or_else(|| Uuid::new_v4().to_string());

    match method {
        "GET" | "HEAD" => {
            // Per MCP Streamable HTTP 2025-03-26, servers that do not expose
            // unsolicited server-initiated SSE streams MUST return 405.
            let mut extra: Vec<(&str, &str)> = vec![
                ("Allow", "POST, DELETE, OPTIONS"),
                ("Mcp-Session-Id", session_header_value.as_str()),
            ];
            for (k, v) in &cors_headers {
                extra.push((*k, v.as_str()));
            }
            drop(conn_permit);
            write_mcp_response(
                stream,
                "405 Method Not Allowed",
                "application/json; charset=utf-8",
                br#"{"error":"GET SSE stream is not enabled; send JSON-RPC requests via POST /mcp"}"#,
                &extra,
                method == "HEAD",
            )?;
            return Ok(());
        }
        "DELETE" => {
            let mut extra: Vec<(&str, &str)> =
                vec![("Mcp-Session-Id", session_header_value.as_str())];
            for (k, v) in &cors_headers {
                extra.push((*k, v.as_str()));
            }
            drop(conn_permit);
            write_mcp_response(
                stream,
                "204 No Content",
                "text/plain; charset=utf-8",
                b"",
                &extra,
                true,
            )?;
            return Ok(());
        }
        "POST" => {}
        _ => {
            let mut extra: Vec<(&str, &str)> = vec![("Allow", "POST, DELETE, OPTIONS")];
            for (k, v) in &cors_headers {
                extra.push((*k, v.as_str()));
            }
            drop(conn_permit);
            write_mcp_error_and_drain(
                stream,
                "405 Method Not Allowed",
                "application/json; charset=utf-8",
                br#"{"error":"method not allowed"}"#,
                &extra,
                false,
                early_drain,
                &ctx.budget,
            )?;
            return Ok(());
        }
    }

    // Validate body framing before optionally sending 100-continue.
    if let Some(len) = headers.content_length
        && len > MAX_MCP_BODY_BYTES
    {
        let extra: Vec<(&str, &str)> = cors_headers.iter().map(|(k, v)| (*k, v.as_str())).collect();
        drop(conn_permit);
        write_mcp_error_and_drain(
            stream,
            "413 Payload Too Large",
            "application/json; charset=utf-8",
            br#"{"error":"request body exceeds 2 MiB limit"}"#,
            &extra,
            false,
            early_drain,
            &ctx.budget,
        )?;
        return Ok(());
    }
    if headers.transfer_encoding_present && !headers.transfer_encoding_chunked {
        let extra: Vec<(&str, &str)> = cors_headers.iter().map(|(k, v)| (*k, v.as_str())).collect();
        drop(conn_permit);
        write_mcp_error_and_drain(
            stream,
            "501 Not Implemented",
            "application/json; charset=utf-8",
            br#"{"error":"unsupported Transfer-Encoding"}"#,
            &extra,
            false,
            early_drain,
            &ctx.budget,
        )?;
        return Ok(());
    }
    if !headers.transfer_encoding_chunked && headers.content_length.is_none() {
        let extra: Vec<(&str, &str)> = cors_headers.iter().map(|(k, v)| (*k, v.as_str())).collect();
        drop(conn_permit);
        write_mcp_error_and_drain(
            stream,
            "411 Length Required",
            "application/json; charset=utf-8",
            br#"{"error":"Content-Length or chunked Transfer-Encoding required"}"#,
            &extra,
            false,
            early_drain,
            &ctx.budget,
        )?;
        return Ok(());
    }

    if headers.expect_continue {
        stream.write_all(b"HTTP/1.1 100 Continue\r\n\r\n")?;
        stream.flush()?;
    }

    let body = match read_http_body(
        stream,
        headers.content_length,
        headers.transfer_encoding_chunked,
        HTTP_BODY_READ_DEADLINE,
    ) {
        Ok(bytes) => bytes,
        Err(BodyReadError::TooLarge) => {
            let extra: Vec<(&str, &str)> =
                cors_headers.iter().map(|(k, v)| (*k, v.as_str())).collect();
            drop(conn_permit);
            write_mcp_error_and_drain(
                stream,
                "413 Payload Too Large",
                "application/json; charset=utf-8",
                br#"{"error":"request body exceeds 2 MiB limit"}"#,
                &extra,
                false,
                EarlyDrainHint::Unknown,
                &ctx.budget,
            )?;
            return Ok(());
        }
        Err(BodyReadError::Timeout) => {
            let extra: Vec<(&str, &str)> =
                cors_headers.iter().map(|(k, v)| (*k, v.as_str())).collect();
            drop(conn_permit);
            write_mcp_response(
                stream,
                "408 Request Timeout",
                "application/json; charset=utf-8",
                br#"{"error":"request body read timed out"}"#,
                &extra,
                false,
            )?;
            return Ok(());
        }
        Err(BodyReadError::Malformed(msg)) => {
            let extra: Vec<(&str, &str)> =
                cors_headers.iter().map(|(k, v)| (*k, v.as_str())).collect();
            let body = json!({ "error": msg }).to_string();
            drop(conn_permit);
            write_mcp_error_and_drain(
                stream,
                "400 Bad Request",
                "application/json; charset=utf-8",
                body.as_bytes(),
                &extra,
                false,
                EarlyDrainHint::Unknown,
                &ctx.budget,
            )?;
            return Ok(());
        }
    };

    let _conn_permit = conn_permit;

    let payload: Value = match serde_json::from_slice(&body) {
        Ok(value) => value,
        Err(error) => {
            let mut extra: Vec<(&str, &str)> =
                vec![("Mcp-Session-Id", session_header_value.as_str())];
            for (k, v) in &cors_headers {
                extra.push((*k, v.as_str()));
            }
            let resp = json!({
                "jsonrpc": "2.0",
                "id": Value::Null,
                "error": { "code": -32700, "message": format!("Parse error: {error}") }
            })
            .to_string();
            write_mcp_response(
                stream,
                "400 Bad Request",
                "application/json; charset=utf-8",
                resp.as_bytes(),
                &extra,
                false,
            )?;
            return Ok(());
        }
    };

    let mut extra: Vec<(&str, &str)> = vec![("Mcp-Session-Id", session_header_value.as_str())];
    for (k, v) in &cors_headers {
        extra.push((*k, v.as_str()));
    }

    match payload {
        Value::Array(items) => {
            if items.is_empty() {
                let resp = json!({
                    "jsonrpc": "2.0",
                    "id": Value::Null,
                    "error": { "code": -32600, "message": "Invalid Request: empty JSON-RPC batch" }
                })
                .to_string();
                write_mcp_response(
                    stream,
                    "400 Bad Request",
                    "application/json; charset=utf-8",
                    resp.as_bytes(),
                    &extra,
                    false,
                )?;
                return Ok(());
            }
            let mut responses = Vec::new();
            for item in &items {
                if let Some(resp) = dispatch_remote_mcp_rpc(item, mcp_backend, &settings, &ctx) {
                    responses.push(resp);
                }
            }
            if responses.is_empty() {
                write_mcp_response(
                    stream,
                    "202 Accepted",
                    "application/json; charset=utf-8",
                    b"",
                    &extra,
                    true,
                )?;
            } else {
                let body = serde_json::to_vec(&Value::Array(responses))?;
                write_mcp_response(
                    stream,
                    "200 OK",
                    "application/json; charset=utf-8",
                    &body,
                    &extra,
                    false,
                )?;
            }
        }
        Value::Object(_) => {
            if let Some(resp) = dispatch_remote_mcp_rpc(&payload, mcp_backend, &settings, &ctx) {
                let body = serde_json::to_vec(&resp)?;
                write_mcp_response(
                    stream,
                    "200 OK",
                    "application/json; charset=utf-8",
                    &body,
                    &extra,
                    false,
                )?;
            } else {
                write_mcp_response(
                    stream,
                    "202 Accepted",
                    "application/json; charset=utf-8",
                    b"",
                    &extra,
                    true,
                )?;
            }
        }
        _ => {
            let resp = json!({
                "jsonrpc": "2.0",
                "id": Value::Null,
                "error": { "code": -32600, "message": "Invalid Request: expected JSON object or array" }
            })
            .to_string();
            write_mcp_response(
                stream,
                "400 Bad Request",
                "application/json; charset=utf-8",
                resp.as_bytes(),
                &extra,
                false,
            )?;
        }
    }

    Ok(())
}

fn is_origin_allowed(origin: &str, host_header: Option<&str>, options: &ServerOptions) -> bool {
    if options.allowed_origins.contains(origin) {
        return true;
    }
    let Ok(parsed) = url::Url::parse(origin) else {
        return false;
    };
    let is_loopback_origin = parsed
        .host_str()
        .is_some_and(|h| h.eq_ignore_ascii_case("localhost") || h == "127.0.0.1" || h == "[::1]");
    if !is_loopback_origin {
        return false;
    }
    let header_val = host_header.and_then(|h| tungstenite::http::HeaderValue::from_str(h).ok());
    origin_matches_host(origin, header_val.as_ref())
}

fn extract_bearer_token(header: &str) -> Option<&str> {
    let (scheme, token) = header.trim().split_once(' ')?;
    if !scheme.eq_ignore_ascii_case("bearer") {
        return None;
    }
    let token = token.trim();
    (!token.is_empty()).then_some(token)
}

enum BodyReadError {
    TooLarge,
    Timeout,
    Malformed(String),
}

fn read_http_body(
    stream: &mut TcpStream,
    content_length: Option<usize>,
    chunked: bool,
    timeout: Duration,
) -> Result<Vec<u8>, BodyReadError> {
    let deadline = Instant::now() + timeout;
    if let Some(len) = content_length {
        if len > MAX_MCP_BODY_BYTES {
            return Err(BodyReadError::TooLarge);
        }
        let mut out = vec![0u8; len];
        let mut read_total = 0;
        while read_total < len {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(BodyReadError::Timeout);
            }
            let _ = stream.set_read_timeout(Some(remaining.min(Duration::from_secs(1))));
            match stream.read(&mut out[read_total..]) {
                Ok(0) => {
                    return Err(BodyReadError::Malformed(
                        "unexpected EOF while reading request body".into(),
                    ));
                }
                Ok(n) => read_total += n,
                Err(e)
                    if matches!(
                        e.kind(),
                        io::ErrorKind::WouldBlock
                            | io::ErrorKind::TimedOut
                            | io::ErrorKind::Interrupted
                    ) =>
                {
                    if Instant::now() >= deadline {
                        return Err(BodyReadError::Timeout);
                    }
                }
                Err(e) => return Err(BodyReadError::Malformed(e.to_string())),
            }
        }
        return Ok(out);
    }

    if chunked {
        return read_chunked_body(stream, deadline);
    }

    Ok(Vec::new())
}

fn read_chunked_body(stream: &mut TcpStream, deadline: Instant) -> Result<Vec<u8>, BodyReadError> {
    let mut out = Vec::new();
    loop {
        let size_line = read_crlf_line(stream, deadline, 128)?;
        let size_hex = size_line.split(';').next().unwrap_or_default().trim();
        let chunk_size = usize::from_str_radix(size_hex, 16)
            .map_err(|_| BodyReadError::Malformed("invalid chunk size".into()))?;
        if chunk_size == 0 {
            // Consume trailing headers until empty CRLF line.
            loop {
                let trailer = read_crlf_line(stream, deadline, 1024)?;
                if trailer.is_empty() {
                    break;
                }
            }
            return Ok(out);
        }
        if out.len().saturating_add(chunk_size) > MAX_MCP_BODY_BYTES {
            return Err(BodyReadError::TooLarge);
        }
        let start = out.len();
        out.resize(start + chunk_size, 0);
        let mut read_total = 0;
        while read_total < chunk_size {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(BodyReadError::Timeout);
            }
            let _ = stream.set_read_timeout(Some(remaining.min(Duration::from_secs(1))));
            match stream.read(&mut out[start + read_total..]) {
                Ok(0) => {
                    return Err(BodyReadError::Malformed(
                        "unexpected EOF inside chunked body".into(),
                    ));
                }
                Ok(n) => read_total += n,
                Err(e)
                    if matches!(
                        e.kind(),
                        io::ErrorKind::WouldBlock
                            | io::ErrorKind::TimedOut
                            | io::ErrorKind::Interrupted
                    ) =>
                {
                    if Instant::now() >= deadline {
                        return Err(BodyReadError::Timeout);
                    }
                }
                Err(e) => return Err(BodyReadError::Malformed(e.to_string())),
            }
        }
        let crlf = read_crlf_line(stream, deadline, 4)?;
        if !crlf.is_empty() {
            return Err(BodyReadError::Malformed("missing CRLF after chunk".into()));
        }
    }
}

fn read_crlf_line(
    stream: &mut TcpStream,
    deadline: Instant,
    max_len: usize,
) -> Result<String, BodyReadError> {
    let mut buf = Vec::new();
    let mut byte = [0u8; 1];
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(BodyReadError::Timeout);
        }
        let _ = stream.set_read_timeout(Some(remaining.min(Duration::from_secs(1))));
        match stream.read(&mut byte) {
            Ok(0) => {
                return Err(BodyReadError::Malformed(
                    "unexpected EOF in chunked stream".into(),
                ));
            }
            Ok(_) => {
                buf.push(byte[0]);
                if buf.ends_with(b"\r\n") {
                    buf.truncate(buf.len() - 2);
                    return String::from_utf8(buf)
                        .map_err(|_| BodyReadError::Malformed("non-UTF8 chunk header".into()));
                }
                if buf.len() > max_len {
                    return Err(BodyReadError::Malformed("chunk line too long".into()));
                }
            }
            Err(e)
                if matches!(
                    e.kind(),
                    io::ErrorKind::WouldBlock
                        | io::ErrorKind::TimedOut
                        | io::ErrorKind::Interrupted
                ) =>
            {
                if Instant::now() >= deadline {
                    return Err(BodyReadError::Timeout);
                }
            }
            Err(e) => return Err(BodyReadError::Malformed(e.to_string())),
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum EarlyDrainHint {
    None,
    ExpectContinueUnacknowledged,
    ExpectedBytes(usize),
    Unknown,
}

impl EarlyDrainHint {
    fn from_headers(method: &str, headers: &ParsedHeaders) -> Self {
        if matches!(method, "GET" | "HEAD" | "OPTIONS" | "DELETE")
            && headers.content_length.unwrap_or(0) == 0
            && !headers.transfer_encoding_present
        {
            return Self::None;
        }
        if headers.expect_continue {
            return Self::ExpectContinueUnacknowledged;
        }
        if let Some(len) = headers.content_length {
            if len == 0 {
                Self::None
            } else {
                Self::ExpectedBytes(len)
            }
        } else if headers.transfer_encoding_present {
            Self::Unknown
        } else {
            Self::None
        }
    }
}

fn write_mcp_response(
    stream: &mut TcpStream,
    status: &str,
    content_type: &str,
    body: &[u8],
    extra_headers: &[(&str, &str)],
    head_only: bool,
) -> io::Result<()> {
    let mut head = format!(
        "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nCache-Control: no-store\r\nX-Content-Type-Options: nosniff\r\nReferrer-Policy: no-referrer\r\n",
        body.len()
    );
    for (name, value) in extra_headers {
        head.push_str(name);
        head.push_str(": ");
        head.push_str(value);
        head.push_str("\r\n");
    }
    head.push_str("Connection: close\r\n\r\n");
    stream.write_all(head.as_bytes())?;
    if !head_only && !body.is_empty() {
        stream.write_all(body)?;
    }
    stream.flush()
}

#[allow(clippy::too_many_arguments)]
fn write_mcp_error_and_drain(
    stream: &mut TcpStream,
    status: &str,
    content_type: &str,
    body: &[u8],
    extra_headers: &[(&str, &str)],
    head_only: bool,
    drain: EarlyDrainHint,
    budget: &Arc<McpBudget>,
) -> io::Result<()> {
    write_mcp_response(stream, status, content_type, body, extra_headers, head_only)?;
    let (limit, deadline) = match drain {
        EarlyDrainHint::None => return Ok(()),
        EarlyDrainHint::ExpectContinueUnacknowledged => {
            (4 * 1024, Instant::now() + EARLY_ERROR_DRAIN_SLICE)
        }
        EarlyDrainHint::ExpectedBytes(len) => (
            len.min(MAX_EARLY_ERROR_DRAIN_BYTES),
            Instant::now() + EARLY_ERROR_DRAIN_DEADLINE,
        ),
        EarlyDrainHint::Unknown => (
            UNKNOWN_LEN_ERROR_DRAIN_BYTES,
            Instant::now() + EARLY_ERROR_DRAIN_DEADLINE,
        ),
    };
    // Cap concurrent early-error drains well below the shared listener limit
    // (64 slots in server.rs) so unauthenticated slow/incomplete uploads cannot
    // starve `/v1` WebSockets or authorized `/mcp` traffic.
    let Some(_drain_permit) = budget.try_acquire_error_drain() else {
        return Ok(());
    };
    // RFC 7230 §6.6 graceful half-close: closing a socket while unread request
    // body bytes sit in the kernel receive buffer emits TCP RST on macOS/BSD
    // and Windows, causing HTTP clients to hit ECONNRESET while reading early
    // 401/403/413 responses. Signal FIN on the write half and drain unread bytes
    // within a bounded byte cap and overall deadline.
    let _ = stream.shutdown(std::net::Shutdown::Write);
    let mut discard = [0u8; 8192];
    let mut drained = 0usize;
    let mut last_activity = Instant::now();
    while drained < limit {
        let now = Instant::now();
        if now >= deadline {
            break;
        }
        let remaining = deadline.saturating_duration_since(now);
        let _ = stream.set_read_timeout(Some(remaining.min(EARLY_ERROR_DRAIN_SLICE)));
        let to_read = discard.len().min(limit - drained);
        match stream.read(&mut discard[..to_read]) {
            Ok(0) => break,
            Ok(n) => {
                drained += n;
                last_activity = Instant::now();
            }
            Err(e)
                if matches!(
                    e.kind(),
                    io::ErrorKind::WouldBlock
                        | io::ErrorKind::TimedOut
                        | io::ErrorKind::Interrupted
                ) =>
            {
                if matches!(drain, EarlyDrainHint::Unknown)
                    && last_activity.elapsed() >= Duration::from_millis(200)
                {
                    break;
                }
            }
            Err(_) => break,
        }
    }
    Ok(())
}

fn dispatch_remote_mcp_rpc(
    req: &Value,
    backend: &dyn RemoteMcpBackend,
    settings: &RemoteMcpSettings,
    ctx: &RemoteMcpHttpContext<'_>,
) -> Option<Value> {
    let Some(method) = req.get("method").and_then(Value::as_str) else {
        let id = req.get("id").cloned()?;
        return Some(json!({
            "jsonrpc": "2.0",
            "id": id,
            "error": { "code": -32600, "message": "Invalid Request: missing method" }
        }));
    };

    // Notifications omit `id` and expect no JSON-RPC response payload (HTTP 202).
    let req_id = req.get("id").cloned()?;
    let params = req.get("params").cloned().unwrap_or_else(|| json!({}));

    let outcome: Result<Value, (i32, String)> = match method {
        "initialize" => {
            let requested = params
                .get("protocolVersion")
                .and_then(Value::as_str)
                .unwrap_or("2025-03-26");
            let negotiated = if SUPPORTED_PROTOCOL_VERSIONS.contains(&requested) {
                requested
            } else {
                "2025-03-26"
            };
            Ok(json!({
                "protocolVersion": negotiated,
                "capabilities": { "tools": { "listChanged": false } },
                "serverInfo": {
                    "name": REMOTE_MCP_SERVER_NAME,
                    "version": env!("CARGO_PKG_VERSION"),
                },
                "instructions": REMOTE_MCP_INSTRUCTIONS,
            }))
        }
        "ping" => Ok(json!({})),
        "tools/list" => Ok(json!({ "tools": remote_mcp_tool_definitions(settings) })),
        "tools/call" => {
            let name = params
                .get("name")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let arguments = params.get("arguments").cloned().unwrap_or_else(|| json!({}));
            match call_remote_mcp_tool(name, &arguments, backend, settings, ctx) {
                Ok(payload) => Ok(json!({
                    "content": [{
                        "type": "text",
                        "text": serde_json::to_string_pretty(&payload).unwrap_or_else(|_| payload.to_string()),
                    }],
                    "structuredContent": payload,
                })),
                Err(error) => Ok(json!({
                    "isError": true,
                    "content": [{
                        "type": "text",
                        "text": error.to_string(),
                    }]
                })),
            }
        }
        _ => Err((-32601, format!("Method not found: {method}"))),
    };

    Some(match outcome {
        Ok(result) => json!({
            "jsonrpc": "2.0",
            "id": req_id,
            "result": result,
        }),
        Err((code, message)) => json!({
            "jsonrpc": "2.0",
            "id": req_id,
            "error": { "code": code, "message": message },
        }),
    })
}

fn remote_mcp_tool_definitions(settings: &RemoteMcpSettings) -> Vec<Value> {
    let mut tools = vec![
        json!({
            "name": "list_projects",
            "description": "List all AgentOS projects/workspaces and how many sessions belong to each.",
            "annotations": { "readOnlyHint": true, "destructiveHint": false, "idempotentHint": true, "openWorldHint": false },
            "inputSchema": {
                "type": "object",
                "properties": {},
                "required": []
            }
        }),
        json!({
            "name": "list_sessions",
            "description": "List AgentOS sessions sorted by most recent activity, with optional filters by project, provider, status, or title search.",
            "annotations": { "readOnlyHint": true, "destructiveHint": false, "idempotentHint": true, "openWorldHint": false },
            "inputSchema": {
                "type": "object",
                "properties": {
                    "project_id": { "type": "string", "description": "Filter by project UUID." },
                    "provider": { "type": "string", "description": "Filter by provider id (e.g. codex, claude, antigravity, cursor, opencode, deepseek)." },
                    "status": { "type": "string", "enum": ["idle", "connecting", "working", "waiting", "background", "failed"], "description": "Filter by session status." },
                    "query": { "type": "string", "description": "Case-insensitive substring filter matched against session title or project path." },
                    "limit": { "type": "integer", "description": "Maximum sessions to return (default 20, max 100)." },
                    "offset": { "type": "integer", "description": "Pagination offset (default 0)." }
                },
                "required": []
            }
        }),
        json!({
            "name": "get_session",
            "description": "Inspect a single AgentOS session: metadata, workspace path, live runtime status, thread goal, pending permission/question requests, and recent messages.",
            "annotations": { "readOnlyHint": true, "destructiveHint": false, "idempotentHint": true, "openWorldHint": false },
            "inputSchema": {
                "type": "object",
                "properties": {
                    "session_id": { "type": "string", "description": "Session UUID, agentos://session/<uuid> link, or legacy numeric session id." },
                    "max_messages": { "type": "integer", "description": "Number of most recent messages to include (default 20, max 100)." },
                    "max_chars": { "type": "integer", "description": "Max characters per message content before truncation (default 4000, max 20000)." },
                    "include_activities": { "type": "boolean", "description": "Include tool activity summaries for recent turns (default false)." }
                },
                "required": ["session_id"]
            }
        }),
        json!({
            "name": "get_messages",
            "description": "Read paginated messages from an AgentOS session transcript without pulling the entire history at once.",
            "annotations": { "readOnlyHint": true, "destructiveHint": false, "idempotentHint": true, "openWorldHint": false },
            "inputSchema": {
                "type": "object",
                "properties": {
                    "session_id": { "type": "string", "description": "Session UUID, agentos://session/<uuid> link, or legacy numeric session id." },
                    "offset": { "type": "integer", "description": "Zero-based starting message index. If omitted, returns the latest `limit` messages." },
                    "limit": { "type": "integer", "description": "Max messages to return (default 20, max 100)." },
                    "max_chars": { "type": "integer", "description": "Max characters per message content (default 4000, max 20000)." }
                },
                "required": ["session_id"]
            }
        }),
        json!({
            "name": "search_sessions",
            "description": "Full-text search across AgentOS session messages and titles.",
            "annotations": { "readOnlyHint": true, "destructiveHint": false, "idempotentHint": true, "openWorldHint": false },
            "inputSchema": {
                "type": "object",
                "properties": {
                    "query": { "type": "string", "description": "Search query text." },
                    "limit": { "type": "integer", "description": "Maximum matches to return (default 20, max 50)." }
                },
                "required": ["query"]
            }
        }),
        json!({
            "name": "list_pending_requests",
            "description": "List all sessions currently paused waiting for permission approval or user-input answers.",
            "annotations": { "readOnlyHint": true, "destructiveHint": false, "idempotentHint": true, "openWorldHint": false },
            "inputSchema": {
                "type": "object",
                "properties": {},
                "required": []
            }
        }),
    ];

    if settings.allow_send {
        tools.extend([
            json!({
                "name": "create_session",
                "description": "Create a new AgentOS session inside an existing project (or by project path), optionally sending an initial message right away.",
                "annotations": { "readOnlyHint": false, "destructiveHint": false, "idempotentHint": false, "openWorldHint": true },
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "project_id": { "type": "string", "description": "Existing project UUID from list_projects." },
                        "project_path": { "type": "string", "description": "Absolute directory path on the AgentOS host (reuses matching project or creates one)." },
                        "provider": { "type": "string", "description": "Agent provider id (e.g. codex, claude, antigravity, cursor, opencode, deepseek). Defaults to codex." },
                        "model": { "type": "string", "description": "Optional model id for the provider." },
                        "runtime_mode": { "type": "string", "enum": ["ask", "autoAcceptEdits", "auto", "fullAccess"], "description": "Access mode for the session (default fullAccess)." },
                        "title": { "type": "string", "description": "Optional explicit title for the session." },
                        "message": { "type": "string", "description": "Optional initial user prompt to send immediately after creating the session." },
                        "wait": { "type": "boolean", "description": "When `message` is provided, wait for the turn to settle or pause (default false)." },
                        "timeout_seconds": { "type": "integer", "description": "Wait timeout in seconds when `wait: true` (default 30, max 60)." },
                        "idempotency_key": { "type": "string", "description": "Optional caller-defined key to prevent duplicate submissions on retry (in-memory, up to 256 recent keys for the running daemon process)." }
                    },
                    "required": []
                }
            }),
            json!({
                "name": "send_message",
                "description": "Send a user message to an AgentOS session. Automatically starts the session's provider runtime if idle, streams events to Desktop/Web, and persists the transcript. Returns immediately by default (`wait: false`) or waits for completion/pause when `wait: true`.",
                "annotations": { "readOnlyHint": false, "destructiveHint": false, "idempotentHint": false, "openWorldHint": true },
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "session_id": { "type": "string", "description": "Target session UUID, agentos://session/<uuid> link, or legacy numeric session id." },
                        "message": { "type": "string", "description": "User prompt text to send to the agent." },
                        "steer": { "type": "boolean", "description": "If the session is currently mid-turn and supports steering, steer the active turn instead of returning busy (default false)." },
                        "wait": { "type": "boolean", "description": "Wait for the turn to finish, park, or pause for approval (default false)." },
                        "timeout_seconds": { "type": "integer", "description": "Max seconds to wait when `wait: true` (default 30, max 60)." },
                        "idempotency_key": { "type": "string", "description": "Optional caller-defined key to deduplicate retries (in-memory, up to 256 recent keys for the running daemon process)." }
                    },
                    "required": ["session_id", "message"]
                }
            }),
            json!({
                "name": "wait_for_turn",
                "description": "Wait up to 60 seconds for an active turn in an AgentOS session to complete, fail, park in background, or pause for approval/input.",
                "annotations": { "readOnlyHint": true, "destructiveHint": false, "idempotentHint": true, "openWorldHint": false },
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "session_id": { "type": "string", "description": "Session UUID." },
                        "turn_id": { "type": "string", "description": "Optional turn UUID returned by send_message." },
                        "timeout_seconds": { "type": "integer", "description": "Max seconds to wait (default 30, max 60)." }
                    },
                    "required": ["session_id"]
                }
            }),
            json!({
                "name": "cancel_session",
                "description": "Cancel an actively running turn in an AgentOS session.",
                "annotations": { "readOnlyHint": false, "destructiveHint": false, "idempotentHint": true, "openWorldHint": false },
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "session_id": { "type": "string", "description": "Session UUID." }
                    },
                    "required": ["session_id"]
                }
            }),
        ]);
    }

    if settings.allow_approvals {
        tools.push(json!({
            "name": "respond_pending_request",
            "description": "Respond to a pending permission approval or interactive user-input question on an active AgentOS session runtime.",
            "annotations": { "readOnlyHint": false, "destructiveHint": true, "idempotentHint": false, "openWorldHint": true },
            "inputSchema": {
                "type": "object",
                "properties": {
                    "session_id": { "type": "string", "description": "Session UUID." },
                    "runtime_id": { "type": "string", "description": "Active runtime UUID from get_session or list_pending_requests." },
                    "request_id": { "type": "string", "description": "Pending request id from get_session or list_pending_requests." },
                    "kind": { "type": "string", "enum": ["permission", "user_input"], "description": "Type of pending request." },
                    "option_id": { "type": "string", "description": "Selected permission option_id (required when kind is permission)." },
                    "answers": {
                        "type": "array",
                        "description": "Answers for each question (required when kind is user_input).",
                        "items": {
                            "type": "object",
                            "properties": {
                                "question_id": { "type": "string" },
                                "answers": { "type": "array", "items": { "type": "string" } }
                            },
                            "required": ["question_id", "answers"]
                        }
                    }
                },
                "required": ["session_id", "runtime_id", "request_id", "kind"]
            }
        }));
    }

    tools
}

fn call_remote_mcp_tool(
    name: &str,
    args: &Value,
    backend: &dyn RemoteMcpBackend,
    settings: &RemoteMcpSettings,
    ctx: &RemoteMcpHttpContext<'_>,
) -> anyhow::Result<Value> {
    match name {
        "list_projects" => {
            let (projects, sessions, default_cwd) = backend.list_projects_and_sessions()?;
            let mut counts: HashMap<Uuid, usize> = HashMap::new();
            for s in &sessions {
                *counts.entry(s.project_id).or_default() += 1;
            }
            let items: Vec<Value> = projects
                .iter()
                .map(|p| {
                    json!({
                        "project_id": p.id.to_string(),
                        "name": p.name,
                        "path": p.path.to_string_lossy(),
                        "created_at": p.created_at,
                        "session_count": counts.get(&p.id).copied().unwrap_or(0),
                    })
                })
                .collect();
            Ok(json!({
                "projects": items,
                "total": items.len(),
                "default_cwd": default_cwd.to_string_lossy(),
            }))
        }

        "list_sessions" => {
            let (projects, sessions, _) = backend.list_projects_and_sessions()?;
            let project_map: HashMap<Uuid, &Project> =
                projects.iter().map(|p| (p.id, p)).collect();

            let filter_project = args
                .get("project_id")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(Uuid::parse_str)
                .transpose()
                .context("invalid project_id UUID")?;
            let filter_provider = args
                .get("provider")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(parse_provider_kind)
                .transpose()?;
            let filter_status = args
                .get("status")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_ascii_lowercase);
            let filter_query = args
                .get("query")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_ascii_lowercase);
            let limit = args
                .get("limit")
                .and_then(Value::as_u64)
                .unwrap_or(20)
                .clamp(1, 100) as usize;
            let offset = args.get("offset").and_then(Value::as_u64).unwrap_or(0) as usize;

            let pending_ids: HashSet<Uuid> = ctx
                .scribe
                .all_pending_requests()
                .into_iter()
                .map(|(sid, _, _)| sid)
                .collect();

            let mut rows: Vec<(u64, Value)> = Vec::new();
            for s in &sessions {
                if let Some(pid) = filter_project
                    && s.project_id != pid
                {
                    continue;
                }
                if let Some(prov) = filter_provider
                    && s.provider != prov
                {
                    continue;
                }
                let live = ctx.scribe.snapshot(s.id);
                let effective_status = live.as_ref().map(|l| l.status).unwrap_or(s.status);
                let status_str = status_id(effective_status);
                if let Some(ref want_status) = filter_status
                    && status_str != want_status
                {
                    continue;
                }
                let project = project_map.get(&s.project_id).copied();
                let workspace_path = s
                    .workspace
                    .path()
                    .or_else(|| project.map(|p| p.path.as_path()))
                    .map(|p| p.to_string_lossy().into_owned())
                    .unwrap_or_default();
                let title = s.display_title().to_owned();
                if let Some(ref q) = filter_query {
                    let hay = format!(
                        "{} {} {}",
                        title.to_ascii_lowercase(),
                        workspace_path.to_ascii_lowercase(),
                        project
                            .map(|p| p.name.to_ascii_lowercase())
                            .unwrap_or_default()
                    );
                    if !hay.contains(q.as_str()) {
                        continue;
                    }
                }
                let active_runtime = backend.active_runtime_for(s.id);
                let sort_ts = s.last_reply_at.unwrap_or(s.updated_at);
                rows.push((
                    sort_ts,
                    json!({
                        "session_id": s.id.to_string(),
                        "title": title,
                        "project_id": s.project_id.to_string(),
                        "project_name": project.map(|p| p.name.as_str()).unwrap_or(""),
                        "workspace_path": workspace_path,
                        "provider": s.provider.id(),
                        "model": s.model,
                        "status": status_str,
                        "has_active_runtime": active_runtime.is_some(),
                        "runtime_id": active_runtime.map(|(rid, _)| rid.to_string()),
                        "has_pending_request": pending_ids.contains(&s.id),
                        "created_at": s.created_at,
                        "updated_at": s.updated_at,
                        "last_reply_at": s.last_reply_at,
                    }),
                ));
            }
            rows.sort_by(|a, b| b.0.cmp(&a.0));
            let total = rows.len();
            let items: Vec<Value> = rows
                .into_iter()
                .skip(offset)
                .take(limit)
                .map(|(_, v)| v)
                .collect();
            let has_more = offset.saturating_add(items.len()) < total;
            Ok(json!({
                "sessions": items,
                "total": total,
                "offset": offset,
                "limit": limit,
                "has_more": has_more,
            }))
        }

        "get_session" => {
            let session_id = resolve_session_id_arg(args, backend)?;
            let max_messages = args
                .get("max_messages")
                .and_then(Value::as_u64)
                .unwrap_or(20)
                .min(100) as usize;
            let max_chars = args
                .get("max_chars")
                .and_then(Value::as_u64)
                .unwrap_or(4000)
                .clamp(200, 20000) as usize;
            let include_activities = args
                .get("include_activities")
                .and_then(Value::as_bool)
                .unwrap_or(false);

            let Some(session) = backend.hydrate_session_by_id(session_id)? else {
                return Ok(json!({ "found": false, "session_id": session_id.to_string() }));
            };
            let (projects, _, _) = backend.list_projects_and_sessions()?;
            let project = projects.iter().find(|p| p.id == session.project_id);
            let workspace_path = session
                .workspace
                .path()
                .or_else(|| project.map(|p| p.path.as_path()))
                .map(|p| p.to_string_lossy().into_owned())
                .unwrap_or_default();

            let live = ctx.scribe.snapshot(session_id);
            let active_runtime = backend.active_runtime_for(session_id);
            let effective_status = live.as_ref().map(|l| l.status).unwrap_or(session.status);

            let total_messages = session.messages.len();
            let start_idx = total_messages.saturating_sub(max_messages);
            let recent_messages: Vec<Value> = session
                .messages
                .iter()
                .enumerate()
                .skip(start_idx)
                .map(|(idx, msg)| format_message_json(idx, msg, max_chars))
                .collect();

            let pending_permissions: Vec<Value> = live
                .as_ref()
                .map(|l| {
                    l.pending_permissions
                        .iter()
                        .map(format_pending_permission)
                        .collect()
                })
                .unwrap_or_default();
            let pending_user_inputs: Vec<Value> = live
                .as_ref()
                .map(|l| {
                    l.pending_user_inputs
                        .iter()
                        .map(format_pending_user_input)
                        .collect()
                })
                .unwrap_or_default();

            let mut out = json!({
                "found": true,
                "session_id": session.id.to_string(),
                "title": session.display_title(),
                "project_id": session.project_id.to_string(),
                "project_name": project.map(|p| p.name.as_str()).unwrap_or(""),
                "workspace_path": workspace_path,
                "provider": session.provider.id(),
                "model": session.model,
                "runtime_mode": runtime_mode_id(session.runtime_mode),
                "status": status_id(effective_status),
                "has_active_runtime": active_runtime.is_some(),
                "runtime_id": active_runtime.map(|(rid, _)| rid.to_string()),
                "active_turn_id": live
                    .as_ref()
                    .and_then(|l| l.active_turn_id)
                    .or_else(|| session.active_turn_id())
                    .map(|id| id.to_string()),
                "thread_goal": session.thread_goal,
                "pending_permissions": pending_permissions,
                "pending_user_inputs": pending_user_inputs,
                "turn_count": session.turns.len(),
                "message_count": total_messages,
                "messages_offset": start_idx,
                "messages": recent_messages,
                "created_at": session.created_at,
                "updated_at": session.updated_at,
                "last_reply_at": session.last_reply_at,
            });
            if include_activities {
                let activities: Vec<Value> = session
                    .transcript_blocks
                    .iter()
                    .rev()
                    .take(20)
                    .rev()
                    .map(|block| {
                        json!({
                            "after_message": block.after_message,
                            "turn_id": block.turn_id.map(|id| id.to_string()),
                            "activities": block.activities.iter().map(|a| {
                                json!({
                                    "kind": a.kind,
                                    "title": a.title,
                                    "tool_name": a.tool_name,
                                    "complete": a.complete,
                                    "failed": a.failed,
                                })
                            }).collect::<Vec<_>>(),
                        })
                    })
                    .collect();
                out["recent_activities"] = Value::Array(activities);
            }
            Ok(out)
        }

        "get_messages" => {
            let session_id = resolve_session_id_arg(args, backend)?;
            let limit = args
                .get("limit")
                .and_then(Value::as_u64)
                .unwrap_or(20)
                .clamp(1, 100) as usize;
            let max_chars = args
                .get("max_chars")
                .and_then(Value::as_u64)
                .unwrap_or(4000)
                .clamp(200, 20000) as usize;
            let Some(session) = backend.hydrate_session_by_id(session_id)? else {
                bail!("session {session_id} not found");
            };
            let total = session.messages.len();
            let offset = match args.get("offset").and_then(Value::as_u64) {
                Some(v) => (v as usize).min(total),
                None => total.saturating_sub(limit),
            };
            let items: Vec<Value> = session
                .messages
                .iter()
                .enumerate()
                .skip(offset)
                .take(limit)
                .map(|(idx, msg)| format_message_json(idx, msg, max_chars))
                .collect();
            let next_offset = offset.saturating_add(items.len());
            Ok(json!({
                "session_id": session.id.to_string(),
                "total": total,
                "offset": offset,
                "limit": limit,
                "has_more": next_offset < total,
                "next_offset": if next_offset < total { Some(next_offset) } else { None },
                "messages": items,
            }))
        }

        "search_sessions" => {
            let query = args
                .get("query")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .ok_or_else(|| anyhow!("query is required"))?;
            let limit = args
                .get("limit")
                .and_then(Value::as_u64)
                .unwrap_or(20)
                .clamp(1, 50) as usize;
            let matches = backend.search_session_messages(query.to_owned(), limit)?;
            let (_, sessions, _) = backend.list_projects_and_sessions()?;
            let session_titles: HashMap<Uuid, String> = sessions
                .iter()
                .map(|s| (s.id, s.display_title().to_owned()))
                .collect();
            let items: Vec<Value> = matches
                .into_iter()
                .map(|m| {
                    json!({
                        "session_id": m.session_id.to_string(),
                        "session_title": session_titles.get(&m.session_id).cloned().unwrap_or_default(),
                        "role": role_id(m.source),
                        "snippet": m.snippet,
                    })
                })
                .collect();
            Ok(json!({
                "query": query,
                "matches": items,
                "total": items.len(),
            }))
        }

        "list_pending_requests" => {
            let pending = ctx.scribe.all_pending_requests();
            let (_, sessions, _) = backend.list_projects_and_sessions()?;
            let title_map: HashMap<Uuid, String> = sessions
                .iter()
                .map(|s| (s.id, s.display_title().to_owned()))
                .collect();
            let mut items = Vec::new();
            for (sid, perms, inputs) in pending {
                let title = title_map.get(&sid).cloned().unwrap_or_default();
                for p in perms {
                    let mut val = format_pending_permission(&p);
                    val["session_id"] = json!(sid.to_string());
                    val["session_title"] = json!(title);
                    items.push(val);
                }
                for q in inputs {
                    let mut val = format_pending_user_input(&q);
                    val["session_id"] = json!(sid.to_string());
                    val["session_title"] = json!(title);
                    items.push(val);
                }
            }
            Ok(json!({
                "pending_requests": items,
                "total": items.len(),
            }))
        }

        "create_session" => {
            if !settings.allow_send {
                bail!("sending messages and creating sessions is disabled in Remote MCP settings");
            }
            let project_id = args
                .get("project_id")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(Uuid::parse_str)
                .transpose()
                .context("invalid project_id UUID")?;
            let project_path = args
                .get("project_path")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(PathBuf::from);
            let provider = args
                .get("provider")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(parse_provider_kind)
                .transpose()?;
            let model = args
                .get("model")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_owned);
            let runtime_mode = args
                .get("runtime_mode")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(parse_runtime_mode)
                .transpose()?;
            let title = args
                .get("title")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_owned);

            let (project, session) = backend.create_session(CreateSessionParams {
                project_id,
                project_path,
                provider,
                model,
                runtime_mode,
                title,
            })?;
            ctx.hub.broadcast_task_state_changed_all();

            let initial_message = args
                .get("message")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|s| !s.is_empty());
            if let Some(prompt) = initial_message {
                let wait = args.get("wait").and_then(Value::as_bool).unwrap_or(false);
                let timeout_secs = args
                    .get("timeout_seconds")
                    .and_then(Value::as_u64)
                    .unwrap_or(DEFAULT_WAIT_TIMEOUT_SECS)
                    .clamp(1, MAX_WAIT_TIMEOUT_SECS);
                let idempotency_key = args
                    .get("idempotency_key")
                    .and_then(Value::as_str)
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .map(str::to_owned);
                let mut sent = execute_send_message(
                    session.id,
                    prompt.to_owned(),
                    false,
                    wait,
                    timeout_secs,
                    idempotency_key,
                    backend,
                    ctx,
                )?;
                sent["project_id"] = json!(project.id.to_string());
                sent["project_name"] = json!(project.name);
                sent["workspace_path"] = json!(project.path.to_string_lossy());
                sent["provider"] = json!(session.provider.id());
                sent["created"] = json!(true);
                return Ok(sent);
            }

            Ok(json!({
                "created": true,
                "session_id": session.id.to_string(),
                "title": session.display_title(),
                "project_id": project.id.to_string(),
                "project_name": project.name,
                "workspace_path": project.path.to_string_lossy(),
                "provider": session.provider.id(),
                "model": session.model,
                "runtime_mode": runtime_mode_id(session.runtime_mode),
                "status": status_id(session.status),
            }))
        }

        "send_message" => {
            if !settings.allow_send {
                bail!("sending messages is disabled in Remote MCP settings");
            }
            let session_id = resolve_session_id_arg(args, backend)?;
            let message = args
                .get("message")
                .and_then(Value::as_str)
                .filter(|s| !s.trim().is_empty())
                .ok_or_else(|| anyhow!("message must be a non-empty string"))?;
            let steer = args.get("steer").and_then(Value::as_bool).unwrap_or(false);
            let wait = args.get("wait").and_then(Value::as_bool).unwrap_or(false);
            let timeout_secs = args
                .get("timeout_seconds")
                .and_then(Value::as_u64)
                .unwrap_or(DEFAULT_WAIT_TIMEOUT_SECS)
                .clamp(1, MAX_WAIT_TIMEOUT_SECS);
            let idempotency_key = args
                .get("idempotency_key")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_owned);
            execute_send_message(
                session_id,
                message.to_owned(),
                steer,
                wait,
                timeout_secs,
                idempotency_key,
                backend,
                ctx,
            )
        }

        "wait_for_turn" => {
            let session_id = resolve_session_id_arg(args, backend)?;
            let turn_id = args
                .get("turn_id")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(Uuid::parse_str)
                .transpose()
                .context("invalid turn_id UUID")?;
            let timeout_secs = args
                .get("timeout_seconds")
                .and_then(Value::as_u64)
                .unwrap_or(DEFAULT_WAIT_TIMEOUT_SECS)
                .clamp(1, MAX_WAIT_TIMEOUT_SECS);
            let Some(_wait_guard) = ctx.budget.try_acquire_wait() else {
                bail!("too many concurrent wait requests; retry shortly or poll get_session");
            };
            wait_for_turn_with_persistence(
                session_id,
                turn_id,
                Duration::from_secs(timeout_secs),
                backend,
                ctx,
            )
        }

        "cancel_session" => {
            if !settings.allow_send {
                bail!("cancelling sessions is disabled in Remote MCP settings");
            }
            let session_id = resolve_session_id_arg(args, backend)?;
            let cancelled = ctx
                .dispatcher
                .run_on_session(session_id, move |backend, hub, active_runtime_id| {
                    let Some(mcp) = backend.remote_mcp() else {
                        bail!("remote MCP backend unavailable");
                    };
                    mcp.cancel_session_in_mailbox(session_id, *active_runtime_id, hub)
                })?;
            Ok(json!({
                "session_id": session_id.to_string(),
                "cancelled": cancelled,
            }))
        }

        "respond_pending_request" => {
            if !settings.allow_approvals {
                bail!("remote approvals are disabled in Remote MCP settings");
            }
            let session_id = resolve_session_id_arg(args, backend)?;
            let runtime_id = args
                .get("runtime_id")
                .and_then(Value::as_str)
                .map(str::trim)
                .ok_or_else(|| anyhow!("runtime_id is required"))
                .and_then(|s| Uuid::parse_str(s).context("invalid runtime_id UUID"))?;
            let request_id = args
                .get("request_id")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .ok_or_else(|| anyhow!("request_id is required"))?
                .to_owned();
            let kind = args
                .get("kind")
                .and_then(Value::as_str)
                .map(str::trim)
                .ok_or_else(|| anyhow!("kind is required (permission or user_input)"))?;

            // Validate that this (session_id, runtime_id, request_id) is genuinely pending.
            let live = ctx
                .scribe
                .snapshot(session_id)
                .ok_or_else(|| anyhow!("session {session_id} has no active runtime"))?;
            if live.runtime_id != runtime_id {
                bail!(
                    "runtime_id mismatch: active runtime is {}, got {}",
                    live.runtime_id,
                    runtime_id
                );
            }

            match kind {
                "permission" => {
                    let option_id = args
                        .get("option_id")
                        .and_then(Value::as_str)
                        .map(str::trim)
                        .filter(|s| !s.is_empty())
                        .ok_or_else(|| anyhow!("option_id is required for kind=permission"))?
                        .to_owned();
                    let pending = live
                        .pending_permissions
                        .iter()
                        .find(|p| p.request_id == request_id)
                        .ok_or_else(|| {
                            anyhow!("permission request {request_id:?} is not pending on session {session_id}")
                        })?;
                    if !pending.options.iter().any(|opt| opt.id == option_id) {
                        bail!("option_id {option_id:?} is not a valid option for request {request_id:?}");
                    }
                    let req_id_clone = request_id.clone();
                    let opt_id_clone = option_id.clone();
                    ctx.dispatcher.run_on_session(
                        session_id,
                        move |backend, hub, _active_runtime_id| {
                            let Some(mcp) = backend.remote_mcp() else {
                                bail!("remote MCP backend unavailable");
                            };
                            mcp.respond_permission_in_mailbox(
                                session_id,
                                runtime_id,
                                req_id_clone,
                                opt_id_clone,
                                hub,
                            )
                        },
                    )?;
                    ctx.scribe.clear_pending_request(session_id, &request_id);
                    Ok(json!({
                        "session_id": session_id.to_string(),
                        "runtime_id": runtime_id.to_string(),
                        "request_id": request_id,
                        "kind": "permission",
                        "option_id": option_id,
                        "resolved": true,
                    }))
                }
                "user_input" => {
                    let raw_answers = args
                        .get("answers")
                        .and_then(Value::as_array)
                        .ok_or_else(|| anyhow!("answers array is required for kind=user_input"))?;
                    if !live
                        .pending_user_inputs
                        .iter()
                        .any(|q| q.request_id == request_id)
                    {
                        bail!("user_input request {request_id:?} is not pending on session {session_id}");
                    }
                    let mut answers = Vec::with_capacity(raw_answers.len());
                    for entry in raw_answers {
                        let question_id = entry
                            .get("question_id")
                            .and_then(Value::as_str)
                            .map(str::trim)
                            .filter(|s| !s.is_empty())
                            .ok_or_else(|| anyhow!("each answer requires a question_id"))?
                            .to_owned();
                        let values = entry
                            .get("answers")
                            .and_then(Value::as_array)
                            .ok_or_else(|| anyhow!("each answer requires an answers string array"))?
                            .iter()
                            .filter_map(Value::as_str)
                            .map(str::to_owned)
                            .collect::<Vec<_>>();
                        answers.push(UserInputAnswer {
                            question_id,
                            answers: values,
                        });
                    }
                    let req_id_clone = request_id.clone();
                    ctx.dispatcher.run_on_session(
                        session_id,
                        move |backend, hub, _active_runtime_id| {
                            let Some(mcp) = backend.remote_mcp() else {
                                bail!("remote MCP backend unavailable");
                            };
                            mcp.respond_user_input_in_mailbox(
                                session_id,
                                runtime_id,
                                req_id_clone,
                                answers,
                                hub,
                            )
                        },
                    )?;
                    ctx.scribe.clear_pending_request(session_id, &request_id);
                    Ok(json!({
                        "session_id": session_id.to_string(),
                        "runtime_id": runtime_id.to_string(),
                        "request_id": request_id,
                        "kind": "user_input",
                        "resolved": true,
                    }))
                }
                other => bail!("unsupported request kind {other:?}; expected permission or user_input"),
            }
        }

        unknown => bail!("unknown tool `{unknown}`"),
    }
}

#[allow(clippy::too_many_arguments)]
fn execute_send_message(
    session_id: Uuid,
    prompt: String,
    steer: bool,
    wait: bool,
    timeout_secs: u64,
    idempotency_key: Option<String>,
    backend: &dyn RemoteMcpBackend,
    ctx: &RemoteMcpHttpContext<'_>,
) -> anyhow::Result<Value> {
    let outcome = ctx
        .dispatcher
        .run_on_session(session_id, move |backend, hub, active_runtime_id| {
            let Some(mcp) = backend.remote_mcp() else {
                bail!("remote MCP backend unavailable");
            };
            mcp.submit_prompt_in_mailbox(
                session_id,
                active_runtime_id,
                hub,
                SubmitPromptParams {
                    prompt,
                    steer,
                    idempotency_key,
                },
            )
        })?;

    if !outcome.accepted {
        let live = ctx.scribe.snapshot(session_id);
        return Ok(json!({
            "accepted": false,
            "session_id": outcome.session_id.to_string(),
            "runtime_id": outcome.runtime_id.to_string(),
            "status": outcome.status,
            "detail": outcome.detail,
            "pending_permissions": live.as_ref().map(|l| l.pending_permissions.iter().map(format_pending_permission).collect::<Vec<_>>()).unwrap_or_default(),
            "pending_user_inputs": live.as_ref().map(|l| l.pending_user_inputs.iter().map(format_pending_user_input).collect::<Vec<_>>()).unwrap_or_default(),
        }));
    }

    if !wait {
        return Ok(json!({
            "accepted": true,
            "deduplicated": outcome.deduplicated,
            "session_id": outcome.session_id.to_string(),
            "runtime_id": outcome.runtime_id.to_string(),
            "turn_id": outcome.turn_id.map(|id| id.to_string()),
            "message_id": outcome.message_id.map(|id| id.to_string()),
            "status": outcome.status,
        }));
    }

    let Some(_wait_guard) = ctx.budget.try_acquire_wait() else {
        return Ok(json!({
            "accepted": true,
            "deduplicated": outcome.deduplicated,
            "session_id": outcome.session_id.to_string(),
            "runtime_id": outcome.runtime_id.to_string(),
            "turn_id": outcome.turn_id.map(|id| id.to_string()),
            "message_id": outcome.message_id.map(|id| id.to_string()),
            "status": "running",
            "wait_skipped": true,
            "detail": "prompt accepted, but concurrent wait limit reached; poll get_session or wait_for_turn",
        }));
    };

    let mut response = wait_for_turn_with_persistence(
        session_id,
        outcome.turn_id,
        Duration::from_secs(timeout_secs),
        backend,
        ctx,
    )?;
    response["accepted"] = json!(true);
    response["deduplicated"] = json!(outcome.deduplicated);
    if response["runtime_id"].is_null() {
        response["runtime_id"] = json!(outcome.runtime_id.to_string());
    }
    response["message_id"] = json!(outcome.message_id.map(|id| id.to_string()));
    Ok(response)
}

fn resolve_persisted_turn_response(
    session_id: Uuid,
    tid: Uuid,
    backend: &dyn RemoteMcpBackend,
    ctx: &RemoteMcpHttpContext<'_>,
) -> anyhow::Result<Option<Value>> {
    let is_currently_active = ctx
        .scribe
        .snapshot(session_id)
        .is_some_and(|snap| snap.active_turn_id == Some(tid));
    if is_currently_active {
        return Ok(None);
    }
    let Some(session) = backend.hydrate_session_by_id(session_id)? else {
        return Ok(None);
    };
    let Some(persisted_turn) = session.turns.iter().find(|t| t.id == tid) else {
        return Ok(None);
    };
    let status_label = match persisted_turn.status {
        TurnStatus::Completed => "completed",
        TurnStatus::Failed => "failed",
        TurnStatus::Interrupted => "interrupted",
        TurnStatus::Running => {
            // If the daemon restarted while this turn was in flight, no live
            // runtime exists anymore; report interrupted immediately rather
            // than blocking until timeout.
            if backend.active_runtime_for(session_id).is_none()
                && ctx.scribe.snapshot(session_id).is_none()
            {
                "interrupted"
            } else {
                return Ok(None);
            }
        }
    };
    let reply = extract_assistant_reply(&session, Some(tid));
    Ok(Some(json!({
        "session_id": session_id.to_string(),
        "turn_id": tid.to_string(),
        "runtime_id": Value::Null,
        "status": status_label,
        "timed_out": false,
        "reply": reply,
        "error": Value::Null,
        "pending_permissions": [],
        "pending_user_inputs": [],
    })))
}

fn wait_for_turn_with_persistence(
    session_id: Uuid,
    target_turn_id: Option<Uuid>,
    timeout: Duration,
    backend: &dyn RemoteMcpBackend,
    ctx: &RemoteMcpHttpContext<'_>,
) -> anyhow::Result<Value> {
    if let Some(tid) = target_turn_id {
        if let Some(record) = ctx.scribe.find_finished_turn(session_id, tid) {
            return build_turn_state_response(
                session_id,
                Some(tid),
                WaitOutcome::TurnFinished(record),
                backend,
            );
        }
        if let Some(resp) = resolve_persisted_turn_response(session_id, tid, backend, ctx)? {
            return Ok(resp);
        }
    }

    let outcome = ctx.scribe.wait_for_turn(session_id, target_turn_id, timeout);
    if matches!(outcome, WaitOutcome::TimedOut { .. })
        && let Some(tid) = target_turn_id
    {
        if let Some(record) = ctx.scribe.find_finished_turn(session_id, tid) {
            return build_turn_state_response(
                session_id,
                Some(tid),
                WaitOutcome::TurnFinished(record),
                backend,
            );
        }
        if let Some(resp) = resolve_persisted_turn_response(session_id, tid, backend, ctx)? {
            return Ok(resp);
        }
    }
    build_turn_state_response(session_id, target_turn_id, outcome, backend)
}

fn extract_assistant_reply(session: &AgentSession, target_turn_id: Option<Uuid>) -> Option<String> {
    let iter = session
        .messages
        .iter()
        .filter(|m| m.role == MessageRole::Assistant);
    let texts: Vec<&str> = if let Some(tid) = target_turn_id {
        iter.filter(|m| m.turn_id == Some(tid))
            .map(|m| m.content.as_str())
            .collect()
    } else {
        iter.last()
            .map(|m| vec![m.content.as_str()])
            .unwrap_or_default()
    };
    if texts.is_empty() {
        None
    } else {
        Some(texts.join("\n\n"))
    }
}

fn build_turn_state_response(
    session_id: Uuid,
    target_turn_id: Option<Uuid>,
    outcome: WaitOutcome,
    backend: &dyn RemoteMcpBackend,
) -> anyhow::Result<Value> {
    let session = backend.hydrate_session_by_id(session_id)?;
    let (
        status_label,
        timed_out,
        runtime_id,
        resolved_turn_id,
        error,
        pending_permissions,
        pending_user_inputs,
    ) = match outcome {
        WaitOutcome::TurnFinished(record) => (
            if record.success {
                "completed"
            } else {
                "failed"
            },
            false,
            Some(record.runtime_id),
            Some(record.turn_id),
            record.error,
            Vec::new(),
            Vec::new(),
        ),
        WaitOutcome::AwaitingPermission {
            snapshot,
            permissions,
        } => (
            "awaiting_permission",
            false,
            Some(snapshot.runtime_id),
            target_turn_id
                .or(snapshot.active_turn_id)
                .or(snapshot.last_finished_turn_id),
            snapshot.last_error,
            permissions.iter().map(format_pending_permission).collect(),
            Vec::new(),
        ),
        WaitOutcome::AwaitingUserInput {
            snapshot,
            user_inputs,
        } => (
            "awaiting_user_input",
            false,
            Some(snapshot.runtime_id),
            target_turn_id
                .or(snapshot.active_turn_id)
                .or(snapshot.last_finished_turn_id),
            snapshot.last_error,
            Vec::new(),
            user_inputs.iter().map(format_pending_user_input).collect(),
        ),
        WaitOutcome::Parked(snapshot) => (
            "background",
            false,
            Some(snapshot.runtime_id),
            target_turn_id
                .or(snapshot.active_turn_id)
                .or(snapshot.last_finished_turn_id),
            snapshot.last_error,
            Vec::new(),
            Vec::new(),
        ),
        WaitOutcome::SessionSettled(snapshot) => (
            match snapshot.status {
                SessionStatus::Failed => "failed",
                SessionStatus::Background => "background",
                _ => "completed",
            },
            false,
            Some(snapshot.runtime_id),
            target_turn_id
                .or(snapshot.last_finished_turn_id)
                .or(snapshot.active_turn_id),
            snapshot.last_error,
            Vec::new(),
            Vec::new(),
        ),
        WaitOutcome::NoActiveRuntime => {
            let status = if let Some(ref s) = session {
                match s.status {
                    SessionStatus::Idle => "completed",
                    SessionStatus::Failed => "failed",
                    SessionStatus::Background => "background",
                    SessionStatus::Waiting => "waiting",
                    SessionStatus::Connecting | SessionStatus::Working => "running",
                }
            } else {
                "unknown"
            };
            (status, false, None, target_turn_id, None, Vec::new(), Vec::new())
        }
        WaitOutcome::TimedOut { snapshot } => {
            let matches_target = target_turn_id.is_none_or(|tid| {
                snapshot
                    .as_ref()
                    .is_some_and(|s| s.active_turn_id == Some(tid))
            });
            let status = if matches_target { "running" } else { "timeout" };
            let perms = snapshot
                .as_ref()
                .map(|s| {
                    s.pending_permissions
                        .iter()
                        .filter(|p| target_turn_id.is_none_or(|tid| p.turn_id == Some(tid)))
                        .map(format_pending_permission)
                        .collect()
                })
                .unwrap_or_default();
            let inputs = snapshot
                .as_ref()
                .map(|s| {
                    s.pending_user_inputs
                        .iter()
                        .filter(|q| target_turn_id.is_none_or(|tid| q.turn_id == Some(tid)))
                        .map(format_pending_user_input)
                        .collect()
                })
                .unwrap_or_default();
            (
                status,
                true,
                if matches_target {
                    snapshot.as_ref().map(|s| s.runtime_id)
                } else {
                    None
                },
                target_turn_id.or_else(|| {
                    snapshot
                        .as_ref()
                        .and_then(|s| s.active_turn_id.or(s.last_finished_turn_id))
                }),
                if matches_target {
                    snapshot.and_then(|s| s.last_error)
                } else {
                    None
                },
                perms,
                inputs,
            )
        }
    };

    let assistant_reply = session
        .as_ref()
        .and_then(|s| extract_assistant_reply(s, target_turn_id));

    Ok(json!({
        "session_id": session_id.to_string(),
        "turn_id": resolved_turn_id.map(|id| id.to_string()),
        "runtime_id": runtime_id.map(|id| id.to_string()),
        "status": status_label,
        "timed_out": timed_out,
        "reply": assistant_reply,
        "error": error,
        "pending_permissions": pending_permissions,
        "pending_user_inputs": pending_user_inputs,
    }))
}

fn resolve_session_id_arg(args: &Value, backend: &dyn RemoteMcpBackend) -> anyhow::Result<Uuid> {
    let raw_str = args.get("session_id").and_then(Value::as_str).map(|s| {
        let trimmed = s.trim();
        trimmed
            .strip_prefix("agentos://session/")
            .or_else(|| trimmed.strip_prefix("codeg://session/"))
            .unwrap_or(trimmed)
            .trim()
    });
    if let Some(text) = raw_str
        && !text.is_empty()
    {
        if let Ok(uuid) = Uuid::parse_str(text) {
            return Ok(uuid);
        }
        if let Ok(legacy) = text.parse::<i64>()
            && let Some(uuid) = backend.resolve_legacy_session_id(legacy)?
        {
            return Ok(uuid);
        }
        bail!("invalid or unknown session_id {text:?}");
    }
    if let Some(legacy) = args.get("session_id").and_then(Value::as_i64)
        && let Some(uuid) = backend.resolve_legacy_session_id(legacy)?
    {
        return Ok(uuid);
    }
    bail!("session_id is required")
}

fn format_message_json(
    index: usize,
    msg: &waku_protocol::model::Message,
    max_chars: usize,
) -> Value {
    let full = msg.display_content.as_deref().unwrap_or(&msg.content);
    let total_chars = full.chars().count();
    let truncated = total_chars > max_chars;
    let content: String = if truncated {
        full.chars().take(max_chars).collect()
    } else {
        full.to_owned()
    };
    json!({
        "index": index,
        "message_id": msg.id.to_string(),
        "turn_id": msg.turn_id.map(|id| id.to_string()),
        "role": role_id(msg.role),
        "provider": msg.provider.map(ProviderKind::id),
        "content": content,
        "truncated": truncated,
        "total_chars": total_chars,
        "streaming": msg.streaming,
        "created_at": msg.created_at,
    })
}

fn format_pending_permission(p: &PendingPermissionRecord) -> Value {
    json!({
        "kind": "permission",
        "runtime_id": p.runtime_id.to_string(),
        "turn_id": p.turn_id.map(|id| id.to_string()),
        "request_id": p.request_id,
        "title": p.title,
        "detail": p.detail,
        "options": p.options.iter().map(|o| json!({
            "option_id": o.id,
            "label": o.label,
            "allow": o.allow,
        })).collect::<Vec<_>>(),
        "created_at": p.created_at,
    })
}

fn format_pending_user_input(q: &PendingUserInputRecord) -> Value {
    json!({
        "kind": "user_input",
        "runtime_id": q.runtime_id.to_string(),
        "turn_id": q.turn_id.map(|id| id.to_string()),
        "request_id": q.request_id,
        "questions": q.questions,
        "created_at": q.created_at,
    })
}

fn status_id(status: SessionStatus) -> &'static str {
    match status {
        SessionStatus::Idle => "idle",
        SessionStatus::Connecting => "connecting",
        SessionStatus::Working => "working",
        SessionStatus::Waiting => "waiting",
        SessionStatus::Background => "background",
        SessionStatus::Failed => "failed",
    }
}

fn role_id(role: MessageRole) -> &'static str {
    match role {
        MessageRole::User => "user",
        MessageRole::Assistant => "assistant",
        MessageRole::System => "system",
    }
}

fn runtime_mode_id(mode: RuntimeMode) -> &'static str {
    match mode {
        RuntimeMode::Ask => "ask",
        RuntimeMode::AutoAcceptEdits => "autoAcceptEdits",
        RuntimeMode::Auto => "auto",
        RuntimeMode::FullAccess => "fullAccess",
    }
}

fn parse_runtime_mode(raw: &str) -> anyhow::Result<RuntimeMode> {
    match raw.trim() {
        "ask" | "plan" => Ok(RuntimeMode::Ask),
        "autoAcceptEdits" | "auto_accept_edits" => Ok(RuntimeMode::AutoAcceptEdits),
        "auto" => Ok(RuntimeMode::Auto),
        "fullAccess" | "full_access" => Ok(RuntimeMode::FullAccess),
        other => bail!("invalid runtime_mode {other:?}"),
    }
}

fn parse_provider_kind(raw: &str) -> anyhow::Result<ProviderKind> {
    let normalized = raw.trim().to_ascii_lowercase();
    for kind in ProviderKind::ALL {
        if kind.id() == normalized {
            return Ok(kind);
        }
    }
    serde_json::from_value(Value::String(raw.trim().to_owned()))
        .with_context(|| format!("unknown provider {raw:?}"))
}

fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

/// Shared bounded idempotency cache for `send_message` retries.
#[derive(Default)]
pub(crate) struct PromptIdempotencyCache {
    entries: Mutex<std::collections::VecDeque<((Uuid, String), SubmitPromptOutcome)>>,
}

impl PromptIdempotencyCache {
    const MAX_ENTRIES: usize = 256;

    pub fn get(&self, session_id: Uuid, key: &str) -> Option<SubmitPromptOutcome> {
        let entries = self.entries.lock();
        entries.iter().rev().find_map(|((sid, k), outcome)| {
            (*sid == session_id && k == key).then(|| {
                let mut cloned = outcome.clone();
                cloned.deduplicated = true;
                cloned
            })
        })
    }

    pub fn insert(&self, session_id: Uuid, key: String, outcome: SubmitPromptOutcome) {
        let mut entries = self.entries.lock();
        entries.retain(|((sid, k), _)| !(*sid == session_id && k == &key));
        entries.push_back(((session_id, key), outcome));
        while entries.len() > Self::MAX_ENTRIES {
            entries.pop_front();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::TcpListener;
    use std::sync::atomic::AtomicBool;

    use crate::daemon::WakuBackend;
    use crate::persistence::StateStore;
    use crate::server::{EventSink, serve};
    use crate::settings::DaemonSettingsStore;
    use waku_protocol::{Request, ResponsePayload};

    fn raw_http_exchange(addr: std::net::SocketAddr, request: &[u8]) -> String {
        let mut stream = TcpStream::connect(addr).unwrap();
        stream.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        stream.set_write_timeout(Some(Duration::from_secs(5))).unwrap();
        stream.write_all(request).unwrap();
        stream.flush().unwrap();
        let mut response = Vec::new();
        let _ = stream.read_to_end(&mut response);
        String::from_utf8_lossy(&response).into_owned()
    }

    fn strict_http_exchange(addr: std::net::SocketAddr, request: &[u8]) -> String {
        let mut stream = TcpStream::connect(addr).unwrap();
        stream.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        stream.set_write_timeout(Some(Duration::from_secs(5))).unwrap();
        stream.write_all(request).unwrap();
        stream.flush().unwrap();
        let mut response = Vec::new();
        stream
            .read_to_end(&mut response)
            .expect("expected clean HTTP response EOF without ECONNRESET");
        String::from_utf8_lossy(&response).into_owned()
    }

    fn split_http_response(raw: &str) -> (u16, String, String) {
        let (head, body) = raw
            .split_once("\r\n\r\n")
            .unwrap_or_else(|| panic!("invalid HTTP response: {raw:?}"));
        let status = head
            .lines()
            .next()
            .and_then(|line| line.split_whitespace().nth(1))
            .and_then(|s| s.parse::<u16>().ok())
            .unwrap_or(0);
        (status, head.to_owned(), body.to_owned())
    }

    fn post_mcp(addr: std::net::SocketAddr, token: &str, body: &Value) -> (u16, String, Value) {
        let body_bytes = serde_json::to_vec(body).unwrap();
        let mut req = format!(
            "POST /mcp HTTP/1.1\r\nHost: {addr}\r\nAuthorization: Bearer {token}\r\nContent-Type: application/json\r\nAccept: application/json, text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            body_bytes.len()
        )
        .into_bytes();
        req.extend_from_slice(&body_bytes);
        let raw = raw_http_exchange(addr, &req);
        let (status, head, text) = split_http_response(&raw);
        let json = if text.trim().is_empty() {
            Value::Null
        } else {
            serde_json::from_str(&text)
                .unwrap_or_else(|e| panic!("non-JSON MCP body ({e}): {text}"))
        };
        (status, head, json)
    }

    /// Mock RemoteMcpBackend backed by real SQLite persistence logic via in-memory
    /// state + real Hub/Scribe event emission so we can test the full Streamable
    /// HTTP, auth, scopes, idempotency, wait_for_turn, pending permissions, and
    /// headless event reduction without spawning external agent CLIs.
    struct MockMcpBackend {
        settings: Mutex<RemoteMcpSettings>,
        projects: Mutex<Vec<Project>>,
        sessions: Mutex<Vec<AgentSession>>,
        runtimes: Mutex<HashMap<Uuid, (Uuid, bool)>>,
        idempotency: PromptIdempotencyCache,
        emit_permission_on_prompt: AtomicBool,
        resolved_permissions: Mutex<Vec<(Uuid, Uuid, String, String)>>,
    }

    impl MockMcpBackend {
        fn new(settings: RemoteMcpSettings) -> Self {
            let project = Project::from_path(PathBuf::from("/tmp/mock-workspace"));
            let mut session = AgentSession::new(project.id, ProviderKind::Codex);
            session.title = "Existing Task".into();
            session.begin_turn("Hello agent");
            session.push_message(MessageRole::Assistant, "Hi from earlier turn!");
            session.finish_active_turn(waku_protocol::model::TurnStatus::Completed);
            Self {
                settings: Mutex::new(settings),
                projects: Mutex::new(vec![project]),
                sessions: Mutex::new(vec![session]),
                runtimes: Mutex::new(HashMap::new()),
                idempotency: PromptIdempotencyCache::default(),
                emit_permission_on_prompt: AtomicBool::new(false),
                resolved_permissions: Mutex::new(Vec::new()),
            }
        }
    }

    impl Backend for MockMcpBackend {
        fn handle(&self, _request: Request, _events: EventSink) -> anyhow::Result<ResponsePayload> {
            Ok(ResponsePayload::Ack)
        }

        fn remote_mcp(&self) -> Option<&dyn RemoteMcpBackend> {
            Some(self)
        }
    }

    impl RemoteMcpBackend for MockMcpBackend {
        fn remote_mcp_settings(&self) -> RemoteMcpSettings {
            self.settings.lock().clone()
        }

        fn list_projects_and_sessions(
            &self,
        ) -> anyhow::Result<(Vec<Project>, Vec<AgentSession>, PathBuf)> {
            Ok((
                self.projects.lock().clone(),
                self.sessions
                    .lock()
                    .iter()
                    .map(AgentSession::list_projection)
                    .collect(),
                PathBuf::from("/tmp/mock-workspace"),
            ))
        }

        fn hydrate_session_by_id(&self, session_id: Uuid) -> anyhow::Result<Option<AgentSession>> {
            Ok(self
                .sessions
                .lock()
                .iter()
                .find(|s| s.id == session_id)
                .cloned())
        }

        fn resolve_legacy_session_id(&self, legacy_id: i64) -> anyhow::Result<Option<Uuid>> {
            if legacy_id == 42 {
                return Ok(self.sessions.lock().first().map(|s| s.id));
            }
            Ok(None)
        }

        fn search_session_messages(
            &self,
            query: String,
            limit: usize,
        ) -> anyhow::Result<Vec<SessionMessageMatch>> {
            let q = query.to_ascii_lowercase();
            let mut out = Vec::new();
            for s in self.sessions.lock().iter() {
                for m in &s.messages {
                    if m.content.to_ascii_lowercase().contains(&q) {
                        out.push(SessionMessageMatch {
                            session_id: s.id,
                            source: m.role,
                            snippet: m.content.clone(),
                        });
                        if out.len() >= limit {
                            return Ok(out);
                        }
                    }
                }
            }
            Ok(out)
        }

        fn create_session(
            &self,
            params: CreateSessionParams,
        ) -> anyhow::Result<(Project, AgentSession)> {
            let project = {
                let projects = self.projects.lock();
                if let Some(pid) = params.project_id {
                    projects
                        .iter()
                        .find(|p| p.id == pid)
                        .cloned()
                        .ok_or_else(|| anyhow!("project not found"))?
                } else {
                    projects[0].clone()
                }
            };
            let mut session =
                AgentSession::new(project.id, params.provider.unwrap_or(ProviderKind::Codex));
            if let Some(title) = params.title {
                session.title = title;
            }
            if let Some(model) = params.model {
                session.model = Some(model);
            }
            if let Some(mode) = params.runtime_mode {
                session.runtime_mode = mode;
            }
            self.sessions.lock().push(session.clone());
            Ok((project, session))
        }

        fn submit_prompt_in_mailbox(
            &self,
            session_id: Uuid,
            active_runtime_id: &mut Option<Uuid>,
            hub: &Arc<Hub>,
            params: SubmitPromptParams,
        ) -> anyhow::Result<SubmitPromptOutcome> {
            if let Some(ref key) = params.idempotency_key
                && let Some(cached) = self.idempotency.get(session_id, key)
            {
                return Ok(cached);
            }
            let runtime_id = {
                let mut runtimes = self.runtimes.lock();
                if let Some((rid, busy)) = runtimes.get_mut(&session_id) {
                    if *busy && !params.steer {
                        return Ok(SubmitPromptOutcome {
                            session_id,
                            runtime_id: *rid,
                            turn_id: None,
                            message_id: None,
                            status: "busy".into(),
                            accepted: false,
                            deduplicated: false,
                            detail: Some("busy".into()),
                        });
                    }
                    *busy = true;
                    *rid
                } else {
                    let rid = Uuid::new_v4();
                    hub.begin_runtime(session_id, rid);
                    runtimes.insert(session_id, (rid, true));
                    rid
                }
            };
            *active_runtime_id = Some(runtime_id);

            let turn_id = Uuid::new_v4();
            let message_id = Uuid::new_v4();
            let events = hub.event_sink(session_id, runtime_id);
            events.send(waku_protocol::event_to_wire(
                DriverEvent::PromptSubmitted {
                    message: params.prompt.clone(),
                    turn_id,
                    message_id,
                },
            )?)?;

            let emit_perm = self.emit_permission_on_prompt.load(Ordering::Acquire);
            let prompt_copy = params.prompt.clone();
            std::thread::spawn(move || {
                let _ = events.send(waku_protocol::event_to_wire(DriverEvent::TurnStarted).unwrap());
                if emit_perm {
                    let _ = events.send(
                        waku_protocol::event_to_wire(DriverEvent::Permission {
                            request_id: "perm-1".into(),
                            title: "Run shell command".into(),
                            detail: "ls -la".into(),
                            options: vec![
                                PermissionOption {
                                    id: "allow".into(),
                                    label: "Allow".into(),
                                    allow: true,
                                },
                                PermissionOption {
                                    id: "deny".into(),
                                    label: "Deny".into(),
                                    allow: false,
                                },
                            ],
                        })
                        .unwrap(),
                    );
                } else {
                    std::thread::sleep(Duration::from_millis(30));
                    let _ = events.send(
                        waku_protocol::event_to_wire(DriverEvent::TextDelta(format!(
                            "Echo: {prompt_copy}"
                        )))
                        .unwrap(),
                    );
                    let _ = events.send(
                        waku_protocol::event_to_wire(DriverEvent::TurnFinished {
                            success: true,
                            summary: None,
                        })
                        .unwrap(),
                    );
                }
            });

            let outcome = SubmitPromptOutcome {
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
                self.idempotency.insert(session_id, key, outcome.clone());
            }
            Ok(outcome)
        }

        fn cancel_session_in_mailbox(
            &self,
            session_id: Uuid,
            _active_runtime_id: Option<Uuid>,
            hub: &Arc<Hub>,
        ) -> anyhow::Result<bool> {
            let rid = {
                let mut runtimes = self.runtimes.lock();
                if let Some((rid, busy)) = runtimes.get_mut(&session_id) {
                    *busy = false;
                    Some(*rid)
                } else {
                    None
                }
            };
            if let Some(rid) = rid {
                let events = hub.event_sink(session_id, rid);
                let _ = events.send(waku_protocol::event_to_wire(DriverEvent::TurnFinished {
                    success: false,
                    summary: Some("Cancelled".into()),
                })?);
                return Ok(true);
            }
            Ok(false)
        }

        fn respond_permission_in_mailbox(
            &self,
            session_id: Uuid,
            runtime_id: Uuid,
            request_id: String,
            option_id: String,
            hub: &Arc<Hub>,
        ) -> anyhow::Result<()> {
            self.resolved_permissions
                .lock()
                .push((session_id, runtime_id, request_id, option_id));
            if let Some((_, busy)) = self.runtimes.lock().get_mut(&session_id) {
                *busy = false;
            }
            let events = hub.event_sink(session_id, runtime_id);
            let _ = events.send(waku_protocol::event_to_wire(DriverEvent::TextDelta(
                "Approved and finished.".into(),
            ))?);
            let _ = events.send(waku_protocol::event_to_wire(DriverEvent::TurnFinished {
                success: true,
                summary: None,
            })?);
            Ok(())
        }

        fn respond_user_input_in_mailbox(
            &self,
            _session_id: Uuid,
            _runtime_id: Uuid,
            _request_id: String,
            _answers: Vec<UserInputAnswer>,
            _hub: &Arc<Hub>,
        ) -> anyhow::Result<()> {
            Ok(())
        }

        fn persist_scribe_event(
            &self,
            event: &SequencedEvent,
            driver_event: &DriverEvent,
            _stream_phase: &mut Option<ScribeStreamPhase>,
            _flush_now: bool,
        ) -> anyhow::Result<bool> {
            let mut sessions = self.sessions.lock();
            if let Some(session) = sessions.iter_mut().find(|s| s.id == event.session_id) {
                match driver_event {
                    DriverEvent::PromptSubmitted {
                        message,
                        turn_id,
                        message_id,
                    } => {
                        session.adopt_submitted_prompt(message, *turn_id, *message_id);
                    }
                    DriverEvent::TextDelta(delta) => {
                        session.push_message(MessageRole::Assistant, delta.clone());
                    }
                    DriverEvent::TurnFinished { success, .. } => {
                        if let Some((_, busy)) = self.runtimes.lock().get_mut(&event.session_id) {
                            *busy = false;
                        }
                        session.status = if *success {
                            SessionStatus::Idle
                        } else {
                            SessionStatus::Failed
                        };
                        session.finish_active_turn(if *success {
                            waku_protocol::model::TurnStatus::Completed
                        } else {
                            waku_protocol::model::TurnStatus::Failed
                        });
                    }
                    _ => {}
                }
            }
            Ok(false)
        }

        fn flush_scribe_session(&self, _session_id: Uuid) -> anyhow::Result<()> {
            Ok(())
        }

        fn active_runtime_for(&self, session_id: Uuid) -> Option<(Uuid, bool)> {
            self.runtimes.lock().get(&session_id).copied()
        }
    }

    fn start_test_server(
        backend: Arc<dyn Backend>,
        options: ServerOptions,
    ) -> (std::net::SocketAddr, Arc<AtomicBool>, std::thread::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let shutdown = Arc::new(AtomicBool::new(false));
        let server_shutdown = shutdown.clone();
        let handle = std::thread::spawn(move || {
            serve(listener, "master-daemon-token".into(), backend, server_shutdown, options)
                .unwrap();
        });
        (addr, shutdown, handle)
    }

    #[test]
    fn disabled_and_auth_and_well_known_and_framing_rules() {
        let settings = RemoteMcpSettings {
            enabled: false,
            token: "agm_secret_123".into(),
            allow_send: true,
            allow_approvals: false,
        };
        let backend = Arc::new(MockMcpBackend::new(settings));
        let mut allowed_origins = HashSet::new();
        allowed_origins.insert("https://trusted.example".into());
        let (addr, shutdown, server) = start_test_server(
            backend.clone(),
            ServerOptions {
                allowed_origins,
                allow_shutdown: true,
                web_dist_dir: None,
            },
        );

        // 1. OAuth discovery returns clean 404.
        let raw = raw_http_exchange(
            addr,
            format!("GET /.well-known/oauth-authorization-server HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n\r\n").as_bytes(),
        );
        let (status, _, _) = split_http_response(&raw);
        assert_eq!(status, 404);

        // 2. Disabled returns 403.
        let (status, _, _) = post_mcp(
            addr,
            "agm_secret_123",
            &json!({"jsonrpc":"2.0","id":1,"method":"ping"}),
        );
        assert_eq!(status, 403);

        // Enable Remote MCP dynamically without restart.
        backend.settings.lock().enabled = true;

        // 3. Wrong token / master daemon token / query-param token all rejected with 401.
        let (status, head, _) = post_mcp(
            addr,
            "master-daemon-token",
            &json!({"jsonrpc":"2.0","id":1,"method":"ping"}),
        );
        assert_eq!(status, 401);
        assert!(head.contains("WWW-Authenticate: Bearer"));

        let raw_query = raw_http_exchange(
            addr,
            format!("POST /mcp?token=agm_secret_123 HTTP/1.1\r\nHost: {addr}\r\nAuthorization: Bearer agm_secret_123\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{{}}").as_bytes(),
        );
        assert_eq!(split_http_response(&raw_query).0, 401);

        // 4. Disallowed Origin rejected with 403; allowed Origin passes OPTIONS preflight.
        let raw_bad_origin = raw_http_exchange(
            addr,
            format!("POST /mcp HTTP/1.1\r\nHost: {addr}\r\nOrigin: https://evil.example\r\nAuthorization: Bearer agm_secret_123\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{{}}").as_bytes(),
        );
        assert_eq!(split_http_response(&raw_bad_origin).0, 403);

        let raw_options = raw_http_exchange(
            addr,
            format!("OPTIONS /mcp HTTP/1.1\r\nHost: {addr}\r\nOrigin: https://trusted.example\r\nConnection: close\r\n\r\n").as_bytes(),
        );
        let (status, head, _) = split_http_response(&raw_options);
        assert_eq!(status, 204);
        assert!(head.contains("Access-Control-Allow-Origin: https://trusted.example"));
        assert!(head.contains("Access-Control-Expose-Headers: Mcp-Session-Id"));

        // 5. GET /mcp -> 405, DELETE /mcp -> 204.
        let raw_get = raw_http_exchange(
            addr,
            format!("GET /mcp HTTP/1.1\r\nHost: {addr}\r\nAuthorization: Bearer agm_secret_123\r\nConnection: close\r\n\r\n").as_bytes(),
        );
        let (status, head, _) = split_http_response(&raw_get);
        assert_eq!(status, 405);
        assert!(head.contains("Allow: POST, DELETE, OPTIONS"));

        let raw_del = raw_http_exchange(
            addr,
            format!("DELETE /mcp HTTP/1.1\r\nHost: {addr}\r\nAuthorization: Bearer agm_secret_123\r\nConnection: close\r\n\r\n").as_bytes(),
        );
        assert_eq!(split_http_response(&raw_del).0, 204);

        // 6. Ambiguous CL + TE -> 400.
        let raw_smuggle = raw_http_exchange(
            addr,
            format!("POST /mcp HTTP/1.1\r\nHost: {addr}\r\nAuthorization: Bearer agm_secret_123\r\nContent-Length: 2\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n{{}}").as_bytes(),
        );
        assert_eq!(split_http_response(&raw_smuggle).0, 400);

        // 7. Chunked body + Expect: 100-continue work cleanly.
        let payload = r#"{"jsonrpc":"2.0","id":7,"method":"ping"}"#;
        let chunked = format!(
            "POST /mcp HTTP/1.1\r\nHost: {addr}\r\nAuthorization: Bearer agm_secret_123\r\nExpect: 100-continue\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n{:x}\r\n{payload}\r\n0\r\n\r\n",
            payload.len()
        );
        let raw_chunked = raw_http_exchange(addr, chunked.as_bytes());
        assert!(raw_chunked.starts_with("HTTP/1.1 100 Continue\r\n\r\n"));
        let rest = raw_chunked.strip_prefix("HTTP/1.1 100 Continue\r\n\r\n").unwrap();
        let (status, _, body) = split_http_response(rest);
        assert_eq!(status, 200);
        let parsed: Value = serde_json::from_str(&body).unwrap();
        assert_eq!(parsed["id"], 7);
        assert_eq!(parsed["result"], json!({}));

        // 8. Notification returns 202 Accepted.
        let (status, _, _) = post_mcp(
            addr,
            "agm_secret_123",
            &json!({"jsonrpc":"2.0","method":"notifications/initialized"}),
        );
        assert_eq!(status, 202);

        shutdown.store(true, Ordering::Release);
        server.join().unwrap();
    }

    #[test]
    fn mcp_tools_read_send_wait_idempotency_and_approval_scopes() {
        let token = "agm_full_test_token";
        let backend = Arc::new(MockMcpBackend::new(RemoteMcpSettings {
            enabled: true,
            token: token.into(),
            allow_send: false,
            allow_approvals: false,
        }));
        let (addr, shutdown, server) =
            start_test_server(backend.clone(), ServerOptions::default());

        // Initialize negotiates protocol version and sets Mcp-Session-Id.
        let (status, head, init) = post_mcp(
            addr,
            token,
            &json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "initialize",
                "params": { "protocolVersion": "2025-06-18", "capabilities": {}, "clientInfo": { "name": "test", "version": "1.0" } }
            }),
        );
        assert_eq!(status, 200);
        assert!(head.contains("Mcp-Session-Id:"));
        assert_eq!(init["result"]["protocolVersion"], "2025-06-18");

        // Read-only scope exposes only 6 read tools and rejects send_message.
        let (_, _, list_ro) = post_mcp(
            addr,
            token,
            &json!({"jsonrpc":"2.0","id":2,"method":"tools/list"}),
        );
        let ro_names: Vec<&str> = list_ro["result"]["tools"]
            .as_array()
            .unwrap()
            .iter()
            .map(|t| t["name"].as_str().unwrap())
            .collect();
        assert_eq!(ro_names.len(), 6);
        assert!(!ro_names.contains(&"send_message"));

        let (_, _, send_denied) = post_mcp(
            addr,
            token,
            &json!({
                "jsonrpc":"2.0","id":3,"method":"tools/call",
                "params":{"name":"send_message","arguments":{"session_id":"42","message":"hi"}}
            }),
        );
        assert_eq!(send_denied["result"]["isError"], true);

        // View sessions, legacy ID resolution, search, and pagination.
        let (_, _, listed) = post_mcp(
            addr,
            token,
            &json!({"jsonrpc":"2.0","id":4,"method":"tools/call","params":{"name":"list_sessions","arguments":{}}}),
        );
        assert_eq!(listed["result"]["structuredContent"]["total"], 1);
        let session_id = listed["result"]["structuredContent"]["sessions"][0]["session_id"]
            .as_str()
            .unwrap()
            .to_owned();

        let (_, _, got_legacy) = post_mcp(
            addr,
            token,
            &json!({"jsonrpc":"2.0","id":5,"method":"tools/call","params":{"name":"get_session","arguments":{"session_id":"42"}}}),
        );
        assert_eq!(got_legacy["result"]["structuredContent"]["found"], true);
        assert_eq!(
            got_legacy["result"]["structuredContent"]["session_id"],
            session_id
        );

        let (_, _, searched) = post_mcp(
            addr,
            token,
            &json!({"jsonrpc":"2.0","id":6,"method":"tools/call","params":{"name":"search_sessions","arguments":{"query":"earlier"}}}),
        );
        assert_eq!(searched["result"]["structuredContent"]["total"], 1);

        // Enable allow_send -> create_session + send_message(wait=true) + idempotency.
        backend.settings.lock().allow_send = true;
        let (_, _, sent) = post_mcp(
            addr,
            token,
            &json!({
                "jsonrpc":"2.0","id":7,"method":"tools/call",
                "params":{
                    "name":"send_message",
                    "arguments":{
                        "session_id": session_id,
                        "message": "ping from remote",
                        "wait": true,
                        "timeout_seconds": 5,
                        "idempotency_key": "op-001"
                    }
                }
            }),
        );
        let sc = &sent["result"]["structuredContent"];
        assert_eq!(sc["accepted"], true);
        assert_eq!(sc["status"], "completed");
        assert_eq!(sc["reply"], "Echo: ping from remote");

        // Duplicate idempotency_key returns deduplicated=true without re-sending.
        let (_, _, dup) = post_mcp(
            addr,
            token,
            &json!({
                "jsonrpc":"2.0","id":8,"method":"tools/call",
                "params":{
                    "name":"send_message",
                    "arguments":{
                        "session_id": session_id,
                        "message": "ping from remote",
                        "wait": false,
                        "idempotency_key": "op-001"
                    }
                }
            }),
        );
        assert_eq!(dup["result"]["structuredContent"]["deduplicated"], true);

        // Pending permission pause + remote approval scope check.
        backend
            .emit_permission_on_prompt
            .store(true, Ordering::Release);
        let (_, _, paused) = post_mcp(
            addr,
            token,
            &json!({
                "jsonrpc":"2.0","id":9,"method":"tools/call",
                "params":{
                    "name":"send_message",
                    "arguments":{
                        "session_id": session_id,
                        "message": "need permission",
                        "wait": true,
                        "timeout_seconds": 5
                    }
                }
            }),
        );
        let psc = &paused["result"]["structuredContent"];
        assert_eq!(psc["status"], "awaiting_permission");
        let runtime_id = psc["runtime_id"].as_str().unwrap().to_owned();
        assert_eq!(psc["pending_permissions"][0]["request_id"], "perm-1");

        // Approval fails while allow_approvals is false.
        let (_, _, approve_denied) = post_mcp(
            addr,
            token,
            &json!({
                "jsonrpc":"2.0","id":10,"method":"tools/call",
                "params":{
                    "name":"respond_pending_request",
                    "arguments":{
                        "session_id": session_id,
                        "runtime_id": runtime_id,
                        "request_id": "perm-1",
                        "kind": "permission",
                        "option_id": "allow"
                    }
                }
            }),
        );
        assert_eq!(approve_denied["result"]["isError"], true);

        // Enable allow_approvals -> approval succeeds and turn completes.
        backend.settings.lock().allow_approvals = true;
        let (_, _, approved) = post_mcp(
            addr,
            token,
            &json!({
                "jsonrpc":"2.0","id":11,"method":"tools/call",
                "params":{
                    "name":"respond_pending_request",
                    "arguments":{
                        "session_id": session_id,
                        "runtime_id": runtime_id,
                        "request_id": "perm-1",
                        "kind": "permission",
                        "option_id": "allow"
                    }
                }
            }),
        );
        assert_eq!(approved["result"]["structuredContent"]["resolved"], true);
        assert_eq!(backend.resolved_permissions.lock().len(), 1);

        let (_, _, waited) = post_mcp(
            addr,
            token,
            &json!({
                "jsonrpc":"2.0","id":12,"method":"tools/call",
                "params":{"name":"wait_for_turn","arguments":{"session_id": session_id, "timeout_seconds": 5}}
            }),
        );
        assert_eq!(waited["result"]["structuredContent"]["status"], "completed");

        shutdown.store(true, Ordering::Release);
        server.join().unwrap();
    }

    #[test]
    fn real_waku_backend_persists_headless_scribe_events_to_sqlite() {
        let root = std::env::temp_dir().join(format!("waku-remote-mcp-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&root).unwrap();
        let settings_store = DaemonSettingsStore::open(root.join("settings.json")).unwrap();
        let mut settings = settings_store.get();
        settings.remote_mcp = RemoteMcpSettings {
            enabled: true,
            token: "agm_sqlite_token".into(),
            allow_send: true,
            allow_approvals: false,
        };
        settings_store.replace(settings).unwrap();

        let backend = Arc::new(
            WakuBackend::new(settings_store, StateStore::daemon(root.join("app.db"))).unwrap(),
        );
        let (addr, shutdown, server) =
            start_test_server(backend.clone(), ServerOptions::default());

        // Create a real session via Remote MCP.
        let (_, _, created) = post_mcp(
            addr,
            "agm_sqlite_token",
            &json!({
                "jsonrpc":"2.0","id":1,"method":"tools/call",
                "params":{"name":"create_session","arguments":{"title":"Headless MCP Task","provider":"codex"}}
            }),
        );
        assert_eq!(created["result"]["structuredContent"]["created"], true);
        let sid = Uuid::parse_str(
            created["result"]["structuredContent"]["session_id"]
                .as_str()
                .unwrap(),
        )
        .unwrap();

        // Simulate headless provider events persisted by WakuBackend's scribe reducer.
        let rid = Uuid::new_v4();
        let epoch = Uuid::new_v4();
        let tid = Uuid::new_v4();
        let mid = Uuid::new_v4();
        let mut phase = None;
        let events = [
            (
                1,
                DriverEvent::PromptSubmitted {
                    message: "Hello headless".into(),
                    turn_id: tid,
                    message_id: mid,
                },
                true,
            ),
            (2, DriverEvent::TurnStarted, true),
            (3, DriverEvent::TextDelta("Headless reply!".into()), false),
            (
                4,
                DriverEvent::TurnFinished {
                    success: true,
                    summary: None,
                },
                true,
            ),
        ];
        for (seq, ev, flush) in events {
            let wire = waku_protocol::event_to_wire(ev.clone()).unwrap();
            let sequenced = SequencedEvent {
                session_id: sid,
                runtime_id: rid,
                epoch,
                sequence: seq,
                event: wire,
            };
            backend
                .persist_scribe_event(&sequenced, &ev, &mut phase, flush)
                .unwrap();
        }

        // Verify via get_session and get_messages over HTTP MCP.
        let (_, _, got) = post_mcp(
            addr,
            "agm_sqlite_token",
            &json!({
                "jsonrpc":"2.0","id":2,"method":"tools/call",
                "params":{"name":"get_session","arguments":{"session_id": sid.to_string()}}
            }),
        );
        let sc = &got["result"]["structuredContent"];
        assert_eq!(sc["found"], true);
        assert_eq!(sc["status"], "idle");
        assert_eq!(sc["message_count"], 2);
        assert_eq!(sc["messages"][0]["role"], "user");
        assert_eq!(sc["messages"][0]["content"], "Hello headless");
        assert_eq!(sc["messages"][1]["role"], "assistant");
        assert_eq!(sc["messages"][1]["content"], "Headless reply!");

        // Verify pagination with get_messages.
        let (_, _, page1) = post_mcp(
            addr,
            "agm_sqlite_token",
            &json!({
                "jsonrpc":"2.0","id":3,"method":"tools/call",
                "params":{"name":"get_messages","arguments":{"session_id": sid.to_string(), "offset": 0, "limit": 1}}
            }),
        );
        assert_eq!(page1["result"]["structuredContent"]["has_more"], true);
        assert_eq!(page1["result"]["structuredContent"]["next_offset"], 1);
        assert_eq!(
            page1["result"]["structuredContent"]["messages"][0]["content"],
            "Hello headless"
        );

        // Verify persistence survived SQLite reload from disk.
        let reloaded = StateStore::daemon(root.join("app.db")).load().unwrap();
        let mut reloaded_session = reloaded
            .sessions
            .into_iter()
            .find(|s| s.id == sid)
            .unwrap();
        StateStore::daemon(root.join("app.db"))
            .hydrate(&mut reloaded_session)
            .unwrap();
        assert_eq!(reloaded_session.messages.len(), 2);
        assert_eq!(reloaded_session.messages[1].content, "Headless reply!");
        assert_eq!(
            reloaded_session.runtime_event_cursor.unwrap().sequence,
            4
        );

        shutdown.store(true, Ordering::Release);
        server.join().unwrap();
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn early_error_responses_survive_large_bodies_and_expect_continue_without_rst() {
        let token = "agm_large_drain_token";
        let backend = Arc::new(MockMcpBackend::new(RemoteMcpSettings {
            enabled: true,
            token: token.into(),
            allow_send: true,
            allow_approvals: false,
        }));
        let (addr, shutdown, server) =
            start_test_server(backend.clone(), ServerOptions::default());

        // (a) 1 MiB POST with a bad token must yield a readable 401 JSON body (no TCP RST).
        let payload_1m = vec![b'x'; 1024 * 1024];
        let mut req_401 = format!(
            "POST /mcp HTTP/1.1\r\nHost: {addr}\r\nAuthorization: Bearer wrong_token\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            payload_1m.len()
        )
        .into_bytes();
        req_401.extend_from_slice(&payload_1m);
        let raw_401 = strict_http_exchange(addr, &req_401);
        let (st_401, _, body_401) = split_http_response(&raw_401);
        assert_eq!(st_401, 401);
        let json_401: Value = serde_json::from_str(&body_401).unwrap();
        assert!(json_401["error"].as_str().unwrap().contains("unauthorized"));

        // (b) Declared 3 MiB POST with valid token must yield a readable 413 JSON body.
        let payload_3m = vec![b'y'; 3 * 1024 * 1024];
        let mut req_413 = format!(
            "POST /mcp HTTP/1.1\r\nHost: {addr}\r\nAuthorization: Bearer {token}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            payload_3m.len()
        )
        .into_bytes();
        req_413.extend_from_slice(&payload_3m);
        let raw_413 = strict_http_exchange(addr, &req_413);
        let (st_413, _, body_413) = split_http_response(&raw_413);
        assert_eq!(st_413, 413);
        let json_413: Value = serde_json::from_str(&body_413).unwrap();
        assert!(json_413["error"].as_str().unwrap().contains("2 MiB"));

        // (c) Expect: 100-continue with a bad token gets 401 immediately and NO 100 Continue.
        let req_expect = format!(
            "POST /mcp HTTP/1.1\r\nHost: {addr}\r\nAuthorization: Bearer wrong_token\r\nExpect: 100-continue\r\nContent-Type: application/json\r\nContent-Length: 1024\r\nConnection: close\r\n\r\n"
        );
        let raw_expect = strict_http_exchange(addr, req_expect.as_bytes());
        assert!(
            !raw_expect.contains("100 Continue"),
            "must not emit 100 Continue for rejected auth: {raw_expect:?}"
        );
        let (st_expect, _, body_expect) = split_http_response(&raw_expect);
        assert_eq!(st_expect, 401);
        let json_expect: Value = serde_json::from_str(&body_expect).unwrap();
        assert!(json_expect["error"].as_str().unwrap().contains("unauthorized"));

        shutdown.store(true, Ordering::Release);
        server.join().unwrap();
    }

    #[test]
    fn wait_for_turn_isolates_concurrent_turns_runtime_end_and_timeouts() {
        let token = "agm_turn_isolation_token";
        let backend = Arc::new(MockMcpBackend::new(RemoteMcpSettings {
            enabled: true,
            token: token.into(),
            allow_send: true,
            allow_approvals: false,
        }));
        let (addr, shutdown, server) =
            start_test_server(backend.clone(), ServerOptions::default());

        let sid = backend.sessions.lock()[0].id;

        // Turn 1 runs and completes normally.
        let (_, _, send1) = post_mcp(
            addr,
            token,
            &json!({
                "jsonrpc": "2.0", "id": 1, "method": "tools/call",
                "params": {
                    "name": "send_message",
                    "arguments": { "session_id": sid.to_string(), "message": "Turn one", "wait": true, "timeout_seconds": 5 }
                }
            }),
        );
        let turn1_id = send1["result"]["structuredContent"]["turn_id"]
            .as_str()
            .unwrap()
            .to_owned();
        assert_eq!(send1["result"]["structuredContent"]["status"], "completed");
        assert_eq!(send1["result"]["structuredContent"]["timed_out"], false);

        // Immediately start Turn 2 which pauses on a permission request.
        backend
            .emit_permission_on_prompt
            .store(true, Ordering::Release);
        let (_, _, send2) = post_mcp(
            addr,
            token,
            &json!({
                "jsonrpc": "2.0", "id": 2, "method": "tools/call",
                "params": {
                    "name": "send_message",
                    "arguments": { "session_id": sid.to_string(), "message": "Turn two", "wait": true, "timeout_seconds": 5 }
                }
            }),
        );
        assert_eq!(
            send2["result"]["structuredContent"]["status"],
            "awaiting_permission"
        );

        // Querying Turn 1 while Turn 2 is awaiting permission must still report Turn 1 completed
        // with empty pending_permissions and Turn 1's reply (no Turn 2 state bleed).
        let (_, _, wait1) = post_mcp(
            addr,
            token,
            &json!({
                "jsonrpc": "2.0", "id": 3, "method": "tools/call",
                "params": {
                    "name": "wait_for_turn",
                    "arguments": { "session_id": sid.to_string(), "turn_id": turn1_id, "timeout_seconds": 5 }
                }
            }),
        );
        let sc1 = &wait1["result"]["structuredContent"];
        assert_eq!(sc1["status"], "completed");
        assert_eq!(sc1["timed_out"], false);
        assert_eq!(sc1["pending_permissions"].as_array().unwrap().len(), 0);
        assert_eq!(sc1["reply"], "Echo: Turn one");

        // Waiting for an unknown turn_id on a live session must time out explicitly (never "completed").
        let unknown_tid = Uuid::new_v4();
        let (_, _, wait_unknown) = post_mcp(
            addr,
            token,
            &json!({
                "jsonrpc": "2.0", "id": 4, "method": "tools/call",
                "params": {
                    "name": "wait_for_turn",
                    "arguments": { "session_id": sid.to_string(), "turn_id": unknown_tid.to_string(), "timeout_seconds": 1 }
                }
            }),
        );
        let scu = &wait_unknown["result"]["structuredContent"];
        assert_eq!(scu["status"], "timeout");
        assert_eq!(scu["timed_out"], true);

        // Direct DaemonEventScribe test: finished_turns survives RuntimeEnded, and epoch change
        // resets stale Working status + active_turn_id.
        let hub = Arc::new(Hub::default());
        let (obs_tx, obs_rx) = crossbeam_channel::unbounded();
        let scribe = DaemonEventScribe::spawn(obs_rx, backend.clone(), hub.clone());
        let rid1 = Uuid::new_v4();
        let epoch1 = Uuid::new_v4();
        let tid_direct = Uuid::new_v4();
        let mid_direct = Uuid::new_v4();
        obs_tx
            .send(HubObservation::Event(SequencedEvent {
                session_id: sid,
                runtime_id: rid1,
                epoch: epoch1,
                sequence: 1,
                event: waku_protocol::event_to_wire(DriverEvent::PromptSubmitted {
                    message: "Direct".into(),
                    turn_id: tid_direct,
                    message_id: mid_direct,
                })
                .unwrap(),
            }))
            .unwrap();
        obs_tx
            .send(HubObservation::Event(SequencedEvent {
                session_id: sid,
                runtime_id: rid1,
                epoch: epoch1,
                sequence: 2,
                event: waku_protocol::event_to_wire(DriverEvent::TurnStarted).unwrap(),
            }))
            .unwrap();
        obs_tx
            .send(HubObservation::Event(SequencedEvent {
                session_id: sid,
                runtime_id: rid1,
                epoch: epoch1,
                sequence: 3,
                event: waku_protocol::event_to_wire(DriverEvent::TurnFinished {
                    success: true,
                    summary: None,
                })
                .unwrap(),
            }))
            .unwrap();
        obs_tx
            .send(HubObservation::RuntimeEnded {
                session_id: sid,
                runtime_id: Some(rid1),
            })
            .unwrap();

        // Wait must resolve immediately from finished_turns even after RuntimeEnded removed live[sid].
        let started = Instant::now();
        let outcome = scribe.wait_for_turn(sid, Some(tid_direct), Duration::from_secs(5));
        assert!(started.elapsed() < Duration::from_secs(1));
        assert!(matches!(outcome, WaitOutcome::TurnFinished(rec) if rec.turn_id == tid_direct && rec.success));

        // Epoch change must reset stale Working status + active_turn_id.
        let tid_stale = Uuid::new_v4();
        obs_tx
            .send(HubObservation::Event(SequencedEvent {
                session_id: sid,
                runtime_id: rid1,
                epoch: epoch1,
                sequence: 4,
                event: waku_protocol::event_to_wire(DriverEvent::PromptSubmitted {
                    message: "Stale".into(),
                    turn_id: tid_stale,
                    message_id: Uuid::new_v4(),
                })
                .unwrap(),
            }))
            .unwrap();
        obs_tx
            .send(HubObservation::Event(SequencedEvent {
                session_id: sid,
                runtime_id: rid1,
                epoch: epoch1,
                sequence: 5,
                event: waku_protocol::event_to_wire(DriverEvent::TurnStarted).unwrap(),
            }))
            .unwrap();
        let epoch2 = Uuid::new_v4();
        obs_tx
            .send(HubObservation::Event(SequencedEvent {
                session_id: sid,
                runtime_id: rid1,
                epoch: epoch2,
                sequence: 1,
                event: waku_protocol::event_to_wire(DriverEvent::Connected {
                    provider_cursor: None,
                })
                .unwrap(),
            }))
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            if let Some(snap) = scribe.snapshot(sid)
                && snap.active_turn_id.is_none()
                && snap.status == SessionStatus::Idle
            {
                break;
            }
            assert!(Instant::now() < deadline, "epoch change did not reset stale Working status");
            std::thread::sleep(Duration::from_millis(10));
        }

        // Runtime/epoch change on a busy turn must also record a failed FinishedTurnRecord
        // for the orphaned turn so waiters wake immediately instead of hanging.
        let orphaned_outcome = scribe.wait_for_turn(sid, Some(tid_stale), Duration::from_secs(2));
        assert!(
            matches!(orphaned_outcome, WaitOutcome::TurnFinished(ref rec) if rec.turn_id == tid_stale && !rec.success),
            "expected orphaned turn to finish as failed on epoch swap, got {orphaned_outcome:?}"
        );

        // Persisted TurnStatus::Running with no active runtime (simulating daemon restart
        // mid-turn) must resolve immediately as "interrupted" instead of timing out.
        let crash_sid = {
            let mut sessions = backend.sessions.lock();
            let project_id = backend.projects.lock()[0].id;
            let mut crashed = AgentSession::new(project_id, ProviderKind::Codex);
            crashed.title = "Crashed Mid-Turn".into();
            crashed.begin_turn("Long running work");
            let crash_sid = crashed.id;
            sessions.push(crashed);
            crash_sid
        };
        let crash_tid = backend
            .sessions
            .lock()
            .iter()
            .find(|s| s.id == crash_sid)
            .unwrap()
            .turns[0]
            .id;
        let started_crash = Instant::now();
        let (_, _, crash_wait) = post_mcp(
            addr,
            token,
            &json!({
                "jsonrpc": "2.0", "id": 5, "method": "tools/call",
                "params": {
                    "name": "wait_for_turn",
                    "arguments": {
                        "session_id": crash_sid.to_string(),
                        "turn_id": crash_tid.to_string(),
                        "timeout_seconds": 10
                    }
                }
            }),
        );
        assert!(
            started_crash.elapsed() < Duration::from_secs(1),
            "persisted Running turn after restart should resolve immediately"
        );
        let sc_crash = &crash_wait["result"]["structuredContent"];
        assert_eq!(sc_crash["status"], "interrupted");
        assert_eq!(sc_crash["timed_out"], false);

        shutdown.store(true, Ordering::Release);
        server.join().unwrap();
    }

    #[test]
    fn error_drain_budget_protects_authenticated_requests_under_unauth_flood() {
        let token = "agm_flood_guard_token";
        let backend = Arc::new(MockMcpBackend::new(RemoteMcpSettings {
            enabled: true,
            token: token.into(),
            allow_send: true,
            allow_approvals: false,
        }));
        let (addr, shutdown, server) =
            start_test_server(backend.clone(), ServerOptions::default());

        // Open 16 unauthenticated POSTs that declare 1 MiB bodies but stay silent.
        // Because MAX_MCP_ERROR_DRAINS is capped at 8, excess connections skip draining
        // and release shared listener slots immediately, keeping authorized /mcp fast.
        let mut flood_streams = Vec::new();
        for _ in 0..16 {
            let mut s = TcpStream::connect(addr).unwrap();
            let req = format!(
                "POST /mcp HTTP/1.1\r\nHost: {addr}\r\nAuthorization: Bearer bad\r\nContent-Length: 1048576\r\nConnection: close\r\n\r\n"
            );
            s.write_all(req.as_bytes()).unwrap();
            s.flush().unwrap();
            flood_streams.push(s);
        }
        std::thread::sleep(Duration::from_millis(80));

        let started = Instant::now();
        let (st, _, resp) = post_mcp(
            addr,
            token,
            &json!({
                "jsonrpc": "2.0", "id": 1, "method": "initialize",
                "params": { "protocolVersion": "2025-06-18", "capabilities": {}, "clientInfo": { "name": "flood-test", "version": "1.0" } }
            }),
        );
        assert_eq!(st, 200);
        assert_eq!(resp["result"]["protocolVersion"], "2025-06-18");
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "authorized initialize was starved by unauth drains: {:?}",
            started.elapsed()
        );

        drop(flood_streams);
        shutdown.store(true, Ordering::Release);
        server.join().unwrap();
    }

    #[test]
    fn terminal_turn_publication_waits_for_backend_persistence() {
        struct BarrierBackend {
            inner: MockMcpBackend,
            enter_tx: crossbeam_channel::Sender<()>,
            release_rx: crossbeam_channel::Receiver<()>,
        }

        impl Backend for BarrierBackend {
            fn handle(&self, req: Request, events: EventSink) -> anyhow::Result<ResponsePayload> {
                self.inner.handle(req, events)
            }
            fn remote_mcp(&self) -> Option<&dyn RemoteMcpBackend> {
                Some(self)
            }
        }

        impl RemoteMcpBackend for BarrierBackend {
            fn remote_mcp_settings(&self) -> RemoteMcpSettings {
                self.inner.remote_mcp_settings()
            }
            fn list_projects_and_sessions(&self) -> anyhow::Result<(Vec<Project>, Vec<AgentSession>, PathBuf)> {
                self.inner.list_projects_and_sessions()
            }
            fn hydrate_session_by_id(&self, session_id: Uuid) -> anyhow::Result<Option<AgentSession>> {
                self.inner.hydrate_session_by_id(session_id)
            }
            fn resolve_legacy_session_id(&self, legacy_id: i64) -> anyhow::Result<Option<Uuid>> {
                self.inner.resolve_legacy_session_id(legacy_id)
            }
            fn search_session_messages(&self, query: String, limit: usize) -> anyhow::Result<Vec<SessionMessageMatch>> {
                self.inner.search_session_messages(query, limit)
            }
            fn create_session(&self, params: CreateSessionParams) -> anyhow::Result<(Project, AgentSession)> {
                self.inner.create_session(params)
            }
            fn submit_prompt_in_mailbox(&self, session_id: Uuid, active_runtime_id: &mut Option<Uuid>, hub: &Arc<Hub>, params: SubmitPromptParams) -> anyhow::Result<SubmitPromptOutcome> {
                self.inner.submit_prompt_in_mailbox(session_id, active_runtime_id, hub, params)
            }
            fn cancel_session_in_mailbox(&self, session_id: Uuid, active_runtime_id: Option<Uuid>, hub: &Arc<Hub>) -> anyhow::Result<bool> {
                self.inner.cancel_session_in_mailbox(session_id, active_runtime_id, hub)
            }
            fn respond_permission_in_mailbox(&self, session_id: Uuid, runtime_id: Uuid, request_id: String, option_id: String, hub: &Arc<Hub>) -> anyhow::Result<()> {
                self.inner.respond_permission_in_mailbox(session_id, runtime_id, request_id, option_id, hub)
            }
            fn respond_user_input_in_mailbox(&self, session_id: Uuid, runtime_id: Uuid, request_id: String, answers: Vec<UserInputAnswer>, hub: &Arc<Hub>) -> anyhow::Result<()> {
                self.inner.respond_user_input_in_mailbox(session_id, runtime_id, request_id, answers, hub)
            }
            fn persist_scribe_event(&self, event: &SequencedEvent, driver_event: &DriverEvent, stream_phase: &mut Option<ScribeStreamPhase>, flush_now: bool) -> anyhow::Result<bool> {
                if matches!(driver_event, DriverEvent::TurnFinished { .. }) {
                    let _ = self.enter_tx.send(());
                    let _ = self.release_rx.recv();
                }
                self.inner.persist_scribe_event(event, driver_event, stream_phase, flush_now)
            }
            fn flush_scribe_session(&self, session_id: Uuid) -> anyhow::Result<()> {
                self.inner.flush_scribe_session(session_id)
            }
            fn active_runtime_for(&self, session_id: Uuid) -> Option<(Uuid, bool)> {
                self.inner.active_runtime_for(session_id)
            }
        }

        let (enter_tx, enter_rx) = crossbeam_channel::bounded(1);
        let (release_tx, release_rx) = crossbeam_channel::bounded(1);
        let backend = Arc::new(BarrierBackend {
            inner: MockMcpBackend::new(RemoteMcpSettings::default()),
            enter_tx,
            release_rx,
        });
        let sid = backend.inner.sessions.lock()[0].id;
        let hub = Arc::new(Hub::default());
        let (obs_tx, obs_rx) = crossbeam_channel::unbounded();
        let scribe = DaemonEventScribe::spawn(obs_rx, backend.clone(), hub);

        let rid = Uuid::new_v4();
        let epoch = Uuid::new_v4();
        let tid = Uuid::new_v4();
        obs_tx
            .send(HubObservation::Event(SequencedEvent {
                session_id: sid,
                runtime_id: rid,
                epoch,
                sequence: 1,
                event: waku_protocol::event_to_wire(DriverEvent::PromptSubmitted {
                    message: "Barrier".into(),
                    turn_id: tid,
                    message_id: Uuid::new_v4(),
                })
                .unwrap(),
            }))
            .unwrap();
        obs_tx
            .send(HubObservation::Event(SequencedEvent {
                session_id: sid,
                runtime_id: rid,
                epoch,
                sequence: 2,
                event: waku_protocol::event_to_wire(DriverEvent::TurnStarted).unwrap(),
            }))
            .unwrap();
        obs_tx
            .send(HubObservation::Event(SequencedEvent {
                session_id: sid,
                runtime_id: rid,
                epoch,
                sequence: 3,
                event: waku_protocol::event_to_wire(DriverEvent::TurnFinished {
                    success: true,
                    summary: None,
                })
                .unwrap(),
            }))
            .unwrap();

        // Wait until scribe enters persist_scribe_event for TurnFinished.
        enter_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("scribe did not enter persist_scribe_event");

        // While terminal persistence is paused, FinishedTurnRecord MUST NOT be published yet.
        assert!(
            scribe.find_finished_turn(sid, tid).is_none(),
            "finished_turns published before persist_scribe_event completed!"
        );

        // Release persistence and verify FinishedTurnRecord becomes visible immediately.
        release_tx.send(()).unwrap();
        let outcome = scribe.wait_for_turn(sid, Some(tid), Duration::from_secs(2));
        assert!(matches!(outcome, WaitOutcome::TurnFinished(rec) if rec.turn_id == tid && rec.success));
    }
}
