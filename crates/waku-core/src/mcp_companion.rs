//! Built-in AgentOS stdio MCP server backed by Waku's SQLite DB.
//!
//! Invoked via `waku-daemon --mcp-stdio --session-id <uuid> --db <path> --cwd <dir>`
//! so every ACP, Codex, and Claude session receives durable task memory
//! (`memory_search`, `memory_read`, `memory_note_write`,
//! `memory_checkpoint_prepare`, `memory_context_rotate`,
//! `memory_knowledge_propose`), persistent goal tracking (`get_goal`,
//! `create_goal`, `report_goal`), and cross-session lookup (`get_session_info`)
//! without shipping or locking a separate sidecar binary.

use std::io::{BufRead as _, BufReader, Write as _};
use std::path::{Path, PathBuf};
use std::process::{Command as ProcessCommand, Stdio};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context as _, anyhow};
use rusqlite::{Connection, OptionalExtension as _, params};
use serde_json::{Value, json};
use uuid::Uuid;
use waku_protocol::memory::{
    MemoryCheckpointRecord, MemoryEntryKind, MemoryKnowledgeRecord, MemoryLibrarySnapshot,
    MemoryNoteRecord,
};
use waku_protocol::model::{
    GoalOperation, ProviderKind, ThreadGoal, ThreadGoalStatus, UserInputAnswer, UserInputOption,
    UserInputQuestion,
};
use waku_protocol::settings::{AgentOsMcpSettings, DaemonSettings};

pub const MCP_SERVER_NAME: &str = "agentos-mcp";
pub const MCP_BIN_ENV: &str = "AGENTOS_WAKU_MCP_BIN";
pub const DISABLE_MCP_ENV: &str = "AGENTOS_WAKU_DISABLE_MCP";
pub const AGENTOS_QUESTION_PREFIX: &str = "agentos-q:";
pub const CODEX_ADVISOR_MODEL: &str = "gpt-6.2-sol";
pub const ANTIGRAVITY_ADVISOR_MODEL: &str = "claude-opus-5-5";
pub const ADVISOR_SYSTEM_PROMPT: &str = "\
You are a read-only senior advisor consulted by another agent inside AgentOS for an independent \
second opinion. Adapt your lens to the question:\n\
- For engineering, architecture, or debugging: inspect relevant code/files, diagnose root \
  causes, subtle concurrency/boundary bugs, or architectural trade-offs, and cite exact \
  file/line references.\n\
- For creative direction, visual/aesthetic critique, storyboarding, or planning: act as a \
  discerning creative/art director — inspect referenced local images/contact sheets or drafts \
  directly when paths are provided, call out generic, flat, or cliché choices, analyze \
  composition, depth, lighting, pacing, and narrative tension, and propose concrete, memorable \
  alternatives.\n\
STRICT RULES:\n\
- You are running in a STRICTLY READ-ONLY session. Never attempt to create, edit, delete, or \
  modify files, git state, or system state.\n\
- Do NOT ask the user questions or request approvals — your output goes back to the calling \
  agent, not directly to the end user.\n\
- Verify claims against actual files or local images in the workspace when paths are involved, \
  keep exploration focused (a few targeted reads/searches at most), and keep your final advice \
  concise (aim for 400–800 tokens): state the verdict/diagnosis first, then concrete steps, \
  trade-offs, or creative improvements.";
pub const MEMORY_MCP_INSTRUCTIONS: &str = "Persistent cross-session memory is available via memory_search (default scope=\"project\") and memory_read. Whenever the user references past state or behavior (e.g. 'before', 'last time', 'previously', 'still connected', 'again', '以前', '上次', '之前', '还在吗', '又...了', '连过', '配过'), or when troubleshooting devices, local network/SSH/proxy topology, credentials, deployment setup, or unexpected environment discrepancies, call memory_search(scope=\"project\") with 1-3 focused keywords BEFORE guessing or running broad diagnostics. Whenever the user's message mentions a past session link such as [Session title](agentos://session/<id>) or [Session title](codeg://session/<id>), call get_session_info(session_id=\"<id>\") to inspect its title, workspace, and recent transcript.";

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct McpCompanionLaunch {
    pub server_name: String,
    pub executable: PathBuf,
    pub args: Vec<String>,
}

#[derive(Clone, Debug)]
pub struct McpContext {
    pub session_id: String,
    pub db_path: PathBuf,
    pub cwd: PathBuf,
    pub caller_provider: Option<ProviderKind>,
    pub settings_path: Option<PathBuf>,
}

impl McpContext {
    pub fn mcp_settings(&self) -> AgentOsMcpSettings {
        let path = self
            .settings_path
            .clone()
            .unwrap_or_else(DaemonSettings::default_path);
        std::fs::read(&path)
            .ok()
            .and_then(|bytes| serde_json::from_slice::<DaemonSettings>(&bytes).ok())
            .map(|settings| settings.agentos_mcp)
            .unwrap_or_default()
    }
}

fn unix_now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

pub fn locate_mcp_executable() -> Option<PathBuf> {
    if let Some(explicit) = std::env::var_os(MCP_BIN_ENV)
        .map(|val| val.to_string_lossy().into_owned())
        .filter(|val| !val.trim().is_empty())
        && let Some(resolved) = crate::command_env::resolve_binary_override(&explicit)
    {
        return Some(resolved);
    }

    let current = std::env::current_exe().ok()?;
    let file_name = current.file_name()?.to_string_lossy();
    if file_name == "waku-daemon" || file_name.eq_ignore_ascii_case("waku-daemon.exe") {
        return Some(current);
    }

    let sibling_name = if cfg!(windows) {
        "waku-daemon.exe"
    } else {
        "waku-daemon"
    };
    let sibling = current.with_file_name(sibling_name);
    if sibling.is_file() {
        return Some(sibling);
    }
    None
}

pub fn companion_launch_for_session(
    session_id: Uuid,
    db_path: &Path,
    cwd: &Path,
) -> Option<McpCompanionLaunch> {
    companion_launch_for_session_with_provider(session_id, db_path, cwd, None, None)
}

pub fn companion_launch_for_session_with_provider(
    session_id: Uuid,
    db_path: &Path,
    cwd: &Path,
    caller_provider: Option<ProviderKind>,
    settings_path: Option<&Path>,
) -> Option<McpCompanionLaunch> {
    if std::env::var(DISABLE_MCP_ENV)
        .ok()
        .is_some_and(|val| val.trim() == "1" || val.trim().eq_ignore_ascii_case("true"))
    {
        return None;
    }
    let executable = locate_mcp_executable()?;
    let mut args = vec![
        "--mcp-stdio".into(),
        "--session-id".into(),
        session_id.to_string(),
        "--db".into(),
        db_path.to_string_lossy().into_owned(),
        "--cwd".into(),
        cwd.to_string_lossy().into_owned(),
        "--parent-pid".into(),
        std::process::id().to_string(),
    ];
    if let Some(provider) = caller_provider
        && let Ok(encoded) = crate::daemon::encode_enum(provider)
    {
        args.push("--caller-provider".into());
        args.push(encoded);
    }
    if let Some(settings) = settings_path {
        args.push("--settings".into());
        args.push(settings.to_string_lossy().into_owned());
    }
    Some(McpCompanionLaunch {
        server_name: MCP_SERVER_NAME.to_owned(),
        executable,
        args,
    })
}

pub fn open_mcp_connection(db_path: &Path) -> anyhow::Result<Connection> {
    if let Some(parent) = db_path.parent()
        && !parent.as_os_str().is_empty()
    {
        std::fs::create_dir_all(parent)?;
    }
    let conn = Connection::open(db_path)
        .with_context(|| format!("could not open MCP SQLite store at {}", db_path.display()))?;
    conn.busy_timeout(Duration::from_secs(5))?;
    let _ = conn.pragma_update(None, "journal_mode", "WAL");
    ensure_mcp_tables(&conn)?;
    Ok(conn)
}

pub fn ensure_mcp_tables(conn: &Connection) -> anyhow::Result<()> {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS agentos_memory_notes (
            session_id TEXT NOT NULL,
            project_path TEXT NOT NULL,
            note_id TEXT NOT NULL,
            scope TEXT NOT NULL DEFAULT 'task_shared',
            revision INTEGER NOT NULL,
            body TEXT NOT NULL,
            source_refs_json TEXT NOT NULL DEFAULT '[]',
            updated_at INTEGER NOT NULL,
            PRIMARY KEY (session_id, note_id, scope)
        );
        CREATE INDEX IF NOT EXISTS idx_agentos_notes_project
            ON agentos_memory_notes(project_path, updated_at DESC);

        CREATE TABLE IF NOT EXISTS agentos_memory_checkpoints (
            checkpoint_id TEXT PRIMARY KEY,
            session_id TEXT NOT NULL,
            project_path TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            created_at INTEGER NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_agentos_checkpoints_session
            ON agentos_memory_checkpoints(session_id, created_at DESC);

        CREATE TABLE IF NOT EXISTS agentos_memory_rotations (
            rotation_id TEXT PRIMARY KEY,
            session_id TEXT NOT NULL,
            checkpoint_id TEXT,
            reason TEXT,
            created_at INTEGER NOT NULL
        );

        CREATE TABLE IF NOT EXISTS agentos_memory_knowledge (
            knowledge_id TEXT PRIMARY KEY,
            scope TEXT NOT NULL DEFAULT 'project',
            project_path TEXT NOT NULL,
            session_id TEXT NOT NULL,
            claim TEXT NOT NULL,
            applicability_json TEXT NOT NULL DEFAULT '{}',
            evidence_json TEXT NOT NULL DEFAULT '[]',
            status TEXT NOT NULL DEFAULT 'candidate',
            created_at INTEGER NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_agentos_knowledge_project
            ON agentos_memory_knowledge(project_path, created_at DESC);

        CREATE TABLE IF NOT EXISTS agentos_goals (
            run_id TEXT PRIMARY KEY,
            session_id TEXT NOT NULL,
            objective TEXT NOT NULL,
            status TEXT NOT NULL,
            max_turns INTEGER,
            max_seconds INTEGER,
            current_turn_id TEXT NOT NULL,
            turn_index INTEGER NOT NULL DEFAULT 1,
            last_reason TEXT,
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_agentos_goals_session
            ON agentos_goals(session_id, updated_at DESC);

        CREATE TABLE IF NOT EXISTS agentos_legacy_session_map (
            legacy_id INTEGER PRIMARY KEY,
            session_id TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS agentos_pending_questions (
            question_id TEXT PRIMARY KEY,
            session_id TEXT NOT NULL,
            questions_json TEXT NOT NULL,
            answers_json TEXT,
            status TEXT NOT NULL DEFAULT 'pending',
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_agentos_questions_session
            ON agentos_pending_questions(session_id, status, created_at ASC);

        CREATE TABLE IF NOT EXISTS agentos_advisor_consultations (
            consultation_id TEXT PRIMARY KEY,
            session_id TEXT NOT NULL,
            caller_provider TEXT,
            advisor_provider TEXT NOT NULL,
            model TEXT,
            question TEXT NOT NULL,
            advice TEXT,
            status TEXT NOT NULL,
            created_at INTEGER NOT NULL,
            completed_at INTEGER
        );
        CREATE INDEX IF NOT EXISTS idx_agentos_advisor_session
            ON agentos_advisor_consultations(session_id, created_at DESC);

        CREATE TABLE IF NOT EXISTS agentos_delegations (
            delegation_id TEXT PRIMARY KEY,
            session_id TEXT NOT NULL,
            target_provider TEXT NOT NULL,
            model TEXT,
            task TEXT NOT NULL,
            read_only INTEGER NOT NULL DEFAULT 0,
            status TEXT NOT NULL,
            result_text TEXT,
            error_text TEXT,
            pid INTEGER,
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_agentos_delegations_session
            ON agentos_delegations(session_id, created_at DESC);",
    )?;
    // Additive columns for host-side auto-rotation & autonomous goal continuation.
    let _ = conn.execute(
        "ALTER TABLE agentos_memory_rotations ADD COLUMN status TEXT NOT NULL DEFAULT 'pending'",
        [],
    );
    let _ = conn.execute(
        "ALTER TABLE agentos_goals ADD COLUMN last_reported_status TEXT",
        [],
    );
    Ok(())
}

pub fn run_stdio_mcp_server(ctx: McpContext) -> anyhow::Result<()> {
    let conn = open_mcp_connection(&ctx.db_path)?;
    let stdin = std::io::stdin();
    let mut stdout = std::io::stdout().lock();
    for line in BufReader::new(stdin.lock()).lines() {
        let line = match line {
            Ok(line) => line,
            Err(_) => break,
        };
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let Ok(request) = serde_json::from_str::<Value>(trimmed) else {
            continue;
        };
        if let Some(response) = dispatch_mcp_request(&conn, &ctx, &request) {
            serde_json::to_writer(&mut stdout, &response)?;
            stdout.write_all(b"\n")?;
            stdout.flush()?;
        }
    }
    Ok(())
}

pub fn dispatch_mcp_request(conn: &Connection, ctx: &McpContext, req: &Value) -> Option<Value> {
    let method = req.get("method").and_then(Value::as_str)?;
    let id = req.get("id").cloned();

    // JSON-RPC notifications omit `id` and expect no response frame.
    let req_id = id?;

    let params = req.get("params").cloned().unwrap_or_else(|| json!({}));
    let outcome = match method {
        "initialize" => {
            let mut init = json!({
                "protocolVersion": "2024-11-05",
                "capabilities": { "tools": {} },
                "serverInfo": {
                    "name": MCP_SERVER_NAME,
                    "version": env!("CARGO_PKG_VERSION"),
                }
            });
            if ctx.mcp_settings().memory_enabled {
                init["instructions"] = json!(MEMORY_MCP_INSTRUCTIONS);
            }
            Ok(init)
        }
        "ping" => Ok(json!({})),
        "tools/list" => Ok(json!({ "tools": mcp_tool_definitions_for(&ctx.mcp_settings()) })),
        "tools/call" => {
            let name = params
                .get("name")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let arguments = params.get("arguments").cloned().unwrap_or_else(|| json!({}));
            match call_mcp_tool(conn, ctx, name, &arguments) {
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

fn tool_allowed(settings: &AgentOsMcpSettings, name: &str) -> bool {
    match name {
        "memory_search"
        | "memory_read"
        | "memory_note_write"
        | "memory_checkpoint_prepare"
        | "memory_context_rotate"
        | "memory_knowledge_propose" => settings.memory_enabled,
        "get_goal" | "create_goal" | "report_goal" => settings.goal_enabled,
        "consult_advisor" => settings.advisor_enabled,
        "ask_user_question" => settings.ask_user_enabled,
        "delegate_to_agent" | "get_delegation_status" | "cancel_delegation" => {
            settings.delegation_enabled
        }
        "get_session_info" => settings.session_info_enabled,
        _ => true,
    }
}

fn call_mcp_tool(
    conn: &Connection,
    ctx: &McpContext,
    name: &str,
    args: &Value,
) -> anyhow::Result<Value> {
    let settings = ctx.mcp_settings();
    if !tool_allowed(&settings, name) {
        anyhow::bail!("tool `{name}` is disabled in AgentOS settings");
    }
    let project_path = ctx.cwd.to_string_lossy().into_owned();
    let now = unix_now();

    match name {
        "memory_note_write" => {
            let note_id = args
                .get("note_id")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .ok_or_else(|| anyhow!("note_id is required"))?;
            let body = args
                .get("body")
                .and_then(Value::as_str)
                .ok_or_else(|| anyhow!("body is required"))?;
            let scope = args
                .get("scope")
                .and_then(Value::as_str)
                .unwrap_or("task_shared");
            let expected_revision = args.get("expected_revision").and_then(Value::as_i64);
            let source_refs = serde_json::to_string(
                args.get("source_refs")
                    .filter(|v| v.is_array())
                    .unwrap_or(&json!([])),
            )?;

            let current_rev: Option<i64> = conn
                .query_row(
                    "SELECT revision FROM agentos_memory_notes WHERE session_id = ?1 AND note_id = ?2 AND scope = ?3",
                    params![ctx.session_id, note_id, scope],
                    |row| row.get(0),
                )
                .optional()?;

            if let Some(expected) = expected_revision
                && current_rev.unwrap_or(0) != expected
            {
                anyhow::bail!(
                    "revision_conflict: expected revision {expected}, current is {}",
                    current_rev.unwrap_or(0)
                );
            }

            let next_rev = current_rev.unwrap_or(0) + 1;
            conn.execute(
                "INSERT INTO agentos_memory_notes (session_id, project_path, note_id, scope, revision, body, source_refs_json, updated_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
                 ON CONFLICT(session_id, note_id, scope) DO UPDATE SET
                    project_path = excluded.project_path,
                    revision = excluded.revision,
                    body = excluded.body,
                    source_refs_json = excluded.source_refs_json,
                    updated_at = excluded.updated_at",
                params![ctx.session_id, project_path, note_id, scope, next_rev, body, source_refs, now],
            )?;

            Ok(json!({
                "id": format!("note:{}:{}:{}", ctx.session_id, scope, note_id),
                "note_id": note_id,
                "scope": scope,
                "revision": next_rev,
                "updated_at": now,
            }))
        }

        "memory_search" => {
            let raw_query = args
                .get("query")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .trim();
            let scope = args
                .get("scope")
                .and_then(Value::as_str)
                .unwrap_or("project");
            let limit = args
                .get("limit")
                .and_then(Value::as_u64)
                .unwrap_or(20)
                .clamp(1, 100) as usize;
            let kinds: Vec<String> = args
                .get("kinds")
                .and_then(Value::as_array)
                .map(|arr| {
                    arr.iter()
                        .filter_map(Value::as_str)
                        .map(str::to_owned)
                        .collect()
                })
                .unwrap_or_default();
            let allow_kind = |k: &str| kinds.is_empty() || kinds.iter().any(|item| item == k);
            let terms = extract_search_terms(raw_query);
            let has_query = !terms.is_empty();

            // Collect (relevance_score, sort_ts, hit_json) across all kinds and rank globally.
            let mut scored_hits: Vec<(f64, i64, Value)> = Vec::new();
            let scan_limit = ((limit * 5).max(100)).min(500) as i64;

            if allow_kind("note") {
                let mut stmt = if scope == "task_shared" {
                    conn.prepare(
                        "SELECT session_id, scope, note_id, revision, body, updated_at
                         FROM agentos_memory_notes
                         WHERE session_id = ?1
                         ORDER BY updated_at DESC LIMIT ?2",
                    )?
                } else {
                    conn.prepare(
                        "SELECT session_id, scope, note_id, revision, body, updated_at
                         FROM agentos_memory_notes
                         WHERE (?1 = 'user' OR project_path = ?2 OR session_id = ?3)
                         ORDER BY updated_at DESC LIMIT ?4",
                    )?
                };
                let map_note_row = |r: &rusqlite::Row<'_>| -> rusqlite::Result<_> {
                    Ok((
                        r.get::<_, String>(0)?,
                        r.get::<_, String>(1)?,
                        r.get::<_, String>(2)?,
                        r.get::<_, i64>(3)?,
                        r.get::<_, String>(4)?,
                        r.get::<_, i64>(5)?,
                    ))
                };
                let collected: Vec<_> = if scope == "task_shared" {
                    stmt.query_map(params![ctx.session_id, scan_limit], map_note_row)?
                        .flatten()
                        .collect()
                } else {
                    stmt.query_map(
                        params![scope, project_path, ctx.session_id, scan_limit],
                        map_note_row,
                    )?
                    .flatten()
                    .collect()
                };
                for (sid, nscope, nid, rev, body, ts) in collected {
                    let combined = format!("{nid}\n{body}");
                    let score = if has_query {
                        score_text_terms(&combined, &terms)
                    } else {
                        1.0
                    };
                    if has_query && score <= 0.0 {
                        continue;
                    }
                    let snippet = extract_match_snippet(&body, &terms, 240);
                    scored_hits.push((
                        score * 1.15,
                        ts,
                        json!({
                            "id": format!("note:{sid}:{nscope}:{nid}"),
                            "kind": "note",
                            "note_id": nid,
                            "revision": rev,
                            "snippet": snippet,
                            "updated_at": ts,
                        }),
                    ));
                }
            }

            if allow_kind("checkpoint") {
                let mut stmt = conn.prepare(
                    "SELECT checkpoint_id, payload_json, created_at
                     FROM agentos_memory_checkpoints
                     WHERE ?1 = 'user'
                        OR (?1 = 'project' AND (project_path = ?2 OR session_id = ?3))
                        OR (?1 = 'task_shared' AND session_id = ?3)
                     ORDER BY created_at DESC LIMIT ?4",
                )?;
                let rows = stmt.query_map(
                    params![scope, project_path, ctx.session_id, scan_limit],
                    |r| {
                        Ok((
                            r.get::<_, String>(0)?,
                            r.get::<_, String>(1)?,
                            r.get::<_, i64>(2)?,
                        ))
                    },
                )?;
                for (cid, payload, ts) in rows.flatten() {
                    let score = if has_query {
                        score_text_terms(&payload, &terms)
                    } else {
                        1.0
                    };
                    if has_query && score <= 0.0 {
                        continue;
                    }
                    let snippet = extract_match_snippet(&payload, &terms, 240);
                    scored_hits.push((
                        score * 1.10,
                        ts,
                        json!({
                            "id": format!("ckpt:{cid}"),
                            "kind": "checkpoint",
                            "snippet": snippet,
                            "created_at": ts,
                        }),
                    ));
                }
            }

            if allow_kind("knowledge") {
                let mut stmt = conn.prepare(
                    "SELECT knowledge_id, claim, applicability_json, scope, status, created_at
                     FROM agentos_memory_knowledge
                     WHERE status != 'rejected' AND (?1 = 'user' OR project_path = ?2 OR scope = 'user')
                     ORDER BY CASE status WHEN 'promoted' THEN 0 ELSE 1 END, created_at DESC LIMIT ?3",
                )?;
                let rows = stmt.query_map(params![scope, project_path, scan_limit], |r| {
                    Ok((
                        r.get::<_, String>(0)?,
                        r.get::<_, String>(1)?,
                        r.get::<_, String>(2)?,
                        r.get::<_, String>(3)?,
                        r.get::<_, String>(4)?,
                        r.get::<_, i64>(5)?,
                    ))
                })?;
                for (kid, claim, app_json, kscope, status, ts) in rows.flatten() {
                    if status == "retracted" {
                        continue;
                    }
                    let combined = format!("{claim}\n{app_json}");
                    let score = if has_query {
                        score_text_terms(&combined, &terms)
                    } else {
                        1.0
                    };
                    if has_query && score <= 0.0 {
                        continue;
                    }
                    let snippet = extract_match_snippet(&claim, &terms, 240);
                    scored_hits.push((
                        score * 1.25,
                        ts,
                        json!({
                            "id": format!("know:{kid}"),
                            "kind": "knowledge",
                            "scope": kscope,
                            "status": status,
                            "snippet": snippet,
                            "created_at": ts,
                        }),
                    ));
                }
            }

            if allow_kind("history") {
                let history_hits = search_waku_message_history(
                    conn,
                    ctx,
                    &project_path,
                    scope,
                    raw_query,
                    &terms,
                    limit,
                );
                scored_hits.extend(history_hits);
            }

            if has_query {
                scored_hits.sort_by(|a, b| {
                    b.0.partial_cmp(&a.0)
                        .unwrap_or(std::cmp::Ordering::Equal)
                        .then_with(|| b.1.cmp(&a.1))
                });
            } else {
                scored_hits.sort_by(|a, b| b.1.cmp(&a.1));
            }
            scored_hits.truncate(limit);
            let hits: Vec<Value> = scored_hits.into_iter().map(|(_, _, v)| v).collect();

            Ok(json!({ "hits": hits }))
        }

        "memory_read" => {
            let ids = args
                .get("ids")
                .and_then(Value::as_array)
                .ok_or_else(|| anyhow!("ids array is required"))?;
            let max_chars = args
                .get("max_chars")
                .and_then(Value::as_u64)
                .unwrap_or(16_000) as usize;
            let mut remaining = max_chars;
            let mut items = Vec::new();

            for raw_id in ids.iter().filter_map(Value::as_str) {
                if remaining == 0 {
                    break;
                }
                if let Some(rest) = raw_id.strip_prefix("note:") {
                    let mut parts = rest.splitn(3, ':');
                    let (Some(sid), Some(scope), Some(nid)) =
                        (parts.next(), parts.next(), parts.next())
                    else {
                        continue;
                    };
                    if let Some((rev, body, refs)) = conn
                        .query_row(
                            "SELECT revision, body, source_refs_json FROM agentos_memory_notes WHERE session_id = ?1 AND scope = ?2 AND note_id = ?3",
                            params![sid, scope, nid],
                            |r| Ok((r.get::<_, i64>(0)?, r.get::<_, String>(1)?, r.get::<_, String>(2)?)),
                        )
                        .optional()?
                    {
                        let text: String = body.chars().take(remaining).collect();
                        remaining = remaining.saturating_sub(text.chars().count());
                        items.push(json!({
                            "id": raw_id,
                            "kind": "note",
                            "note_id": nid,
                            "revision": rev,
                            "text": text,
                            "source_refs": serde_json::from_str::<Value>(&refs).unwrap_or_else(|_| json!([])),
                        }));
                    }
                } else if let Some(cid) = raw_id.strip_prefix("ckpt:") {
                    if let Some(payload) = conn
                        .query_row(
                            "SELECT payload_json FROM agentos_memory_checkpoints WHERE checkpoint_id = ?1",
                            params![cid],
                            |r| r.get::<_, String>(0),
                        )
                        .optional()?
                    {
                        let text: String = payload.chars().take(remaining).collect();
                        remaining = remaining.saturating_sub(text.chars().count());
                        items.push(json!({
                            "id": raw_id,
                            "kind": "checkpoint",
                            "text": text,
                        }));
                    }
                } else if let Some(kid) = raw_id.strip_prefix("know:") {
                    if let Some((claim, evidence)) = conn
                        .query_row(
                            "SELECT claim, evidence_json FROM agentos_memory_knowledge WHERE knowledge_id = ?1",
                            params![kid],
                            |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)),
                        )
                        .optional()?
                    {
                        let text: String = claim.chars().take(remaining).collect();
                        remaining = remaining.saturating_sub(text.chars().count());
                        items.push(json!({
                            "id": raw_id,
                            "kind": "knowledge",
                            "text": text,
                            "evidence": serde_json::from_str::<Value>(&evidence).unwrap_or_else(|_| json!([])),
                        }));
                    }
                } else if let Some(mid) = raw_id.strip_prefix("msg:") {
                    if let Ok(Some((role, content))) = conn
                        .query_row(
                            "SELECT role, content FROM messages WHERE id = ?1",
                            params![mid],
                            |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)),
                        )
                        .optional()
                    {
                        let text: String = content.chars().take(remaining).collect();
                        remaining = remaining.saturating_sub(text.chars().count());
                        items.push(json!({
                            "id": raw_id,
                            "kind": "history",
                            "role": role,
                            "text": text,
                        }));
                    }
                }
            }
            Ok(json!({ "items": items }))
        }

        "memory_checkpoint_prepare" => {
            let cid = Uuid::new_v4().to_string();
            let payload = json!({
                "checkpoint_id": cid,
                "session_id": ctx.session_id,
                "completed": args.get("completed").cloned().unwrap_or_else(|| json!([])),
                "in_progress": args.get("in_progress").cloned().unwrap_or_else(|| json!([])),
                "next_actions": args.get("next_actions").cloned().unwrap_or_else(|| json!([])),
                "open_questions": args.get("open_questions").cloned().unwrap_or_else(|| json!([])),
                "constraints": args.get("constraints").cloned().unwrap_or_else(|| json!([])),
                "rejected_approaches": args.get("rejected_approaches").cloned().unwrap_or_else(|| json!([])),
                "created_at": now,
            });
            conn.execute(
                "INSERT INTO agentos_memory_checkpoints (checkpoint_id, session_id, project_path, payload_json, created_at)
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                params![cid, ctx.session_id, project_path, serde_json::to_string(&payload)?, now],
            )?;
            Ok(json!({
                "checkpoint_id": cid,
                "id": format!("ckpt:{cid}"),
                "status": "persisted",
            }))
        }

        "memory_context_rotate" => {
            let rotation_id = Uuid::new_v4().to_string();
            let checkpoint_id = args
                .get("checkpoint_id")
                .and_then(Value::as_str)
                .map(str::to_owned);
            let reason = args
                .get("reason")
                .and_then(Value::as_str)
                .map(str::to_owned);
            conn.execute(
                "INSERT INTO agentos_memory_rotations (rotation_id, session_id, checkpoint_id, reason, status, created_at)
                 VALUES (?1, ?2, ?3, ?4, 'pending', ?5)",
                params![rotation_id, ctx.session_id, checkpoint_id, reason, now],
            )?;
            Ok(json!({
                "rotation_id": rotation_id,
                "checkpoint_id": checkpoint_id,
                "scheduled": true,
                "message": "Context rotation recorded for the next safe turn boundary.",
            }))
        }

        "memory_knowledge_propose" => {
            let claim = args
                .get("claim")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .ok_or_else(|| anyhow!("claim is required"))?;
            let scope = args
                .get("scope")
                .and_then(Value::as_str)
                .unwrap_or("project");
            let kid = Uuid::new_v4().to_string();
            let applicability_value = match args.get("applicability") {
                Some(v) if v.is_object() => v.clone(),
                Some(Value::String(raw)) => serde_json::from_str::<Value>(raw)
                    .ok()
                    .filter(Value::is_object)
                    .unwrap_or_else(|| json!({ "raw": raw })),
                _ => json!({}),
            };
            let applicability = serde_json::to_string(&applicability_value)?;
            let evidence = serde_json::to_string(
                args.get("evidence")
                    .filter(|v| v.is_array())
                    .unwrap_or(&json!([])),
            )?;
            conn.execute(
                "INSERT INTO agentos_memory_knowledge (knowledge_id, scope, project_path, session_id, claim, applicability_json, evidence_json, status, created_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, 'candidate', ?8)",
                params![kid, scope, project_path, ctx.session_id, claim, applicability, evidence, now],
            )?;
            Ok(json!({
                "knowledge_id": kid,
                "id": format!("know:{kid}"),
                "status": "candidate",
            }))
        }

        "get_goal" => {
            let active = conn
                .query_row(
                    "SELECT run_id, objective, status, max_turns, max_seconds, current_turn_id, turn_index, last_reason
                     FROM agentos_goals
                     WHERE session_id = ?1
                     ORDER BY updated_at DESC LIMIT 1",
                    params![ctx.session_id],
                    |r| {
                        Ok(json!({
                            "hasGoal": true,
                            "runId": r.get::<_, String>(0)?,
                            "objective": r.get::<_, String>(1)?,
                            "status": r.get::<_, String>(2)?,
                            "maxTurns": r.get::<_, Option<i64>>(3)?,
                            "maxSeconds": r.get::<_, Option<i64>>(4)?,
                            "turnId": r.get::<_, String>(5)?,
                            "turnIndex": r.get::<_, i64>(6)?,
                            "lastReason": r.get::<_, Option<String>>(7)?,
                        }))
                    },
                )
                .optional()?;
            Ok(active.unwrap_or_else(|| json!({ "hasGoal": false })))
        }

        "create_goal" => {
            let objective = args
                .get("objective")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .ok_or_else(|| anyhow!("objective is required"))?;
            let existing: Option<(String, String, String)> = conn
                .query_row(
                    "SELECT run_id, current_turn_id, objective FROM agentos_goals
                     WHERE session_id = ?1 AND status IN ('active', 'continue', 'blocked')
                     ORDER BY updated_at DESC LIMIT 1",
                    params![ctx.session_id],
                    |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
                )
                .optional()?;
            if let Some((run_id, turn_id, current_obj)) = existing {
                return Ok(json!({
                    "created": false,
                    "alreadyActive": true,
                    "runId": run_id,
                    "turnId": turn_id,
                    "objective": current_obj,
                }));
            }
            let run_id = Uuid::new_v4().to_string();
            let turn_id = Uuid::new_v4().to_string();
            let max_turns = args.get("maxTurns").and_then(Value::as_i64);
            if let Some(limit) = max_turns
                && limit < 1
            {
                anyhow::bail!("maxTurns must be >= 1");
            }
            let max_seconds = args.get("maxSeconds").and_then(Value::as_i64);
            if let Some(limit) = max_seconds
                && limit < 1
            {
                anyhow::bail!("maxSeconds must be >= 1");
            }
            conn.execute(
                "INSERT INTO agentos_goals (run_id, session_id, objective, status, max_turns, max_seconds, current_turn_id, turn_index, created_at, updated_at)
                 VALUES (?1, ?2, ?3, 'active', ?4, ?5, ?6, 1, ?7, ?7)",
                params![run_id, ctx.session_id, objective, max_turns, max_seconds, turn_id, now],
            )?;
            Ok(json!({
                "created": true,
                "runId": run_id,
                "turnId": turn_id,
                "objective": objective,
                "status": "active",
                "turnIndex": 1,
                "maxTurns": max_turns,
                "maxSeconds": max_seconds,
            }))
        }

        "report_goal" => {
            let run_id = args
                .get("runId")
                .and_then(Value::as_str)
                .ok_or_else(|| anyhow!("runId is required"))?;
            let turn_id = args
                .get("turnId")
                .and_then(Value::as_str)
                .ok_or_else(|| anyhow!("turnId is required"))?;
            let status = args
                .get("status")
                .and_then(Value::as_str)
                .ok_or_else(|| anyhow!("status is required"))?;
            if !matches!(status, "complete" | "continue" | "blocked") {
                anyhow::bail!("invalid goal status: {status}");
            }
            let reason = args
                .get("reason")
                .and_then(Value::as_str)
                .ok_or_else(|| anyhow!("reason is required"))?;

            let existing: Option<(String, i64, Option<i64>, Option<i64>, i64)> = conn
                .query_row(
                    "SELECT status, turn_index, max_turns, max_seconds, created_at
                     FROM agentos_goals
                     WHERE run_id = ?1 AND current_turn_id = ?2",
                    params![run_id, turn_id],
                    |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?)),
                )
                .optional()?;
            let Some((current_status, turn_index, max_turns, max_seconds, created_at)) = existing
            else {
                anyhow::bail!("no active goal matched runId={run_id} and turnId={turn_id}");
            };
            if current_status == "complete" {
                anyhow::bail!("goal {run_id} is already complete");
            }

            if status == "continue" {
                let turns_exhausted = max_turns.is_some_and(|limit| turn_index >= limit);
                let elapsed_seconds = now.saturating_sub(created_at);
                let time_exhausted = max_seconds.is_some_and(|limit| elapsed_seconds >= limit);
                if turns_exhausted || time_exhausted {
                    let exhaustion_reason = if turns_exhausted {
                        format!(
                            "goal maxTurns budget exhausted ({turn_index}/{}); cannot continue ({reason})",
                            max_turns.unwrap_or(turn_index)
                        )
                    } else {
                        format!(
                            "goal maxSeconds budget exhausted ({elapsed_seconds}s/{}s); cannot continue ({reason})",
                            max_seconds.unwrap_or(elapsed_seconds)
                        )
                    };
                    conn.execute(
                        "UPDATE agentos_goals
                         SET status = 'blocked',
                             last_reported_status = 'blocked',
                             last_reason = ?1,
                             updated_at = ?2
                         WHERE run_id = ?3 AND current_turn_id = ?4",
                        params![exhaustion_reason, now, run_id, turn_id],
                    )?;
                    return Ok(json!({
                        "recorded": true,
                        "runId": run_id,
                        "status": "blocked",
                        "budgetExhausted": true,
                        "turnIndex": turn_index,
                        "maxTurns": max_turns,
                        "maxSeconds": max_seconds,
                        "nextTurnId": turn_id,
                        "reason": exhaustion_reason,
                    }));
                }
            }

            let next_turn_id = if status == "continue" {
                Uuid::new_v4().to_string()
            } else {
                turn_id.to_owned()
            };
            let next_turn_index = if status == "continue" {
                turn_index + 1
            } else {
                turn_index
            };
            let stored_status = if status == "continue" {
                "active"
            } else {
                status
            };
            conn.execute(
                "UPDATE agentos_goals
                 SET status = ?1,
                     last_reported_status = ?2,
                     last_reason = ?3,
                     current_turn_id = ?4,
                     turn_index = ?5,
                     updated_at = ?6
                 WHERE run_id = ?7 AND current_turn_id = ?8",
                params![
                    stored_status,
                    status,
                    reason,
                    next_turn_id,
                    next_turn_index,
                    now,
                    run_id,
                    turn_id
                ],
            )?;
            Ok(json!({
                "recorded": true,
                "runId": run_id,
                "status": status,
                "budgetExhausted": false,
                "turnIndex": next_turn_index,
                "maxTurns": max_turns,
                "maxSeconds": max_seconds,
                "nextTurnId": next_turn_id,
            }))
        }

        "get_session_info" => {
            let max_messages = args
                .get("max_messages")
                .and_then(Value::as_u64)
                .unwrap_or(20)
                .min(200) as usize;
            let raw_text = args.get("session_id").and_then(Value::as_str).map(|s| {
                let trimmed = s.trim();
                trimmed
                    .strip_prefix("agentos://session/")
                    .or_else(|| trimmed.strip_prefix("codeg://session/"))
                    .unwrap_or(trimmed)
                    .trim()
            });
            let legacy_num = args
                .get("session_id")
                .and_then(Value::as_i64)
                .or_else(|| raw_text.and_then(|s| s.parse::<i64>().ok()));
            let resolved_sid = if let Some(num) = legacy_num {
                conn.query_row(
                    "SELECT session_id FROM agentos_legacy_session_map WHERE legacy_id = ?1",
                    params![num],
                    |r| r.get::<_, String>(0),
                )
                .optional()?
                .unwrap_or_else(|| format!("codeg-conv-{num}"))
            } else if let Some(text) = raw_text.filter(|s| !s.is_empty()) {
                text.to_owned()
            } else {
                anyhow::bail!("session_id is required");
            };

            let has_sessions: bool = conn
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='sessions')",
                    [],
                    |r| r.get(0),
                )
                .unwrap_or(false);
            if !has_sessions {
                return Ok(json!({ "found": false, "session_id": resolved_sid }));
            }
            let has_projects: bool = conn
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='projects')",
                    [],
                    |r| r.get(0),
                )
                .unwrap_or(false);
            let row = if has_projects {
                conn.query_row(
                    "SELECT COALESCE(NULLIF(NULLIF(s.title, 'New task'), ''), NULLIF(s.auto_title, ''), s.title),
                            s.provider, s.model, s.status, s.created_at, s.updated_at,
                            p.name, p.path
                     FROM sessions s
                     LEFT JOIN projects p ON p.id = s.project_id
                     WHERE s.id = ?1",
                    params![resolved_sid],
                    |r| {
                        Ok((
                            r.get::<_, String>(0)?,
                            r.get::<_, String>(1)?,
                            r.get::<_, Option<String>>(2)?,
                            r.get::<_, String>(3)?,
                            r.get::<_, Option<i64>>(4)?,
                            r.get::<_, Option<i64>>(5)?,
                            r.get::<_, Option<String>>(6)?,
                            r.get::<_, Option<String>>(7)?,
                        ))
                    },
                )
                .optional()?
            } else {
                conn.query_row(
                    "SELECT title, provider, model, status, NULL, NULL, NULL, NULL FROM sessions WHERE id = ?1",
                    params![resolved_sid],
                    |r| {
                        Ok((
                            r.get::<_, String>(0)?,
                            r.get::<_, String>(1)?,
                            r.get::<_, Option<String>>(2)?,
                            r.get::<_, String>(3)?,
                            r.get::<_, Option<i64>>(4)?,
                            r.get::<_, Option<i64>>(5)?,
                            r.get::<_, Option<String>>(6)?,
                            r.get::<_, Option<String>>(7)?,
                        ))
                    },
                )
                .optional()?
            };
            let Some((
                title,
                provider,
                model,
                status,
                created_at,
                updated_at,
                project_name,
                workspace_folder,
            )) = row
            else {
                return Ok(json!({ "found": false, "session_id": resolved_sid }));
            };

            let total_messages: i64 = conn
                .query_row(
                    "SELECT count(*) FROM messages WHERE session_id = ?1",
                    params![resolved_sid],
                    |r| r.get(0),
                )
                .unwrap_or(0);
            let mut recent = Vec::new();
            if max_messages > 0
                && let Ok(mut stmt) = conn.prepare(
                    "SELECT role, content FROM messages WHERE session_id = ?1 ORDER BY position DESC LIMIT ?2",
                )
                && let Ok(rows) = stmt.query_map(params![resolved_sid, max_messages as i64], |r| {
                    Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?))
                })
            {
                let mut collected: Vec<_> = rows.flatten().collect();
                collected.reverse();
                for (role, content) in collected {
                    let compact: String = content.chars().take(1200).collect();
                    recent.push(json!({ "role": role, "content": compact }));
                }
            }
            Ok(json!({
                "found": true,
                "session_id": resolved_sid,
                "title": title,
                "provider": provider,
                "model": model,
                "status": status,
                "project_name": project_name,
                "workspace_folder": workspace_folder,
                "created_at": created_at,
                "updated_at": updated_at,
                "message_count": total_messages,
                "messages": recent,
            }))
        }

        "ask_user_question" => {
            let raw_questions = args
                .get("questions")
                .and_then(Value::as_array)
                .filter(|items| !items.is_empty() && items.len() <= 4)
                .ok_or_else(|| anyhow!("questions must be an array of 1 to 4 question objects"))?;
            let mut parsed = Vec::with_capacity(raw_questions.len());
            for (idx, item) in raw_questions.iter().enumerate() {
                let header = item
                    .get("header")
                    .and_then(Value::as_str)
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .unwrap_or("Question")
                    .to_owned();
                let question = item
                    .get("question")
                    .and_then(Value::as_str)
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .ok_or_else(|| anyhow!("questions[{idx}].question is required"))?
                    .to_owned();
                let multi_select = item
                    .get("multiSelect")
                    .and_then(Value::as_bool)
                    .unwrap_or(false);
                let mut options = Vec::new();
                if let Some(raw_opts) = item.get("options").and_then(Value::as_array) {
                    for opt in raw_opts {
                        if let Some(label) = opt
                            .get("label")
                            .and_then(Value::as_str)
                            .map(str::trim)
                            .filter(|s| !s.is_empty())
                        {
                            let description = opt
                                .get("description")
                                .and_then(Value::as_str)
                                .map(str::trim)
                                .filter(|s| !s.is_empty())
                                .map(str::to_owned);
                            options.push(UserInputOption {
                                label: label.to_owned(),
                                description,
                            });
                        }
                    }
                }
                parsed.push(UserInputQuestion {
                    id: format!("q{}", idx + 1),
                    header,
                    question,
                    options,
                    multi_select,
                });
            }

            let qid = Uuid::new_v4().to_string();
            conn.execute(
                "INSERT INTO agentos_pending_questions (question_id, session_id, questions_json, status, created_at, updated_at)
                 VALUES (?1, ?2, ?3, 'pending', ?4, ?4)",
                params![qid, ctx.session_id, serde_json::to_string(&parsed)?, now],
            )?;

            let timeout_secs = args
                .get("timeout_seconds")
                .and_then(Value::as_u64)
                .unwrap_or(600)
                .clamp(5, 1800);
            let deadline = Instant::now() + Duration::from_secs(timeout_secs);
            loop {
                let state: Option<(String, Option<String>)> = conn
                    .query_row(
                        "SELECT status, answers_json FROM agentos_pending_questions WHERE question_id = ?1",
                        params![qid],
                        |r| Ok((r.get(0)?, r.get(1)?)),
                    )
                    .optional()?;
                match state {
                    Some((status, answers_json)) if status == "answered" => {
                        let answers: Value = answers_json
                            .as_deref()
                            .and_then(|raw| serde_json::from_str(raw).ok())
                            .unwrap_or_else(|| json!([]));
                        return Ok(json!({
                            "questionId": qid,
                            "answered": true,
                            "dismissed": false,
                            "answers": answers,
                        }));
                    }
                    Some((status, _)) if status == "cancelled" => {
                        return Ok(json!({
                            "questionId": qid,
                            "answered": false,
                            "dismissed": true,
                            "answers": [],
                        }));
                    }
                    _ => {}
                }
                if Instant::now() >= deadline {
                    let _ = conn.execute(
                        "UPDATE agentos_pending_questions SET status = 'cancelled', updated_at = ?1 WHERE question_id = ?2",
                        params![unix_now(), qid],
                    );
                    return Ok(json!({
                        "questionId": qid,
                        "answered": false,
                        "dismissed": true,
                        "timedOut": true,
                        "answers": [],
                    }));
                }
                std::thread::sleep(Duration::from_millis(150));
            }
        }

        "consult_advisor" => {
            let question = args
                .get("question")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .ok_or_else(|| anyhow!("question is required"))?;
            let context = args
                .get("context")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|s| !s.is_empty());
            let files: Vec<String> = args
                .get("files")
                .and_then(Value::as_array)
                .map(|arr| {
                    arr.iter()
                        .filter_map(Value::as_str)
                        .map(str::trim)
                        .filter(|s| !s.is_empty())
                        .map(str::to_owned)
                        .collect()
                })
                .unwrap_or_default();
            let requested = args
                .get("advisor_provider")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .unwrap_or(settings.advisor_provider.as_str());
            let advisor_provider = resolve_advisor_provider(requested, ctx.caller_provider)?;
            let model = args
                .get("model")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_owned)
                .or_else(|| default_advisor_model(advisor_provider).map(str::to_owned));

            let mut prompt = format!("{ADVISOR_SYSTEM_PROMPT}\n\n");
            prompt.push_str(&format!("Question:\n{question}\n"));
            if let Some(extra) = context {
                prompt.push_str(&format!("\nContext & hypotheses:\n{extra}\n"));
            }
            if !files.is_empty() {
                prompt.push_str("\nRelevant local workspace files / images to inspect:\n");
                for f in &files {
                    prompt.push_str(&format!("- {f}\n"));
                }
            }

            let cid = Uuid::new_v4().to_string();
            let caller_name = ctx.caller_provider.map(|p| p.id().to_owned());
            conn.execute(
                "INSERT INTO agentos_advisor_consultations
                 (consultation_id, session_id, caller_provider, advisor_provider, model, question, status, created_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'running', ?7)",
                params![
                    cid,
                    ctx.session_id,
                    caller_name,
                    advisor_provider.id(),
                    model,
                    question,
                    now
                ],
            )?;

            match run_isolated_oneshot_agent(
                advisor_provider,
                &ctx.cwd,
                &prompt,
                model.as_deref(),
                true,
                Duration::from_secs(120),
                None,
            ) {
                Ok(advice) => {
                    let completed_at = unix_now();
                    let _ = conn.execute(
                        "UPDATE agentos_advisor_consultations
                         SET status = 'completed', advice = ?1, completed_at = ?2
                         WHERE consultation_id = ?3",
                        params![advice, completed_at, cid],
                    );
                    Ok(json!({
                        "consultationId": cid,
                        "advisorProvider": advisor_provider.id(),
                        "model": model,
                        "readOnlyEnforced": true,
                        "advice": advice,
                    }))
                }
                Err(error) => {
                    let _ = conn.execute(
                        "UPDATE agentos_advisor_consultations
                         SET status = 'failed', advice = ?1, completed_at = ?2
                         WHERE consultation_id = ?3",
                        params![error.to_string(), unix_now(), cid],
                    );
                    Err(error)
                }
            }
        }

        "delegate_to_agent" => {
            let target_raw = args
                .get("provider")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .ok_or_else(|| anyhow!("provider is required (codex or antigravity)"))?;
            let target_provider = resolve_advisor_provider(target_raw, ctx.caller_provider)?;
            let task = args
                .get("task")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .ok_or_else(|| anyhow!("task is required"))?
                .to_owned();
            let read_only = args
                .get("read_only")
                .and_then(Value::as_bool)
                .unwrap_or(false);
            let wait = args.get("wait").and_then(Value::as_bool).unwrap_or(true);
            let model = args
                .get("model")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_owned);
            let timeout = Duration::from_secs(
                args.get("timeout_seconds")
                    .and_then(Value::as_u64)
                    .unwrap_or(180)
                    .clamp(10, 600),
            );

            let delegation_id = Uuid::new_v4().to_string();
            conn.execute(
                "INSERT INTO agentos_delegations
                 (delegation_id, session_id, target_provider, model, task, read_only, status, created_at, updated_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'running', ?7, ?7)",
                params![
                    delegation_id,
                    ctx.session_id,
                    target_provider.id(),
                    model,
                    task,
                    if read_only { 1 } else { 0 },
                    now
                ],
            )?;

            if wait {
                let db_path = ctx.db_path.clone();
                let did_for_pid = delegation_id.clone();
                let pid_cb = move |pid: u32| {
                    if let Ok(c) = open_mcp_connection(&db_path) {
                        let _ = c.execute(
                            "UPDATE agentos_delegations SET pid = ?1, updated_at = ?2 WHERE delegation_id = ?3",
                            params![pid as i64, unix_now(), did_for_pid],
                        );
                    }
                };
                match run_isolated_oneshot_agent(
                    target_provider,
                    &ctx.cwd,
                    &task,
                    model.as_deref(),
                    read_only,
                    timeout,
                    Some(Box::new(pid_cb)),
                ) {
                    Ok(output) => {
                        let _ = conn.execute(
                            "UPDATE agentos_delegations
                             SET status = 'completed', result_text = ?1, pid = NULL, updated_at = ?2
                             WHERE delegation_id = ?3",
                            params![output, unix_now(), delegation_id],
                        );
                        Ok(json!({
                            "delegationId": delegation_id,
                            "provider": target_provider.id(),
                            "readOnly": read_only,
                            "status": "completed",
                            "result": output,
                        }))
                    }
                    Err(error) => {
                        let _ = conn.execute(
                            "UPDATE agentos_delegations
                             SET status = 'failed', error_text = ?1, pid = NULL, updated_at = ?2
                             WHERE delegation_id = ?3",
                            params![error.to_string(), unix_now(), delegation_id],
                        );
                        Err(error)
                    }
                }
            } else {
                let db_path = ctx.db_path.clone();
                let cwd = ctx.cwd.clone();
                let did = delegation_id.clone();
                let model_clone = model.clone();
                std::thread::Builder::new()
                    .name(format!("waku-delegate-{did}"))
                    .spawn(move || {
                        let db_for_pid = db_path.clone();
                        let did_for_pid = did.clone();
                        let pid_cb = move |pid: u32| {
                            if let Ok(c) = open_mcp_connection(&db_for_pid) {
                                let _ = c.execute(
                                    "UPDATE agentos_delegations SET pid = ?1, updated_at = ?2 WHERE delegation_id = ?3",
                                    params![pid as i64, unix_now(), did_for_pid],
                                );
                            }
                        };
                        let outcome = run_isolated_oneshot_agent(
                            target_provider,
                            &cwd,
                            &task,
                            model_clone.as_deref(),
                            read_only,
                            timeout,
                            Some(Box::new(pid_cb)),
                        );
                        if let Ok(c) = open_mcp_connection(&db_path) {
                            match outcome {
                                Ok(text) => {
                                    let _ = c.execute(
                                        "UPDATE agentos_delegations
                                         SET status = 'completed', result_text = ?1, pid = NULL, updated_at = ?2
                                         WHERE delegation_id = ?3 AND status = 'running'",
                                        params![text, unix_now(), did],
                                    );
                                }
                                Err(err) => {
                                    let _ = c.execute(
                                        "UPDATE agentos_delegations
                                         SET status = 'failed', error_text = ?1, pid = NULL, updated_at = ?2
                                         WHERE delegation_id = ?3 AND status = 'running'",
                                        params![err.to_string(), unix_now(), did],
                                    );
                                }
                            }
                        }
                    })?;
                Ok(json!({
                    "delegationId": delegation_id,
                    "provider": target_provider.id(),
                    "readOnly": read_only,
                    "status": "running",
                }))
            }
        }

        "get_delegation_status" => {
            let delegation_id = args
                .get("delegation_id")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .ok_or_else(|| anyhow!("delegation_id is required"))?;
            let row = conn
                .query_row(
                    "SELECT target_provider, model, read_only, status, result_text, error_text, created_at, updated_at
                     FROM agentos_delegations WHERE delegation_id = ?1",
                    params![delegation_id],
                    |r| {
                        Ok(json!({
                            "found": true,
                            "delegationId": delegation_id,
                            "provider": r.get::<_, String>(0)?,
                            "model": r.get::<_, Option<String>>(1)?,
                            "readOnly": r.get::<_, i64>(2)? != 0,
                            "status": r.get::<_, String>(3)?,
                            "result": r.get::<_, Option<String>>(4)?,
                            "error": r.get::<_, Option<String>>(5)?,
                            "createdAt": r.get::<_, i64>(6)?,
                            "updatedAt": r.get::<_, i64>(7)?,
                        }))
                    },
                )
                .optional()?;
            Ok(row.unwrap_or_else(|| json!({ "found": false, "delegationId": delegation_id })))
        }

        "cancel_delegation" => {
            let delegation_id = args
                .get("delegation_id")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .ok_or_else(|| anyhow!("delegation_id is required"))?;
            let pid: Option<i64> = conn
                .query_row(
                    "SELECT pid FROM agentos_delegations WHERE delegation_id = ?1",
                    params![delegation_id],
                    |r| r.get(0),
                )
                .optional()?
                .flatten();
            #[cfg(unix)]
            if let Some(pid) = pid
                && pid > 0
            {
                unsafe {
                    libc::kill(pid as libc::pid_t, libc::SIGTERM);
                }
            }
            let updated = conn.execute(
                "UPDATE agentos_delegations
                 SET status = 'cancelled', pid = NULL, updated_at = ?1
                 WHERE delegation_id = ?2 AND status = 'running'",
                params![unix_now(), delegation_id],
            )?;
            Ok(json!({
                "delegationId": delegation_id,
                "cancelled": updated > 0,
            }))
        }

        other => anyhow::bail!("unknown MCP tool: {other}"),
    }
}

pub(crate) fn default_advisor_model(provider: ProviderKind) -> Option<&'static str> {
    match provider {
        ProviderKind::Codex => Some(CODEX_ADVISOR_MODEL),
        ProviderKind::Antigravity => Some(ANTIGRAVITY_ADVISOR_MODEL),
        _ => None,
    }
}

pub(crate) fn resolve_advisor_provider(
    requested: &str,
    caller: Option<ProviderKind>,
) -> anyhow::Result<ProviderKind> {
    match requested.trim().to_ascii_lowercase().as_str() {
        "codex" => Ok(ProviderKind::Codex),
        "antigravity" | "agy" => Ok(ProviderKind::Antigravity),
        "" | "auto" => {
            let agy_ready = crate::agent_profiles::find_antigravity_binary().is_some()
                && crate::agent_profiles::antigravity_has_stored_credentials();
            let codex_ready = crate::model::find_codex_binary().is_some();
            match caller {
                Some(ProviderKind::Codex) => {
                    if agy_ready {
                        Ok(ProviderKind::Antigravity)
                    } else if codex_ready {
                        Ok(ProviderKind::Codex)
                    } else {
                        anyhow::bail!("no advisor provider binary available (codex/antigravity)")
                    }
                }
                Some(ProviderKind::Antigravity) => {
                    if codex_ready {
                        Ok(ProviderKind::Codex)
                    } else if agy_ready {
                        Ok(ProviderKind::Antigravity)
                    } else {
                        anyhow::bail!("no advisor provider binary available (codex/antigravity)")
                    }
                }
                _ => {
                    if codex_ready {
                        Ok(ProviderKind::Codex)
                    } else if agy_ready {
                        Ok(ProviderKind::Antigravity)
                    } else {
                        anyhow::bail!("no advisor provider binary available (codex/antigravity)")
                    }
                }
            }
        }
        other => anyhow::bail!("unsupported advisor/delegation provider: {other}"),
    }
}

fn run_isolated_oneshot_agent(
    provider: ProviderKind,
    cwd: &Path,
    prompt: &str,
    model: Option<&str>,
    read_only: bool,
    timeout: Duration,
    on_pid: Option<Box<dyn FnOnce(u32) + Send>>,
) -> anyhow::Result<String> {
    match provider {
        ProviderKind::Codex => run_isolated_codex_oneshot(cwd, prompt, model, read_only, timeout, on_pid),
        ProviderKind::Antigravity => {
            run_isolated_antigravity_oneshot(cwd, prompt, model, read_only, timeout, on_pid)
        }
        other => anyhow::bail!("isolated one-shot runner does not support {}", other.display_name()),
    }
}

fn build_codex_oneshot_args(cwd: &Path, model: Option<&str>, read_only: bool) -> Vec<String> {
    let mut args = vec![
        "-c".to_owned(),
        "approval_policy=\"never\"".to_owned(),
        "-c".to_owned(),
        "cli_auth_credentials_store=\"file\"".to_owned(),
    ];
    if read_only {
        args.extend([
            "-c".to_owned(),
            "features.view_image=true".to_owned(),
            "-c".to_owned(),
            "include_apply_patch_tool=false".to_owned(),
            "-c".to_owned(),
            "web_search=\"disabled\"".to_owned(),
            "-c".to_owned(),
            "features.hooks=false".to_owned(),
            "-c".to_owned(),
            "features.plugins=false".to_owned(),
            "-c".to_owned(),
            "features.apps=false".to_owned(),
            "-c".to_owned(),
            "features.multi_agent=false".to_owned(),
        ]);
    }
    args.extend([
        "exec".to_owned(),
        "--json".to_owned(),
        "--skip-git-repo-check".to_owned(),
        "--sandbox".to_owned(),
        if read_only {
            "read-only".to_owned()
        } else {
            "workspace-write".to_owned()
        },
        "-C".to_owned(),
        cwd.to_string_lossy().into_owned(),
    ]);
    if let Some(model) = model {
        args.extend(["-m".to_owned(), model.to_owned()]);
    }
    args
}

fn run_isolated_codex_oneshot(
    cwd: &Path,
    prompt: &str,
    model: Option<&str>,
    read_only: bool,
    timeout: Duration,
    on_pid: Option<Box<dyn FnOnce(u32) + Send>>,
) -> anyhow::Result<String> {
    let binary = crate::model::find_codex_binary()
        .ok_or_else(|| anyhow!("codex CLI not found in PATH or ChatGPT.app"))?;
    let mut cmd = ProcessCommand::new(&binary);
    crate::agent_profiles::apply_to_command(ProviderKind::Codex, &mut cmd)?;
    cmd.args(build_codex_oneshot_args(cwd, model, read_only));
    cmd.arg(prompt)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());

    let mut child = cmd
        .spawn()
        .with_context(|| format!("failed to spawn isolated Codex advisor ({})", binary.display()))?;
    if let Some(cb) = on_pid {
        cb(child.id());
    }
    let stdout = child.stdout.take().ok_or_else(|| anyhow!("codex stdout missing"))?;
    let reader_handle = std::thread::spawn(move || {
        let mut last_message = String::new();
        for line in BufReader::new(stdout).lines().map_while(Result::ok) {
            if let Ok(event) = serde_json::from_str::<Value>(&line) {
                let item = event.get("item");
                if event.get("type").and_then(Value::as_str) == Some("item.completed")
                    && item
                        .and_then(|i| i.get("type"))
                        .and_then(Value::as_str)
                        == Some("agent_message")
                    && let Some(text) = item.and_then(|i| i.get("text")).and_then(Value::as_str)
                {
                    last_message = text.to_owned();
                }
            }
        }
        last_message
    });

    let deadline = Instant::now() + timeout;
    loop {
        if let Some(status) = child.try_wait()? {
            let text = reader_handle.join().unwrap_or_default();
            if status.success() && !text.trim().is_empty() {
                return Ok(text);
            }
            anyhow::bail!(
                "isolated Codex one-shot exited with status {status} (output len={})",
                text.len()
            );
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            anyhow::bail!("isolated Codex one-shot timed out after {}s", timeout.as_secs());
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}

fn run_isolated_antigravity_oneshot(
    cwd: &Path,
    prompt: &str,
    model: Option<&str>,
    read_only: bool,
    timeout: Duration,
    on_pid: Option<Box<dyn FnOnce(u32) + Send>>,
) -> anyhow::Result<String> {
    let binary = crate::agent_profiles::find_antigravity_binary()
        .ok_or_else(|| anyhow!("Antigravity ACP binary (agy / agy_acp_server.par) not found"))?;
    if !crate::agent_profiles::antigravity_has_stored_credentials() {
        anyhow::bail!("Antigravity isolated profile is not signed in (missing acp_token.json)");
    }
    let launch_args = crate::agent_profiles::antigravity_acp_launch_args(&binary, !read_only)?;
    let mut cmd = ProcessCommand::new(&binary);
    crate::agent_profiles::apply_to_command(ProviderKind::Antigravity, &mut cmd)?;
    cmd.args(launch_args);
    cmd.current_dir(cwd)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null());
    let mut child = cmd
        .spawn()
        .with_context(|| format!("failed to spawn Antigravity ACP advisor ({})", binary.display()))?;
    if let Some(cb) = on_pid {
        cb(child.id());
    }
    let mut stdin = child.stdin.take().ok_or_else(|| anyhow!("ACP stdin missing"))?;
    let stdout = child.stdout.take().ok_or_else(|| anyhow!("ACP stdout missing"))?;

    let (tx, rx) = crossbeam_channel::unbounded::<Value>();
    std::thread::spawn(move || {
        for line in BufReader::new(stdout).lines().map_while(Result::ok) {
            if let Ok(msg) = serde_json::from_str::<Value>(&line) {
                let _ = tx.send(msg);
            }
        }
    });

    let send_msg = |stdin: &mut std::process::ChildStdin, msg: Value| -> anyhow::Result<()> {
        serde_json::to_writer(&mut *stdin, &msg)?;
        stdin.write_all(b"\n")?;
        stdin.flush()?;
        Ok(())
    };

    let deadline = Instant::now() + timeout;
    let wait_for_id = |stdin: &mut std::process::ChildStdin,
                           target_id: i64,
                           collected: &mut String|
     -> anyhow::Result<Value> {
        loop {
            let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
                anyhow::bail!("isolated Antigravity ACP timed out after {}s", timeout.as_secs());
            };
            let msg = rx
                .recv_timeout(remaining)
                .map_err(|_| anyhow!("isolated Antigravity ACP closed unexpectedly"))?;
            if msg.get("method").and_then(Value::as_str) == Some("session/update") {
                if let Some(update) = msg.pointer("/params/update")
                    && update.get("sessionUpdate").and_then(Value::as_str)
                        == Some("agent_message_chunk")
                    && let Some(text) = update.pointer("/content/text").and_then(Value::as_str)
                {
                    collected.push_str(text);
                }
                continue;
            }
            if msg.get("method").and_then(Value::as_str) == Some("session/request_permission")
                && let Some(req_id) = msg.get("id").cloned()
            {
                let kind = msg
                    .pointer("/params/toolCall/kind")
                    .and_then(Value::as_str)
                    .unwrap_or("");
                let allow_read = read_only && matches!(kind, "read" | "search");
                let allow_any = !read_only;
                let outcome = if allow_read || allow_any {
                    msg.pointer("/params/options")
                        .and_then(Value::as_array)
                        .and_then(|opts| {
                            opts.iter().find(|o| {
                                matches!(
                                    o.get("kind").and_then(Value::as_str),
                                    Some("allow_once" | "allow_always")
                                )
                            })
                        })
                        .and_then(|o| o.get("optionId").and_then(Value::as_str))
                        .map(|opt_id| json!({ "outcome": "selected", "optionId": opt_id }))
                        .unwrap_or_else(|| json!({ "outcome": "cancelled" }))
                } else {
                    json!({ "outcome": "cancelled" })
                };
                send_msg(
                    stdin,
                    json!({ "jsonrpc": "2.0", "id": req_id, "result": { "outcome": outcome } }),
                )?;
                continue;
            }
            if msg.get("id").and_then(Value::as_i64) == Some(target_id) {
                return Ok(msg);
            }
        }
    };

    let mut output = String::new();
    let result = (|| -> anyhow::Result<String> {
        send_msg(
            &mut stdin,
            json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "initialize",
                "params": {
                    "protocolVersion": 1,
                    "clientCapabilities": { "terminal": false },
                    "clientInfo": { "name": "agentos-advisor", "version": env!("CARGO_PKG_VERSION") }
                }
            }),
        )?;
        let _ = wait_for_id(&mut stdin, 1, &mut output)?;

        send_msg(
            &mut stdin,
            json!({
                "jsonrpc": "2.0",
                "id": 2,
                "method": "session/new",
                "params": { "cwd": cwd.to_string_lossy(), "mcpServers": [] }
            }),
        )?;
        let new_resp = wait_for_id(&mut stdin, 2, &mut output)?;
        let sid = new_resp
            .pointer("/result/sessionId")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("Antigravity advisor session/new returned no sessionId"))?
            .to_owned();

        if let Some(model_id) = model {
            let _ = send_msg(
                &mut stdin,
                json!({
                    "jsonrpc": "2.0",
                    "id": 3,
                    "method": "session/set_config_option",
                    "params": { "sessionId": sid, "configId": "model", "value": model_id }
                }),
            );
            let _ = wait_for_id(&mut stdin, 3, &mut output);
        }

        send_msg(
            &mut stdin,
            json!({
                "jsonrpc": "2.0",
                "id": 4,
                "method": "session/prompt",
                "params": {
                    "sessionId": sid,
                    "prompt": [{ "type": "text", "text": prompt }]
                }
            }),
        )?;
        let _ = wait_for_id(&mut stdin, 4, &mut output)?;
        Ok(output)
    })();

    let _ = child.kill();
    let _ = child.wait();
    result
}

// ── Host-side runtime loop helpers (called by waku-daemon) ──────────────────

pub fn claim_pending_questions(
    db_path: &Path,
    session_id: Uuid,
) -> anyhow::Result<Vec<(String, Vec<UserInputQuestion>)>> {
    let conn = open_mcp_connection(db_path)?;
    let sid = session_id.to_string();
    let mut stmt = conn.prepare(
        "SELECT question_id, questions_json FROM agentos_pending_questions
         WHERE session_id = ?1 AND status = 'pending'
         ORDER BY created_at ASC",
    )?;
    let rows: Vec<(String, String)> = stmt
        .query_map(params![sid], |r| Ok((r.get(0)?, r.get(1)?)))?
        .flatten()
        .collect();
    let mut claimed = Vec::new();
    let now = unix_now();
    for (qid, raw_json) in rows {
        let updated = conn.execute(
            "UPDATE agentos_pending_questions SET status = 'dispatched', updated_at = ?1
             WHERE question_id = ?2 AND status = 'pending'",
            params![now, qid],
        )?;
        if updated > 0
            && let Ok(questions) = serde_json::from_str::<Vec<UserInputQuestion>>(&raw_json)
        {
            claimed.push((qid, questions));
        }
    }
    Ok(claimed)
}

pub fn resolve_pending_question(
    db_path: &Path,
    question_id: &str,
    answers: &[UserInputAnswer],
) -> anyhow::Result<()> {
    let conn = open_mcp_connection(db_path)?;
    conn.execute(
        "UPDATE agentos_pending_questions
         SET status = 'answered', answers_json = ?1, updated_at = ?2
         WHERE question_id = ?3",
        params![serde_json::to_string(answers)?, unix_now(), question_id],
    )?;
    Ok(())
}

pub fn cancel_pending_questions(db_path: &Path, session_id: Uuid) -> anyhow::Result<()> {
    let conn = open_mcp_connection(db_path)?;
    conn.execute(
        "UPDATE agentos_pending_questions
         SET status = 'cancelled', updated_at = ?1
         WHERE session_id = ?2 AND status IN ('pending', 'dispatched')",
        params![unix_now(), session_id.to_string()],
    )?;
    Ok(())
}

/// Consumes any pending `memory_context_rotate` request for `session_id` and
/// constructs a bounded restore package combining active goal, bound/latest
/// checkpoint, working notes, and recent project knowledge.
pub fn take_pending_context_rotation(
    db_path: &Path,
    session_id: Uuid,
) -> anyhow::Result<Option<String>> {
    let conn = open_mcp_connection(db_path)?;
    let sid = session_id.to_string();
    let pending: Option<(String, Option<String>, Option<String>)> = conn
        .query_row(
            "SELECT rotation_id, checkpoint_id, reason FROM agentos_memory_rotations
             WHERE session_id = ?1 AND status = 'pending'
             ORDER BY created_at DESC LIMIT 1",
            params![sid],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .optional()?;
    let Some((rotation_id, checkpoint_id, reason)) = pending else {
        return Ok(None);
    };
    conn.execute(
        "UPDATE agentos_memory_rotations SET status = 'applied' WHERE rotation_id = ?1",
        params![rotation_id],
    )?;

    let mut pkg = String::from("<agent_context_rotation>\n");
    if let Some(reason) = reason {
        pkg.push_str(&format!("Rotation reason: {reason}\n"));
    }
    if let Ok(Some((obj, status, turn_idx))) = conn
        .query_row(
            "SELECT objective, status, turn_index FROM agentos_goals
             WHERE session_id = ?1 ORDER BY updated_at DESC LIMIT 1",
            params![sid],
            |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?, r.get::<_, i64>(2)?)),
        )
        .optional()
    {
        pkg.push_str(&format!(
            "\n## Active Goal (status={status}, turn={turn_idx})\n{obj}\n"
        ));
    }

    let ckpt_json: Option<String> = if let Some(cid) = checkpoint_id.as_deref() {
        let stripped = cid.strip_prefix("ckpt:").unwrap_or(cid);
        conn.query_row(
            "SELECT payload_json FROM agentos_memory_checkpoints WHERE checkpoint_id = ?1",
            params![stripped],
            |r| r.get(0),
        )
        .optional()?
    } else {
        conn.query_row(
            "SELECT payload_json FROM agentos_memory_checkpoints
             WHERE session_id = ?1 ORDER BY created_at DESC LIMIT 1",
            params![sid],
            |r| r.get(0),
        )
        .optional()?
    };
    if let Some(raw) = ckpt_json {
        pkg.push_str(&format!("\n## Recovery Checkpoint\n{raw}\n"));
    }

    if let Ok(mut stmt) = conn.prepare(
        "SELECT note_id, revision, body FROM agentos_memory_notes
         WHERE session_id = ?1 ORDER BY updated_at DESC LIMIT 8",
    ) && let Ok(rows) = stmt.query_map(params![sid], |r| {
        Ok((
            r.get::<_, String>(0)?,
            r.get::<_, i64>(1)?,
            r.get::<_, String>(2)?,
        ))
    }) {
        let notes: Vec<_> = rows.flatten().collect();
        if !notes.is_empty() {
            pkg.push_str("\n## Working Notes\n");
            for (nid, rev, body) in notes {
                let preview: String = body.chars().take(1200).collect();
                pkg.push_str(&format!("- **{nid}** (rev {rev}): {preview}\n"));
            }
        }
    }
    pkg.push_str("</agent_context_rotation>");
    Ok(Some(pkg))
}

/// Checks whether the agent reported `continue` for an active goal on the turn
/// that just finished, atomically claims it (`last_reported_status = 'dispatched'`),
/// and returns the updated `ThreadGoal` plus the autonomous follow-up prompt.
pub fn take_pending_goal_continuation(
    db_path: &Path,
    session_id: Uuid,
) -> anyhow::Result<Option<(ThreadGoal, String)>> {
    let conn = open_mcp_connection(db_path)?;
    let sid = session_id.to_string();
    let now = unix_now();
    let row: Option<(String, String, Option<i64>, Option<i64>, String, i64, Option<String>, i64)> =
        conn.query_row(
            "SELECT run_id, objective, max_turns, max_seconds, current_turn_id, turn_index, last_reason, created_at
             FROM agentos_goals
             WHERE session_id = ?1 AND status = 'active' AND last_reported_status = 'continue'
             ORDER BY updated_at DESC LIMIT 1",
            params![sid],
            |r| {
                Ok((
                    r.get(0)?,
                    r.get(1)?,
                    r.get(2)?,
                    r.get(3)?,
                    r.get(4)?,
                    r.get(5)?,
                    r.get(6)?,
                    r.get(7)?,
                ))
            },
        )
        .optional()?;
    let Some((run_id, objective, max_turns, max_seconds, turn_id, turn_index, last_reason, created_at)) =
        row
    else {
        return Ok(None);
    };

    let elapsed = now.saturating_sub(created_at);
    if max_turns.is_some_and(|limit| turn_index > limit)
        || max_seconds.is_some_and(|limit| elapsed >= limit)
    {
        let _ = conn.execute(
            "UPDATE agentos_goals SET status = 'blocked', last_reported_status = 'blocked', updated_at = ?1 WHERE run_id = ?2",
            params![now, run_id],
        );
        return Ok(None);
    }

    let updated = conn.execute(
        "UPDATE agentos_goals SET last_reported_status = 'dispatched', updated_at = ?1
         WHERE run_id = ?2 AND last_reported_status = 'continue'",
        params![now, run_id],
    )?;
    if updated == 0 {
        return Ok(None);
    }

    let goal = ThreadGoal {
        objective: objective.clone(),
        status: ThreadGoalStatus::Active,
        token_budget: None,
        tokens_used: 0,
        time_used_seconds: elapsed,
    };
    let next_action = last_reason.unwrap_or_else(|| "Proceed with the next step.".to_owned());
    let prompt = format!(
        "[AgentOS Goal Auto-Continue | runId={run_id} | turnId={turn_id} | turn={turn_index}]\n\
         Objective: {objective}\n\
         Planned next action: {next_action}\n\n\
         Continue executing this goal. When this turn finishes, call `report_goal` with runId=\"{run_id}\" and turnId=\"{turn_id}\"."
    );
    Ok(Some((goal, prompt)))
}

pub fn pause_active_goal_on_cancel(db_path: &Path, session_id: Uuid) -> anyhow::Result<()> {
    let conn = open_mcp_connection(db_path)?;
    conn.execute(
        "UPDATE agentos_goals
         SET status = 'blocked', last_reported_status = 'cancelled', last_reason = 'Paused by user cancel', updated_at = ?1
         WHERE session_id = ?2 AND status = 'active'",
        params![unix_now(), session_id.to_string()],
    )?;
    Ok(())
}

/// Bridges Waku's `/goal` UI operations (`Refresh` / `Set` / `Clear`) to
/// `agentos_goals` for non-Codex providers (Antigravity, OpenCode, Kimi, etc.).
pub fn apply_host_goal_operation(
    db_path: &Path,
    session_id: Uuid,
    operation: GoalOperation,
) -> anyhow::Result<Option<ThreadGoal>> {
    let conn = open_mcp_connection(db_path)?;
    let sid = session_id.to_string();
    let now = unix_now();
    match operation {
        GoalOperation::Clear => {
            conn.execute(
                "UPDATE agentos_goals SET status = 'complete', last_reported_status = 'cleared', updated_at = ?1
                 WHERE session_id = ?2 AND status IN ('active', 'blocked')",
                params![now, sid],
            )?;
            Ok(None)
        }
        GoalOperation::Set {
            objective,
            status,
            replace,
        } => {
            if replace {
                conn.execute(
                    "UPDATE agentos_goals SET status = 'complete', last_reported_status = 'replaced', updated_at = ?1
                     WHERE session_id = ?2 AND status IN ('active', 'blocked')",
                    params![now, sid],
                )?;
            }
            let existing: Option<(String, String, String, i64)> = conn
                .query_row(
                    "SELECT run_id, objective, status, created_at FROM agentos_goals
                     WHERE session_id = ?1 AND status IN ('active', 'blocked')
                     ORDER BY updated_at DESC LIMIT 1",
                    params![sid],
                    |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
                )
                .optional()?;
            if let Some((run_id, cur_obj, cur_status, created_at)) = existing {
                let next_obj = objective.unwrap_or(cur_obj);
                let next_status_str = match status {
                    Some(ThreadGoalStatus::Active) => "active",
                    Some(_) => "blocked",
                    None => cur_status.as_str(),
                };
                conn.execute(
                    "UPDATE agentos_goals SET objective = ?1, status = ?2, updated_at = ?3 WHERE run_id = ?4",
                    params![next_obj, next_status_str, now, run_id],
                )?;
                Ok(Some(ThreadGoal {
                    objective: next_obj,
                    status: if next_status_str == "blocked" {
                        ThreadGoalStatus::Paused
                    } else {
                        ThreadGoalStatus::Active
                    },
                    token_budget: None,
                    tokens_used: 0,
                    time_used_seconds: now.saturating_sub(created_at),
                }))
            } else {
                let Some(obj) = objective.filter(|s| !s.trim().is_empty()) else {
                    return Ok(None);
                };
                let run_id = Uuid::new_v4().to_string();
                let turn_id = Uuid::new_v4().to_string();
                let next_status_str = match status {
                    Some(ThreadGoalStatus::Paused) => "blocked",
                    _ => "active",
                };
                conn.execute(
                    "INSERT INTO agentos_goals (run_id, session_id, objective, status, current_turn_id, turn_index, created_at, updated_at)
                     VALUES (?1, ?2, ?3, ?4, ?5, 1, ?6, ?6)",
                    params![run_id, sid, obj, next_status_str, turn_id, now],
                )?;
                Ok(Some(ThreadGoal {
                    objective: obj,
                    status: if next_status_str == "blocked" {
                        ThreadGoalStatus::Paused
                    } else {
                        ThreadGoalStatus::Active
                    },
                    token_budget: None,
                    tokens_used: 0,
                    time_used_seconds: 0,
                }))
            }
        }
        GoalOperation::Refresh => {
            let row: Option<(String, String, i64)> = conn
                .query_row(
                    "SELECT objective, status, created_at FROM agentos_goals
                     WHERE session_id = ?1 AND status IN ('active', 'blocked')
                     ORDER BY updated_at DESC LIMIT 1",
                    params![sid],
                    |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
                )
                .optional()?;
            Ok(row.map(|(objective, status, created_at)| ThreadGoal {
                objective,
                status: if status == "blocked" {
                    ThreadGoalStatus::Paused
                } else {
                    ThreadGoalStatus::Active
                },
                token_budget: None,
                tokens_used: 0,
                time_used_seconds: now.saturating_sub(created_at),
            }))
        }
    }
}

/// Host helper for the Memory Library UI: load knowledge items, working notes,
/// and recovery checkpoints from the daemon's SQLite database.
pub fn load_memory_library(
    db_path: &Path,
    project_path: Option<&str>,
    query: Option<&str>,
) -> anyhow::Result<MemoryLibrarySnapshot> {
    let conn = open_mcp_connection(db_path)?;
    ensure_mcp_tables(&conn)?;
    let has_sessions: bool = conn
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='sessions')",
            [],
            |r| r.get(0),
        )
        .unwrap_or(false);
    let filter_project = project_path
        .map(str::trim)
        .filter(|path| !path.is_empty());
    let needle = query
        .map(|q| q.trim().to_lowercase())
        .filter(|q| !q.is_empty());

    let matches_query = |fields: &[&str]| -> bool {
        let Some(ref needle) = needle else {
            return true;
        };
        fields
            .iter()
            .any(|field| field.to_lowercase().contains(needle.as_str()))
    };

    let mut knowledge = Vec::new();
    {
        let sql = if has_sessions {
            "SELECT k.knowledge_id, k.scope, k.project_path, k.session_id,
                    COALESCE(NULLIF(NULLIF(s.title, 'New task'), ''), NULLIF(s.auto_title, ''), s.title),
                    k.claim, k.applicability_json, k.evidence_json, k.status, k.created_at
             FROM agentos_memory_knowledge k
             LEFT JOIN sessions s ON s.id = k.session_id
             ORDER BY CASE k.status WHEN 'promoted' THEN 0 WHEN 'candidate' THEN 1 ELSE 2 END,
                      k.created_at DESC
             LIMIT 500"
        } else {
            "SELECT knowledge_id, scope, project_path, session_id, NULL,
                    claim, applicability_json, evidence_json, status, created_at
             FROM agentos_memory_knowledge
             ORDER BY CASE status WHEN 'promoted' THEN 0 WHEN 'candidate' THEN 1 ELSE 2 END,
                      created_at DESC
             LIMIT 500"
        };
        let mut stmt = conn.prepare(sql)?;
        let rows = stmt.query_map([], |r| {
            Ok(MemoryKnowledgeRecord {
                knowledge_id: r.get(0)?,
                scope: r.get(1)?,
                project_path: r.get(2)?,
                session_id: r.get(3)?,
                session_title: r.get(4)?,
                claim: r.get(5)?,
                applicability_json: r.get(6)?,
                evidence_json: r.get(7)?,
                status: r.get(8)?,
                created_at: r.get(9)?,
            })
        })?;
        for item in rows.flatten() {
            if let Some(proj) = filter_project
                && item.scope != "user"
                && item.project_path != proj
            {
                continue;
            }
            if !matches_query(&[
                &item.claim,
                &item.knowledge_id,
                &item.project_path,
                &item.status,
                item.session_title.as_deref().unwrap_or(""),
            ]) {
                continue;
            }
            knowledge.push(item);
        }
    }

    let mut notes = Vec::new();
    {
        let sql = if has_sessions {
            "SELECT n.session_id,
                    COALESCE(NULLIF(NULLIF(s.title, 'New task'), ''), NULLIF(s.auto_title, ''), s.title),
                    n.project_path, n.note_id, n.scope, n.revision, n.body,
                    n.source_refs_json, n.updated_at
             FROM agentos_memory_notes n
             LEFT JOIN sessions s ON s.id = n.session_id
             ORDER BY n.updated_at DESC
             LIMIT 500"
        } else {
            "SELECT session_id, NULL, project_path, note_id, scope, revision, body,
                    source_refs_json, updated_at
             FROM agentos_memory_notes
             ORDER BY updated_at DESC
             LIMIT 500"
        };
        let mut stmt = conn.prepare(sql)?;
        let rows = stmt.query_map([], |r| {
            Ok(MemoryNoteRecord {
                session_id: r.get(0)?,
                session_title: r.get(1)?,
                project_path: r.get(2)?,
                note_id: r.get(3)?,
                scope: r.get(4)?,
                revision: r.get(5)?,
                body: r.get(6)?,
                source_refs_json: r.get(7)?,
                updated_at: r.get(8)?,
            })
        })?;
        for item in rows.flatten() {
            if let Some(proj) = filter_project
                && item.project_path != proj
            {
                continue;
            }
            if !matches_query(&[
                &item.note_id,
                &item.body,
                &item.project_path,
                item.session_title.as_deref().unwrap_or(""),
            ]) {
                continue;
            }
            notes.push(item);
        }
    }

    let mut checkpoints = Vec::new();
    {
        let sql = if has_sessions {
            "SELECT c.checkpoint_id, c.session_id,
                    COALESCE(NULLIF(NULLIF(s.title, 'New task'), ''), NULLIF(s.auto_title, ''), s.title),
                    c.project_path, c.payload_json, c.created_at
             FROM agentos_memory_checkpoints c
             LEFT JOIN sessions s ON s.id = c.session_id
             ORDER BY c.created_at DESC
             LIMIT 500"
        } else {
            "SELECT checkpoint_id, session_id, NULL, project_path, payload_json, created_at
             FROM agentos_memory_checkpoints
             ORDER BY created_at DESC
             LIMIT 500"
        };
        let mut stmt = conn.prepare(sql)?;
        let rows = stmt.query_map([], |r| {
            Ok(MemoryCheckpointRecord {
                checkpoint_id: r.get(0)?,
                session_id: r.get(1)?,
                session_title: r.get(2)?,
                project_path: r.get(3)?,
                payload_json: r.get(4)?,
                created_at: r.get(5)?,
            })
        })?;
        for item in rows.flatten() {
            if let Some(proj) = filter_project
                && item.project_path != proj
            {
                continue;
            }
            if !matches_query(&[
                &item.checkpoint_id,
                &item.payload_json,
                &item.project_path,
                item.session_title.as_deref().unwrap_or(""),
            ]) {
                continue;
            }
            checkpoints.push(item);
        }
    }

    Ok(MemoryLibrarySnapshot {
        knowledge,
        notes,
        checkpoints,
    })
}

/// Host helper for the Memory Library UI: promote, revert to candidate, or
/// reject a knowledge entry.
pub fn update_memory_knowledge_status(
    db_path: &Path,
    knowledge_id: &str,
    status: &str,
) -> anyhow::Result<()> {
    let normalized = match status.trim() {
        "promoted" | "candidate" | "rejected" => status.trim(),
        other => anyhow::bail!("unsupported knowledge status `{other}`"),
    };
    let conn = open_mcp_connection(db_path)?;
    ensure_mcp_tables(&conn)?;
    conn.execute(
        "UPDATE agentos_memory_knowledge SET status = ?1 WHERE knowledge_id = ?2",
        params![normalized, knowledge_id.trim()],
    )?;
    Ok(())
}

/// Host helper for the Memory Library UI: delete one knowledge item, note, or
/// recovery checkpoint.
pub fn delete_memory_entry(
    db_path: &Path,
    kind: MemoryEntryKind,
    id: &str,
    note_id: Option<&str>,
    scope: Option<&str>,
) -> anyhow::Result<()> {
    let conn = open_mcp_connection(db_path)?;
    ensure_mcp_tables(&conn)?;
    match kind {
        MemoryEntryKind::Knowledge => {
            conn.execute(
                "DELETE FROM agentos_memory_knowledge WHERE knowledge_id = ?1",
                params![id.trim()],
            )?;
        }
        MemoryEntryKind::Checkpoint => {
            conn.execute(
                "DELETE FROM agentos_memory_checkpoints WHERE checkpoint_id = ?1",
                params![id.trim()],
            )?;
        }
        MemoryEntryKind::Note => {
            let note_slug = note_id
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .ok_or_else(|| anyhow!("note_id is required to delete a note"))?;
            if let Some(scope) = scope.map(str::trim).filter(|s| !s.is_empty()) {
                conn.execute(
                    "DELETE FROM agentos_memory_notes WHERE session_id = ?1 AND note_id = ?2 AND scope = ?3",
                    params![id.trim(), note_slug, scope],
                )?;
            } else {
                conn.execute(
                    "DELETE FROM agentos_memory_notes WHERE session_id = ?1 AND note_id = ?2",
                    params![id.trim(), note_slug],
                )?;
            }
        }
    }
    Ok(())
}

/// Expand non-ASCII alphanumeric (CJK) runs into overlapping bigrams + single
/// chars so SQLite FTS5 `unicode61` supports Chinese substring recall.
fn fts_normalize_cjk(text: &str) -> String {
    let mut out = String::with_capacity(text.len() * 2);
    let mut run: Vec<char> = Vec::new();
    let flush = |run: &mut Vec<char>, out: &mut String| {
        if run.len() >= 2 {
            for w in run.windows(2) {
                out.push(w[0]);
                out.push(w[1]);
                out.push(' ');
            }
            for c in run.iter() {
                out.push(*c);
                out.push(' ');
            }
        } else {
            for c in run.iter() {
                out.push(*c);
                out.push(' ');
            }
        }
        run.clear();
    };
    for c in text.chars() {
        if c.is_alphanumeric() && !c.is_ascii() {
            run.push(c);
        } else {
            flush(&mut run, &mut out);
            if c.is_ascii_alphanumeric() {
                out.push(c.to_ascii_lowercase());
            } else {
                out.push(' ');
            }
        }
    }
    flush(&mut run, &mut out);
    out
}

/// Split a free-text query into deduplicated, lowercased semantic terms
/// (keeping IPs/identifiers like `192.168.1.1` or `UNNC-Living` intact).
fn extract_search_terms(query: &str) -> Vec<String> {
    let mut terms = Vec::new();
    for raw in query.split_whitespace() {
        let trimmed = raw.trim_matches(|c: char| {
            matches!(
                c,
                '"' | '\'' | '`' | ',' | ';' | '(' | ')' | '[' | ']' | '{' | '}' | '，' | '。' | '？' | '！' | '、' | '：'
            )
        });
        let lower = trimmed.to_lowercase();
        if !lower.is_empty() && !terms.contains(&lower) {
            terms.push(lower);
            if terms.len() >= 12 {
                break;
            }
        }
    }
    terms
}

/// Convert one whitespace-delimited query term into an FTS5 expression that
/// matches `fts_normalize_cjk`-indexed text cleanly (phrases for dotted/hyphenated
/// ASCII identifiers, bigrams for CJK runs ≥ 2 without single-char noise).
fn term_to_fts_expr(term: &str) -> Option<String> {
    let mut parts: Vec<String> = Vec::new();
    let mut cjk_run: Vec<char> = Vec::new();
    let mut ascii_tokens: Vec<String> = Vec::new();
    let mut current_ascii = String::new();

    let flush_cjk = |run: &mut Vec<char>, parts: &mut Vec<String>| {
        if run.len() >= 2 {
            for w in run.windows(2) {
                let bg: String = w.iter().collect();
                let tok = format!("\"{bg}\"");
                if !parts.contains(&tok) {
                    parts.push(tok);
                }
            }
        } else if let Some(&c) = run.first() {
            let tok = format!("\"{c}\"");
            if !parts.contains(&tok) {
                parts.push(tok);
            }
        }
        run.clear();
    };
    let flush_ascii = |cur: &mut String, toks: &mut Vec<String>| {
        if !cur.is_empty() {
            toks.push(std::mem::take(cur));
        }
    };
    let commit_ascii_phrase = |toks: &mut Vec<String>, parts: &mut Vec<String>| {
        if !toks.is_empty() {
            let phrase = format!("\"{}\"", toks.join(" "));
            if !parts.contains(&phrase) {
                parts.push(phrase);
            }
            toks.clear();
        }
    };

    for c in term.chars() {
        if c.is_alphanumeric() && !c.is_ascii() {
            flush_ascii(&mut current_ascii, &mut ascii_tokens);
            commit_ascii_phrase(&mut ascii_tokens, &mut parts);
            cjk_run.push(c);
        } else if c.is_ascii_alphanumeric() {
            flush_cjk(&mut cjk_run, &mut parts);
            current_ascii.push(c.to_ascii_lowercase());
        } else {
            flush_cjk(&mut cjk_run, &mut parts);
            flush_ascii(&mut current_ascii, &mut ascii_tokens);
        }
    }
    flush_cjk(&mut cjk_run, &mut parts);
    flush_ascii(&mut current_ascii, &mut ascii_tokens);
    commit_ascii_phrase(&mut ascii_tokens, &mut parts);

    match parts.len() {
        0 => None,
        1 => parts.into_iter().next(),
        _ => Some(format!("({})", parts.join(" AND "))),
    }
}

/// Build `(strict_and_fts, relaxed_or_fts)` queries for two-stage BM25 recall.
fn build_fts_queries(terms: &[String]) -> (Option<String>, Option<String>) {
    let mut group_exprs = Vec::new();
    for t in terms {
        if let Some(expr) = term_to_fts_expr(t)
            && !group_exprs.contains(&expr)
        {
            group_exprs.push(expr);
        }
    }
    match group_exprs.len() {
        0 => (None, None),
        1 => (group_exprs.into_iter().next(), None),
        _ => {
            let strict = group_exprs.join(" AND ");
            let relaxed = group_exprs.join(" OR ");
            (Some(strict), Some(relaxed))
        }
    }
}

/// Score a piece of text against query terms: rewards distinct-term coverage,
/// term specificity (longer tokens / IPs / identifiers), and capped TF.
fn score_text_terms(text: &str, terms: &[String]) -> f64 {
    if terms.is_empty() {
        return 1.0;
    }
    let lower = text.to_lowercase();
    let mut matched_terms = 0usize;
    let mut specificity_sum = 0.0f64;
    let mut tf_bonus = 0.0f64;

    for term in terms {
        let count = lower.matches(term.as_str()).count();
        if count > 0 {
            matched_terms += 1;
            let char_len = term.chars().count() as f64;
            let has_digit_or_punct = term
                .chars()
                .any(|c| c.is_ascii_digit() || c == '.' || c == '-' || c == '_');
            let weight = (char_len.min(12.0) / 3.0).max(0.6)
                + if has_digit_or_punct { 0.8 } else { 0.0 };
            specificity_sum += weight;
            tf_bonus += ((count as f64).min(5.0)).ln_1p() * 0.35;
        }
    }

    if matched_terms == 0 {
        return 0.0;
    }

    let coverage = matched_terms as f64 / terms.len() as f64;
    let full_match_bonus = if matched_terms == terms.len() { 6.0 } else { 0.0 };
    coverage * 6.0 + specificity_sum + tf_bonus + full_match_bonus
}

/// Extract a UTF-8-safe snippet centered around the highest-specificity matched
/// term instead of blindly taking the first N characters.
fn extract_match_snippet(text: &str, terms: &[String], max_chars: usize) -> String {
    if text.chars().count() <= max_chars {
        return text.to_string();
    }
    if terms.is_empty() {
        let mut s: String = text.chars().take(max_chars).collect();
        s.push_str("...");
        return s;
    }
    let lower = text.to_lowercase();
    // Prefer longer / more specific terms when choosing where to center the snippet.
    let mut ordered_terms: Vec<&String> = terms.iter().collect();
    ordered_terms.sort_by(|a, b| b.chars().count().cmp(&a.chars().count()));

    let mut match_char_idx: Option<usize> = None;
    for term in ordered_terms {
        if let Some(byte_pos) = lower.find(term.as_str()) {
            match_char_idx = Some(lower[..byte_pos].chars().count());
            break;
        }
    }
    let Some(center_idx) = match_char_idx else {
        let mut s: String = text.chars().take(max_chars).collect();
        s.push_str("...");
        return s;
    };

    let total_chars = text.chars().count();
    let lead = (max_chars / 4).min(60);
    let start = center_idx.saturating_sub(lead);
    let end = (start + max_chars).min(total_chars);
    let start = end.saturating_sub(max_chars);

    let slice: String = text.chars().skip(start).take(end - start).collect();
    let mut out = String::with_capacity(slice.len() + 6);
    if start > 0 {
        out.push_str("...");
    }
    out.push_str(&slice);
    if end < total_chars {
        out.push_str("...");
    }
    out
}

/// Detect raw self-echo payloads from `memory_search` tool calls/results so
/// searches never pollute themselves.
fn is_memory_search_echo(text: &str) -> bool {
    let trimmed = text.trim();
    if trimmed.starts_with('{')
        && (trimmed.contains("\"toolSummary\"") || trimmed.contains("\"memory_search\""))
        && trimmed.contains("\"query\"")
        && trimmed.contains("\"scope\"")
    {
        return true;
    }
    false
}

/// Incrementally sync non-streaming rows from `messages` into `agentos_messages_fts`.
/// Returns `true` when the FTS index is ready for querying.
fn sync_messages_fts(conn: &Connection) -> bool {
    if conn
        .execute_batch(
            "CREATE VIRTUAL TABLE IF NOT EXISTS agentos_messages_fts USING fts5(content, tokenize='unicode61');
             CREATE TABLE IF NOT EXISTS agentos_messages_fts_sync (
                 msg_rowid INTEGER PRIMARY KEY,
                 content_len INTEGER NOT NULL
             );",
        )
        .is_err()
    {
        return false;
    }

    let pending: Vec<(i64, String)> = {
        let Ok(mut stmt) = conn.prepare(
            "SELECT m.rowid, m.content
             FROM messages m
             LEFT JOIN agentos_messages_fts_sync s ON s.msg_rowid = m.rowid
             WHERE m.streaming = 0
               AND (s.msg_rowid IS NULL OR s.content_len != length(m.content))",
        ) else {
            return false;
        };
        let Ok(rows) = stmt.query_map([], |r| Ok((r.get::<_, i64>(0)?, r.get::<_, String>(1)?)))
        else {
            return false;
        };
        rows.flatten().collect()
    };

    if !pending.is_empty() {
        if conn.execute_batch("BEGIN IMMEDIATE").is_err() {
            return false;
        }
        let mut ok = true;
        {
            let del = conn.prepare("DELETE FROM agentos_messages_fts WHERE rowid = ?1");
            let ins = conn
                .prepare("INSERT INTO agentos_messages_fts(rowid, content) VALUES (?1, ?2)");
            let mark = conn.prepare(
                "INSERT OR REPLACE INTO agentos_messages_fts_sync(msg_rowid, content_len) VALUES (?1, ?2)",
            );
            if let (Ok(mut del), Ok(mut ins), Ok(mut mark)) = (del, ins, mark) {
                for (rowid, content) in pending {
                    let norm = fts_normalize_cjk(&content);
                    let len = content.len() as i64;
                    if del.execute(params![rowid]).is_err()
                        || ins.execute(params![rowid, norm]).is_err()
                        || mark.execute(params![rowid, len]).is_err()
                    {
                        ok = false;
                        break;
                    }
                }
            } else {
                ok = false;
            }
        }
        if ok {
            if conn.execute_batch("COMMIT").is_err() {
                let _ = conn.execute_batch("ROLLBACK");
                return false;
            }
        } else {
            let _ = conn.execute_batch("ROLLBACK");
            return false;
        }
    }

    true
}

fn search_waku_message_history(
    conn: &Connection,
    ctx: &McpContext,
    project_path: &str,
    scope: &str,
    raw_query: &str,
    terms: &[String],
    limit: usize,
) -> Vec<(f64, i64, Value)> {
    let has_messages: bool = conn
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='messages')",
            [],
            |r| r.get(0),
        )
        .unwrap_or(false);
    if !has_messages {
        return Vec::new();
    }
    let has_sessions_and_projects: bool = conn
        .query_row(
            "SELECT COUNT(*) = 2 FROM sqlite_master WHERE type='table' AND name IN ('sessions', 'projects')",
            [],
            |r| r.get(0),
        )
        .unwrap_or(false);

    let mut out: Vec<(f64, i64, Value)> = Vec::new();
    let mut seen_ids = std::collections::HashSet::<String>::new();

    // Empty query: return most recent non-streaming messages within scope.
    if raw_query.is_empty() || terms.is_empty() {
        let sql = if has_sessions_and_projects {
            match scope {
                "task_shared" => {
                    "SELECT m.id, m.session_id, COALESCE(s.title, ''), m.role, m.content, m.created_at
                     FROM messages m
                     LEFT JOIN sessions s ON s.id = m.session_id
                     WHERE m.streaming = 0 AND m.session_id = ?1
                     ORDER BY m.created_at DESC, m.position DESC LIMIT ?3"
                }
                "user" => {
                    "SELECT m.id, m.session_id, COALESCE(s.title, ''), m.role, m.content, m.created_at
                     FROM messages m
                     LEFT JOIN sessions s ON s.id = m.session_id
                     WHERE m.streaming = 0 AND (?1 != '' OR 1=1) AND (?2 != '' OR 1=1)
                     ORDER BY m.created_at DESC, m.position DESC LIMIT ?3"
                }
                _ => {
                    "SELECT m.id, m.session_id, COALESCE(s.title, ''), m.role, m.content, m.created_at
                     FROM messages m
                     LEFT JOIN sessions s ON s.id = m.session_id
                     LEFT JOIN projects p ON p.id = s.project_id
                     WHERE m.streaming = 0 AND (p.path = ?2 OR m.session_id = ?1)
                     ORDER BY m.created_at DESC, m.position DESC LIMIT ?3"
                }
            }
        } else if scope == "task_shared" {
            "SELECT id, session_id, '', role, content, created_at FROM messages
             WHERE streaming = 0 AND session_id = ?1 AND (?2 != '' OR 1=1)
             ORDER BY created_at DESC, position DESC LIMIT ?3"
        } else {
            "SELECT id, session_id, '', role, content, created_at FROM messages
             WHERE streaming = 0 AND (?1 != '' OR 1=1) AND (?2 != '' OR 1=1)
             ORDER BY created_at DESC, position DESC LIMIT ?3"
        };
        if let Ok(mut stmt) = conn.prepare(sql)
            && let Ok(rows) = stmt.query_map(
                params![ctx.session_id, project_path, (limit * 2) as i64],
                |r| {
                    Ok((
                        r.get::<_, String>(0)?,
                        r.get::<_, String>(1)?,
                        r.get::<_, String>(2)?,
                        r.get::<_, String>(3)?,
                        r.get::<_, String>(4)?,
                        r.get::<_, i64>(5)?,
                    ))
                },
            )
        {
            for (mid, sid, title, role, content, ts) in rows.flatten() {
                if is_memory_search_echo(&content) || !seen_ids.insert(mid.clone()) {
                    continue;
                }
                let snippet = extract_match_snippet(&content, terms, 240);
                let mut hit = json!({
                    "id": format!("msg:{mid}"),
                    "kind": "history",
                    "session_id": sid,
                    "role": role,
                    "snippet": snippet,
                    "created_at": ts,
                });
                if !title.is_empty() {
                    hit["session_title"] = json!(title);
                }
                out.push((1.0, ts, hit));
            }
        }
        return out;
    }

    // Two-stage FTS5 BM25 search (strict AND -> relaxed OR fallback).
    let (strict_fts, relaxed_fts) = build_fts_queries(terms);
    if sync_messages_fts(conn)
        && let Some(strict_q) = strict_fts
    {
        let fts_sql = if has_sessions_and_projects {
            match scope {
                "task_shared" => {
                    "SELECT m.id, m.session_id, COALESCE(s.title, ''), m.role, m.content, m.created_at,
                            bm25(agentos_messages_fts) AS rank_score
                     FROM agentos_messages_fts f
                     JOIN messages m ON m.rowid = f.rowid
                     LEFT JOIN sessions s ON s.id = m.session_id
                     WHERE agentos_messages_fts MATCH ?1
                       AND m.streaming = 0
                       AND m.session_id = ?2
                       AND (?3 != '' OR 1=1)
                     ORDER BY rank_score ASC, m.created_at DESC
                     LIMIT ?4"
                }
                "user" => {
                    "SELECT m.id, m.session_id, COALESCE(s.title, ''), m.role, m.content, m.created_at,
                            bm25(agentos_messages_fts) AS rank_score
                     FROM agentos_messages_fts f
                     JOIN messages m ON m.rowid = f.rowid
                     LEFT JOIN sessions s ON s.id = m.session_id
                     WHERE agentos_messages_fts MATCH ?1
                       AND m.streaming = 0
                       AND (?2 != '' OR 1=1)
                       AND (?3 != '' OR 1=1)
                     ORDER BY rank_score ASC, m.created_at DESC
                     LIMIT ?4"
                }
                _ => {
                    "SELECT m.id, m.session_id, COALESCE(s.title, ''), m.role, m.content, m.created_at,
                            bm25(agentos_messages_fts) AS rank_score
                     FROM agentos_messages_fts f
                     JOIN messages m ON m.rowid = f.rowid
                     LEFT JOIN sessions s ON s.id = m.session_id
                     LEFT JOIN projects p ON p.id = s.project_id
                     WHERE agentos_messages_fts MATCH ?1
                       AND m.streaming = 0
                       AND (p.path = ?3 OR m.session_id = ?2)
                     ORDER BY rank_score ASC, m.created_at DESC
                     LIMIT ?4"
                }
            }
        } else if scope == "task_shared" {
            "SELECT m.id, m.session_id, '', m.role, m.content, m.created_at,
                    bm25(agentos_messages_fts) AS rank_score
             FROM agentos_messages_fts f
             JOIN messages m ON m.rowid = f.rowid
             WHERE agentos_messages_fts MATCH ?1
               AND m.streaming = 0
               AND m.session_id = ?2
               AND (?3 != '' OR 1=1)
             ORDER BY rank_score ASC, m.created_at DESC
             LIMIT ?4"
        } else {
            "SELECT m.id, m.session_id, '', m.role, m.content, m.created_at,
                    bm25(agentos_messages_fts) AS rank_score
             FROM agentos_messages_fts f
             JOIN messages m ON m.rowid = f.rowid
             WHERE agentos_messages_fts MATCH ?1
               AND m.streaming = 0
               AND (?2 != '' OR 1=1)
               AND (?3 != '' OR 1=1)
             ORDER BY rank_score ASC, m.created_at DESC
             LIMIT ?4"
        };

        let fetch_limit = ((limit * 4).max(40)).min(200) as i64;
        let run_stage = |fts_query: &str,
                         stage_bonus: f64,
                         seen: &mut std::collections::HashSet<String>,
                         acc: &mut Vec<(f64, i64, Value)>| {
            if let Ok(mut stmt) = conn.prepare(fts_sql)
                && let Ok(rows) = stmt.query_map(
                    params![fts_query, ctx.session_id, project_path, fetch_limit],
                    |r| {
                        Ok((
                            r.get::<_, String>(0)?,
                            r.get::<_, String>(1)?,
                            r.get::<_, String>(2)?,
                            r.get::<_, String>(3)?,
                            r.get::<_, String>(4)?,
                            r.get::<_, i64>(5)?,
                            r.get::<_, f64>(6)?,
                        ))
                    },
                )
            {
                for (mid, sid, title, role, content, ts, bm25_val) in rows.flatten() {
                    if is_memory_search_echo(&content) || !seen.insert(mid.clone()) {
                        continue;
                    }
                    let bm25_boost = (-bm25_val).clamp(0.0, 20.0) * 0.25;
                    let term_score = score_text_terms(&content, terms);
                    let score = stage_bonus + bm25_boost + term_score;
                    let snippet = extract_match_snippet(&content, terms, 240);
                    let mut hit = json!({
                        "id": format!("msg:{mid}"),
                        "kind": "history",
                        "session_id": sid,
                        "role": role,
                        "snippet": snippet,
                        "created_at": ts,
                    });
                    if !title.is_empty() {
                        hit["session_title"] = json!(title);
                    }
                    acc.push((score, ts, hit));
                }
            }
        };

        run_stage(&strict_q, 8.0, &mut seen_ids, &mut out);
        if out.len() < limit
            && let Some(relaxed_q) = relaxed_fts
        {
            run_stage(&relaxed_q, 0.0, &mut seen_ids, &mut out);
        }
        return out;
    }

    // Fallback (e.g. read-only DB where FTS table cannot be created): bounded scan + multi-term scoring.
    let fallback_sql = if has_sessions_and_projects && scope == "project" {
        "SELECT m.id, m.session_id, COALESCE(s.title, ''), m.role, m.content, m.created_at
         FROM messages m
         LEFT JOIN sessions s ON s.id = m.session_id
         LEFT JOIN projects p ON p.id = s.project_id
         WHERE m.streaming = 0 AND (p.path = ?2 OR m.session_id = ?1)
         ORDER BY m.created_at DESC LIMIT 2000"
    } else if scope == "task_shared" {
        "SELECT id, session_id, '', role, content, created_at FROM messages
         WHERE streaming = 0 AND session_id = ?1 AND (?2 != '' OR 1=1)
         ORDER BY position DESC LIMIT 2000"
    } else {
        "SELECT id, session_id, '', role, content, created_at FROM messages
         WHERE streaming = 0 AND (?1 != '' OR 1=1) AND (?2 != '' OR 1=1)
         ORDER BY created_at DESC LIMIT 2000"
    };
    if let Ok(mut stmt) = conn.prepare(fallback_sql)
        && let Ok(rows) = stmt.query_map(params![ctx.session_id, project_path], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, String>(2)?,
                r.get::<_, String>(3)?,
                r.get::<_, String>(4)?,
                r.get::<_, i64>(5)?,
            ))
        })
    {
        for (mid, sid, title, role, content, ts) in rows.flatten() {
            if is_memory_search_echo(&content) || !seen_ids.insert(mid.clone()) {
                continue;
            }
            let score = score_text_terms(&content, terms);
            if score <= 0.0 {
                continue;
            }
            let snippet = extract_match_snippet(&content, terms, 240);
            let mut hit = json!({
                "id": format!("msg:{mid}"),
                "kind": "history",
                "session_id": sid,
                "role": role,
                "snippet": snippet,
                "created_at": ts,
            });
            if !title.is_empty() {
                hit["session_title"] = json!(title);
            }
            out.push((score, ts, hit));
        }
    }
    out
}

fn mcp_tool_definitions_for(settings: &AgentOsMcpSettings) -> Vec<Value> {
    let mut tools = Vec::new();
    if settings.memory_enabled {
        tools.extend([
            json!({
                "name": "memory_search",
                "description": "Search persistent cross-session memory across this workspace (past sessions, device/network/SSH/proxy/env configs, prior fixes, working notes, checkpoints, and project knowledge). CALL THIS FIRST (default scope=\"project\") whenever the user refers to past behavior or prior setup ('before', 'last time', 'still connected', 'again', '以前', '上次', '之前', '还在吗', '连过', '配过'), or when live checks contradict expectations or involve environment/device/config troubleshooting. Use 1-3 focused keywords (e.g. 'AX3000T 192.168.1.1' or 'adb 无线') rather than long multi-topic strings; pass returned ids to memory_read for full text.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "query": {
                            "type": "string",
                            "description": "Focused keyword query (1-3 terms work best; space-separated terms are ranked by relevance with partial-match fallback). Empty string returns most recent items."
                        },
                        "scope": {
                            "type": "string",
                            "enum": ["project", "task_shared", "user"],
                            "description": "project (default): history, notes, checkpoints, and knowledge across all sessions in this project. task_shared: only the current session. user: cross-project history and user-level knowledge."
                        },
                        "kinds": {
                            "type": "array",
                            "items": { "type": "string", "enum": ["history", "note", "checkpoint", "knowledge", "artifact"] },
                            "description": "Restrict to these memory kinds; omit to search all."
                        },
                        "limit": {
                            "type": "integer",
                            "description": "Max hits (default 20, max 100)."
                        },
                        "toolSummary": { "type": "string" }
                    },
                    "required": ["toolSummary"]
                }
            }),
            json!({
                "name": "memory_read",
                "description": "Read full memory items returned by memory_search.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "ids": { "type": "array", "items": { "type": "string" } },
                        "max_chars": { "type": "number" },
                        "toolSummary": { "type": "string" }
                    },
                    "required": ["ids", "toolSummary"]
                }
            }),
            json!({
                "name": "memory_note_write",
                "description": "Write or update a versioned working note for this task.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "note_id": { "type": "string" },
                        "body": { "type": "string" },
                        "expected_revision": { "type": "integer" },
                        "scope": { "type": "string", "enum": ["task_shared", "agent_private"] },
                        "source_refs": { "type": "array", "items": { "type": "string" } },
                        "toolSummary": { "type": "string" }
                    },
                    "required": ["note_id", "body", "toolSummary"]
                }
            }),
            json!({
                "name": "memory_checkpoint_prepare",
                "description": "Persist a recovery checkpoint for this task.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "completed": { "type": "array", "items": { "type": "string" } },
                        "in_progress": { "type": "array", "items": { "type": "string" } },
                        "next_actions": { "type": "array", "items": { "type": "string" } },
                        "open_questions": { "type": "array", "items": { "type": "string" } },
                        "constraints": { "type": "array", "items": { "type": "string" } },
                        "rejected_approaches": { "type": "array", "items": { "type": "string" } },
                        "toolSummary": { "type": "string" }
                    },
                    "required": ["toolSummary"]
                }
            }),
            json!({
                "name": "memory_context_rotate",
                "description": "Schedule a clean context rotation bound to a recovery checkpoint.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "checkpoint_id": { "type": "string" },
                        "reason": { "type": "string" },
                        "toolSummary": { "type": "string" }
                    },
                    "required": ["toolSummary"]
                }
            }),
            json!({
                "name": "memory_knowledge_propose",
                "description": "Propose a durable project-level or user-level lesson.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "claim": { "type": "string" },
                        "scope": { "type": "string", "enum": ["project", "user", "task_shared"] },
                        "applicability": {
                            "type": "string",
                            "description": "Optional JSON object string specifying applicability rules, e.g. {\"paths\":[\"src/**\"]}."
                        },
                        "evidence": { "type": "array", "items": { "type": "string" } },
                        "toolSummary": { "type": "string" }
                    },
                    "required": ["claim", "toolSummary"]
                }
            }),
        ]);
    }
    if settings.goal_enabled {
        tools.extend([
            json!({
                "name": "get_goal",
                "description": "Read the current persistent goal and its runId/turnId for this session.",
                "inputSchema": {
                    "type": "object",
                    "properties": { "toolSummary": { "type": "string" } },
                    "required": ["toolSummary"]
                }
            }),
            json!({
                "name": "create_goal",
                "description": "Create a persistent goal for this session.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "objective": { "type": "string" },
                        "maxTurns": { "type": "integer" },
                        "maxSeconds": { "type": "integer" },
                        "toolSummary": { "type": "string" }
                    },
                    "required": ["objective", "toolSummary"]
                }
            }),
            json!({
                "name": "report_goal",
                "description": "Report this goal turn outcome (complete, continue, or blocked).",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "runId": { "type": "string" },
                        "turnId": { "type": "string" },
                        "status": { "type": "string", "enum": ["complete", "continue", "blocked"] },
                        "reason": { "type": "string" },
                        "toolSummary": { "type": "string" }
                    },
                    "required": ["runId", "turnId", "status", "reason", "toolSummary"]
                }
            }),
        ]);
    }
    if settings.session_info_enabled {
        tools.push(json!({
            "name": "get_session_info",
            "description": "Look up a past session mentioned in the user's prompt (e.g. [Title](agentos://session/<uuid>) or [Title](codeg://session/<id>)) by UUID, agentos://session/<uuid> link, or legacy numeric codeg session id, returning its title, workspace folder, provider/model, message count, and recent transcript.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "session_id": {
                        "type": "string",
                        "description": "Session UUID, agentos://session/<uuid> URL, or legacy numeric codeg conversation id."
                    },
                    "max_messages": { "type": "integer" },
                    "toolSummary": { "type": "string" }
                },
                "required": ["session_id", "toolSummary"]
            }
        }));
    }
    if settings.ask_user_enabled {
        tools.push(json!({
            "name": "ask_user_question",
            "description": "Ask the user 1 to 4 structured multiple-choice questions and block until answered.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "questions": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "header": { "type": "string" },
                                "question": { "type": "string" },
                                "multiSelect": { "type": "boolean" },
                                "options": {
                                    "type": "array",
                                    "items": {
                                        "type": "object",
                                        "properties": {
                                            "label": { "type": "string" },
                                            "description": { "type": "string" }
                                        },
                                        "required": ["label", "description"]
                                    }
                                }
                            },
                            "required": ["header", "question", "multiSelect", "options"]
                        }
                    },
                    "toolSummary": { "type": "string" }
                },
                "required": ["questions", "toolSummary"]
            }
        }));
    }
    if settings.advisor_enabled {
        tools.push(json!({
            "name": "consult_advisor",
            "description": "Consult a read-only senior advisor agent (defaulting to heterogeneous Codex gpt-6.2-sol <-> Antigravity claude-opus-5-5) for an independent second opinion on complex architecture, subtle multi-file bugs, repeated failures, or risky changes, AS WELL AS creative direction, visual/aesthetic critique (inspecting local contact sheets, screenshots, UI previews, or storyboards), and non-code planning. SYNCHRONOUS & STRICTLY READ-ONLY: the advisor can read/search local files and view local images directly, and cannot browse the web, edit files, run mutating commands, or talk to the user.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "question": {
                        "type": "string",
                        "description": "The specific architectural trade-off, debugging hypothesis, code-review question, or creative/aesthetic/planning critique to ask the advisor."
                    },
                    "context": {
                        "type": "string",
                        "description": "Optional concise background: relevant file or local image/contact-sheet paths, user preferences/aesthetic criteria, approaches already tried, or short error snippets."
                    },
                    "files": { "type": "array", "items": { "type": "string" } },
                    "advisor_provider": { "type": "string", "enum": ["auto", "codex", "antigravity"] },
                    "model": { "type": "string" },
                    "toolSummary": { "type": "string" }
                },
                "required": ["question", "toolSummary"]
            }
        }));
    }
    if settings.delegation_enabled {
        tools.extend([
            json!({
                "name": "delegate_to_agent",
                "description": "Delegate a scoped subtask to an isolated Codex or Antigravity child worker.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "provider": { "type": "string", "enum": ["auto", "codex", "antigravity"] },
                        "task": { "type": "string" },
                        "read_only": { "type": "boolean" },
                        "wait": { "type": "boolean" },
                        "model": { "type": "string" },
                        "timeout_seconds": { "type": "integer" },
                        "toolSummary": { "type": "string" }
                    },
                    "required": ["provider", "task", "toolSummary"]
                }
            }),
            json!({
                "name": "get_delegation_status",
                "description": "Inspect status and result of a delegated child worker.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "delegation_id": { "type": "string" },
                        "toolSummary": { "type": "string" }
                    },
                    "required": ["delegation_id", "toolSummary"]
                }
            }),
            json!({
                "name": "cancel_delegation",
                "description": "Cancel a running delegated child worker.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "delegation_id": { "type": "string" },
                        "toolSummary": { "type": "string" }
                    },
                    "required": ["delegation_id", "toolSummary"]
                }
            }),
        ]);
    }
    tools
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mcp_stdio_round_trips_memory_notes_search_checkpoints_context_rotate_and_goals() {
        let dir = std::env::temp_dir().join(format!("waku-mcp-test-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let db_path = dir.join("app.db");
        let conn = open_mcp_connection(&db_path).unwrap();
        let ctx = McpContext {
            session_id: Uuid::new_v4().to_string(),
            db_path: db_path.clone(),
            cwd: dir.clone(),
            caller_provider: Some(ProviderKind::Codex),
            settings_path: Some(dir.join("settings.json")),
        };

        // 1. initialize + tools/list
        let init = dispatch_mcp_request(
            &conn,
            &ctx,
            &json!({"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {}}),
        )
        .unwrap();
        assert_eq!(init["result"]["serverInfo"]["name"], "agentos-mcp");

        let list = dispatch_mcp_request(
            &conn,
            &ctx,
            &json!({"jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": {}}),
        )
        .unwrap();
        // Default features: memory (6) + goal (3) + session_info (1) + ask_user_question (1) + consult_advisor (1) = 12
        assert_eq!(list["result"]["tools"].as_array().unwrap().len(), 12);

        // 2. memory_note_write + revision conflict check + memory_search + memory_read
        let write1 = dispatch_mcp_request(
            &conn,
            &ctx,
            &json!({
                "jsonrpc": "2.0",
                "id": 3,
                "method": "tools/call",
                "params": {
                    "name": "memory_note_write",
                    "arguments": {
                        "note_id": "plan",
                        "body": "Switch foundation to pure Rust Waku",
                        "toolSummary": "Save plan note"
                    }
                }
            }),
        )
        .unwrap();
        let note_id_ref = write1["result"]["structuredContent"]["id"]
            .as_str()
            .unwrap()
            .to_owned();
        assert_eq!(write1["result"]["structuredContent"]["revision"], 1);

        let conflict = dispatch_mcp_request(
            &conn,
            &ctx,
            &json!({
                "jsonrpc": "2.0",
                "id": 4,
                "method": "tools/call",
                "params": {
                    "name": "memory_note_write",
                    "arguments": {
                        "note_id": "plan",
                        "body": "stale write",
                        "expected_revision": 99,
                        "toolSummary": "Conflict check"
                    }
                }
            }),
        )
        .unwrap();
        assert_eq!(conflict["result"]["isError"], true);

        let search = dispatch_mcp_request(
            &conn,
            &ctx,
            &json!({
                "jsonrpc": "2.0",
                "id": 5,
                "method": "tools/call",
                "params": {
                    "name": "memory_search",
                    "arguments": {
                        "query": "Waku",
                        "toolSummary": "Search memory"
                    }
                }
            }),
        )
        .unwrap();
        assert_eq!(
            search["result"]["structuredContent"]["hits"]
                .as_array()
                .unwrap()
                .len(),
            1
        );

        let read = dispatch_mcp_request(
            &conn,
            &ctx,
            &json!({
                "jsonrpc": "2.0",
                "id": 6,
                "method": "tools/call",
                "params": {
                    "name": "memory_read",
                    "arguments": {
                        "ids": [note_id_ref],
                        "toolSummary": "Read note"
                    }
                }
            }),
        )
        .unwrap();
        assert_eq!(
            read["result"]["structuredContent"]["items"][0]["text"],
            "Switch foundation to pure Rust Waku"
        );

        // 3. checkpoint + context rotate
        let ckpt = dispatch_mcp_request(
            &conn,
            &ctx,
            &json!({
                "jsonrpc": "2.0",
                "id": 7,
                "method": "tools/call",
                "params": {
                    "name": "memory_checkpoint_prepare",
                    "arguments": {
                        "completed": ["P1", "P2"],
                        "next_actions": ["P3"],
                        "toolSummary": "Prepare checkpoint"
                    }
                }
            }),
        )
        .unwrap();
        let cid = ckpt["result"]["structuredContent"]["checkpoint_id"]
            .as_str()
            .unwrap()
            .to_owned();

        let rot = dispatch_mcp_request(
            &conn,
            &ctx,
            &json!({
                "jsonrpc": "2.0",
                "id": 8,
                "method": "tools/call",
                "params": {
                    "name": "memory_context_rotate",
                    "arguments": {
                        "checkpoint_id": cid,
                        "reason": "context window rotation",
                        "toolSummary": "Rotate context"
                    }
                }
            }),
        )
        .unwrap();
        assert_eq!(rot["result"]["structuredContent"]["scheduled"], true);

        // 4. create_goal -> get_goal -> report_goal
        let created = dispatch_mcp_request(
            &conn,
            &ctx,
            &json!({
                "jsonrpc": "2.0",
                "id": 9,
                "method": "tools/call",
                "params": {
                    "name": "create_goal",
                    "arguments": {
                        "objective": "Deliver P1-P4 cleanly",
                        "maxTurns": 10,
                        "toolSummary": "Create goal"
                    }
                }
            }),
        )
        .unwrap();
        let run_id = created["result"]["structuredContent"]["runId"]
            .as_str()
            .unwrap()
            .to_owned();
        let turn_id = created["result"]["structuredContent"]["turnId"]
            .as_str()
            .unwrap()
            .to_owned();

        let got = dispatch_mcp_request(
            &conn,
            &ctx,
            &json!({
                "jsonrpc": "2.0",
                "id": 10,
                "method": "tools/call",
                "params": {
                    "name": "get_goal",
                    "arguments": { "toolSummary": "Get goal" }
                }
            }),
        )
        .unwrap();
        assert_eq!(got["result"]["structuredContent"]["hasGoal"], true);
        assert_eq!(got["result"]["structuredContent"]["runId"], run_id);

        let reported = dispatch_mcp_request(
            &conn,
            &ctx,
            &json!({
                "jsonrpc": "2.0",
                "id": 11,
                "method": "tools/call",
                "params": {
                    "name": "report_goal",
                    "arguments": {
                        "runId": run_id,
                        "turnId": turn_id,
                        "status": "complete",
                        "reason": "All stages verified",
                        "toolSummary": "Report goal"
                    }
                }
            }),
        )
        .unwrap();
        assert_eq!(reported["result"]["structuredContent"]["status"], "complete");

        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn goal_enforces_max_turns_and_max_seconds_limits() {
        let dir = std::env::temp_dir().join(format!("waku-mcp-goal-budget-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let db = dir.join("app.db");
        let conn = open_mcp_connection(&db).unwrap();
        let ctx = McpContext {
            session_id: Uuid::new_v4().to_string(),
            db_path: db,
            cwd: dir.clone(),
            caller_provider: Some(ProviderKind::Codex),
            settings_path: Some(dir.join("settings.json")),
        };

        // 1. maxTurns = 1 must refuse continue and stay at turnIndex = 1 (blocked).
        let created = dispatch_mcp_request(
            &conn,
            &ctx,
            &json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "tools/call",
                "params": {
                    "name": "create_goal",
                    "arguments": {
                        "objective": "One-turn goal",
                        "maxTurns": 1,
                        "toolSummary": "Create 1-turn goal"
                    }
                }
            }),
        )
        .unwrap();
        let run_id = created["result"]["structuredContent"]["runId"]
            .as_str()
            .unwrap()
            .to_owned();
        let turn_id = created["result"]["structuredContent"]["turnId"]
            .as_str()
            .unwrap()
            .to_owned();

        let cont = dispatch_mcp_request(
            &conn,
            &ctx,
            &json!({
                "jsonrpc": "2.0",
                "id": 2,
                "method": "tools/call",
                "params": {
                    "name": "report_goal",
                    "arguments": {
                        "runId": run_id,
                        "turnId": turn_id,
                        "status": "continue",
                        "reason": "try exceeding maxTurns=1",
                        "toolSummary": "Report continue at maxTurns limit"
                    }
                }
            }),
        )
        .unwrap();
        assert_eq!(cont["result"]["structuredContent"]["status"], "blocked");
        assert_eq!(cont["result"]["structuredContent"]["budgetExhausted"], true);
        assert_eq!(cont["result"]["structuredContent"]["turnIndex"], 1);

        let got = dispatch_mcp_request(
            &conn,
            &ctx,
            &json!({
                "jsonrpc": "2.0",
                "id": 3,
                "method": "tools/call",
                "params": {
                    "name": "get_goal",
                    "arguments": { "toolSummary": "Read blocked goal" }
                }
            }),
        )
        .unwrap();
        assert_eq!(got["result"]["structuredContent"]["status"], "blocked");
        assert_eq!(got["result"]["structuredContent"]["turnIndex"], 1);

        // 2. maxTurns = 2 allows turn 1 -> 2 and blocks turn 2 -> 3.
        let ctx2 = McpContext {
            session_id: Uuid::new_v4().to_string(),
            db_path: ctx.db_path.clone(),
            cwd: dir.clone(),
            caller_provider: Some(ProviderKind::Codex),
            settings_path: Some(dir.join("settings.json")),
        };
        let created2 = dispatch_mcp_request(
            &conn,
            &ctx2,
            &json!({
                "jsonrpc": "2.0",
                "id": 4,
                "method": "tools/call",
                "params": {
                    "name": "create_goal",
                    "arguments": {
                        "objective": "Two-turn goal",
                        "maxTurns": 2,
                        "toolSummary": "Create 2-turn goal"
                    }
                }
            }),
        )
        .unwrap();
        let run2 = created2["result"]["structuredContent"]["runId"]
            .as_str()
            .unwrap()
            .to_owned();
        let turn2_1 = created2["result"]["structuredContent"]["turnId"]
            .as_str()
            .unwrap()
            .to_owned();

        let step1 = dispatch_mcp_request(
            &conn,
            &ctx2,
            &json!({
                "jsonrpc": "2.0",
                "id": 5,
                "method": "tools/call",
                "params": {
                    "name": "report_goal",
                    "arguments": {
                        "runId": run2,
                        "turnId": turn2_1,
                        "status": "continue",
                        "reason": "advance to turn 2",
                        "toolSummary": "Continue to turn 2"
                    }
                }
            }),
        )
        .unwrap();
        assert_eq!(step1["result"]["structuredContent"]["status"], "continue");
        assert_eq!(step1["result"]["structuredContent"]["budgetExhausted"], false);
        assert_eq!(step1["result"]["structuredContent"]["turnIndex"], 2);
        let turn2_2 = step1["result"]["structuredContent"]["nextTurnId"]
            .as_str()
            .unwrap()
            .to_owned();

        let step2 = dispatch_mcp_request(
            &conn,
            &ctx2,
            &json!({
                "jsonrpc": "2.0",
                "id": 6,
                "method": "tools/call",
                "params": {
                    "name": "report_goal",
                    "arguments": {
                        "runId": run2,
                        "turnId": turn2_2,
                        "status": "continue",
                        "reason": "try turn 3",
                        "toolSummary": "Try turn 3"
                    }
                }
            }),
        )
        .unwrap();
        assert_eq!(step2["result"]["structuredContent"]["status"], "blocked");
        assert_eq!(step2["result"]["structuredContent"]["budgetExhausted"], true);
        assert_eq!(step2["result"]["structuredContent"]["turnIndex"], 2);

        // 3. maxSeconds exhaustion also blocks continue.
        let ctx3 = McpContext {
            session_id: Uuid::new_v4().to_string(),
            db_path: ctx.db_path.clone(),
            cwd: dir.clone(),
            caller_provider: Some(ProviderKind::Codex),
            settings_path: Some(dir.join("settings.json")),
        };
        let created3 = dispatch_mcp_request(
            &conn,
            &ctx3,
            &json!({
                "jsonrpc": "2.0",
                "id": 7,
                "method": "tools/call",
                "params": {
                    "name": "create_goal",
                    "arguments": {
                        "objective": "Timed goal",
                        "maxSeconds": 5,
                        "toolSummary": "Create timed goal"
                    }
                }
            }),
        )
        .unwrap();
        let run3 = created3["result"]["structuredContent"]["runId"]
            .as_str()
            .unwrap()
            .to_owned();
        let turn3 = created3["result"]["structuredContent"]["turnId"]
            .as_str()
            .unwrap()
            .to_owned();
        conn.execute(
            "UPDATE agentos_goals SET created_at = created_at - 10 WHERE run_id = ?1",
            params![run3],
        )
        .unwrap();
        let expired = dispatch_mcp_request(
            &conn,
            &ctx3,
            &json!({
                "jsonrpc": "2.0",
                "id": 8,
                "method": "tools/call",
                "params": {
                    "name": "report_goal",
                    "arguments": {
                        "runId": run3,
                        "turnId": turn3,
                        "status": "continue",
                        "reason": "try after expiry",
                        "toolSummary": "Continue after maxSeconds"
                    }
                }
            }),
        )
        .unwrap();
        assert_eq!(expired["result"]["structuredContent"]["status"], "blocked");
        assert_eq!(expired["result"]["structuredContent"]["budgetExhausted"], true);
        assert_eq!(expired["result"]["structuredContent"]["turnIndex"], 1);

        let _ = std::fs::remove_dir_all(dir);
    }

    fn assert_gemini_compatible_schema(tool_name: &str, path: &str, schema: &Value) {
        let obj = schema
            .as_object()
            .unwrap_or_else(|| panic!("{tool_name}:{path} must be a JSON object"));
        assert!(
            !obj.contains_key("anyOf") && !obj.contains_key("oneOf"),
            "{tool_name}:{path} uses unsupported anyOf/oneOf for Gemini"
        );
        let ty = obj
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or_else(|| panic!("{tool_name}:{path} missing explicit `type`"));
        assert!(!ty.is_empty(), "{tool_name}:{path} empty `type`");
        if ty == "object" {
            let props = obj
                .get("properties")
                .and_then(Value::as_object)
                .unwrap_or_else(|| panic!("{tool_name}:{path} object schema missing `properties`"));
            assert!(
                !props.is_empty(),
                "{tool_name}:{path} object schema must declare non-empty `properties`"
            );
            for (k, v) in props {
                assert_gemini_compatible_schema(tool_name, &format!("{path}.{k}"), v);
            }
        } else if ty == "array" {
            let items = obj
                .get("items")
                .unwrap_or_else(|| panic!("{tool_name}:{path} array schema missing `items`"));
            assert_gemini_compatible_schema(tool_name, &format!("{path}[]"), items);
        }
    }

    #[test]
    fn all_mcp_tool_schemas_are_gemini_compatible() {
        let all_on = AgentOsMcpSettings {
            memory_enabled: true,
            goal_enabled: true,
            advisor_enabled: true,
            advisor_provider: "auto".into(),
            ask_user_enabled: true,
            delegation_enabled: true,
            session_info_enabled: true,
        };
        let tools = mcp_tool_definitions_for(&all_on);
        assert_eq!(tools.len(), 15);
        for tool in &tools {
            let name = tool["name"].as_str().unwrap();
            assert_gemini_compatible_schema(name, "inputSchema", &tool["inputSchema"]);
        }
    }

    #[test]
    fn feature_flags_gate_tools_list_and_block_disabled_calls() {
        let dir = std::env::temp_dir().join(format!("waku-mcp-gates-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let db = dir.join("app.db");
        let settings_path = dir.join("settings.json");
        let mut daemon_settings = DaemonSettings::default();
        daemon_settings.agentos_mcp.advisor_enabled = false;
        daemon_settings.agentos_mcp.delegation_enabled = false;
        daemon_settings.agentos_mcp.ask_user_enabled = false;
        std::fs::write(
            &settings_path,
            serde_json::to_vec(&daemon_settings).unwrap(),
        )
        .unwrap();

        let conn = open_mcp_connection(&db).unwrap();
        let ctx = McpContext {
            session_id: Uuid::new_v4().to_string(),
            db_path: db,
            cwd: dir.clone(),
            caller_provider: Some(ProviderKind::Antigravity),
            settings_path: Some(settings_path),
        };
        let list = dispatch_mcp_request(
            &conn,
            &ctx,
            &json!({"jsonrpc": "2.0", "id": 1, "method": "tools/list", "params": {}}),
        )
        .unwrap();
        let names: Vec<&str> = list["result"]["tools"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|t| t["name"].as_str())
            .collect();
        assert!(!names.contains(&"consult_advisor"));
        assert!(!names.contains(&"ask_user_question"));
        assert!(!names.contains(&"delegate_to_agent"));
        assert!(names.contains(&"memory_search"));

        let blocked = dispatch_mcp_request(
            &conn,
            &ctx,
            &json!({
                "jsonrpc": "2.0",
                "id": 2,
                "method": "tools/call",
                "params": {
                    "name": "consult_advisor",
                    "arguments": { "question": "ping", "toolSummary": "test" }
                }
            }),
        )
        .unwrap();
        assert_eq!(blocked["result"]["isError"], true);
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn ask_user_question_and_host_goal_rotate_loop_round_trip() {
        let dir = std::env::temp_dir().join(format!("waku-mcp-host-loop-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let db = dir.join("app.db");
        let sid = Uuid::new_v4();
        let conn = open_mcp_connection(&db).unwrap();
        let ctx = McpContext {
            session_id: sid.to_string(),
            db_path: db.clone(),
            cwd: dir.clone(),
            caller_provider: Some(ProviderKind::Antigravity),
            settings_path: Some(dir.join("settings.json")),
        };

        // 1. Host /goal Set + Refresh + agent report_goal(continue) -> take_pending_goal_continuation
        let set_goal = apply_host_goal_operation(
            &db,
            sid,
            GoalOperation::Set {
                objective: Some("Ship Mac daily driver".into()),
                status: Some(ThreadGoalStatus::Active),
                replace: true,
            },
        )
        .unwrap()
        .unwrap();
        assert_eq!(set_goal.objective, "Ship Mac daily driver");
        let got = dispatch_mcp_request(
            &conn,
            &ctx,
            &json!({
                "jsonrpc": "2.0", "id": 1, "method": "tools/call",
                "params": { "name": "get_goal", "arguments": { "toolSummary": "get" } }
            }),
        )
        .unwrap();
        let run_id = got["result"]["structuredContent"]["runId"].as_str().unwrap().to_owned();
        let turn_id = got["result"]["structuredContent"]["turnId"].as_str().unwrap().to_owned();
        dispatch_mcp_request(
            &conn,
            &ctx,
            &json!({
                "jsonrpc": "2.0", "id": 2, "method": "tools/call",
                "params": {
                    "name": "report_goal",
                    "arguments": {
                        "runId": run_id,
                        "turnId": turn_id,
                        "status": "continue",
                        "reason": "run final verification",
                        "toolSummary": "continue"
                    }
                }
            }),
        )
        .unwrap();
        let (next_goal, next_prompt) = take_pending_goal_continuation(&db, sid).unwrap().unwrap();
        assert_eq!(next_goal.objective, "Ship Mac daily driver");
        assert!(next_prompt.contains("run final verification"));
        // Claiming a second time must return None (atomic dispatch).
        assert!(take_pending_goal_continuation(&db, sid).unwrap().is_none());

        // 2. Context rotation builds host restore package and consumes rotation once.
        dispatch_mcp_request(
            &conn,
            &ctx,
            &json!({
                "jsonrpc": "2.0", "id": 3, "method": "tools/call",
                "params": {
                    "name": "memory_note_write",
                    "arguments": { "note_id": "plan", "body": "Keep Mac snappy", "toolSummary": "note" }
                }
            }),
        )
        .unwrap();
        dispatch_mcp_request(
            &conn,
            &ctx,
            &json!({
                "jsonrpc": "2.0", "id": 4, "method": "tools/call",
                "params": {
                    "name": "memory_context_rotate",
                    "arguments": { "reason": "context full", "toolSummary": "rotate" }
                }
            }),
        )
        .unwrap();
        let pkg = take_pending_context_rotation(&db, sid).unwrap().unwrap();
        assert!(pkg.contains("<agent_context_rotation>"));
        assert!(pkg.contains("Keep Mac snappy"));
        assert!(take_pending_context_rotation(&db, sid).unwrap().is_none());

        // 3. ask_user_question blocks until host claims & resolves via SQLite.
        let db_bg = db.clone();
        let resolver = std::thread::spawn(move || {
            for _ in 0..40 {
                if let Ok(claimed) = claim_pending_questions(&db_bg, sid)
                    && let Some((qid, qs)) = claimed.into_iter().next()
                {
                    assert_eq!(qs.len(), 1);
                    assert_eq!(qs[0].header, "Mode");
                    resolve_pending_question(
                        &db_bg,
                        &qid,
                        &[UserInputAnswer {
                            question_id: qs[0].id.clone(),
                            answers: vec!["Mac".into()],
                        }],
                    )
                    .unwrap();
                    return;
                }
                std::thread::sleep(Duration::from_millis(50));
            }
            panic!("never saw pending question");
        });
        let ask_resp = dispatch_mcp_request(
            &conn,
            &ctx,
            &json!({
                "jsonrpc": "2.0", "id": 5, "method": "tools/call",
                "params": {
                    "name": "ask_user_question",
                    "arguments": {
                        "questions": [{
                            "header": "Mode",
                            "question": "Which platform first?",
                            "multiSelect": false,
                            "options": [
                                { "label": "Mac", "description": "Desktop first" },
                                { "label": "Web", "description": "Mobile first" }
                            ]
                        }],
                        "toolSummary": "Ask platform priority"
                    }
                }
            }),
        )
        .unwrap();
        resolver.join().unwrap();
        assert_eq!(ask_resp["result"]["structuredContent"]["answered"], true);
        assert_eq!(
            ask_resp["result"]["structuredContent"]["answers"][0]["answers"][0],
            "Mac"
        );

        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn memory_search_fts_bm25_relaxed_or_scope_snippets_and_instructions() {
        let dir = std::env::temp_dir().join(format!("waku-mcp-search-opt-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let db = dir.join("app.db");
        let conn = open_mcp_connection(&db).unwrap();

        // Set up Waku projects/sessions/messages schema.
        conn.execute_batch(
            "CREATE TABLE projects (
                id TEXT PRIMARY KEY, name TEXT NOT NULL, path TEXT NOT NULL,
                position INTEGER NOT NULL, created_at INTEGER NOT NULL
            );
            CREATE TABLE sessions (
                id TEXT PRIMARY KEY, project_id TEXT NOT NULL, title TEXT NOT NULL,
                provider TEXT NOT NULL, model TEXT, status TEXT NOT NULL,
                created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL, last_reply_at INTEGER
            );
            CREATE TABLE messages (
                id TEXT PRIMARY KEY, session_id TEXT NOT NULL, turn_id TEXT,
                position INTEGER NOT NULL, role TEXT NOT NULL, content TEXT NOT NULL,
                created_at INTEGER NOT NULL, streaming INTEGER NOT NULL
            );",
        )
        .unwrap();

        let proj_a = dir.to_string_lossy().to_string();
        let proj_b = "/tmp/other-project".to_string();
        conn.execute(
            "INSERT INTO projects(id, name, path, position, created_at) VALUES ('pa', 'Diag', ?1, 0, 100), ('pb', 'Other', ?2, 1, 100)",
            params![proj_a, proj_b],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO sessions(id, project_id, title, provider, status, created_at, updated_at)
             VALUES
               ('s-old', 'pa', '路由器与手机ADB排错', 'codex', 'idle', 100, 100),
               ('s-cur', 'pa', '手机ADB还在吗', 'antigravity', 'running', 200, 200),
               ('s-other', 'pb', '跨项目路由器笔记', 'claude', 'idle', 150, 150)",
            [],
        )
        .unwrap();

        let long_prefix = "无关背景日志 ".repeat(80);
        let router_msg = format!(
            "{long_prefix}AX3000T 路由器 (192.168.1.1) 通过 phy1-sta0 连在 UNNC-Living，OpenClash @localnetwork 放行 172.16.0.0/12，手机无线 ADB 172.19.115.104:5555 可直连。"
        );
        let rax_msg = "手机连接 RAX 2.4G 后拿到 192.168.1.6，无线 ADB 可用。";
        let echo_msg = r#"{"query":"192.168.1.6 192.168.1.10 RAX SK-D841N AX3000T","scope":"project","toolSummary":"Search project memory"}"#;
        let other_proj_msg = "另一个项目里提到的 AX3000T 192.168.1.1 OpenClash 备忘。";

        conn.execute(
            "INSERT INTO messages(id, session_id, position, role, content, created_at, streaming)
             VALUES
               ('m1', 's-old', 0, 'assistant', ?1, 101, 0),
               ('m2', 's-old', 1, 'assistant', ?2, 102, 0),
               ('m3', 's-cur', 0, 'assistant', ?3, 201, 0),
               ('m4', 's-other', 0, 'assistant', ?4, 151, 0)",
            params![router_msg, rax_msg, echo_msg, other_proj_msg],
        )
        .unwrap();

        let ctx = McpContext {
            session_id: "s-cur".to_string(),
            db_path: db.clone(),
            cwd: dir.clone(),
            caller_provider: Some(ProviderKind::Antigravity),
            settings_path: Some(dir.join("settings.json")),
        };

        // 1. initialize returns proactive memory instructions.
        let init = dispatch_mcp_request(
            &conn,
            &ctx,
            &json!({"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {}}),
        )
        .unwrap();
        let instructions = init["result"]["instructions"].as_str().unwrap_or("");
        assert!(instructions.contains("memory_search"));
        assert!(instructions.contains("以前"));

        // 2. Multi-keyword query with conflicting/extra terms falls back to relaxed OR,
        //    excludes self-echo m3, isolates project scope (excludes m4), and finds m1 & m2.
        let resp = dispatch_mcp_request(
            &conn,
            &ctx,
            &json!({
                "jsonrpc": "2.0", "id": 2, "method": "tools/call",
                "params": {
                    "name": "memory_search",
                    "arguments": {
                        "query": "192.168.1.6 192.168.1.10 RAX SK-D841N AX3000T",
                        "toolSummary": "Search router and RAX history"
                    }
                }
            }),
        )
        .unwrap();
        let hits = resp["result"]["structuredContent"]["hits"]
            .as_array()
            .unwrap();
        let ids: Vec<&str> = hits.iter().filter_map(|h| h["id"].as_str()).collect();
        assert!(ids.contains(&"msg:m1"), "should find AX3000T msg: {ids:?}");
        assert!(ids.contains(&"msg:m2"), "should find RAX 192.168.1.6 msg: {ids:?}");
        assert!(!ids.contains(&"msg:m3"), "must filter self-echo: {ids:?}");
        assert!(
            !ids.contains(&"msg:m4"),
            "default project scope must exclude other projects: {ids:?}"
        );
        // Match-centered snippet for m1 must show AX3000T instead of only the long prefix.
        let m1_hit = hits.iter().find(|h| h["id"] == "msg:m1").unwrap();
        let snippet = m1_hit["snippet"].as_str().unwrap();
        assert!(
            snippet.contains("AX3000T"),
            "snippet should center around match, got: {snippet}"
        );
        assert_eq!(m1_hit["session_title"], "路由器与手机ADB排错");

        // 3. CJK + ASCII mixed query hits m1 with default project scope.
        let cjk_resp = dispatch_mcp_request(
            &conn,
            &ctx,
            &json!({
                "jsonrpc": "2.0", "id": 3, "method": "tools/call",
                "params": {
                    "name": "memory_search",
                    "arguments": {
                        "query": "路由器 无线 ADB",
                        "toolSummary": "Search CJK + ASCII"
                    }
                }
            }),
        )
        .unwrap();
        let cjk_hits = cjk_resp["result"]["structuredContent"]["hits"]
            .as_array()
            .unwrap();
        assert_eq!(cjk_hits[0]["id"], "msg:m1");

        // 4. scope="user" includes cross-project hit m4.
        let user_resp = dispatch_mcp_request(
            &conn,
            &ctx,
            &json!({
                "jsonrpc": "2.0", "id": 4, "method": "tools/call",
                "params": {
                    "name": "memory_search",
                    "arguments": {
                        "query": "AX3000T OpenClash",
                        "scope": "user",
                        "toolSummary": "Search across projects"
                    }
                }
            }),
        )
        .unwrap();
        let user_ids: Vec<&str> = user_resp["result"]["structuredContent"]["hits"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|h| h["id"].as_str())
            .collect();
        assert!(user_ids.contains(&"msg:m1"));
        assert!(user_ids.contains(&"msg:m4"));

        // 5. scope="task_shared" stays strictly inside current session s-cur (and filters echo m3).
        let task_resp = dispatch_mcp_request(
            &conn,
            &ctx,
            &json!({
                "jsonrpc": "2.0", "id": 5, "method": "tools/call",
                "params": {
                    "name": "memory_search",
                    "arguments": {
                        "query": "AX3000T",
                        "scope": "task_shared",
                        "toolSummary": "Search current session only"
                    }
                }
            }),
        )
        .unwrap();
        assert!(
            task_resp["result"]["structuredContent"]["hits"]
                .as_array()
                .unwrap()
                .is_empty()
        );

        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn get_session_info_resolves_agentos_url_auto_title_and_workspace() {
        let dir = std::env::temp_dir().join(format!("agentos-sessinfo-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let db = dir.join("app.db");
        let conn = open_mcp_connection(&db).unwrap();
        ensure_mcp_tables(&conn).unwrap();
        conn.execute_batch(
            "CREATE TABLE projects (id TEXT PRIMARY KEY, name TEXT NOT NULL, path TEXT NOT NULL, position INTEGER NOT NULL, created_at INTEGER NOT NULL);
             CREATE TABLE sessions (id TEXT PRIMARY KEY, project_id TEXT NOT NULL, title TEXT NOT NULL, auto_title TEXT, provider TEXT NOT NULL, model TEXT, status TEXT NOT NULL, created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL, last_reply_at INTEGER);
             CREATE TABLE messages (id TEXT PRIMARY KEY, session_id TEXT NOT NULL, turn_id TEXT, position INTEGER NOT NULL, role TEXT NOT NULL, content TEXT NOT NULL, display_content TEXT, attachments TEXT, created_at INTEGER NOT NULL, streaming INTEGER NOT NULL, provider TEXT);",
        )
        .unwrap();
        let sid = Uuid::new_v4().to_string();
        conn.execute(
            "INSERT INTO projects VALUES ('p1', 'AgentOS', '/tmp/AgentOS', 0, 100)",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO sessions VALUES (?1, 'p1', 'New task', '修复 OAuth 登录弹窗', 'codex', 'gpt-6.2-sol', 'idle', 100, 200, 200)",
            params![sid],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO messages VALUES ('m1', ?1, 't1', 0, 'user', '为什么老是弹登录', NULL, '[]', 110, 0, NULL)",
            params![sid],
        )
        .unwrap();
        let ctx = McpContext {
            session_id: Uuid::new_v4().to_string(),
            db_path: db.clone(),
            cwd: PathBuf::from("/tmp/AgentOS"),
            caller_provider: Some(ProviderKind::Codex),
            settings_path: None,
        };
        let resp = dispatch_mcp_request(
            &conn,
            &ctx,
            &json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "tools/call",
                "params": {
                    "name": "get_session_info",
                    "arguments": {
                        "session_id": format!("agentos://session/{sid}"),
                        "toolSummary": "Inspect referenced session"
                    }
                }
            }),
        )
        .unwrap();
        let sc = &resp["result"]["structuredContent"];
        assert_eq!(sc["found"], true);
        assert_eq!(sc["title"], "修复 OAuth 登录弹窗");
        assert_eq!(sc["project_name"], "AgentOS");
        assert_eq!(sc["workspace_folder"], "/tmp/AgentOS");
        assert_eq!(sc["message_count"], 1);
        assert_eq!(sc["messages"].as_array().unwrap().len(), 1);
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn memory_library_load_promote_reject_and_delete_roundtrip() {
        let dir = std::env::temp_dir().join(format!("agentos-memlib-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let db = dir.join("app.db");
        let conn = open_mcp_connection(&db).unwrap();
        ensure_mcp_tables(&conn).unwrap();
        let sid = Uuid::new_v4().to_string();
        let ctx = McpContext {
            session_id: sid.clone(),
            db_path: db.clone(),
            cwd: PathBuf::from("/tmp/ProjectA"),
            caller_provider: Some(ProviderKind::Codex),
            settings_path: None,
        };
        let kn_resp = dispatch_mcp_request(
            &conn,
            &ctx,
            &json!({
                "jsonrpc": "2.0", "id": 1, "method": "tools/call",
                "params": {
                    "name": "memory_knowledge_propose",
                    "arguments": {
                        "claim": "Always run CNB stop after remote builds",
                        "scope": "project",
                        "toolSummary": "Propose knowledge"
                    }
                }
            }),
        )
        .unwrap();
        let kid = kn_resp["result"]["structuredContent"]["knowledge_id"]
            .as_str()
            .unwrap()
            .to_owned();
        dispatch_mcp_request(
            &conn,
            &ctx,
            &json!({
                "jsonrpc": "2.0", "id": 2, "method": "tools/call",
                "params": {
                    "name": "memory_note_write",
                    "arguments": {
                        "note_id": "plan",
                        "body": "Step 1 done",
                        "toolSummary": "Write note"
                    }
                }
            }),
        )
        .unwrap();
        let cp_resp = dispatch_mcp_request(
            &conn,
            &ctx,
            &json!({
                "jsonrpc": "2.0", "id": 3, "method": "tools/call",
                "params": {
                    "name": "memory_checkpoint_prepare",
                    "arguments": {
                        "completed": ["Built UI"],
                        "next_actions": ["Run tests"],
                        "toolSummary": "Prepare checkpoint"
                    }
                }
            }),
        )
        .unwrap();
        let cpid = cp_resp["result"]["structuredContent"]["checkpoint_id"]
            .as_str()
            .unwrap()
            .to_owned();

        let snap = load_memory_library(&db, Some("/tmp/ProjectA"), None).unwrap();
        assert_eq!(snap.knowledge.len(), 1);
        assert_eq!(snap.notes.len(), 1);
        assert_eq!(snap.checkpoints.len(), 1);

        update_memory_knowledge_status(&db, &kid, "rejected").unwrap();
        let search_after_reject = dispatch_mcp_request(
            &conn,
            &ctx,
            &json!({
                "jsonrpc": "2.0", "id": 4, "method": "tools/call",
                "params": {
                    "name": "memory_search",
                    "arguments": {
                        "query": "CNB stop",
                        "kinds": ["knowledge"],
                        "toolSummary": "Verify rejected knowledge hidden"
                    }
                }
            }),
        )
        .unwrap();
        assert!(
            search_after_reject["result"]["structuredContent"]["hits"]
                .as_array()
                .unwrap()
                .is_empty()
        );

        update_memory_knowledge_status(&db, &kid, "promoted").unwrap();
        let snap2 = load_memory_library(&db, Some("/tmp/ProjectA"), Some("CNB")).unwrap();
        assert_eq!(snap2.knowledge.len(), 1);
        assert_eq!(snap2.knowledge[0].status, "promoted");

        delete_memory_entry(&db, MemoryEntryKind::Knowledge, &kid, None, None).unwrap();
        delete_memory_entry(
            &db,
            MemoryEntryKind::Note,
            &sid,
            Some("plan"),
            Some("task_shared"),
        )
        .unwrap();
        delete_memory_entry(&db, MemoryEntryKind::Checkpoint, &cpid, None, None).unwrap();
        let snap3 = load_memory_library(&db, None, None).unwrap();
        assert!(snap3.knowledge.is_empty());
        assert!(snap3.notes.is_empty());
        assert!(snap3.checkpoints.is_empty());

        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn advisor_defaults_match_age180_creative_and_readonly_isolation() {
        assert_eq!(default_advisor_model(ProviderKind::Codex), Some("gpt-6.2-sol"));
        assert_eq!(
            default_advisor_model(ProviderKind::Antigravity),
            Some("claude-opus-5-5")
        );
        assert!(ADVISOR_SYSTEM_PROMPT.contains("creative/art director"));
        assert!(ADVISOR_SYSTEM_PROMPT.contains("local images/contact sheets"));

        let ro_args = build_codex_oneshot_args(Path::new("/tmp/ws"), Some("gpt-6.2-sol"), true);
        assert!(ro_args.iter().any(|a| a == "features.view_image=true"));
        assert!(ro_args.iter().any(|a| a == "include_apply_patch_tool=false"));
        assert!(ro_args.iter().any(|a| a == "web_search=\"disabled\""));
        assert!(ro_args.iter().any(|a| a == "features.multi_agent=false"));
        assert!(ro_args.iter().any(|a| a == "read-only"));
        assert!(ro_args.windows(2).any(|w| w == ["-m", "gpt-6.2-sol"]));
    }
}
