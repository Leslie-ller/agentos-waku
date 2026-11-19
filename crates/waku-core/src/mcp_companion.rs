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
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context as _, anyhow};
use rusqlite::{Connection, OptionalExtension as _, params};
use serde_json::{Value, json};
use uuid::Uuid;

pub const MCP_SERVER_NAME: &str = "agentos-mcp";
pub const MCP_BIN_ENV: &str = "AGENTOS_WAKU_MCP_BIN";
pub const DISABLE_MCP_ENV: &str = "AGENTOS_WAKU_DISABLE_MCP";

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
    if std::env::var(DISABLE_MCP_ENV)
        .ok()
        .is_some_and(|val| val.trim() == "1" || val.trim().eq_ignore_ascii_case("true"))
    {
        return None;
    }
    let executable = locate_mcp_executable()?;
    Some(McpCompanionLaunch {
        server_name: MCP_SERVER_NAME.to_owned(),
        executable,
        args: vec![
            "--mcp-stdio".into(),
            "--session-id".into(),
            session_id.to_string(),
            "--db".into(),
            db_path.to_string_lossy().into_owned(),
            "--cwd".into(),
            cwd.to_string_lossy().into_owned(),
            "--parent-pid".into(),
            std::process::id().to_string(),
        ],
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
        );",
    )?;
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
        "initialize" => Ok(json!({
            "protocolVersion": "2024-11-05",
            "capabilities": { "tools": {} },
            "serverInfo": {
                "name": MCP_SERVER_NAME,
                "version": env!("CARGO_PKG_VERSION"),
            }
        })),
        "ping" => Ok(json!({})),
        "tools/list" => Ok(json!({ "tools": mcp_tool_definitions() })),
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

fn call_mcp_tool(
    conn: &Connection,
    ctx: &McpContext,
    name: &str,
    args: &Value,
) -> anyhow::Result<Value> {
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
            let query = args
                .get("query")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .trim()
                .to_lowercase();
            let scope = args
                .get("scope")
                .and_then(Value::as_str)
                .unwrap_or("task_shared");
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

            let mut hits = Vec::new();
            let like_pattern = format!("%{query}%");

            if allow_kind("note") {
                let mut stmt = if scope == "task_shared" {
                    conn.prepare(
                        "SELECT session_id, scope, note_id, revision, body, updated_at
                         FROM agentos_memory_notes
                         WHERE session_id = ?1 AND (?2 = '' OR lower(note_id) LIKE ?3 OR lower(body) LIKE ?3)
                         ORDER BY updated_at DESC LIMIT ?4",
                    )?
                } else {
                    conn.prepare(
                        "SELECT session_id, scope, note_id, revision, body, updated_at
                         FROM agentos_memory_notes
                         WHERE (?1 = 'user' OR project_path = ?5) AND (?2 = '' OR lower(note_id) LIKE ?3 OR lower(body) LIKE ?3)
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
                    stmt.query_map(
                        params![ctx.session_id, query, like_pattern, limit as i64],
                        map_note_row,
                    )?
                    .flatten()
                    .collect()
                } else {
                    stmt.query_map(
                        params![scope, query, like_pattern, limit as i64, project_path],
                        map_note_row,
                    )?
                    .flatten()
                    .collect()
                };
                for row in collected {
                    let snippet: String = row.4.chars().take(240).collect();
                    hits.push(json!({
                        "id": format!("note:{}:{}:{}", row.0, row.1, row.2),
                        "kind": "note",
                        "note_id": row.2,
                        "revision": row.3,
                        "snippet": snippet,
                        "updated_at": row.5,
                    }));
                }
            }

            if allow_kind("checkpoint") && hits.len() < limit {
                let mut stmt = conn.prepare(
                    "SELECT checkpoint_id, payload_json, created_at
                     FROM agentos_memory_checkpoints
                     WHERE (?1 = 'project' AND project_path = ?2) OR session_id = ?3
                     ORDER BY created_at DESC LIMIT ?4",
                )?;
                let rows = stmt.query_map(
                    params![scope, project_path, ctx.session_id, (limit - hits.len()) as i64],
                    |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?, r.get::<_, i64>(2)?)),
                )?;
                for (cid, payload, ts) in rows.flatten() {
                    if query.is_empty() || payload.to_lowercase().contains(&query) {
                        let snippet: String = payload.chars().take(240).collect();
                        hits.push(json!({
                            "id": format!("ckpt:{cid}"),
                            "kind": "checkpoint",
                            "snippet": snippet,
                            "created_at": ts,
                        }));
                    }
                }
            }

            if allow_kind("knowledge") && hits.len() < limit {
                let mut stmt = conn.prepare(
                    "SELECT knowledge_id, claim, scope, status, created_at
                     FROM agentos_memory_knowledge
                     WHERE (?1 = 'user' OR project_path = ?2 OR scope = 'user')
                       AND (?3 = '' OR lower(claim) LIKE ?4)
                     ORDER BY created_at DESC LIMIT ?5",
                )?;
                let rows = stmt.query_map(
                    params![
                        scope,
                        project_path,
                        query,
                        like_pattern,
                        (limit - hits.len()) as i64
                    ],
                    |r| {
                        Ok((
                            r.get::<_, String>(0)?,
                            r.get::<_, String>(1)?,
                            r.get::<_, String>(2)?,
                            r.get::<_, String>(3)?,
                            r.get::<_, i64>(4)?,
                        ))
                    },
                )?;
                for (kid, claim, kscope, status, ts) in rows.flatten() {
                    hits.push(json!({
                        "id": format!("know:{kid}"),
                        "kind": "knowledge",
                        "scope": kscope,
                        "status": status,
                        "snippet": claim,
                        "created_at": ts,
                    }));
                }
            }

            if allow_kind("history") && hits.len() < limit {
                // Search Waku's own `messages` table if initialized in this DB.
                let has_messages: bool = conn
                    .query_row(
                        "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='messages')",
                        [],
                        |r| r.get(0),
                    )
                    .unwrap_or(false);
                if has_messages {
                    let sql = if scope == "task_shared" {
                        "SELECT id, session_id, role, content, created_at FROM messages
                         WHERE session_id = ?1 AND (?2 = '' OR lower(content) LIKE ?3)
                         ORDER BY position DESC LIMIT ?4"
                    } else {
                        "SELECT id, session_id, role, content, created_at FROM messages
                         WHERE (?1 != '' OR 1=1) AND (?2 = '' OR lower(content) LIKE ?3)
                         ORDER BY created_at DESC LIMIT ?4"
                    };
                    if let Ok(mut stmt) = conn.prepare(sql)
                        && let Ok(rows) = stmt.query_map(
                            params![
                                ctx.session_id,
                                query,
                                like_pattern,
                                (limit - hits.len()) as i64
                            ],
                            |r| {
                                Ok((
                                    r.get::<_, String>(0)?,
                                    r.get::<_, String>(1)?,
                                    r.get::<_, String>(2)?,
                                    r.get::<_, String>(3)?,
                                    r.get::<_, i64>(4)?,
                                ))
                            },
                        )
                    {
                        for (mid, sid, role, content, ts) in rows.flatten() {
                            let snippet: String = content.chars().take(240).collect();
                            hits.push(json!({
                                "id": format!("msg:{mid}"),
                                "kind": "history",
                                "session_id": sid,
                                "role": role,
                                "snippet": snippet,
                                "created_at": ts,
                            }));
                        }
                    }
                }
            }

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
                "INSERT INTO agentos_memory_rotations (rotation_id, session_id, checkpoint_id, reason, created_at)
                 VALUES (?1, ?2, ?3, ?4, ?5)",
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
            let applicability = serde_json::to_string(
                args.get("applicability")
                    .filter(|v| v.is_object())
                    .unwrap_or(&json!({})),
            )?;
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
            conn.execute(
                "UPDATE agentos_goals
                 SET status = ?1,
                     last_reason = ?2,
                     current_turn_id = ?3,
                     turn_index = ?4,
                     updated_at = ?5
                 WHERE run_id = ?6 AND current_turn_id = ?7",
                params![status, reason, next_turn_id, next_turn_index, now, run_id, turn_id],
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
            let resolved_sid = if let Some(num) = args.get("session_id").and_then(Value::as_i64) {
                conn.query_row(
                    "SELECT session_id FROM agentos_legacy_session_map WHERE legacy_id = ?1",
                    params![num],
                    |r| r.get::<_, String>(0),
                )
                .optional()?
                .unwrap_or_else(|| format!("codeg-conv-{num}"))
            } else if let Some(text) = args.get("session_id").and_then(Value::as_str) {
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
            let row = conn
                .query_row(
                    "SELECT title, provider, model, status FROM sessions WHERE id = ?1",
                    params![resolved_sid],
                    |r| {
                        Ok((
                            r.get::<_, String>(0)?,
                            r.get::<_, String>(1)?,
                            r.get::<_, Option<String>>(2)?,
                            r.get::<_, String>(3)?,
                        ))
                    },
                )
                .optional()?;
            let Some((title, provider, model, status)) = row else {
                return Ok(json!({ "found": false, "session_id": resolved_sid }));
            };

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
                    let compact: String = content.chars().take(600).collect();
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
                "messages": recent,
            }))
        }

        other => anyhow::bail!("unknown MCP tool: {other}"),
    }
}

fn mcp_tool_definitions() -> Vec<Value> {
    vec![
        json!({
            "name": "memory_search",
            "description": "Search task notes, checkpoints, project knowledge, and session history.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "query": { "type": "string" },
                    "scope": { "type": "string", "enum": ["task_shared", "project", "user"] },
                    "kinds": { "type": "array", "items": { "type": "string", "enum": ["history", "note", "checkpoint", "knowledge", "artifact"] } },
                    "limit": { "type": "integer" },
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
                    "applicability": { "type": "object" },
                    "evidence": { "type": "array", "items": { "type": "string" } },
                    "toolSummary": { "type": "string" }
                },
                "required": ["claim", "toolSummary"]
            }
        }),
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
        json!({
            "name": "get_session_info",
            "description": "Look up a session by UUID or legacy codeg session number.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "session_id": {},
                    "max_messages": { "type": "integer" },
                    "toolSummary": { "type": "string" }
                },
                "required": ["session_id", "toolSummary"]
            }
        }),
    ]
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
        assert_eq!(list["result"]["tools"].as_array().unwrap().len(), 10);

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
}
