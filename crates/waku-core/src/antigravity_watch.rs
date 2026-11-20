//! Antigravity SQLite trajectory watcher (AGE-175) & native failure lookup.
//!
//! `agy acp` returns `PromptResponse { stop_reason: EndTurn }` as soon as the
//! foreground turn finishes even when background shell commands (`TaskDetails`),
//! timers/daemons (`schedule`), or subagents (`invoke_subagent`) keep running
//! inside `agy --worker`, and does NOT emit ACP `session/update` notifications
//! when a `<SYSTEM_MESSAGE>` (`task_notification` / `agent_message`) wakes the
//! worker out-of-turn. Meanwhile, `agy --worker` writes every step and in-place
//! status transition to `<GEMINI_HOME>/antigravity-acp/conversations/<sid>.db`.
//!
//! This watcher polls that SQLite trajectory strictly read-only (`?mode=ro` +
//! `PRAGMA query_only = ON`, gated by `.db` + `.db-wal` stat stamps) and maps
//! live background work and autonomous wakeup turns directly onto Waku's native
//! `DriverEvent::BackgroundWork`, `TurnParked`, `TurnStarted`, `TextDelta`,
//! `ReasoningDelta`, `Activity`, and `TurnFinished` events.

use std::collections::{HashMap, HashSet};
use std::fs;
use std::io::{Read as _, Seek as _, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use rusqlite::{Connection, OpenFlags};

use crate::model::{
    ActivityKind, BackgroundWorkEvent, BackgroundWorkItem, BackgroundWorkKey, BackgroundWorkKind,
    BackgroundWorkStatus, DriverEvent,
};
use crate::driver::DriverEventSender;

const POLL_ACTIVE: Duration = Duration::from_secs(1);
const POLL_IDLE: Duration = Duration::from_secs(3);
const AUTONOMOUS_TOOL_GAP_TIMEOUT: Duration = Duration::from_secs(60);
const MAX_STEP_PAYLOAD_BYTES: usize = 4 * 1024 * 1024;
const MAX_LOG_TAIL_BYTES: usize = 16 * 1024;

// `exa.cortex_pb.CortexStepStatus` wire values (verified against CLI 1.5.7+).
const STEP_STATUS_PENDING: i32 = 1;
const STEP_STATUS_RUNNING: i32 = 2;
const STEP_STATUS_DONE: i32 = 3;
const STEP_STATUS_INVALID: i32 = 4;
const STEP_STATUS_CLEARED: i32 = 5;
const STEP_STATUS_CANCELED: i32 = 6;
const STEP_STATUS_ERROR: i32 = 7;
const STEP_STATUS_GENERATING: i32 = 8;
const STEP_STATUS_WAITING: i32 = 9;
const STEP_STATUS_QUEUED: i32 = 11;
const STEP_STATUS_INTERRUPTED: i32 = 12;

fn is_step_in_progress(status: i32) -> bool {
    matches!(
        status,
        STEP_STATUS_PENDING
            | STEP_STATUS_RUNNING
            | STEP_STATUS_GENERATING
            | STEP_STATUS_WAITING
            | STEP_STATUS_QUEUED
    )
}

fn is_step_terminal(status: i32) -> bool {
    matches!(
        status,
        STEP_STATUS_DONE
            | STEP_STATUS_INVALID
            | STEP_STATUS_CLEARED
            | STEP_STATUS_CANCELED
            | STEP_STATUS_ERROR
            | STEP_STATUS_INTERRUPTED
    )
}

fn unix_ms_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

// ---------------------------------------------------------------------------
// Minimal zero-dependency protobuf wire decoder for `gemini_coder.Step`
// ---------------------------------------------------------------------------

struct WireReader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> WireReader<'a> {
    fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0 }
    }

    fn is_eof(&self) -> bool {
        self.pos >= self.buf.len()
    }

    fn read_varint(&mut self) -> Option<u64> {
        let mut result = 0_u64;
        let mut shift = 0_u32;
        while self.pos < self.buf.len() && shift < 64 {
            let byte = self.buf[self.pos];
            self.pos += 1;
            result |= u64::from(byte & 0x7f) << shift;
            if byte & 0x80 == 0 {
                return Some(result);
            }
            shift += 7;
        }
        None
    }

    fn read_tag(&mut self) -> Option<(u32, u8)> {
        let key = self.read_varint()?;
        Some(((key >> 3) as u32, (key & 0x07) as u8))
    }

    fn read_bytes(&mut self) -> Option<&'a [u8]> {
        let len = self.read_varint()? as usize;
        let end = self.pos.checked_add(len)?;
        if end > self.buf.len() {
            return None;
        }
        let slice = &self.buf[self.pos..end];
        self.pos = end;
        Some(slice)
    }

    fn read_string(&mut self) -> Option<String> {
        let bytes = self.read_bytes()?;
        Some(String::from_utf8_lossy(bytes).into_owned())
    }

    fn skip_field(&mut self, wire_type: u8) -> Option<()> {
        match wire_type {
            0 => {
                self.read_varint()?;
                Some(())
            }
            1 => {
                let end = self.pos.checked_add(8)?;
                if end > self.buf.len() {
                    return None;
                }
                self.pos = end;
                Some(())
            }
            2 => {
                self.read_bytes()?;
                Some(())
            }
            5 => {
                let end = self.pos.checked_add(4)?;
                if end > self.buf.len() {
                    return None;
                }
                self.pos = end;
                Some(())
            }
            _ => None,
        }
    }
}

#[derive(Clone, Debug, Default)]
struct DecodedToolCall {
    id: String,
    name: String,
    arguments_json: String,
}

#[derive(Clone, Debug, Default)]
struct DecodedTaskDetails {
    id: String,
    log_uri: String,
    progress: String,
    description: String,
    is_daemon: bool,
    title: String,
}

#[derive(Clone, Debug, Default)]
struct DecodedSystemMessage {
    event_type: String,
    sender: String,
    content: String,
    render_title: String,
}

#[derive(Clone, Debug, Default)]
struct DecodedSubagent {
    subagent_name: String,
    prompt: String,
    conversation_id: String,
}

#[derive(Clone, Debug, Default)]
struct DecodedPlanner {
    response: String,
    thinking: String,
    has_tool_calls: bool,
}

#[derive(Clone, Debug, Default)]
struct DecodedError {
    user_error_message: String,
    short_error: String,
    full_error: String,
}

#[derive(Clone, Debug, Default)]
struct DecodedStep {
    idx: i64,
    status: i32,
    created_at_secs: Option<i64>,
    tool_call: Option<DecodedToolCall>,
    task_details: Option<DecodedTaskDetails>,
    system_message: Option<DecodedSystemMessage>,
    subagent: Option<DecodedSubagent>,
    planner: Option<DecodedPlanner>,
    error: Option<DecodedError>,
    generic_args: Vec<(String, String)>,
    generic_result: Option<String>,
}

fn decode_timestamp_secs(bytes: &[u8]) -> Option<i64> {
    let mut r = WireReader::new(bytes);
    while !r.is_eof() {
        let (tag, wt) = r.read_tag()?;
        if tag == 1 && wt == 0 {
            return Some(r.read_varint()? as i64);
        }
        r.skip_field(wt)?;
    }
    None
}

fn decode_tool_call(bytes: &[u8]) -> DecodedToolCall {
    let mut out = DecodedToolCall::default();
    let mut r = WireReader::new(bytes);
    while !r.is_eof() {
        let Some((tag, wt)) = r.read_tag() else {
            break;
        };
        match (tag, wt) {
            (1, 2) => {
                if let Some(s) = r.read_string() {
                    out.id = s;
                }
            }
            (2, 2) => {
                if let Some(s) = r.read_string() {
                    out.name = s;
                }
            }
            (3, 2) => {
                if let Some(s) = r.read_string() {
                    out.arguments_json = s;
                }
            }
            _ => {
                if r.skip_field(wt).is_none() {
                    break;
                }
            }
        }
    }
    out
}

fn decode_task_details(bytes: &[u8]) -> DecodedTaskDetails {
    let mut out = DecodedTaskDetails::default();
    let mut r = WireReader::new(bytes);
    while !r.is_eof() {
        let Some((tag, wt)) = r.read_tag() else {
            break;
        };
        match (tag, wt) {
            (1, 2) => {
                if let Some(s) = r.read_string() {
                    out.id = s;
                }
            }
            (2, 2) => {
                if let Some(s) = r.read_string() {
                    out.log_uri = s;
                }
            }
            (3, 2) => {
                if let Some(s) = r.read_string() {
                    out.progress = s;
                }
            }
            (4, 2) => {
                if let Some(s) = r.read_string() {
                    out.description = s;
                }
            }
            (6, 0) => {
                if let Some(v) = r.read_varint() {
                    out.is_daemon = v != 0;
                }
            }
            (7, 2) => {
                if let Some(s) = r.read_string() {
                    out.title = s;
                }
            }
            _ => {
                if r.skip_field(wt).is_none() {
                    break;
                }
            }
        }
    }
    out
}

fn decode_render_title(bytes: &[u8]) -> String {
    let mut title = String::new();
    let mut markdown = String::new();
    let mut r = WireReader::new(bytes);
    while !r.is_eof() {
        let Some((tag, wt)) = r.read_tag() else {
            break;
        };
        match (tag, wt) {
            (1, 2) => {
                if let Some(s) = r.read_string() {
                    title = s;
                }
            }
            (2, 2) => {
                if let Some(s) = r.read_string() {
                    markdown = s;
                }
            }
            _ => {
                if r.skip_field(wt).is_none() {
                    break;
                }
            }
        }
    }
    if !title.trim().is_empty() {
        title
    } else {
        markdown
    }
}

fn decode_system_message(bytes: &[u8]) -> DecodedSystemMessage {
    let mut out = DecodedSystemMessage::default();
    let mut r = WireReader::new(bytes);
    while !r.is_eof() {
        let Some((tag, wt)) = r.read_tag() else {
            break;
        };
        match (tag, wt) {
            (2, 2) => {
                if let Some(sub) = r.read_bytes() {
                    out.render_title = decode_render_title(sub);
                }
            }
            (3, 2) => {
                if let Some(s) = r.read_string() {
                    out.event_type = s;
                }
            }
            (4, 2) => {
                if let Some(sub) = r.read_bytes() {
                    let mut sr = WireReader::new(sub);
                    while !sr.is_eof() {
                        let Some((stag, swt)) = sr.read_tag() else {
                            break;
                        };
                        match (stag, swt) {
                            (3, 2) => {
                                if let Some(s) = sr.read_string() {
                                    out.sender = s;
                                }
                            }
                            (4, 2) => {
                                if let Some(s) = sr.read_string() {
                                    out.content = s;
                                }
                            }
                            _ => {
                                if sr.skip_field(swt).is_none() {
                                    break;
                                }
                            }
                        }
                    }
                }
            }
            _ => {
                if r.skip_field(wt).is_none() {
                    break;
                }
            }
        }
    }
    out
}

fn decode_subagent(bytes: &[u8]) -> DecodedSubagent {
    let mut out = DecodedSubagent::default();
    let mut r = WireReader::new(bytes);
    while !r.is_eof() {
        let Some((tag, wt)) = r.read_tag() else {
            break;
        };
        match (tag, wt) {
            (1, 2) => {
                if let Some(s) = r.read_string() {
                    out.subagent_name = s;
                }
            }
            (2, 2) => {
                if let Some(s) = r.read_string() {
                    out.prompt = s;
                }
            }
            (5, 2) => {
                if let Some(s) = r.read_string() {
                    out.conversation_id = s;
                }
            }
            _ => {
                if r.skip_field(wt).is_none() {
                    break;
                }
            }
        }
    }
    out
}

fn decode_planner(bytes: &[u8]) -> DecodedPlanner {
    let mut out = DecodedPlanner::default();
    let mut r = WireReader::new(bytes);
    while !r.is_eof() {
        let Some((tag, wt)) = r.read_tag() else {
            break;
        };
        match (tag, wt) {
            (1, 2) => {
                if let Some(s) = r.read_string() {
                    out.response = s;
                }
            }
            (3, 2) => {
                if let Some(s) = r.read_string() {
                    out.thinking = s;
                }
            }
            (7, 2) => {
                // `PlannerResponse.tool_calls` (repeated ToolCall): when present,
                // this planner step is mid-cascade and will be followed by tool
                // execution + another PlannerResponse.
                if let Some(bytes) = r.read_bytes()
                    && !bytes.is_empty()
                {
                    out.has_tool_calls = true;
                }
            }
            _ => {
                if r.skip_field(wt).is_none() {
                    break;
                }
            }
        }
    }
    out
}

fn decode_error_details(bytes: &[u8]) -> DecodedError {
    let mut out = DecodedError::default();
    let mut r = WireReader::new(bytes);
    while !r.is_eof() {
        let Some((tag, wt)) = r.read_tag() else {
            break;
        };
        match (tag, wt) {
            (1, 2) => {
                if let Some(s) = r.read_string() {
                    out.user_error_message = s;
                }
            }
            (2, 2) => {
                if let Some(s) = r.read_string() {
                    out.short_error = s;
                }
            }
            (3, 2) => {
                if let Some(s) = r.read_string() {
                    out.full_error = s;
                }
            }
            _ => {
                if r.skip_field(wt).is_none() {
                    break;
                }
            }
        }
    }
    out
}

fn decode_generic_step(bytes: &[u8]) -> (Vec<(String, String)>, Option<String>) {
    let mut args = Vec::new();
    let mut result_text = None;
    let mut r = WireReader::new(bytes);
    while !r.is_eof() {
        let Some((tag, wt)) = r.read_tag() else {
            break;
        };
        match (tag, wt) {
            (1, 2) => {
                if let Some(sub) = r.read_bytes() {
                    let mut name = String::new();
                    let mut val = String::new();
                    let mut sr = WireReader::new(sub);
                    while !sr.is_eof() {
                        let Some((stag, swt)) = sr.read_tag() else {
                            break;
                        };
                        match (stag, swt) {
                            (1, 2) => {
                                if let Some(s) = sr.read_string() {
                                    name = s;
                                }
                            }
                            (2, 2) => {
                                if let Some(s) = sr.read_string() {
                                    val = s;
                                }
                            }
                            _ => {
                                if sr.skip_field(swt).is_none() {
                                    break;
                                }
                            }
                        }
                    }
                    if !name.is_empty() {
                        args.push((name, val));
                    }
                }
            }
            (2, 2) => {
                if let Some(sub) = r.read_bytes() {
                    let mut sr = WireReader::new(sub);
                    while !sr.is_eof() {
                        let Some((stag, swt)) = sr.read_tag() else {
                            break;
                        };
                        if stag == 1 && swt == 2 {
                            if let Some(s) = sr.read_string()
                                && !s.trim().is_empty()
                            {
                                result_text = Some(s);
                            }
                        } else if sr.skip_field(swt).is_none() {
                            break;
                        }
                    }
                }
            }
            _ => {
                if r.skip_field(wt).is_none() {
                    break;
                }
            }
        }
    }
    (args, result_text)
}

fn decode_step(
    idx: i64,
    col_status: i32,
    col_task_details: Option<&[u8]>,
    payload: &[u8],
) -> DecodedStep {
    let mut step = DecodedStep {
        idx,
        status: col_status,
        ..Default::default()
    };
    if let Some(td_bytes) = col_task_details
        && !td_bytes.is_empty()
    {
        let td = decode_task_details(td_bytes);
        if !td.id.trim().is_empty() {
            step.task_details = Some(td);
        }
    }

    let mut r = WireReader::new(payload);
    while !r.is_eof() {
        let Some((tag, wt)) = r.read_tag() else {
            break;
        };
        match (tag, wt) {
            (4, 0) => {
                if let Some(v) = r.read_varint()
                    && v != 0
                {
                    step.status = v as i32;
                }
            }
            (5, 2) => {
                if let Some(sub) = r.read_bytes() {
                    let mut mr = WireReader::new(sub);
                    while !mr.is_eof() {
                        let Some((mtag, mwt)) = mr.read_tag() else {
                            break;
                        };
                        match (mtag, mwt) {
                            (1, 2) => {
                                if let Some(ts) = mr.read_bytes() {
                                    step.created_at_secs = decode_timestamp_secs(ts);
                                }
                            }
                            (4, 2) => {
                                if let Some(tc) = mr.read_bytes() {
                                    let decoded = decode_tool_call(tc);
                                    if !decoded.name.is_empty() || !decoded.id.is_empty() {
                                        step.tool_call = Some(decoded);
                                    }
                                }
                            }
                            _ => {
                                if mr.skip_field(mwt).is_none() {
                                    break;
                                }
                            }
                        }
                    }
                }
            }
            (20, 2) => {
                if let Some(sub) = r.read_bytes() {
                    step.planner = Some(decode_planner(sub));
                }
            }
            (24, 2) => {
                if let Some(sub) = r.read_bytes() {
                    let mut er = WireReader::new(sub);
                    while !er.is_eof() {
                        let Some((etag, ewt)) = er.read_tag() else {
                            break;
                        };
                        if etag == 1 && ewt == 2 {
                            if let Some(err_bytes) = er.read_bytes() {
                                step.error = Some(decode_error_details(err_bytes));
                            }
                        } else if er.skip_field(ewt).is_none() {
                            break;
                        }
                    }
                }
            }
            (31, 2) => {
                if let Some(sub) = r.read_bytes()
                    && step.error.is_none()
                {
                    step.error = Some(decode_error_details(sub));
                }
            }
            (114, 2) => {
                if let Some(sub) = r.read_bytes() {
                    step.system_message = Some(decode_system_message(sub));
                }
            }
            (140, 2) => {
                if let Some(sub) = r.read_bytes() {
                    let (args, res) = decode_generic_step(sub);
                    step.generic_args = args;
                    step.generic_result = res;
                }
            }
            (143, 2) => {
                if let Some(sub) = r.read_bytes() {
                    let subagent = decode_subagent(sub);
                    if !subagent.conversation_id.trim().is_empty() {
                        step.subagent = Some(subagent);
                    }
                }
            }
            (148, 2) => {
                if let Some(sub) = r.read_bytes() {
                    let td = decode_task_details(sub);
                    if !td.id.trim().is_empty() {
                        step.task_details = Some(td);
                    }
                }
            }
            _ => {
                if r.skip_field(wt).is_none() {
                    break;
                }
            }
        }
    }
    step
}

// ---------------------------------------------------------------------------
// SQLite Trajectory Reader (strictly read-only)
// ---------------------------------------------------------------------------

pub fn antigravity_conversations_dir() -> Option<PathBuf> {
    let profile = crate::agent_profiles::resolve_isolated_profile(crate::model::ProviderKind::Antigravity)
        .ok()
        .flatten()?;
    Some(
        profile
            .directory
            .join("antigravity-acp")
            .join("conversations"),
    )
}

fn open_readonly(db: &Path) -> Option<Connection> {
    if !db.is_file() || fs::metadata(db).ok()?.len() == 0 {
        return None;
    }
    let conn = Connection::open_with_flags(
        db,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .ok()?;
    let _ = conn.busy_timeout(Duration::from_millis(250));
    let _ = conn.execute_batch("PRAGMA query_only = ON;");
    Some(conn)
}

fn db_stat_stamp(db: &Path) -> Option<((u64, u128), Option<(u64, u128)>)> {
    let stamp_of = |p: &Path| -> Option<(u64, u128)> {
        let meta = fs::metadata(p).ok()?;
        let mtime = meta
            .modified()
            .ok()?
            .duration_since(UNIX_EPOCH)
            .ok()?
            .as_nanos();
        Some((meta.len(), mtime))
    };
    let main = stamp_of(db)?;
    let mut wal_os = db.as_os_str().to_os_string();
    wal_os.push("-wal");
    let wal = stamp_of(Path::new(&wal_os));
    Some((main, wal))
}

fn max_step_idx(db: &Path) -> i64 {
    let Some(conn) = open_readonly(db) else {
        return -1;
    };
    conn.query_row("SELECT COALESCE(MAX(idx), -1) FROM steps", [], |r| r.get(0))
        .unwrap_or(-1)
}

fn read_steps_since(db: &Path, min_idx: i64) -> Vec<DecodedStep> {
    let Some(conn) = open_readonly(db) else {
        return Vec::new();
    };
    let Ok(mut stmt) = conn.prepare(
        "SELECT idx, status, task_details, step_payload FROM steps WHERE idx >= ?1 ORDER BY idx ASC",
    ) else {
        return Vec::new();
    };
    let Ok(rows) = stmt.query_map([min_idx], |r| {
        Ok((
            r.get::<_, i64>(0)?,
            r.get::<_, i32>(1).unwrap_or(0),
            r.get::<_, Option<Vec<u8>>>(2).unwrap_or(None),
            r.get::<_, Option<Vec<u8>>>(3).unwrap_or(None),
        ))
    }) else {
        return Vec::new();
    };
    let mut steps = Vec::new();
    for row in rows.flatten() {
        let (idx, status, td_col, payload) = row;
        let payload_bytes = payload.unwrap_or_default();
        if payload_bytes.len() > MAX_STEP_PAYLOAD_BYTES {
            continue;
        }
        steps.push(decode_step(idx, status, td_col.as_deref(), &payload_bytes));
    }
    steps
}

fn file_uri_to_path(uri: &str) -> Option<PathBuf> {
    let trimmed = uri.trim();
    if trimmed.is_empty() {
        return None;
    }
    if let Some(rest) = trimmed.strip_prefix("file://") {
        return Some(PathBuf::from(rest));
    }
    if Path::new(trimmed).is_absolute() {
        return Some(PathBuf::from(trimmed));
    }
    None
}

fn read_log_tail(path: &Path) -> Option<String> {
    let mut file = fs::File::open(path).ok()?;
    let len = file.metadata().ok()?.len() as usize;
    if len == 0 {
        return None;
    }
    let start = len.saturating_sub(MAX_LOG_TAIL_BYTES);
    if start > 0 {
        let _ = file.seek(SeekFrom::Start(start as u64));
    }
    let mut buf = Vec::with_capacity(len.saturating_sub(start));
    file.read_to_end(&mut buf).ok()?;
    let text = String::from_utf8_lossy(&buf).into_owned();
    (!text.trim().is_empty()).then_some(text)
}

fn is_server_restart_notice(sys: &DecodedSystemMessage) -> bool {
    sys.event_type.trim().is_empty() && sys.sender.trim() == "system"
}

fn is_wakeup_system_message(sys: &DecodedSystemMessage) -> bool {
    matches!(
        sys.event_type.trim(),
        "task_notification" | "agent_message"
    )
}

fn inspect_subagent_status(conversations_dir: &Path, sub_id: &str) -> BackgroundWorkStatus {
    let child_db = conversations_dir.join(format!("{sub_id}.db"));
    let Some(conn) = open_readonly(&child_db) else {
        return BackgroundWorkStatus::Starting;
    };
    let Ok(mut stmt) = conn.prepare("SELECT status FROM steps ORDER BY idx DESC LIMIT 8") else {
        return BackgroundWorkStatus::Running;
    };
    let statuses: Vec<i32> = stmt
        .query_map([], |r| r.get::<_, i32>(0))
        .ok()
        .map(|rows| rows.flatten().collect())
        .unwrap_or_default();
    if statuses.is_empty() {
        return BackgroundWorkStatus::Running;
    }
    if statuses.iter().any(|&s| is_step_in_progress(s)) {
        return BackgroundWorkStatus::Running;
    }
    match statuses[0] {
        STEP_STATUS_ERROR => BackgroundWorkStatus::Failed,
        STEP_STATUS_CANCELED | STEP_STATUS_INTERRUPTED => BackgroundWorkStatus::Stopped,
        _ => BackgroundWorkStatus::Completed,
    }
}

fn extract_background_items(
    conversations_dir: &Path,
    steps: &[DecodedStep],
) -> Vec<(BackgroundWorkItem, bool)> {
    let restart_idx = steps
        .iter()
        .filter(|s| {
            s.system_message
                .as_ref()
                .is_some_and(is_server_restart_notice)
        })
        .map(|s| s.idx)
        .max();

    let mut task_notifs: HashMap<&str, (i64, String)> = HashMap::new();
    for step in steps {
        if let Some(sys) = step.system_message.as_ref()
            && sys.event_type.trim() == "task_notification"
            && !sys.sender.trim().is_empty()
        {
            task_notifs.insert(sys.sender.trim(), (step.idx, sys.content.clone()));
        }
    }

    let mut out = Vec::new();
    for step in steps {
        if let Some(td) = step.task_details.as_ref() {
            let task_id = td.id.trim();
            if task_id.is_empty() {
                continue;
            }
            let is_schedule = step
                .tool_call
                .as_ref()
                .is_some_and(|c| c.name.trim() == "schedule");
            let kind = if is_schedule || td.is_daemon {
                BackgroundWorkKind::Monitor
            } else {
                BackgroundWorkKind::Process
            };
            let title = [td.title.as_str(), td.description.as_str(), task_id]
                .into_iter()
                .map(str::trim)
                .find(|s| !s.is_empty())
                .unwrap_or("Background task")
                .lines()
                .next()
                .unwrap_or("Background task")
                .chars()
                .take(120)
                .collect::<String>();

            let notif = task_notifs.get(task_id);
            let status = if is_step_in_progress(step.status) {
                if restart_idx.is_some_and(|r| step.idx < r) {
                    BackgroundWorkStatus::Stopped
                } else if notif.is_some_and(|(n_idx, _)| *n_idx > step.idx) {
                    BackgroundWorkStatus::Completed
                } else if kind == BackgroundWorkKind::Monitor {
                    BackgroundWorkStatus::Monitoring
                } else {
                    BackgroundWorkStatus::Running
                }
            } else if matches!(step.status, STEP_STATUS_CANCELED | STEP_STATUS_INTERRUPTED) {
                BackgroundWorkStatus::Stopped
            } else if step.status == STEP_STATUS_ERROR {
                BackgroundWorkStatus::Failed
            } else {
                BackgroundWorkStatus::Completed
            };

            let mut item = BackgroundWorkItem::new(kind, task_id.to_owned(), title, status);
            item.background = true;
            if let Some(secs) = step.created_at_secs
                && secs > 0
            {
                item.started_at_ms = (secs as u64) * 1000;
            }
            item.updated_at_ms = unix_ms_now();
            if !td.description.trim().is_empty() {
                item.detail = Some(td.description.trim().chars().take(240).collect());
            } else if !td.progress.trim().is_empty() {
                item.detail = Some(td.progress.trim().chars().take(240).collect());
            }
            item.origin_activity_id = step
                .tool_call
                .as_ref()
                .map(|c| c.id.trim().to_owned())
                .filter(|id| !id.is_empty());
            if let Some(log_path) = file_uri_to_path(&td.log_uri) {
                item.output = read_log_tail(&log_path);
            }
            if item.output.is_none() {
                item.output = step
                    .generic_result
                    .clone()
                    .or_else(|| notif.map(|(_, c)| c.clone()))
                    .filter(|s| !s.trim().is_empty());
            }
            let counts_as_detached_wait = !td.is_daemon && status.is_live();
            out.push((item, counts_as_detached_wait));
            continue;
        }

        if let Some(sub) = step.subagent.as_ref()
            && step.status != STEP_STATUS_ERROR
        {
            let sub_id = sub.conversation_id.trim();
            if sub_id.is_empty() {
                continue;
            }
            let mut killed = restart_idx.is_some_and(|r| step.idx < r);
            let mut agent_reply: Option<String> = None;
            for later in steps.iter().filter(|l| l.idx > step.idx) {
                if let Some(sys) = later.system_message.as_ref()
                    && sys.event_type.trim() == "agent_message"
                    && sys.sender.trim() == sub_id
                    && !sys.content.trim().is_empty()
                {
                    agent_reply = Some(sys.content.clone());
                }
                if let Some(tc) = later.tool_call.as_ref()
                    && tc.name.trim() == "manage_subagents"
                    && later.status != STEP_STATUS_ERROR
                {
                    let args_str = &tc.arguments_json;
                    if args_str.contains("kill_all")
                        || (args_str.contains("kill") && args_str.contains(sub_id))
                    {
                        killed = true;
                    }
                }
            }
            let status = if killed {
                BackgroundWorkStatus::Stopped
            } else {
                inspect_subagent_status(conversations_dir, sub_id)
            };
            let title = if !sub.subagent_name.trim().is_empty() {
                sub.subagent_name.trim().to_owned()
            } else {
                format!("Subagent {}", &sub_id[..sub_id.len().min(8)])
            };
            let mut item = BackgroundWorkItem::new(
                BackgroundWorkKind::Subagent,
                sub_id.to_owned(),
                title,
                status,
            );
            item.background = true;
            if let Some(secs) = step.created_at_secs
                && secs > 0
            {
                item.started_at_ms = (secs as u64) * 1000;
            }
            item.updated_at_ms = unix_ms_now();
            if !sub.prompt.trim().is_empty() {
                item.detail = Some(sub.prompt.trim().chars().take(240).collect());
            }
            item.output = agent_reply;
            let counts_as_detached_wait = status.is_live();
            out.push((item, counts_as_detached_wait));
        }
    }
    out
}

fn extract_turn_failure(steps: &[DecodedStep], after_idx: i64) -> Option<String> {
    let last = steps.iter().rev().find(|s| s.idx > after_idx)?;
    let err = last.error.as_ref()?;
    let terminal_banner = err.user_error_message.trim().trim_end_matches('.')
        == "Agent execution terminated due to error";
    if !terminal_banner && last.status != STEP_STATUS_ERROR {
        return None;
    }
    [
        err.short_error.as_str(),
        err.full_error.as_str(),
        err.user_error_message.as_str(),
    ]
    .into_iter()
    .map(str::trim)
    .find(|s| !s.is_empty())
    .map(|s| s.chars().take(800).collect())
}

// ---------------------------------------------------------------------------
// Live Background Watcher Thread
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub struct ForegroundSettlement {
    pub has_live_detached_work: bool,
    pub native_failure: Option<String>,
}

struct WatcherShared {
    foreground_active: bool,
    foreground_start_idx: i64,
    birth_step_idx: i64,
    synced_step_idx: i64,
    autonomous_turn_open: bool,
    last_autonomous_progress_at: Option<Instant>,
    has_live_detached_work: bool,
    emitted_thinking_len: HashMap<i64, usize>,
    emitted_response_len: HashMap<i64, usize>,
    emitted_tools: HashSet<(i64, bool)>,
    last_item_fingerprints: HashMap<BackgroundWorkKey, (BackgroundWorkStatus, Option<usize>)>,
    last_stamp: Option<((u64, u128), Option<(u64, u128)>)>,
    force_poll: bool,
}

pub struct AntigravityTrajectoryWatcher {
    conversations_dir: PathBuf,
    db_path: PathBuf,
    events: DriverEventSender,
    shared: Arc<(Mutex<WatcherShared>, Condvar)>,
    stop: Arc<AtomicBool>,
    handle: Option<std::thread::JoinHandle<()>>,
}

impl AntigravityTrajectoryWatcher {
    pub fn start(
        conversations_dir: PathBuf,
        native_session_id: &str,
        events: DriverEventSender,
    ) -> Self {
        let db_path = conversations_dir.join(format!("{native_session_id}.db"));
        let initial_max = max_step_idx(&db_path);
        let shared = Arc::new((
            Mutex::new(WatcherShared {
                foreground_active: false,
                foreground_start_idx: initial_max,
                birth_step_idx: (initial_max + 1).max(0),
                synced_step_idx: initial_max,
                autonomous_turn_open: false,
                last_autonomous_progress_at: None,
                has_live_detached_work: false,
                emitted_thinking_len: HashMap::new(),
                emitted_response_len: HashMap::new(),
                emitted_tools: HashSet::new(),
                last_item_fingerprints: HashMap::new(),
                last_stamp: None,
                force_poll: false,
            }),
            Condvar::new(),
        ));
        let stop = Arc::new(AtomicBool::new(false));

        let t_dir = conversations_dir.clone();
        let t_db = db_path.clone();
        let t_events = events.clone();
        let t_shared = shared.clone();
        let t_stop = stop.clone();

        let handle = std::thread::Builder::new()
            .name("waku-agy-bg-watch".into())
            .spawn(move || {
                watch_loop(&t_dir, &t_db, &t_events, &t_shared, &t_stop);
            })
            .ok();

        Self {
            conversations_dir,
            db_path,
            events,
            shared,
            stop,
            handle,
        }
    }

    pub fn mark_foreground_start(&self) {
        let cur_max = max_step_idx(&self.db_path);
        let (lock, cvar) = &*self.shared;
        if let Ok(mut state) = lock.lock() {
            state.foreground_active = true;
            state.foreground_start_idx = cur_max;
            state.autonomous_turn_open = false;
            state.last_autonomous_progress_at = None;
            state.force_poll = true;
            cvar.notify_one();
        }
    }

    pub fn on_foreground_settled(&self, produced_content: bool) -> ForegroundSettlement {
        let (lock, cvar) = &*self.shared;
        let Ok(mut state) = lock.lock() else {
            return ForegroundSettlement {
                has_live_detached_work: false,
                native_failure: None,
            };
        };
        state.foreground_active = false;
        let steps = read_steps_since(&self.db_path, state.birth_step_idx);
        let native_failure = (!produced_content)
            .then(|| extract_turn_failure(&steps, state.foreground_start_idx))
            .flatten();
        if let Some(max_idx) = steps.last().map(|s| s.idx) {
            state.synced_step_idx = state.synced_step_idx.max(max_idx);
        }
        let has_live = sync_background_items(&self.conversations_dir, &steps, &self.events, &mut state);
        state.has_live_detached_work = has_live;
        state.last_stamp = db_stat_stamp(&self.db_path);
        state.force_poll = true;
        cvar.notify_one();
        ForegroundSettlement {
            has_live_detached_work: has_live,
            native_failure,
        }
    }

    pub fn has_live_detached_work(&self) -> bool {
        self.shared
            .0
            .lock()
            .map(|s| s.has_live_detached_work || s.autonomous_turn_open)
            .unwrap_or(false)
    }

    pub fn refresh_now(&self) {
        let (lock, cvar) = &*self.shared;
        if let Ok(mut state) = lock.lock() {
            state.force_poll = true;
            cvar.notify_one();
        }
    }
}

impl Drop for AntigravityTrajectoryWatcher {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        self.shared.1.notify_all();
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

fn sync_background_items(
    conversations_dir: &Path,
    steps: &[DecodedStep],
    events: &DriverEventSender,
    state: &mut WatcherShared,
) -> bool {
    let items = extract_background_items(conversations_dir, steps);
    let mut any_live_detached = false;
    for (item, counts_detached) in items {
        if counts_detached {
            any_live_detached = true;
        }
        let fp = (item.status, item.output.as_ref().map(String::len));
        if state.last_item_fingerprints.get(&item.key) != Some(&fp) {
            state.last_item_fingerprints.insert(item.key.clone(), fp);
            let _ = events.send(DriverEvent::BackgroundWork(BackgroundWorkEvent::Upsert(
                item,
            )));
        }
    }
    any_live_detached
}

fn watch_loop(
    conversations_dir: &Path,
    db_path: &Path,
    events: &DriverEventSender,
    shared: &(Mutex<WatcherShared>, Condvar),
    stop: &AtomicBool,
) {
    let (lock, cvar) = shared;
    while !stop.load(Ordering::Acquire) {
        let wait_duration = {
            let Ok(mut state) = lock.lock() else {
                break;
            };
            let stamp = db_stat_stamp(db_path);
            let gap_timed_out = state.autonomous_turn_open
                && state
                    .last_autonomous_progress_at
                    .is_some_and(|t| t.elapsed() >= AUTONOMOUS_TOOL_GAP_TIMEOUT);
            let should_scan =
                state.force_poll || gap_timed_out || (stamp.is_some() && stamp != state.last_stamp);
            state.force_poll = false;
            if should_scan {
                state.last_stamp = stamp;
                let steps = read_steps_since(db_path, state.birth_step_idx);
                let any_live_detached =
                    sync_background_items(conversations_dir, &steps, events, &mut state);
                state.has_live_detached_work = any_live_detached;

                if !state.foreground_active {
                    drive_autonomous_wakeups(&steps, any_live_detached, events, &mut state);
                }
            }
            if state.has_live_detached_work || state.autonomous_turn_open || state.foreground_active
            {
                POLL_ACTIVE
            } else {
                POLL_IDLE
            }
        };

        let Ok(guard) = lock.lock() else {
            break;
        };
        if !stop.load(Ordering::Acquire) && !guard.force_poll {
            let _ = cvar.wait_timeout(guard, wait_duration);
        }
    }
}

fn drive_autonomous_wakeups(
    steps: &[DecodedStep],
    any_live_detached: bool,
    events: &DriverEventSender,
    state: &mut WatcherShared,
) {
    let post_steps: Vec<&DecodedStep> = steps
        .iter()
        .filter(|s| s.idx > state.synced_step_idx)
        .collect();
    if post_steps.is_empty() {
        return;
    }

    let has_wakeup_trigger = post_steps.iter().any(|s| {
        s.system_message
            .as_ref()
            .is_some_and(is_wakeup_system_message)
            || s.planner.is_some()
            || s.tool_call.is_some()
    });
    if !state.autonomous_turn_open && !has_wakeup_trigger {
        return;
    }

    let mut any_visible_work = false;
    for step in &post_steps {
        if let Some(planner) = step.planner.as_ref() {
            let prev_t = state
                .emitted_thinking_len
                .get(&step.idx)
                .copied()
                .unwrap_or(0);
            if planner.thinking.len() > prev_t && planner.thinking.is_char_boundary(prev_t) {
                if !state.autonomous_turn_open {
                    state.autonomous_turn_open = true;
                    let _ = events.send(DriverEvent::TurnStarted);
                }
                let delta = planner.thinking[prev_t..].to_owned();
                state
                    .emitted_thinking_len
                    .insert(step.idx, planner.thinking.len());
                let _ = events.send(DriverEvent::ReasoningDelta(delta));
                any_visible_work = true;
            }
            let prev_r = state
                .emitted_response_len
                .get(&step.idx)
                .copied()
                .unwrap_or(0);
            if planner.response.len() > prev_r && planner.response.is_char_boundary(prev_r) {
                if !state.autonomous_turn_open {
                    state.autonomous_turn_open = true;
                    let _ = events.send(DriverEvent::TurnStarted);
                }
                let delta = planner.response[prev_r..].to_owned();
                state
                    .emitted_response_len
                    .insert(step.idx, planner.response.len());
                let _ = events.send(DriverEvent::TextDelta(delta));
                any_visible_work = true;
            }
        }
        if let Some(tc) = step.tool_call.as_ref()
            && !tc.name.trim().is_empty()
        {
            let complete = is_step_terminal(step.status);
            if state.emitted_tools.insert((step.idx, complete)) {
                if !state.autonomous_turn_open {
                    state.autonomous_turn_open = true;
                    let _ = events.send(DriverEvent::TurnStarted);
                }
                let kind = classify_agy_tool_kind(&tc.name);
                let id = if tc.id.trim().is_empty() {
                    format!("agy-auto-step-{}", step.idx)
                } else {
                    tc.id.trim().to_owned()
                };
                let _ = events.send(DriverEvent::Activity {
                    id: Some(id),
                    kind,
                    title: tc.name.trim().to_owned(),
                    detail: step.generic_result.as_deref().map(|s| s.chars().take(240).collect()),
                    complete,
                });
                any_visible_work = true;
            }
        }
    }

    if any_visible_work {
        state.last_autonomous_progress_at = Some(Instant::now());
    }

    if !state.autonomous_turn_open {
        return;
    }

    // Check if every step in this autonomous wakeup cascade has reached a
    // terminal status (excluding background TaskDetails / Subagent steps which
    // stay in progress across turns by design).
    let foreground_step_still_running = post_steps.iter().any(|s| {
        s.task_details.is_none() && s.subagent.is_none() && is_step_in_progress(s.status)
    });
    if foreground_step_still_running {
        return;
    }

    // Never close the autonomous turn immediately after a tool step (e.g.
    // `read_file` -> DONE) or a tool-calling `PlannerResponse`: `agy --worker`
    // is about to insert the follow-up `PlannerResponse` with the final answer
    // on the next tick. Close immediately once the last non-background step is
    // a terminal final `PlannerResponse` (no `tool_calls`) or terminal error/cancel,
    // and fall back to `AUTONOMOUS_TOOL_GAP_TIMEOUT` only if the worker stalls.
    let last_cascade_step = post_steps
        .iter()
        .rev()
        .find(|s| s.task_details.is_none() && s.subagent.is_none());
    let reached_terminal_reply = last_cascade_step.is_some_and(|s| {
        s.error.is_some()
            || matches!(
                s.status,
                STEP_STATUS_ERROR | STEP_STATUS_CANCELED | STEP_STATUS_INTERRUPTED
            )
            || s.planner
                .as_ref()
                .is_some_and(|p| !p.has_tool_calls && is_step_terminal(s.status))
    });
    let gap_timed_out = state
        .last_autonomous_progress_at
        .is_some_and(|t| t.elapsed() >= AUTONOMOUS_TOOL_GAP_TIMEOUT);

    if reached_terminal_reply || gap_timed_out {
        state.autonomous_turn_open = false;
        state.last_autonomous_progress_at = None;
        if let Some(max_idx) = post_steps.last().map(|s| s.idx) {
            state.synced_step_idx = state.synced_step_idx.max(max_idx);
        }
        if any_live_detached {
            let _ = events.send(DriverEvent::TurnParked);
        } else {
            let _ = events.send(DriverEvent::TurnFinished {
                success: true,
                summary: None,
            });
        }
    }
}

fn classify_agy_tool_kind(name: &str) -> ActivityKind {
    match name.trim() {
        "run_command" | "manage_task" => ActivityKind::Command,
        "edit_file" | "write_file" => ActivityKind::FileChange,
        "read_file" => ActivityKind::FileRead,
        "grep_search" | "find_by_name" => ActivityKind::FileSearch,
        "list_dir" => ActivityKind::FileList,
        "search_web" => ActivityKind::Search,
        other => ActivityKind::from_tool_name(other),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn encode_varint(mut v: u64, out: &mut Vec<u8>) {
        while v >= 0x80 {
            out.push(((v as u8) & 0x7f) | 0x80);
            v >>= 7;
        }
        out.push(v as u8);
    }

    fn encode_field_varint(tag: u32, v: u64, out: &mut Vec<u8>) {
        encode_varint((u64::from(tag) << 3) | 0, out);
        encode_varint(v, out);
    }

    fn encode_field_bytes(tag: u32, bytes: &[u8], out: &mut Vec<u8>) {
        encode_varint((u64::from(tag) << 3) | 2, out);
        encode_varint(bytes.len() as u64, out);
        out.extend_from_slice(bytes);
    }

    fn encode_field_str(tag: u32, s: &str, out: &mut Vec<u8>) {
        encode_field_bytes(tag, s.as_bytes(), out);
    }

    #[test]
    fn decodes_background_task_and_autonomous_wakeup_steps() {
        let dir = std::env::temp_dir().join(format!("waku-agy-watch-{}", uuid::Uuid::new_v4()));
        fs::create_dir_all(&dir).unwrap();
        let db = dir.join("sess-1.db");
        let conn = Connection::open(&db).unwrap();
        conn.execute_batch(
            "CREATE TABLE steps (
                idx INTEGER PRIMARY KEY,
                step_type INTEGER NOT NULL DEFAULT 0,
                status INTEGER NOT NULL DEFAULT 0,
                task_details BLOB,
                step_payload BLOB
            );",
        )
        .unwrap();

        // Step 0: background run_command TaskDetails (RUNNING)
        let mut td = Vec::new();
        encode_field_str(1, "sess-1/task-0", &mut td);
        encode_field_str(4, "Cargo cloud test", &mut td);
        encode_field_str(7, "Cloud build", &mut td);
        let mut step0 = Vec::new();
        encode_field_varint(4, STEP_STATUS_RUNNING as u64, &mut step0);
        encode_field_bytes(148, &td, &mut step0);
        conn.execute(
            "INSERT INTO steps (idx, status, task_details, step_payload) VALUES (0, ?1, ?2, ?3)",
            rusqlite::params![STEP_STATUS_RUNNING, td, step0],
        )
        .unwrap();

        let (events, rx) = crate::driver::test_event_channel();
        let watcher = AntigravityTrajectoryWatcher::start(dir.clone(), "sess-1", events);
        // Pretend foreground turn started before step 0 so step 0 belongs to this session.
        watcher.shared.0.lock().unwrap().birth_step_idx = 0;
        watcher.shared.0.lock().unwrap().synced_step_idx = -1;
        let settlement = watcher.on_foreground_settled(true);
        assert!(settlement.has_live_detached_work);
        assert!(settlement.native_failure.is_none());

        let mut saw_running_bg = false;
        while let Ok(ev) = rx.try_recv() {
            if let DriverEvent::BackgroundWork(BackgroundWorkEvent::Upsert(item)) = ev
                && item.key.provider_id == "sess-1/task-0"
                && item.status == BackgroundWorkStatus::Running
            {
                saw_running_bg = true;
            }
        }
        assert!(saw_running_bg, "expected running background work item");

        // Now simulate task completion + <SYSTEM_MESSAGE> task_notification + autonomous PlannerResponse!
        let mut sys_agent = Vec::new();
        encode_field_str(3, "sess-1/task-0", &mut sys_agent);
        encode_field_str(4, "exit code 0", &mut sys_agent);
        let mut sys_msg = Vec::new();
        encode_field_str(3, "task_notification", &mut sys_msg);
        encode_field_bytes(4, &sys_agent, &mut sys_msg);
        let mut step1 = Vec::new();
        encode_field_varint(4, STEP_STATUS_DONE as u64, &mut step1);
        encode_field_bytes(114, &sys_msg, &mut step1);

        let mut planner = Vec::new();
        encode_field_str(1, "Background build succeeded!", &mut planner);
        encode_field_str(3, "Checking output", &mut planner);
        let mut step2 = Vec::new();
        encode_field_varint(4, STEP_STATUS_DONE as u64, &mut step2);
        encode_field_bytes(20, &planner, &mut step2);

        conn.execute(
            "UPDATE steps SET status = ?1 WHERE idx = 0",
            rusqlite::params![STEP_STATUS_DONE],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO steps (idx, status, step_payload) VALUES (1, ?1, ?2)",
            rusqlite::params![STEP_STATUS_DONE, step1],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO steps (idx, status, step_payload) VALUES (2, ?1, ?2)",
            rusqlite::params![STEP_STATUS_DONE, step2],
        )
        .unwrap();

        watcher.refresh_now();
        std::thread::sleep(Duration::from_millis(200));

        let mut saw_completed_bg = false;
        let mut saw_turn_started = false;
        let mut saw_text = String::new();
        let mut saw_turn_finished = false;
        while let Ok(ev) = rx.try_recv() {
            match ev {
                DriverEvent::BackgroundWork(BackgroundWorkEvent::Upsert(item))
                    if item.key.provider_id == "sess-1/task-0"
                        && item.status == BackgroundWorkStatus::Completed =>
                {
                    saw_completed_bg = true;
                }
                DriverEvent::TurnStarted => saw_turn_started = true,
                DriverEvent::TextDelta(t) => saw_text.push_str(&t),
                DriverEvent::TurnFinished { success: true, .. } => saw_turn_finished = true,
                _ => {}
            }
        }
        assert!(saw_completed_bg);
        assert!(saw_turn_started);
        assert_eq!(saw_text, "Background build succeeded!");
        assert!(saw_turn_finished);

        drop(watcher);
        drop(conn);
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn autonomous_wakeup_waits_for_final_planner_after_tool_step() {
        let dir =
            std::env::temp_dir().join(format!("waku-agy-watch-gap-{}", uuid::Uuid::new_v4()));
        fs::create_dir_all(&dir).unwrap();
        let db = dir.join("sess-gap.db");
        let conn = Connection::open(&db).unwrap();
        conn.execute_batch(
            "CREATE TABLE steps (
                idx INTEGER PRIMARY KEY,
                step_type INTEGER NOT NULL DEFAULT 0,
                status INTEGER NOT NULL DEFAULT 0,
                task_details BLOB,
                step_payload BLOB
            );",
        )
        .unwrap();

        // Step 0: background TaskDetails RUNNING
        let mut td = Vec::new();
        encode_field_str(1, "sess-gap/task-0", &mut td);
        encode_field_str(7, "Build HTML", &mut td);
        let mut step0 = Vec::new();
        encode_field_varint(4, STEP_STATUS_RUNNING as u64, &mut step0);
        encode_field_bytes(148, &td, &mut step0);
        conn.execute(
            "INSERT INTO steps (idx, status, task_details, step_payload) VALUES (0, ?1, ?2, ?3)",
            rusqlite::params![STEP_STATUS_RUNNING, td, step0],
        )
        .unwrap();

        let (events, rx) = crate::driver::test_event_channel();
        let watcher = AntigravityTrajectoryWatcher::start(dir.clone(), "sess-gap", events);
        watcher.shared.0.lock().unwrap().birth_step_idx = 0;
        watcher.shared.0.lock().unwrap().synced_step_idx = -1;
        let settlement = watcher.on_foreground_settled(true);
        assert!(settlement.has_live_detached_work);
        while rx.try_recv().is_ok() {}

        // Phase 1: background finishes, <SYSTEM_MESSAGE> wakes worker, PlannerResponse calls read_file,
        // read_file completes (DONE), BEFORE final PlannerResponse is written (1s real-world gap!).
        conn.execute(
            "UPDATE steps SET status = ?1 WHERE idx = 0",
            rusqlite::params![STEP_STATUS_DONE],
        )
        .unwrap();

        let mut sys_agent = Vec::new();
        encode_field_str(3, "sess-gap/task-0", &mut sys_agent);
        encode_field_str(4, "done", &mut sys_agent);
        let mut sys_msg = Vec::new();
        encode_field_str(3, "task_notification", &mut sys_msg);
        encode_field_bytes(4, &sys_agent, &mut sys_msg);
        let mut step1 = Vec::new();
        encode_field_varint(4, STEP_STATUS_DONE as u64, &mut step1);
        encode_field_bytes(114, &sys_msg, &mut step1);

        // Step 2: PlannerResponse with tag 7 (tool_calls) and no final response text
        let mut tc_blob = Vec::new();
        encode_field_str(1, "call-read-1", &mut tc_blob);
        encode_field_str(2, "read_file", &mut tc_blob);
        let mut planner_tc = Vec::new();
        encode_field_bytes(7, &tc_blob, &mut planner_tc);
        let mut step2 = Vec::new();
        encode_field_varint(4, STEP_STATUS_DONE as u64, &mut step2);
        encode_field_bytes(20, &planner_tc, &mut step2);

        // Step 3: Tool execution step (read_file -> DONE)
        let mut metadata = Vec::new();
        encode_field_bytes(4, &tc_blob, &mut metadata);
        let mut step3 = Vec::new();
        encode_field_varint(4, STEP_STATUS_DONE as u64, &mut step3);
        encode_field_bytes(5, &metadata, &mut step3);

        for (idx, payload) in [(1_i64, step1), (2, step2), (3, step3)] {
            conn.execute(
                "INSERT INTO steps (idx, status, step_payload) VALUES (?1, ?2, ?3)",
                rusqlite::params![idx, STEP_STATUS_DONE, payload],
            )
            .unwrap();
        }

        watcher.refresh_now();
        std::thread::sleep(Duration::from_millis(200));

        let mut saw_started = false;
        let mut saw_read_activity = false;
        let mut saw_premature_finish = false;
        while let Ok(ev) = rx.try_recv() {
            match ev {
                DriverEvent::TurnStarted => saw_started = true,
                DriverEvent::Activity {
                    kind: ActivityKind::FileRead,
                    complete: true,
                    ..
                } => saw_read_activity = true,
                DriverEvent::TurnFinished { .. } | DriverEvent::TurnParked => {
                    saw_premature_finish = true;
                }
                _ => {}
            }
        }
        assert!(saw_started, "expected TurnStarted on tool step");
        assert!(saw_read_activity, "expected FileRead activity");
        assert!(
            !saw_premature_finish,
            "must NOT prematurely close autonomous turn right after tool step finishes"
        );

        // Phase 2: 1s later agy writes step 4 (final PlannerResponse with no tool_calls)
        let mut final_planner = Vec::new();
        encode_field_str(1, "Final character analysis ready!", &mut final_planner);
        let mut step4 = Vec::new();
        encode_field_varint(4, STEP_STATUS_DONE as u64, &mut step4);
        encode_field_bytes(20, &final_planner, &mut step4);
        conn.execute(
            "INSERT INTO steps (idx, status, step_payload) VALUES (4, ?1, ?2)",
            rusqlite::params![STEP_STATUS_DONE, step4],
        )
        .unwrap();

        watcher.refresh_now();
        std::thread::sleep(Duration::from_millis(200));

        let mut final_text = String::new();
        let mut saw_finish = false;
        while let Ok(ev) = rx.try_recv() {
            match ev {
                DriverEvent::TextDelta(t) => final_text.push_str(&t),
                DriverEvent::TurnFinished { success: true, .. } => saw_finish = true,
                _ => {}
            }
        }
        assert_eq!(final_text, "Final character analysis ready!");
        assert!(saw_finish, "expected TurnFinished after final PlannerResponse");

        drop(watcher);
        drop(conn);
        let _ = fs::remove_dir_all(dir);
    }
}
