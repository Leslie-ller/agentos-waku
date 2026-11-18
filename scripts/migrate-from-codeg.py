#!/usr/bin/env python3
"""Official idempotent, read-only migrator from legacy codeg SQLite DB to AgentOS Waku.

Guarantees:
1. Opens source `codeg` database strictly read-only (`file:...?mode=ro` + `PRAGMA query_only = ON`)
   and verifies `(size, mtime_ns)` before and after execution (`source_zero_modified == True`).
2. Initializes target `app.db` with Waku's exact `db/migrations/*.sql` table + `agentos_*` MCP tables.
3. Migrates all folders -> `projects`, all active & archived conversations -> `sessions` +
   `session_details` + `messages` (zero message truncation), tool call/result pairs -> `activities`
   transcript blocks, native `external_id` -> `provider_cursor`, cross-agent `conversation_handoff` ->
   `parked_cursors`, and `memory_note_head` / `memory_checkpoint` / `memory_knowledge` / legacy IDs.
4. Deterministic UUIDv5 keys + transactional upserts make repeated runs 100% idempotent.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
from itertools import groupby
import json
from pathlib import Path
import sqlite3
import sys
import time
import uuid

NAMESPACE = uuid.UUID("6ba7b810-9dad-11d1-80b4-00c04fd430c8")  # NAMESPACE_DNS
TOOL_ARG_PREVIEW_LIMIT = 4000
TOOL_OUT_PREVIEW_LIMIT = 8000


def uid(seed: str) -> str:
    return str(uuid.uuid5(NAMESPACE, f"agentos.waku.migrate.v1.{seed}"))


def parse_ts(ts_str: str | None, fallback: int) -> int:
    if not ts_str:
        return fallback
    try:
        cleaned = ts_str.strip()
        if cleaned.endswith(" +00:00"):
            cleaned = cleaned[: -len(" +00:00")] + "+00:00"
        cleaned = cleaned.replace(" ", "T", 1)
        if cleaned.endswith("Z"):
            cleaned = cleaned[:-1] + "+00:00"
        return int(datetime.fromisoformat(cleaned).timestamp())
    except Exception:
        return fallback


def map_provider(agent_type: str | None) -> str:
    raw = (agent_type or "").strip().lower()
    if "antigravity" in raw or "gemini" in raw:
        return "antigravity"
    if "codex" in raw:
        return "codex"
    if "deepseek" in raw:
        return "deepSeek"
    if "claude" in raw:
        return "claude"
    if "kimi" in raw:
        return "kimi"
    if "open_code" in raw or "opencode" in raw or "open_claw" in raw:
        return "openCode"
    if "devin" in raw or "cursor" in raw:
        return "cursor"
    return "codex"


def default_model_for_provider(provider: str) -> str:
    return {
        "antigravity": "gemini-3.8-pro-high",
        "codex": "gpt-5.4",
        "deepSeek": "deepseek-reasoner",
        "claude": "claude-sonnet-4-5",
        "openCode": "gpt-5.4",
        "kimi": "kimi-k2.5",
        "cursor": "gpt-5.4",
    }.get(provider, "gpt-5.4")


def make_cursor(provider: str, native_id: str | None) -> dict | None:
    if not native_id:
        return None
    cleaned = native_id.strip()
    if not cleaned:
        return None
    if provider == "codex":
        return {"provider": "codex", "threadId": cleaned}
    if provider == "amp":
        return {"provider": "amp", "threadId": cleaned}
    return {"provider": provider, "sessionId": cleaned}


def classify_tool_kind(tool_name: str) -> str:
    lowered = tool_name.lower()
    if any(k in lowered for k in ("bash", "command", "ran ", "shell", "terminal", "exec")):
        return "command"
    if any(k in lowered for k in ("edit", "patch", "write", "create_file", "replace")):
        return "fileChange"
    if any(k in lowered for k in ("read", "view_file")):
        return "fileRead"
    if any(k in lowered for k in ("grep", "search_file", "find", "glob", "ripgrep")):
        return "fileSearch"
    if any(k in lowered for k in ("ls", "list_dir")):
        return "fileList"
    if any(k in lowered for k in ("plan", "todo")):
        return "plan"
    if "web" in lowered:
        return "search"
    return "tool"


def cap_preview(text: str | None, limit: int) -> tuple[str | None, int, int]:
    """Return `(capped_text, original_len, truncated_chars)`."""
    if text is None:
        return None, 0, 0
    orig = len(text)
    if orig <= limit:
        return text, orig, 0
    kept = text[:limit] + f"\n… [preview capped at {limit} of {orig} chars]"
    return kept, orig, orig - limit


def apply_waku_schema(dst: sqlite3.Connection, repo_root: Path, now_ts: int) -> None:
    dst.execute(
        """CREATE TABLE IF NOT EXISTS migrations (
            tag TEXT PRIMARY KEY,
            applied_at INTEGER NOT NULL
        )"""
    )
    mig_dir = repo_root / "db" / "migrations"
    sql_files = sorted(mig_dir.glob("*.sql"), key=lambda p: int(p.stem.split("_", 1)[0]))
    for sql_path in sql_files:
        tag = sql_path.stem
        exists = dst.execute("SELECT 1 FROM migrations WHERE tag = ?", (tag,)).fetchone()
        if exists:
            continue
        raw_sql = sql_path.read_text(encoding="utf-8")
        with dst:
            for stmt in raw_sql.split("--> statement-breakpoint"):
                if stmt.strip():
                    dst.executescript(stmt)
            dst.execute(
                "INSERT INTO migrations(tag, applied_at) VALUES(?, ?)",
                (tag, now_ts),
            )

    dst.executescript(
        """
        CREATE TABLE IF NOT EXISTS agentos_memory_notes (
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
        """
    )


def convert_events(
    conv_id: int,
    ev_iter,
    base_created_at: int,
) -> tuple[list[dict], list[dict], list[dict], dict]:
    messages: list[dict] = []
    blocks: list[dict] = []
    turns: list[dict] = []

    current_turn_id: str | None = None
    turn_count = 0
    pending_activities: list[dict] = []
    pending_by_call_id: dict[str, int] = {}

    ev_count = 0
    truncated_arg_fields = 0
    truncated_out_fields = 0
    orig_tool_chars = 0
    kept_tool_chars = 0

    def flush_activities() -> None:
        nonlocal pending_activities, pending_by_call_id
        if not pending_activities:
            return
        blocks.append(
            {
                "after_message": len(messages),
                "turn_id": current_turn_id,
                "content": {
                    "kind": "activities",
                    "data": pending_activities,
                },
            }
        )
        pending_activities = []
        pending_by_call_id = {}

    for seq, (_task_ref, _task_seq, kind, _role, payload_str, call_id, ev_created) in enumerate(
        ev_iter
    ):
        ev_count += 1
        ts = parse_ts(ev_created, base_created_at + seq)
        try:
            payload = json.loads(payload_str) if payload_str else {}
        except Exception:
            payload = {}

        if kind == "user_input":
            flush_activities()
            if current_turn_id and turns:
                turns[-1]["completed_at"] = max(turns[-1]["completed_at"], ts)
            turn_count += 1
            current_turn_id = uid(f"c{conv_id}-t{turn_count}")
            turns.append(
                {
                    "id": current_turn_id,
                    "turn_count": turn_count,
                    "status": "completed",
                    "provider_turn_started": True,
                    "started_at": ts,
                    "completed_at": ts + 1,
                    "checkpoint": None,
                }
            )
            text = payload.get("text")
            if not isinstance(text, str) or not text:
                text = "(attachment / user prompt)"
            messages.append(
                {
                    "id": uid(f"c{conv_id}-m{len(messages)}"),
                    "turn_id": current_turn_id,
                    "role": "user",
                    "content": text,
                    "created_at": ts,
                    "streaming": False,
                }
            )

        elif kind == "assistant_message":
            flush_activities()
            if current_turn_id is None:
                turn_count += 1
                current_turn_id = uid(f"c{conv_id}-t{turn_count}")
                turns.append(
                    {
                        "id": current_turn_id,
                        "turn_count": turn_count,
                        "status": "completed",
                        "provider_turn_started": True,
                        "started_at": ts,
                        "completed_at": ts + 1,
                        "checkpoint": None,
                    }
                )
            text = payload.get("text")
            if not isinstance(text, str) or not text.strip():
                continue
            if (
                messages
                and messages[-1]["role"] == "assistant"
                and messages[-1]["turn_id"] == current_turn_id
            ):
                messages[-1]["content"] += "\n\n" + text
            else:
                messages.append(
                    {
                        "id": uid(f"c{conv_id}-m{len(messages)}"),
                        "turn_id": current_turn_id,
                        "role": "assistant",
                        "content": text,
                        "created_at": ts,
                        "streaming": False,
                    }
                )
            if turns:
                turns[-1]["completed_at"] = max(turns[-1]["completed_at"], ts)

        elif kind == "tool_call":
            if current_turn_id is None:
                turn_count += 1
                current_turn_id = uid(f"c{conv_id}-t{turn_count}")
                turns.append(
                    {
                        "id": current_turn_id,
                        "turn_count": turn_count,
                        "status": "completed",
                        "provider_turn_started": True,
                        "started_at": ts,
                        "completed_at": ts + 1,
                        "checkpoint": None,
                    }
                )
            tool_name = str(payload.get("tool") or "tool")
            raw_input = payload.get("input")
            input_str = (
                raw_input
                if isinstance(raw_input, str)
                else json.dumps(raw_input, ensure_ascii=False)
                if raw_input is not None
                else None
            )
            act_kind = classify_tool_kind(tool_name)
            display_target = tool_name
            if input_str:
                try:
                    parsed_in = json.loads(input_str)
                    if isinstance(parsed_in, dict):
                        for k in ("command", "CommandLine", "path", "Path", "query", "pattern"):
                            val = parsed_in.get(k)
                            if isinstance(val, str) and val.strip():
                                display_target = val.strip().splitlines()[0][:160]
                                break
                except Exception:
                    pass

            capped_args, arg_orig, arg_trunc = cap_preview(input_str, TOOL_ARG_PREVIEW_LIMIT)
            orig_tool_chars += arg_orig
            kept_tool_chars += min(arg_orig, TOOL_ARG_PREVIEW_LIMIT)
            if arg_trunc > 0:
                truncated_arg_fields += 1

            act = {
                "id": uid(f"c{conv_id}-act-{seq}"),
                "source_id": call_id or f"seq-{seq}",
                "kind": act_kind,
                "title": display_target if act_kind == "command" else tool_name,
                "tool_name": tool_name[:80],
                "detail": display_target[:300],
                "arguments": capped_args,
                "output": None,
                "failed": False,
                "complete": True,
                "display_target": display_target[:160],
            }
            idx = len(pending_activities)
            pending_activities.append(act)
            if call_id:
                pending_by_call_id[call_id] = idx

        elif kind == "tool_result":
            out_val = payload.get("output")
            if out_val is None:
                out_val = payload.get("preview")
            out_str = (
                out_val
                if isinstance(out_val, str)
                else json.dumps(out_val, ensure_ascii=False)
                if out_val is not None
                else None
            )
            is_err = bool(payload.get("is_error"))
            target_idx = (
                pending_by_call_id.get(call_id)
                if call_id
                else (len(pending_activities) - 1 if pending_activities else None)
            )
            if target_idx is not None and 0 <= target_idx < len(pending_activities):
                capped_out, out_orig, out_trunc = cap_preview(out_str, TOOL_OUT_PREVIEW_LIMIT)
                orig_tool_chars += out_orig
                kept_tool_chars += min(out_orig, TOOL_OUT_PREVIEW_LIMIT)
                if out_trunc > 0:
                    truncated_out_fields += 1
                pending_activities[target_idx]["output"] = capped_out
                pending_activities[target_idx]["failed"] = is_err

    flush_activities()
    stats = {
        "event_count": ev_count,
        "truncated_arg_fields": truncated_arg_fields,
        "truncated_out_fields": truncated_out_fields,
        "orig_tool_chars": orig_tool_chars,
        "kept_tool_chars": kept_tool_chars,
    }
    return messages, blocks, turns, stats


def recover_from_handoff_archive(
    conv_id: int, archive_json: str | None, base_created_at: int
) -> tuple[list[dict], list[dict]]:
    if not archive_json:
        return [], []
    try:
        items = json.loads(archive_json)
    except Exception:
        return [], []
    if not isinstance(items, list):
        return [], []
    messages: list[dict] = []
    turns: list[dict] = []
    turn_count = 0
    current_turn_id: str | None = None
    for idx, item in enumerate(items):
        if not isinstance(item, dict):
            continue
        role = item.get("role")
        if role not in ("user", "assistant"):
            continue
        ts = parse_ts(item.get("timestamp"), base_created_at + idx)
        parts = []
        for blk in item.get("blocks") or []:
            if isinstance(blk, dict) and isinstance(blk.get("text"), str) and blk["text"].strip():
                parts.append(blk["text"])
        if not parts and isinstance(item.get("content"), str) and item["content"].strip():
            parts.append(item["content"])
        text = "\n\n".join(parts).strip()
        if not text:
            continue
        if role == "user" or current_turn_id is None:
            turn_count += 1
            current_turn_id = uid(f"c{conv_id}-arch-t{turn_count}")
            turns.append(
                {
                    "id": current_turn_id,
                    "turn_count": turn_count,
                    "status": "completed",
                    "provider_turn_started": True,
                    "started_at": ts,
                    "completed_at": ts + 1,
                    "checkpoint": None,
                }
            )
        messages.append(
            {
                "id": uid(f"c{conv_id}-arch-m{len(messages)}"),
                "turn_id": current_turn_id,
                "role": role,
                "content": text,
                "created_at": ts,
                "streaming": False,
            }
        )
    return messages, turns


def parse_parked_cursors(
    retired_json: str | None, active_provider: str, active_cursor: dict | None, parked_at: int
) -> list[dict]:
    if not retired_json:
        return []
    try:
        entries = json.loads(retired_json)
    except Exception:
        return []
    if not isinstance(entries, list):
        return []
    by_provider: dict[str, dict] = {}
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        prov = map_provider(entry.get("agent"))
        sid = entry.get("session_id")
        cursor = make_cursor(prov, sid if isinstance(sid, str) else None)
        if not cursor:
            continue
        if prov == active_provider and active_cursor == cursor:
            continue
        turn_cnt = int(entry.get("native_turn_count") or 0)
        by_provider[prov] = {
            "provider": prov,
            "cursor": cursor,
            "parkedAtTurnCount": max(turn_cnt, 0),
            "parkedAtMessageCount": 0,
            "parkedAt": parked_at,
        }
    return list(by_provider.values())


def file_stamp(path: Path) -> tuple[int, int] | None:
    if not path.exists():
        return None
    st = path.stat()
    return (st.st_size, st.st_mtime_ns)


def run_migration(source_db: Path, target_dir: Path, report_path: Path | None) -> dict:
    t0 = time.time()
    repo_root = Path(__file__).resolve().parent.parent
    source_db = source_db.resolve()
    if not source_db.is_file():
        raise FileNotFoundError(f"Source DB does not exist: {source_db}")

    src_stamp_before = file_stamp(source_db)
    wal_stamp_before = file_stamp(Path(f"{source_db}-wal"))

    target_dir.mkdir(parents=True, exist_ok=True)
    dst_db_path = target_dir / "app.db"
    now_ts = int(datetime.now(timezone.utc).timestamp())

    src_uri = f"file:{source_db}?mode=ro"
    src = sqlite3.connect(src_uri, uri=True, timeout=30.0)
    src.execute("PRAGMA query_only = ON;")

    dst = sqlite3.connect(dst_db_path, timeout=30.0)
    dst.execute("PRAGMA journal_mode = WAL;")
    dst.execute("PRAGMA synchronous = NORMAL;")
    apply_waku_schema(dst, repo_root, now_ts)

    # 1. Migrate folders -> projects
    folder_rows = src.execute(
        """SELECT id, name, path, created_at, deleted_at, sort_order
           FROM folder
           ORDER BY (deleted_at IS NOT NULL) ASC, id ASC"""
    ).fetchall()

    path_to_proj_id: dict[str, str] = {}
    proj_id_to_path: dict[str, str] = {}
    folder_id_to_proj_id: dict[int, str] = {}
    next_position = 0

    with dst:
        default_path = str(repo_root)
        default_proj_id = uid(f"project-path:{default_path}")
        path_to_proj_id[default_path] = default_proj_id
        proj_id_to_path[default_proj_id] = default_path
        dst.execute(
            """INSERT INTO projects(id, name, path, position, created_at)
               VALUES(?, ?, ?, ?, ?)
               ON CONFLICT(id) DO UPDATE SET name = excluded.name, path = excluded.path""",
            (default_proj_id, "AgentOS (Unassigned)", default_path, next_position, now_ts - 86400),
        )
        next_position += 1

        for fid, fname, fpath, fcreated, _fdeleted, _sort_order in folder_rows:
            clean_path = (fpath or f"/Volumes/data/Codex/AgentOS/legacy-folder-{fid}").strip()
            if clean_path in path_to_proj_id:
                pid = path_to_proj_id[clean_path]
                folder_id_to_proj_id[fid] = pid
                continue
            pid = uid(f"project-path:{clean_path}")
            path_to_proj_id[clean_path] = pid
            proj_id_to_path[pid] = clean_path
            folder_id_to_proj_id[fid] = pid
            proj_name = (fname or Path(clean_path).name or f"Folder {fid}").strip()
            created_ts = parse_ts(fcreated, now_ts - 86400)
            dst.execute(
                """INSERT INTO projects(id, name, path, position, created_at)
                   VALUES(?, ?, ?, ?, ?)
                   ON CONFLICT(id) DO UPDATE SET
                       name = excluded.name,
                       path = excluded.path""",
                (pid, proj_name, clean_path, next_position, created_ts),
            )
            next_position += 1

    # 2. Load conversations & handoffs metadata
    conv_rows = src.execute(
        """SELECT id, folder_id, title, agent_type, status, model, external_id,
                  message_count, created_at, updated_at, deleted_at, origin_cwd
           FROM conversation
           ORDER BY id ASC"""
    ).fetchall()
    conv_by_id = {row[0]: row for row in conv_rows}

    handoff_by_conv: dict[int, tuple] = {}
    for cid, rev, arch_json, retired_json, ctx_prefix in src.execute(
        "SELECT conversation_id, revision, archive_json, retired_sessions_json, context_prefix FROM conversation_handoff"
    ).fetchall():
        handoff_by_conv[cid] = (rev, arch_json, retired_json, ctx_prefix)

    # Accounting counters
    total_messages = 0
    total_activities = 0
    total_turns = 0
    total_message_utf8_bytes = 0
    total_details_utf8_bytes = 0
    preserved_native_cursors = 0
    migrated_parked_cursor_sessions = 0
    migrated_parked_cursors_total = 0
    archive_recovered_sessions = 0
    placeholder_empty_sessions = 0
    active_conversations = 0
    soft_deleted_conversations = 0
    provider_counts: dict[str, int] = {}
    truncation_records: list[dict] = []
    total_truncated_arg_fields = 0
    total_truncated_out_fields = 0

    processed_conv_ids: set[int] = set()
    conv_id_to_sess_id: dict[int, str] = {}
    conv_id_to_proj_path: dict[int, str] = {}

    def persist_conversation(
        conv_row: tuple,
        msgs: list[dict],
        blocks: list[dict],
        turns: list[dict],
        ev_stats: dict,
    ) -> None:
        nonlocal total_messages, total_activities, total_turns
        nonlocal total_message_utf8_bytes, total_details_utf8_bytes
        nonlocal preserved_native_cursors, migrated_parked_cursor_sessions
        nonlocal migrated_parked_cursors_total, archive_recovered_sessions
        nonlocal placeholder_empty_sessions, active_conversations, soft_deleted_conversations
        nonlocal total_truncated_arg_fields, total_truncated_out_fields

        (
            cid,
            fid,
            raw_title,
            agent_type,
            _status,
            raw_model,
            external_id,
            msg_cnt,
            c_at,
            u_at,
            d_at,
            origin_cwd,
        ) = conv_row

        sess_id = uid(f"legacy-conv:{cid}")
        conv_id_to_sess_id[cid] = sess_id

        if d_at is None:
            active_conversations += 1
        else:
            soft_deleted_conversations += 1

        proj_id = folder_id_to_proj_id.get(fid)
        if not proj_id and isinstance(origin_cwd, str) and origin_cwd.strip() in path_to_proj_id:
            proj_id = path_to_proj_id[origin_cwd.strip()]
        if not proj_id:
            proj_id = default_proj_id
        conv_id_to_proj_path[cid] = proj_id_to_path.get(proj_id, default_path)

        provider = map_provider(agent_type)
        provider_counts[provider] = provider_counts.get(provider, 0) + 1
        model = (raw_model or "").strip() or default_model_for_provider(provider)

        created_ts = parse_ts(c_at, now_ts - 86400)
        updated_ts = max(parse_ts(u_at, created_ts), created_ts)
        clean_title = (raw_title or f"会话 #{cid}").strip()
        if d_at is not None and not clean_title.startswith("[归档]"):
            clean_title = f"[归档] {clean_title}"

        native_cursor = make_cursor(provider, external_id)
        if native_cursor is not None:
            preserved_native_cursors += 1

        handoff = handoff_by_conv.get(cid)
        parked_cursors: list[dict] = []
        if handoff is not None:
            _rev, arch_json, retired_json, _ctx_prefix = handoff
            parked_cursors = parse_parked_cursors(
                retired_json, provider, native_cursor, updated_ts
            )
            if parked_cursors:
                migrated_parked_cursor_sessions += 1
                migrated_parked_cursors_total += len(parked_cursors)
            if not msgs and arch_json:
                msgs, turns = recover_from_handoff_archive(cid, arch_json, created_ts)
                if msgs:
                    archive_recovered_sessions += 1

        # Guarantee `AgentSession::has_started()` is true even for metadata-only rows.
        if not msgs and not turns and native_cursor is None:
            placeholder_empty_sessions += 1
            t_id = uid(f"c{cid}-placeholder-t1")
            turns = [
                {
                    "id": t_id,
                    "turn_count": 1,
                    "status": "completed",
                    "provider_turn_started": True,
                    "started_at": created_ts,
                    "completed_at": updated_ts,
                    "checkpoint": None,
                }
            ]
            msgs = [
                {
                    "id": uid(f"c{cid}-placeholder-m0"),
                    "turn_id": t_id,
                    "role": "assistant",
                    "content": (
                        f"（历史归档记录 `conv:{cid}` · 标题：{clean_title} · "
                        f"原始统计消息数：{int(msg_cnt or 0)} · 模型：{model}）"
                    ),
                    "created_at": created_ts,
                    "streaming": False,
                }
            ]

        ev_count = ev_stats["event_count"]
        trunc_args = ev_stats["truncated_arg_fields"]
        trunc_outs = ev_stats["truncated_out_fields"]
        total_truncated_arg_fields += trunc_args
        total_truncated_out_fields += trunc_outs
        if trunc_args > 0 or trunc_outs > 0:
            truncation_records.append(
                {
                    "legacy_conv_id": cid,
                    "session_id": sess_id,
                    "title": clean_title[:120],
                    "event_count": ev_count,
                    "truncated_arg_fields": trunc_args,
                    "truncated_out_fields": trunc_outs,
                    "original_tool_chars": ev_stats["orig_tool_chars"],
                    "kept_tool_chars": ev_stats["kept_tool_chars"],
                }
            )

        act_count = sum(len(b["content"]["data"]) for b in blocks)
        total_messages += len(msgs)
        total_activities += act_count
        total_turns += len(turns)

        sess_obj = {
            "id": sess_id,
            "title": "New task",
            "auto_title": clean_title[:240],
            "project_id": proj_id,
            "provider": provider,
            "model": model,
            "runtime_mode": "fullAccess",
            "status": "idle",
            "created_at": created_ts,
            "updated_at": updated_ts,
            "last_reply_at": updated_ts,
            "provider_cursor": native_cursor,
            "parked_cursors": parked_cursors,
            "context_usage": (
                {"tokens": min(ev_count * 18, 250000), "window": 272000}
                if ev_count > 0
                else None
            ),
            "transcript_blocks": blocks,
            "turns": turns,
        }
        details_json = json.dumps(sess_obj, ensure_ascii=False)
        total_details_utf8_bytes += len(details_json.encode("utf-8"))

        dst.execute(
            """INSERT INTO sessions(
                id, project_id, title, auto_title, provider, model, status,
                created_at, updated_at, last_reply_at
            ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                project_id = excluded.project_id,
                title = excluded.title,
                auto_title = excluded.auto_title,
                provider = excluded.provider,
                model = excluded.model,
                status = excluded.status,
                created_at = excluded.created_at,
                updated_at = excluded.updated_at,
                last_reply_at = excluded.last_reply_at""",
            (
                sess_id,
                proj_id,
                "New task",
                sess_obj["auto_title"],
                provider,
                model,
                "idle",
                created_ts,
                updated_ts,
                updated_ts,
            ),
        )
        dst.execute(
            """INSERT INTO session_details(session_id, data) VALUES(?, ?)
               ON CONFLICT(session_id) DO UPDATE SET data = excluded.data""",
            (sess_id, details_json),
        )
        dst.execute("DELETE FROM messages WHERE session_id = ?", (sess_id,))
        if msgs:
            msg_tuples = []
            for pos, m in enumerate(msgs):
                content_str = m["content"]
                total_message_utf8_bytes += len(content_str.encode("utf-8"))
                msg_tuples.append(
                    (
                        m["id"],
                        sess_id,
                        m["turn_id"],
                        pos,
                        m["role"],
                        content_str,
                        None,
                        "[]",
                        m["created_at"],
                        0,
                    )
                )
            dst.executemany(
                """INSERT INTO messages(
                    id, session_id, turn_id, position, role, content,
                    display_content, attachments, created_at, streaming
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                msg_tuples,
            )
        dst.execute(
            """INSERT INTO agentos_legacy_session_map(legacy_id, session_id)
               VALUES(?, ?)
               ON CONFLICT(legacy_id) DO UPDATE SET session_id = excluded.session_id""",
            (cid, sess_id),
        )

    # 3. Stream all `memory_event` rows grouped by `task_ref` in a single ordered scan
    ev_cursor = src.execute(
        """SELECT task_ref, task_seq, kind, role, payload, call_id, created_at
           FROM memory_event
           WHERE task_ref LIKE 'conv:%'
           ORDER BY task_ref ASC, task_seq ASC"""
    )

    empty_ev_stats = {
        "event_count": 0,
        "truncated_arg_fields": 0,
        "truncated_out_fields": 0,
        "orig_tool_chars": 0,
        "kept_tool_chars": 0,
    }

    with dst:
        for task_ref, group_iter in groupby(ev_cursor, key=lambda r: r[0]):
            try:
                cid = int(str(task_ref).split(":", 1)[1])
            except Exception:
                for _ in group_iter:
                    pass
                continue
            conv_row = conv_by_id.get(cid)
            if conv_row is None:
                for _ in group_iter:
                    pass
                continue
            created_ts = parse_ts(conv_row[8], now_ts - 86400)
            msgs, blocks, turns, ev_stats = convert_events(cid, group_iter, created_ts)
            persist_conversation(conv_row, msgs, blocks, turns, ev_stats)
            processed_conv_ids.add(cid)

        # Persist remaining conversations that had 0 rows in `memory_event`
        for conv_row in conv_rows:
            cid = conv_row[0]
            if cid in processed_conv_ids:
                continue
            persist_conversation(conv_row, [], [], [], empty_ev_stats)
            processed_conv_ids.add(cid)

    # 4. Migrate memory notes (active heads), checkpoints, and project knowledge
    migrated_notes = 0
    migrated_checkpoints = 0
    migrated_knowledge = 0

    def resolve_task_ref(task_ref: str | None) -> tuple[str, str]:
        if isinstance(task_ref, str) and task_ref.startswith("conv:"):
            try:
                cid = int(task_ref.split(":", 1)[1])
                if cid in conv_id_to_sess_id:
                    return conv_id_to_sess_id[cid], conv_id_to_proj_path[cid]
            except Exception:
                pass
        fallback_sid = uid(f"legacy-task:{task_ref or 'unknown'}")
        return fallback_sid, default_path

    with dst:
        note_rows = src.execute(
            """SELECT h.task_ref, h.note_id, h.scope, h.revision, h.updated_at,
                      n.body, n.source_refs
               FROM memory_note_head h
               JOIN memory_note n
                 ON n.task_ref = h.task_ref
                AND n.note_id = h.note_id
                AND n.scope = h.scope
                AND n.revision = h.revision"""
        ).fetchall()
        for task_ref, note_id, scope, revision, u_at, body, src_refs in note_rows:
            sid, ppath = resolve_task_ref(task_ref)
            dst.execute(
                """INSERT INTO agentos_memory_notes(
                    session_id, project_path, note_id, scope, revision, body, source_refs_json, updated_at
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(session_id, note_id, scope) DO UPDATE SET
                    project_path = excluded.project_path,
                    revision = excluded.revision,
                    body = excluded.body,
                    source_refs_json = excluded.source_refs_json,
                    updated_at = excluded.updated_at""",
                (
                    sid,
                    ppath,
                    note_id,
                    scope or "task_shared",
                    int(revision or 1),
                    body or "",
                    src_refs or "[]",
                    parse_ts(u_at, now_ts),
                ),
            )
            migrated_notes += 1

        cp_rows = src.execute(
            "SELECT checkpoint_id, task_ref, payload, created_at FROM memory_checkpoint"
        ).fetchall()
        for cp_id, task_ref, payload, c_at in cp_rows:
            sid, ppath = resolve_task_ref(task_ref)
            dst.execute(
                """INSERT INTO agentos_memory_checkpoints(
                    checkpoint_id, session_id, project_path, payload_json, created_at
                ) VALUES(?, ?, ?, ?, ?)
                ON CONFLICT(checkpoint_id) DO UPDATE SET
                    session_id = excluded.session_id,
                    project_path = excluded.project_path,
                    payload_json = excluded.payload_json,
                    created_at = excluded.created_at""",
                (cp_id, sid, ppath, payload or "{}", parse_ts(c_at, now_ts)),
            )
            migrated_checkpoints += 1

        kn_rows = src.execute(
            """SELECT knowledge_id, scope, project_id, task_ref, claim,
                      applicability, source_refs, status, created_at
               FROM memory_knowledge"""
        ).fetchall()
        for kn_id, scope, fid, task_ref, claim, app_json, src_refs, status, c_at in kn_rows:
            sid, ppath = resolve_task_ref(task_ref)
            if isinstance(fid, int) and fid in folder_id_to_proj_id:
                ppath = proj_id_to_path.get(folder_id_to_proj_id[fid], ppath)
            dst.execute(
                """INSERT INTO agentos_memory_knowledge(
                    knowledge_id, scope, project_path, session_id, claim,
                    applicability_json, evidence_json, status, created_at
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(knowledge_id) DO UPDATE SET
                    scope = excluded.scope,
                    project_path = excluded.project_path,
                    session_id = excluded.session_id,
                    claim = excluded.claim,
                    applicability_json = excluded.applicability_json,
                    evidence_json = excluded.evidence_json,
                    status = excluded.status,
                    created_at = excluded.created_at""",
                (
                    kn_id,
                    scope or "project",
                    ppath,
                    sid,
                    claim or "",
                    app_json or "{}",
                    src_refs or "[]",
                    status or "candidate",
                    parse_ts(c_at, now_ts),
                ),
            )
            migrated_knowledge += 1

    # Query exact target table counts
    dst_counts = {
        "projects": dst.execute("SELECT count(*) FROM projects").fetchone()[0],
        "sessions": dst.execute("SELECT count(*) FROM sessions").fetchone()[0],
        "session_details": dst.execute("SELECT count(*) FROM session_details").fetchone()[0],
        "messages": dst.execute("SELECT count(*) FROM messages").fetchone()[0],
        "agentos_memory_notes": dst.execute(
            "SELECT count(*) FROM agentos_memory_notes"
        ).fetchone()[0],
        "agentos_memory_checkpoints": dst.execute(
            "SELECT count(*) FROM agentos_memory_checkpoints"
        ).fetchone()[0],
        "agentos_memory_knowledge": dst.execute(
            "SELECT count(*) FROM agentos_memory_knowledge"
        ).fetchone()[0],
        "agentos_legacy_session_map": dst.execute(
            "SELECT count(*) FROM agentos_legacy_session_map"
        ).fetchone()[0],
    }

    dst.execute("PRAGMA wal_checkpoint(TRUNCATE);")
    dst.close()
    src.close()

    src_stamp_after = file_stamp(source_db)
    wal_stamp_after = file_stamp(Path(f"{source_db}-wal"))
    source_zero_modified = (
        src_stamp_before == src_stamp_after and wal_stamp_before == wal_stamp_after
    )
    if not source_zero_modified:
        raise RuntimeError(
            f"Source DB stamp changed unexpectedly: before={src_stamp_before}, after={src_stamp_after}"
        )

    truncation_records.sort(
        key=lambda r: r["original_tool_chars"] - r["kept_tool_chars"], reverse=True
    )

    report = {
        "source_db": str(source_db),
        "source_uri_mode": "ro",
        "source_stamp_before": src_stamp_before,
        "source_stamp_after": src_stamp_after,
        "source_zero_modified": source_zero_modified,
        "target_db": str(dst_db_path),
        "target_db_bytes": dst_db_path.stat().st_size,
        "elapsed_seconds": round(time.time() - t0, 3),
        "source_counts": {
            "folders": len(folder_rows),
            "conversations_total": len(conv_rows),
            "conversations_active": active_conversations,
            "conversations_archived": soft_deleted_conversations,
            "conversation_handoffs": len(handoff_by_conv),
            "memory_note_heads": migrated_notes,
            "memory_checkpoints": migrated_checkpoints,
            "memory_knowledge": migrated_knowledge,
        },
        "target_counts": dst_counts,
        "fidelity": {
            "provider_distribution": provider_counts,
            "preserved_native_resume_cursors": preserved_native_cursors,
            "migrated_parked_cursor_sessions": migrated_parked_cursor_sessions,
            "migrated_parked_cursors_total": migrated_parked_cursors_total,
            "archive_recovered_sessions": archive_recovered_sessions,
            "placeholder_metadata_only_sessions": placeholder_empty_sessions,
            "total_turns": total_turns,
            "total_tool_activities": total_activities,
            "total_message_utf8_bytes": total_message_utf8_bytes,
            "total_session_details_utf8_bytes": total_details_utf8_bytes,
            "messages_truncated_count": 0,
            "tool_preview_truncation": {
                "arg_char_limit": TOOL_ARG_PREVIEW_LIMIT,
                "output_char_limit": TOOL_OUT_PREVIEW_LIMIT,
                "sessions_with_capped_tool_previews": len(truncation_records),
                "capped_tool_arg_fields": total_truncated_arg_fields,
                "capped_tool_output_fields": total_truncated_out_fields,
                "top_20_sessions": truncation_records[:20],
            },
        },
    }

    if report_path is not None:
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(
            json.dumps(report, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
        )

    return report


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Idempotent read-only migration from legacy codeg SQLite DB to AgentOS Waku"
    )
    parser.add_argument(
        "--source",
        required=True,
        type=Path,
        help="Path to legacy codeg SQLite DB (opened strictly with ?mode=ro)",
    )
    parser.add_argument(
        "--target-dir",
        required=True,
        type=Path,
        help="Target AgentOS Waku data directory containing app.db",
    )
    parser.add_argument(
        "--report",
        type=Path,
        default=None,
        help="Optional path to write JSON reconciliation report",
    )
    args = parser.parse_args()

    report = run_migration(args.source, args.target_dir, args.report)
    print(
        json.dumps(
            {
                "elapsed_seconds": report["elapsed_seconds"],
                "source_zero_modified": report["source_zero_modified"],
                "target_db_mb": round(report["target_db_bytes"] / 1048576.0, 2),
                "source_counts": report["source_counts"],
                "target_counts": report["target_counts"],
                "messages_truncated_count": report["fidelity"]["messages_truncated_count"],
                "preserved_native_resume_cursors": report["fidelity"][
                    "preserved_native_resume_cursors"
                ],
                "migrated_parked_cursors_total": report["fidelity"][
                    "migrated_parked_cursors_total"
                ],
            },
            indent=2,
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
