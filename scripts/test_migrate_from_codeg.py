#!/usr/bin/env python3
"""Unit tests for migrate-from-codeg.py deduplication and --preserve-waku-tail."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest
import uuid


SCRIPT_PATH = Path(__file__).resolve().parent / "migrate-from-codeg.py"
SPEC = importlib.util.spec_from_file_location("migrate_from_codeg", SCRIPT_PATH)
assert SPEC and SPEC.loader
MOD = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MOD)


def create_minimal_codeg_db(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.executescript(
        """
        CREATE TABLE folder (
            id INTEGER PRIMARY KEY,
            name TEXT,
            path TEXT,
            created_at TEXT,
            deleted_at TEXT,
            sort_order INTEGER DEFAULT 0,
            kind TEXT DEFAULT 'regular',
            alias TEXT,
            is_open INTEGER DEFAULT 1
        );
        CREATE TABLE conversation (
            id INTEGER PRIMARY KEY,
            folder_id INTEGER,
            title TEXT,
            agent_type TEXT,
            status TEXT,
            model TEXT,
            external_id TEXT,
            message_count INTEGER DEFAULT 0,
            created_at TEXT,
            updated_at TEXT,
            deleted_at TEXT,
            origin_cwd TEXT
        );
        CREATE TABLE conversation_handoff (
            conversation_id INTEGER PRIMARY KEY,
            revision INTEGER,
            archive_json TEXT,
            retired_sessions_json TEXT,
            context_prefix TEXT
        );
        CREATE TABLE memory_event (
            event_id TEXT,
            task_ref TEXT,
            task_seq INTEGER,
            source_session TEXT,
            source_event_id TEXT,
            source_revision INTEGER DEFAULT 0,
            kind TEXT,
            role TEXT,
            payload TEXT,
            call_id TEXT,
            created_at TEXT
        );
        CREATE TABLE memory_note_head (
            task_ref TEXT, note_id TEXT, scope TEXT, revision INTEGER, updated_at TEXT
        );
        CREATE TABLE memory_note (
            task_ref TEXT, note_id TEXT, scope TEXT, revision INTEGER, body TEXT, source_refs TEXT
        );
        CREATE TABLE memory_checkpoint (
            checkpoint_id TEXT, task_ref TEXT, payload TEXT, created_at TEXT
        );
        CREATE TABLE memory_knowledge (
            knowledge_id TEXT, scope TEXT, project_id INTEGER, task_ref TEXT,
            claim TEXT, applicability TEXT, source_refs TEXT, status TEXT, created_at TEXT
        );
        """
    )
    return conn


class MigrateDedupTests(unittest.TestCase):
    def test_dedups_agy_replays_and_handoff_dups_while_keeping_genuine_repeats(self) -> None:
        rows = [
            # Turn 1: genuine user + tool + assistant
            ("conv:1", 1, "native:antigravity:s1", "agy-0-user", "user_input", "user", '{"text":"继续"}', None, "2026-11-17T08:00:00Z"),
            ("conv:1", 2, "native:antigravity:s1", "agy-1", "tool_call", "assistant", '{"tool":"run_command","input":{"command":"ls"}}', "call-1", "2026-11-17T08:00:01Z"),
            ("conv:1", 3, "native:antigravity:s1", "agy-1", "tool_result", "tool", '{"output":"ok"}', "call-1", "2026-11-17T08:00:02Z"),
            ("conv:1", 4, "native:antigravity:s1", "agy-1", "assistant_message", "assistant", '{"text":"第一轮完成"}', None, "2026-11-17T08:00:03Z"),
            # Turn 2: genuine repeat "继续" with a DIFFERENT source_event_id -> MUST BE KEPT
            ("conv:1", 5, "native:antigravity:s1", "agy-20-user", "user_input", "user", '{"text":"继续"}', None, "2026-11-17T08:05:00Z"),
            ("conv:1", 6, "native:antigravity:s1", "agy-2", "assistant_message", "assistant", '{"text":"第二轮完成"}', None, "2026-11-17T08:05:05Z"),
            # Replay sweep of Turn 1 & Turn 2 with shifted agy-<idx> assistant IDs -> MUST BE DROPPED
            ("conv:1", 10, "native:antigravity:s1", "agy-0-user", "user_input", "user", '{"text":"继续"}', None, "2026-11-17T09:00:00Z"),
            ("conv:1", 11, "native:antigravity:s1", "agy-9", "tool_call", "assistant", '{"tool":"run_command","input":{"command":"ls"}}', "call-1", "2026-11-17T09:00:01Z"),
            ("conv:1", 12, "native:antigravity:s1", "agy-9", "tool_result", "tool", '{"output":"ok"}', "call-1", "2026-11-17T09:00:02Z"),
            ("conv:1", 13, "native:antigravity:s1", "agy-9", "assistant_message", "assistant", '{"text":"第一轮完成"}', None, "2026-11-17T09:00:03Z"),
            ("conv:1", 14, "native:antigravity:s1", "handoff-1-agy-20-user", "user_input", "user", '{"text":"继续"}', None, "2026-11-17T09:00:04Z"),
            ("conv:1", 15, "native:antigravity:s1", "agy-10", "assistant_message", "assistant", '{"text":"第二轮完成"}', None, "2026-11-17T09:00:05Z"),
            # Turn 3: brand-new turn after replay -> MUST BE KEPT
            ("conv:1", 16, "native:antigravity:s1", "agy-40-user", "user_input", "user", '{"text":"最后一步"}', None, "2026-11-17T09:10:00Z"),
            ("conv:1", 17, "native:antigravity:s1", "agy-11", "assistant_message", "assistant", '{"text":"全部完成"}', None, "2026-11-17T09:10:05Z"),
        ]
        msgs, blocks, turns, stats = MOD.convert_events(1, rows, 1700000000)
        self.assertEqual([m["role"] for m in msgs], ["user", "assistant", "user", "assistant", "user", "assistant"])
        self.assertEqual([m["content"] for m in msgs], ["继续", "第一轮完成", "继续", "第二轮完成", "最后一步", "全部完成"])
        self.assertEqual(len(turns), 3)
        self.assertEqual(len(blocks), 1)
        self.assertEqual(len(blocks[0]["content"]["data"]), 1)
        self.assertEqual(stats["deduplicated_events"], 6)

    def test_preserve_waku_tail_keeps_v4_turns_messages_blocks_and_runtime_mode(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            src_db = root / "codeg.db"
            dst_dir = root / "waku-data"
            conn = create_minimal_codeg_db(src_db)
            conn.execute(
                "INSERT INTO folder(id, name, path, created_at, is_open) VALUES (1, 'demo', '/tmp/demo-proj', '2026-11-17T00:00:00Z', 1)"
            )
            conn.execute(
                """INSERT INTO conversation(id, folder_id, title, agent_type, status, model, external_id, created_at, updated_at)
                   VALUES (10, 1, 'Original Title', 'antigravity', 'idle', 'gemini-4-neon', 'ext-10', '2026-11-17T00:00:00Z', '2026-11-18T00:00:00Z')"""
            )
            # Duplicate events in source
            for seq, ts in [(1, "2026-11-17T01:00:00Z"), (2, "2026-11-17T02:00:00Z")]:
                conn.execute(
                    """INSERT INTO memory_event(task_ref, task_seq, source_session, source_event_id, kind, role, payload, created_at)
                       VALUES ('conv:10', ?, 'native:antigravity:ext-10', 'agy-0-user', 'user_input', 'user', '{"text":"hello"}', ?)""",
                    (seq * 10, ts),
                )
                conn.execute(
                    """INSERT INTO memory_event(task_ref, task_seq, source_session, source_event_id, kind, role, payload, created_at)
                       VALUES ('conv:10', ?, 'native:antigravity:ext-10', ?, 'assistant_message', 'assistant', '{"text":"world"}', ?)""",
                    (seq * 10 + 1, f"agy-{seq}", ts),
                )
            conn.commit()
            conn.close()

            # Initial migration
            MOD.run_migration(src_db, dst_dir, None, active_only=False)

            # Simulate user chatting in Waku: append a UUIDv4 turn, 2 UUIDv4 messages, 1 shifted block, and switch to fullAccess
            dst = sqlite3.connect(dst_dir / "app.db")
            sess_id = MOD.uid("legacy-conv:10")
            raw_details = json.loads(
                dst.execute("SELECT data FROM session_details WHERE session_id = ?", (sess_id,)).fetchone()[0]
            )
            # Artificially inflate old migrated messages to simulate pre-dedup DB (4 v5 msgs instead of 2)
            fake_v5_tid = MOD.uid("c10-t99")
            for idx, (role, text) in enumerate([("user", "dup"), ("assistant", "dup")], start=2):
                dst.execute(
                    """INSERT INTO messages(id, session_id, turn_id, position, role, content, attachments, created_at, streaming)
                       VALUES (?, ?, ?, ?, ?, ?, '[]', 1700000100, 0)""",
                    (MOD.uid(f"c10-fake-{idx}"), sess_id, fake_v5_tid, idx, role, text),
                )
            v4_turn_id = str(uuid.uuid4())
            v4_u_id = str(uuid.uuid4())
            v4_a_id = str(uuid.uuid4())
            dst.execute(
                """INSERT INTO messages(id, session_id, turn_id, position, role, content, attachments, created_at, streaming)
                   VALUES (?, ?, ?, 4, 'user', 'Waku native follow-up', '[]', 1795182000, 0)""",
                (v4_u_id, sess_id, v4_turn_id),
            )
            dst.execute(
                """INSERT INTO messages(id, session_id, turn_id, position, role, content, attachments, created_at, streaming, provider)
                   VALUES (?, ?, ?, 5, 'assistant', 'Waku native reply', '[]', 1795182010, 0, 'antigravity')""",
                (v4_a_id, sess_id, v4_turn_id),
            )
            raw_details["runtime_mode"] = "fullAccess"
            raw_details["auto_title"] = "AGY Live Summary Title"
            raw_details["updated_at"] = 1795182010
            raw_details["turns"].append(
                {
                    "id": v4_turn_id,
                    "turn_count": 5,
                    "status": "completed",
                    "provider": "antigravity",
                    "provider_turn_started": True,
                    "started_at": 1795182000,
                    "completed_at": 1795182010,
                    "checkpoint": {"turn_count": 5, "git_ref": "refs/waku/test-turn-5", "status": "ready"},
                }
            )
            raw_details["transcript_blocks"].append(
                {
                    "after_message": 5,
                    "turn_id": v4_turn_id,
                    "content": {"kind": "activities", "data": [{"id": str(uuid.uuid4()), "title": "live-act"}]},
                }
            )
            dst.execute(
                "UPDATE session_details SET data = ? WHERE session_id = ?",
                (json.dumps(raw_details), sess_id),
            )
            dst.commit()
            dst.close()

            # Re-run with --preserve-waku-tail
            report = MOD.run_migration(src_db, dst_dir, None, active_only=False, preserve_waku_tail=True)
            self.assertEqual(report["fidelity"]["preserved_waku_tail_sessions"], 1)
            self.assertEqual(report["fidelity"]["preserved_waku_tail_messages"], 2)

            dst = sqlite3.connect(dst_dir / "app.db")
            msgs = dst.execute(
                "SELECT id, position, role, content FROM messages WHERE session_id = ? ORDER BY position ASC",
                (sess_id,),
            ).fetchall()
            self.assertEqual([m[3] for m in msgs], ["hello", "world", "Waku native follow-up", "Waku native reply"])
            self.assertEqual([m[1] for m in msgs], [0, 1, 2, 3])
            self.assertEqual(msgs[2][0], v4_u_id)
            self.assertEqual(msgs[3][0], v4_a_id)

            updated_details = json.loads(
                dst.execute("SELECT data FROM session_details WHERE session_id = ?", (sess_id,)).fetchone()[0]
            )
            self.assertEqual(updated_details["runtime_mode"], "fullAccess")
            self.assertEqual(updated_details["auto_title"], "AGY Live Summary Title")
            self.assertEqual(updated_details["turns"][-1]["id"], v4_turn_id)
            self.assertEqual(updated_details["turns"][-1]["checkpoint"]["git_ref"], "refs/waku/test-turn-5")
            # after_message shifted from 5 -> 3 (4 old v5 msgs -> 2 deduplicated v5 msgs, delta = -2)
            v4_blks = [b for b in updated_details["transcript_blocks"] if b["turn_id"] == v4_turn_id]
            self.assertEqual(len(v4_blks), 1)
            self.assertEqual(v4_blks[0]["after_message"], 3)
            dst.close()


if __name__ == "__main__":
    unittest.main()
