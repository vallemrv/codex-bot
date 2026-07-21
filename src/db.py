"""
SQLite persistence for claude-bot.

Claude persists the real conversation state on disk (~/.claude/projects/...),
so we only track:
  - the active session pointer (directory + claude_session_id + model)
  - per-session model preference, so reactivating a session restores its model

Session discovery (listing sessions per project) uses the Agent SDK's native
list_sessions(), so we don't duplicate that here.
"""

import sqlite3
import time
from pathlib import Path

DB_PATH = Path(__file__).parent.parent / "bot.db"


def _conn() -> sqlite3.Connection:
    # timeout: wait (instead of raising "database is locked") when a concurrent
    # writer holds the lock — the bot fires DB ops from the queue + callbacks.
    con = sqlite3.connect(DB_PATH, timeout=10)
    con.row_factory = sqlite3.Row
    # WAL lets readers and a writer coexist without blocking each other.
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA busy_timeout=10000")
    return con


def init() -> None:
    with _conn() as con:
        con.execute("""
            CREATE TABLE IF NOT EXISTS active (
                id                INTEGER PRIMARY KEY CHECK (id = 1),
                directory         TEXT NOT NULL,
                claude_session_id TEXT,
                model             TEXT,
                effort            TEXT
            )
        """)
        con.execute("""
            CREATE TABLE IF NOT EXISTS session_meta (
                claude_session_id TEXT PRIMARY KEY,
                directory         TEXT NOT NULL,
                model             TEXT,
                title             TEXT,
                created_at        REAL,
                effort            TEXT
            )
        """)
        # Idempotent column additions for existing DBs (SQLite has no ADD COLUMN IF NOT EXISTS).
        for table, col in [("active", "effort"), ("session_meta", "effort")]:
            try:
                con.execute(f"ALTER TABLE {table} ADD COLUMN {col} TEXT")
            except Exception:  # noqa: BLE001
                pass  # column already exists
        # Persisted callback keystore: maps the small ints embedded in
        # callback_data to their real string value. Persisting it means inline
        # buttons from before a restart still resolve (no silent "" surprises).
        con.execute("""
            CREATE TABLE IF NOT EXISTS keystore (
                k     INTEGER PRIMARY KEY,
                value TEXT NOT NULL UNIQUE
            )
        """)
        # In-flight task ledger: a row exists while a prompt is running so a
        # restart can detect tasks that were interrupted mid-flight and clean
        # up their orphaned status messages.
        con.execute("""
            CREATE TABLE IF NOT EXISTS inflight (
                skey       TEXT PRIMARY KEY,
                directory  TEXT NOT NULL,
                model      TEXT,
                prompt     TEXT,
                msg_id     INTEGER,
                started_at REAL
            )
        """)
        # Generic catalog cache (models_catalog persists the live /v1/models
        # response here, so picker buttons survive a restart and offline starts).
        con.execute("""
            CREATE TABLE IF NOT EXISTS catalog_cache (
                key        TEXT PRIMARY KEY,
                value      TEXT NOT NULL,
                updated_at REAL NOT NULL
            )
        """)


# --------------------------------------------------------------------------- #
# Active session pointer
# --------------------------------------------------------------------------- #
def get_active() -> dict | None:
    """Return {directory, claude_session_id, model, effort} or None."""
    with _conn() as con:
        row = con.execute(
            "SELECT directory, claude_session_id, model, effort FROM active WHERE id = 1"
        ).fetchone()
        return dict(row) if row else None


def set_active(directory: str, claude_session_id: str | None, model: str | None,
               effort: str | None = None) -> None:
    with _conn() as con:
        con.execute("""
            INSERT INTO active (id, directory, claude_session_id, model, effort)
            VALUES (1, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                directory = excluded.directory,
                claude_session_id = excluded.claude_session_id,
                model = excluded.model,
                effort = excluded.effort
        """, (directory, claude_session_id, model, effort))


def update_active_session_id(claude_session_id: str) -> None:
    """Fill in the session id once Claude creates it on the first prompt."""
    with _conn() as con:
        con.execute(
            "UPDATE active SET claude_session_id = ? WHERE id = 1",
            (claude_session_id,),
        )


def update_active_effort(effort: str | None) -> None:
    """Update only the effort on the active pointer."""
    with _conn() as con:
        con.execute("UPDATE active SET effort = ? WHERE id = 1", (effort,))


def clear_active() -> None:
    with _conn() as con:
        con.execute("DELETE FROM active WHERE id = 1")


# --------------------------------------------------------------------------- #
# Per-session metadata (model + title)
# --------------------------------------------------------------------------- #
def remember_session(claude_session_id: str, directory: str,
                     model: str | None, title: str | None) -> None:
    if not claude_session_id:
        return
    with _conn() as con:
        existing = con.execute(
            "SELECT title FROM session_meta WHERE claude_session_id = ?",
            (claude_session_id,),
        ).fetchone()
        # Keep the first title we recorded for this session.
        keep_title = (existing["title"] if existing and existing["title"] else title)
        con.execute("""
            INSERT INTO session_meta (claude_session_id, directory, model, title, created_at)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(claude_session_id) DO UPDATE SET
                directory = excluded.directory,
                model = excluded.model,
                title = ?
        """, (claude_session_id, directory, model, keep_title, time.time(), keep_title))


def get_session_meta(claude_session_id: str) -> dict | None:
    with _conn() as con:
        row = con.execute(
            "SELECT claude_session_id, directory, model, title, effort FROM session_meta "
            "WHERE claude_session_id = ?",
            (claude_session_id,),
        ).fetchone()
        return dict(row) if row else None


def set_session_model(claude_session_id: str, model: str) -> None:
    with _conn() as con:
        con.execute(
            "UPDATE session_meta SET model = ? WHERE claude_session_id = ?",
            (model, claude_session_id),
        )


def set_session_effort(claude_session_id: str, effort: str | None) -> None:
    with _conn() as con:
        con.execute(
            "UPDATE session_meta SET effort = ? WHERE claude_session_id = ?",
            (effort, claude_session_id),
        )


def set_session_title(claude_session_id: str, title: str) -> None:
    """Set a custom title for a session (upsert; survives later prompts)."""
    if not claude_session_id:
        return
    with _conn() as con:
        con.execute("""
            INSERT INTO session_meta (claude_session_id, directory, model, title, created_at)
            VALUES (?, '', NULL, ?, ?)
            ON CONFLICT(claude_session_id) DO UPDATE SET title = excluded.title
        """, (claude_session_id, title, time.time()))


def forget_session(claude_session_id: str) -> None:
    with _conn() as con:
        con.execute(
            "DELETE FROM session_meta WHERE claude_session_id = ?",
            (claude_session_id,),
        )


# --------------------------------------------------------------------------- #
# Persisted callback keystore (compresses long callback_data values to ints)
# --------------------------------------------------------------------------- #
def load_keystore() -> dict[int, str]:
    """Return the whole keystore as {k: value}. Called once at startup."""
    with _conn() as con:
        rows = con.execute("SELECT k, value FROM keystore").fetchall()
        return {row["k"]: row["value"] for row in rows}


def keystore_put(k: int, value: str) -> None:
    with _conn() as con:
        con.execute(
            "INSERT OR IGNORE INTO keystore (k, value) VALUES (?, ?)",
            (k, value),
        )


# --------------------------------------------------------------------------- #
# In-flight task ledger (survives restarts → orphan detection)
# --------------------------------------------------------------------------- #
def inflight_add(skey: str, directory: str, model: str | None,
                 prompt: str | None, msg_id: int | None) -> None:
    with _conn() as con:
        con.execute("""
            INSERT INTO inflight (skey, directory, model, prompt, msg_id, started_at)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(skey) DO UPDATE SET
                directory = excluded.directory,
                model = excluded.model,
                prompt = excluded.prompt,
                msg_id = excluded.msg_id,
                started_at = excluded.started_at
        """, (skey, directory, model, prompt, msg_id, time.time()))


def inflight_remove(skey: str) -> None:
    with _conn() as con:
        con.execute("DELETE FROM inflight WHERE skey = ?", (skey,))


def inflight_all() -> list[dict]:
    with _conn() as con:
        rows = con.execute(
            "SELECT skey, directory, model, prompt, msg_id, started_at FROM inflight"
        ).fetchall()
        return [dict(r) for r in rows]


def inflight_clear() -> None:
    with _conn() as con:
        con.execute("DELETE FROM inflight")


# --------------------------------------------------------------------------- #
# Generic catalog cache (currently: live models list from /v1/models)
# --------------------------------------------------------------------------- #
def catalog_get(key: str) -> dict | None:
    with _conn() as con:
        row = con.execute(
            "SELECT value, updated_at FROM catalog_cache WHERE key = ?", (key,)
        ).fetchone()
        return dict(row) if row else None


def catalog_set(key: str, value: str) -> None:
    with _conn() as con:
        con.execute("""
            INSERT INTO catalog_cache (key, value, updated_at) VALUES (?, ?, ?)
            ON CONFLICT(key) DO UPDATE SET
                value = excluded.value,
                updated_at = excluded.updated_at
        """, (key, value, time.time()))
