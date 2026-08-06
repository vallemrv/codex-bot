"""
codex-bot — Telegram remote control for codex-cli.

Architecture:
- Claude Agent SDK replaces the OpenCode server + SSE.
- Each prompt runs a ClaudeSDKClient in a background asyncio.Task; its streamed
  events drive a live status message and, on completion, the final reply.
- Conversation continuity via resume=<claude_session_id> (Claude persists state
  on disk). Session discovery uses the SDK's native list_sessions().
- Single-admin model: only TELEGRAM_ADMIN_ID may use the bot.
"""

import os
import time
import shutil
import asyncio
import logging
import itertools
import contextvars
from pathlib import Path
from collections import deque

from dotenv import load_dotenv
from telegram import (
    Update, InlineKeyboardButton, InlineKeyboardMarkup, BotCommand,
    BotCommandScopeChat, MenuButtonCommands,
)
from telegram.ext import (
    Application, CommandHandler, MessageHandler,
    CallbackQueryHandler, ContextTypes, filters,
)
from telegram.error import BadRequest, RetryAfter, NetworkError

import db
import md2tgv2
import transcription as grok_stt
import codex_client as cc
import gitops

# --------------------------------------------------------------------------- #
# Config
# --------------------------------------------------------------------------- #
load_dotenv(Path(__file__).parent.parent / ".env")
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
# httpx logs full Telegram API URLs, including the bot token.
logging.getLogger("httpx").setLevel(logging.WARNING)
logger = logging.getLogger("codex-bot")

TOKEN     = os.environ["TELEGRAM_BOT_TOKEN"]
ADMIN_ID  = int(os.environ["TELEGRAM_ADMIN_ID"])
WORKSPACE = Path(os.getenv("DEFAULT_WORKSPACE", "~/proyectos")).expanduser()
DEFAULT_PERMISSION_MODE = os.getenv("PERMISSION_MODE", "bypassPermissions")
TASK_TIMEOUT = int(os.getenv("TASK_TIMEOUT", "1800"))
BOT_DIR = str(Path(__file__).parent.parent.resolve())
TMP_DIR = Path("/tmp/codex-bot-media")
# Scratch dir for /tmp throwaway sessions. Under /tmp so it's wiped on reboot,
# and outside WORKSPACE so it never shows up as a project in /open.
SCRATCH_DIR = Path("/tmp/codex-tmp")
RESTART_FLAG = Path("/tmp/codex-bot-restarting.flag")

MCP_SERVER = cc.build_mcp_server()

# Module-level state (single admin → globals are fine)
APP: Application | None = None
PERMISSION_MODE = DEFAULT_PERMISSION_MODE

STATUSES: dict = {}        # skey -> status dict
_FLUSHER_TASK = None       # asyncio.Task — single status-render worker
_STATUS_WAKE = None        # asyncio.Event — pinged when a status needs a redraw
RUNNING: dict = {}         # skey -> {"client", "task", "directory"}
QUEUES: dict = {}          # skey -> deque[{"text","directory","model"}]
MSG2SESS: dict = {}        # bot message_id -> {"skey","directory"}
KNOWN_SID: dict = {}       # skey -> real claude session id once known
KEYSTORE: dict = {}        # int -> str   (compress long strings for callback_data)
KEYSTORE_REV: dict = {}    # str -> int   (inverse index for O(1) _key lookups)
# Ephemeral counter for perm/question ids — NOT persisted; these ids are only
# resolved in-memory via PENDING_PERMS/PENDING_Q, so no keystore entry needed.
_EPHEMERAL_QID = itertools.count(1)
PENDING_PERMS: dict = {}   # qid -> asyncio.Future
PENDING_Q: dict = {}       # qid -> {"future", "options": [str]}
LAST_EDITED: dict = {}     # skey -> {relpath: icon}  (files from most recent run)
UNDO_STACK: dict = {}      # directory -> [snapshot_sha, ...]  (más reciente al final)
REDO_STACK: dict = {}      # directory -> [snapshot_sha, ...]
SEND_MODE = {"on": False, "target": None, "pending_text": None,
             "oneshot": False, "oneshot_pre": False}
MKDIR_PENDING: dict = {}   # {"path","msg_id"}
PENDING_INPUT: dict = {}   # {"kind": "rename"|"btw", "sid", "directory", "msg_id", "ts"}

# Per-task context (cwd, sid, model) propagated to the permission/question
# bridges. A ContextVar isolates this per asyncio.Task, so concurrent sessions
# don't mix up which project a permission/question belongs to.
CURRENT_CTX: contextvars.ContextVar = contextvars.ContextVar("current_ctx", default=None)

# Status rendering is decoupled from the SDK event stream: events only flag the
# status "dirty" (cheap, in-memory) and wake the flush worker; the worker owns the
# actual Telegram edits. It is event-driven, not a fixed grid: it sleeps exactly
# until the next status is actionable and wakes the instant something changes, so a
# discrete event (a new tool after a quiet stretch) paints almost immediately
# instead of waiting out a tick. STATUS_MIN_INTERVAL is the hard floor between
# edits to one message (the real Telegram rate-limit guard, decoupled from
# responsiveness); STATUS_LIVENESS_INTERVAL forces a redraw during silence so the
# elapsed clock keeps ticking even while no events arrive.
STATUS_MIN_INTERVAL = 2.0     # min seconds between edits to one status (rate-limit floor)
STATUS_LIVENESS_INTERVAL = 4  # redraw at least this often so elapsed time stays fresh
# Telegram flood control is *per chat*, not per message: N concurrent live statuses
# editing every STATUS_MIN_INTERVAL would multiply the aggregate edit rate by N and
# trip a flood ban (which then freezes a status for minutes). We scale both intervals
# by the number of live statuses so the aggregate rate stays roughly constant no
# matter how many sessions run at once. With one session this is a no-op.
STATUS_RETRY_CAP = 120        # sanity ceiling for a Telegram RetryAfter backoff (s)
MSG_TRACK_LIMIT = 200
# Cualquier respuesta que no quepa en un solo mensaje de Telegram (~4096) se
# manda como respuesta.md en vez de trocearse en varios. El corte va por debajo
# del límite duro para dejar margen a la cabecera + el escapado de MarkdownV2.
MD_FILE_THRESHOLD = 3500
PENDING_FLOW_TTL = 300  # s — after this, a stale mkdir/question flow is ignored


# --------------------------------------------------------------------------- #
# Auth + key store
# --------------------------------------------------------------------------- #
def admin_only(func):
    async def wrapper(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        user = update.effective_user
        if not user or user.id != ADMIN_ID:
            return
        return await func(update, ctx)
    return wrapper


def _key(value: str) -> int:
    k = KEYSTORE_REV.get(value)
    if k is not None:
        return k
    k = (max(KEYSTORE) + 1) if KEYSTORE else 0
    KEYSTORE[k] = value
    KEYSTORE_REV[value] = k
    try:
        db.keystore_put(k, value)
    except Exception as exc:  # noqa: BLE001
        logger.warning(f"keystore_put failed: {exc}")
    return k


# Sentinel returned by _val for an int that isn't in the keystore (e.g. a
# button from before a wipe). Distinct from "" so callbacks can detect a
# stale/expired menu and tell the user to reopen it instead of acting on "".
KEY_MISSING = "\x00__missing__"


def _val(k: int) -> str:
    return KEYSTORE.get(k, KEY_MISSING)


def _vals(*ks: int) -> list[str] | None:
    """Resolve several keystore ints at once. Returns None if ANY is missing
    (stale callback), so callers can bail out with a single guard."""
    out = []
    for k in ks:
        v = KEYSTORE.get(k, KEY_MISSING)
        if v == KEY_MISSING:
            return None
        out.append(v)
    return out


async def _expired(q) -> None:
    """Tell the user a button no longer resolves (keystore lost its value,
    typically after a data wipe) and that they should reopen the menu."""
    try:
        await q.edit_message_text(
            "⚠️ Este menú ha caducado (el bot perdió su contexto). "
            "Vuelve a abrirlo con /open o el comando correspondiente.")
    except BadRequest:
        await APP.bot.send_message(
            ADMIN_ID,
            "⚠️ Ese botón ha caducado. Vuelve a abrir el menú con /open.")


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
def _skey(directory: str, claude_session_id: str | None) -> str:
    return claude_session_id if claude_session_id else f"new::{directory}"


def _resume_for(skey: str) -> str | None:
    if skey and not skey.startswith("new::"):
        return skey
    return KNOWN_SID.get(skey)


def _canonical_skey(skey: str) -> str:
    """Map a skey to the key under which its task is actually tracked.

    A brand-new session runs its first prompt under the placeholder key
    `new::{dir}`; that's what lands in STATUSES/RUNNING/QUEUES. But once the
    `session` event materializes a real id (and the active pointer is updated to
    it), a later message resolves to that real id instead — which is NOT in
    STATUSES, so the busy-check would miss and dispatch a *second* concurrent
    task for the same session. Resolve the real id back to the live placeholder
    so both are seen as one session (queued, not run in parallel).
    """
    if skey in STATUSES:
        return skey
    for placeholder, real in KNOWN_SID.items():
        if real == skey and placeholder in STATUSES:
            return placeholder
    return skey


def _session_label(s) -> str:
    sid = getattr(s, "session_id", "")
    if sid:
        meta = db.get_session_meta(sid)
        if meta and meta.get("title"):
            return meta["title"]
    return (getattr(s, "custom_title", None) or getattr(s, "summary", None)
            or getattr(s, "first_prompt", None) or sid[:8]
            or "sesión")


def _find_session(sid: str, cwd: str):
    """Return the SDK session object for sid, or None."""
    for s in _list_sessions(directory=cwd):
        if s.session_id == sid:
            return s
    return None


def _context_bar(file_size: int, model: str | None = None) -> str:
    """Rough context-usage indicator from conversation JSONL file size.
    Window depends on the model (1M for opus-1m, else 200K)."""
    window = cc.context_window(model)
    est = file_size // 6
    pct = min(99, est * 100 // window)
    icon = "🟢" if pct < 40 else ("🟡" if pct < 70 else ("🟠" if pct < 90 else "🔴"))
    kb = file_size / 1024
    size_str = f"{kb:.0f} KB" if kb < 1024 else f"{kb / 1024:.1f} MB"
    tip = " — considera sesión nueva" if pct >= 80 else ""
    win_tag = " /1M" if window >= 1_000_000 else ""
    return f"{icon} ctx ~{pct}%{win_tag} ({size_str}){tip}"


def _ctx_pct(tokens_input: int, model: str | None = None,
             window: int | None = None) -> tuple[str, int]:
    """Returns (inline indicator, pct) from real input token count.
    Window depends on the model (1M for opus-1m, else 200K)."""
    window = window or cc.context_window(model)
    pct = min(99, tokens_input * 100 // window)
    icon = "🟢" if pct < 40 else ("🟡" if pct < 70 else ("🟠" if pct < 90 else "🔴"))
    win_tag = " /1M" if window >= 1_000_000 else ""
    return f"{icon} ctx {pct}%{win_tag}", pct


def _model_label(model: str) -> str:
    """Human-friendly label for a model alias in picker buttons."""
    return cc.MODEL_LABELS.get(model, model)


def _model_rows(cb_for_model, current: str | None = None) -> list:
    """Build one button row per model in cc.MODELS.
    cb_for_model(m) → callback_data string.
    If current is given, the matching model gets '✅ ' prefix; others get '🧩 '.
    If current is None, all get '🧩 '."""
    rows = []
    for m in cc.MODELS:
        if current is not None:
            prefix = "✅ " if m == current else "🧩 "
        else:
            prefix = "🧩 "
        rows.append([InlineKeyboardButton(prefix + _model_label(m),
                                          callback_data=cb_for_model(m))])
    return rows


def _session_card(s, meta: dict | None, cwd: str) -> str:
    """Multi-line session summary used in activation messages and restart notice."""
    model = (meta or {}).get("model") or cc.DEFAULT_MODEL
    title = (_session_label(s).replace("`", "'").replace("*", "·")
             .replace("_", "\\_"))[:70]
    branch = getattr(s, "git_branch", None)
    last_mod = getattr(s, "last_modified", None)
    file_size = getattr(s, "file_size", 0) or 0

    dir_line = f"📂 `{Path(cwd).name}`"
    if branch:
        dir_line += f"  🌿 `{branch}`"

    ago_str = ""
    if last_mod:
        ago = time.time() - last_mod / 1000
        if ago < 60:
            ago_str = "ahora mismo"
        elif ago < 3600:
            ago_str = f"hace {int(ago // 60)} min"
        elif ago < 86400:
            ago_str = f"hace {int(ago // 3600)} h"
        else:
            ago_str = f"hace {int(ago // 86400)} d"

    lines = [
        dir_line,
        f"🧩 `{model}`" + (f"  🕐 {ago_str}" if ago_str else ""),
        f"💬 {title}",
    ]
    if file_size:
        lines.append(_context_bar(file_size, model))
    return "\n".join(lines)


def _active_card(cwd: str, sid: str | None, model: str | None,
                 header: str = "🔄 *Sesión activa*") -> str:
    """Uniform 'active session' block used whenever the active pointer changes.
    Resolves the live SDK session (for branch / age / context bar / title) and
    falls back gracefully to project + model + stored title when the session
    object isn't available yet (e.g. a new session not materialized, or list
    failed). `header` lets callers say e.g. '🔄 Sesión activa' vs '✅ Nueva sesión'."""
    meta = db.get_session_meta(sid) if sid else None
    model = model or (meta or {}).get("model") or cc.DEFAULT_MODEL
    s = _find_session(sid, cwd) if sid else None
    if s:
        return f"{header}\n{_session_card(s, meta, cwd)}"
    # Fallback: no SDK object (new/unmaterialized session or list error).
    title = (meta or {}).get("title")
    lines = [header, f"📂 `{Path(cwd).name or '?'}`", f"🧩 `{model}`"]
    if title:
        lines.append(f"💬 {title.replace('`', chr(39)).replace('_', chr(92)+'_')[:50]}")
    elif not sid:
        lines.append("🆕 _sesión nueva — envía tu primer prompt_")
    return "\n".join(lines)


def _active_line(cwd: str, sid: str | None, model: str | None) -> str:
    """One-line active-session summary for secondary notices (queue, cancel).
    Compact: project · model · short title."""
    meta = db.get_session_meta(sid) if sid else None
    model = model or (meta or {}).get("model") or cc.DEFAULT_MODEL
    parts = [f"📂 `{Path(cwd).name or '?'}`", f"🧩 `{model}`"]
    title = (meta or {}).get("title")
    if not title and sid:
        s = _find_session(sid, cwd)
        if s:
            title = _session_label(s)
    if title:
        parts.append(f"💬 _{title.replace('`', chr(39)).replace('_', chr(92)+'_')[:35]}_")
    return " · ".join(parts)


def _list_sessions(directory: str | None = None) -> list:
    try:
        return cc.list_sessions(directory=directory)
    except Exception as exc:  # noqa: BLE001
        logger.warning(f"list_sessions failed: {exc}")
        return []


def _format_elapsed(seconds: float) -> str:
    return f"{int(seconds // 60):02d}:{int(seconds % 60):02d}"


async def _delete_msg(bot, msg_id: int):
    try:
        await bot.delete_message(chat_id=ADMIN_ID, message_id=msg_id)
    except Exception:  # noqa: BLE001
        pass


async def _safe_send(text: str, parse_mode: str | None = "MarkdownV2",
                     plain_fallback: str | None = None, **kwargs):
    """Send a message to the admin resiliently:
      - retries once on RetryAfter (flood control) and on transient NetworkError
      - on BadRequest (bad markdown) falls back to plain text
    Returns the sent Message, or None if it ultimately failed (always logged,
    never raised, so callers in a finally block don't lose their flow)."""
    for attempt in range(2):
        try:
            return await APP.bot.send_message(ADMIN_ID, text, parse_mode=parse_mode,
                                              **kwargs)
        except BadRequest:
            if plain_fallback is not None:
                try:
                    return await APP.bot.send_message(ADMIN_ID, plain_fallback,
                                                      **kwargs)
                except Exception as exc:  # noqa: BLE001
                    logger.error(f"_safe_send plain fallback failed: {exc}")
                    return None
            logger.error("_safe_send BadRequest with no plain fallback")
            return None
        except RetryAfter as e:
            if attempt == 0:
                await asyncio.sleep(float(getattr(e, "retry_after", 1)) + 0.5)
                continue
            logger.error("_safe_send giving up after RetryAfter")
            return None
        except NetworkError as e:
            if attempt == 0:
                await asyncio.sleep(1.0)
                continue
            logger.error(f"_safe_send giving up after NetworkError: {e}")
            return None
        except Exception as exc:  # noqa: BLE001
            logger.error(f"_safe_send unexpected error: {exc}")
            return None
    return None


def _track_msg(message_id: int, skey: str, directory: str):
    MSG2SESS[message_id] = {"skey": skey, "directory": directory}
    while len(MSG2SESS) > MSG_TRACK_LIMIT:
        MSG2SESS.pop(next(iter(MSG2SESS)))


def _active_model() -> str:
    active = db.get_active()
    return (active or {}).get("model") or cc.DEFAULT_MODEL


# --------------------------------------------------------------------------- #
# Live status (fed by the SDK stream)
# --------------------------------------------------------------------------- #
# Field that best summarizes each tool's action in the live status line.
_TOOL_ARG_FIELDS = {
    "Bash": "command", "Read": "file_path", "Write": "file_path",
    "Edit": "file_path", "MultiEdit": "file_path", "NotebookEdit": "notebook_path",
    "Glob": "pattern", "Grep": "pattern", "Task": "description",
    "WebFetch": "url", "WebSearch": "query",
}


def _tool_arg(name: str, inp: dict, directory: str) -> str:
    if not inp:
        return ""
    field = _TOOL_ARG_FIELDS.get(name)
    val = inp.get(field) if field else None
    if not val:
        for k in ("file_path", "path", "command", "pattern", "query", "url"):
            if inp.get(k):
                val, field = inp[k], k
                break
    if not val:
        return ""
    val = str(val)
    if field in ("file_path", "path", "notebook_path"):
        try:
            val = str(Path(val).relative_to(directory))
        except ValueError:
            pass
    val = val.replace("\n", " ").strip()
    if len(val) > 60:
        val = val[:57] + "…"
    return val.replace("`", "'")


def _build_status_text(st: dict) -> str:
    icons = {"busy": "🔴", "thinking": "🤔", "idle": "🟢", "error": "❌", "pending": "⚪"}
    state = st.get("state", "busy")
    icon = icons.get(state, "⚪")
    labels = {"busy": "TRABAJANDO", "thinking": "PENSANDO", "idle": "OK",
              "error": "ERROR", "pending": "ESPERANDO"}
    cwd_name = Path(st.get("directory", "")).name or "?"
    model = st.get("model") or "?"
    effort = st.get("effort")
    elapsed = _format_elapsed(time.time() - st.get("start_time", time.time()))

    sess_label = (st.get("session_label") or "").replace("`", "'").replace("*", "·").replace("_", "\\_")
    label_line = f"💬 _{sess_label[:50]}_" if sess_label else ""
    effort_str = f" · ⚡`{effort or 'high'}`"
    lines = [
        f"{icon} *{labels.get(state, state.upper())}* | 📂 `{cwd_name}`",
        f"🧩 `{model}`{effort_str} | ⏱ `{elapsed}`",
    ]
    if label_line:
        lines.append(label_line)
    files = st.get("files_edited", {})
    if files:
        items = list(files.items())[:4]
        fs = ", ".join(f"{ico}`{rp}`" for rp, ico in items)
        if len(files) > 4:
            fs += f" +{len(files)-4}"
        lines.append(f"📝 {fs}")
    if st.get("tool"):
        line = f"🔧 `{st['tool']}`"
        if st.get("tool_arg"):
            line += f": `{st['tool_arg']}`"
        lines.append(line)
    else:
        tools = list(dict.fromkeys(st.get("tools_seen", [])))
        if tools:
            lines.append("⚡ " + " · ".join(f"`{t}`" for t in tools[-5:]))
    if st.get("reasoning_text"):
        snip = st["reasoning_text"][-200:].replace("`", "'").replace("*", "").replace("_", "\\_")
        lines.append(f"💭 _{snip}_")
    if st.get("stream_text"):
        snip = st["stream_text"][-200:].replace("`", "'").replace("*", "").replace("_", "\\_")
        lines.append(f"✍️ _{snip}_")
    tok_in = st.get("tokens_input", 0)
    if tok_in:
        ctx_str, _ = _ctx_pct(tok_in, st.get("model"), st.get("context_window"))
        lines.append(ctx_str)
    lines.append("\n_Pulsa_ /esc _para cancelar_")
    return "\n".join(lines)


def _touch_status(st: dict | None):
    """Mark a status as needing a redraw and wake the flush worker. Cheap and
    non-blocking — called from the SDK event loop so consuming events never waits
    on a Telegram round-trip. Only the first touch after a flush rings the wake
    bell: once a status is already dirty its actionable deadline is fixed, so
    further touches (per-token deltas) would just spin the worker for nothing."""
    if st:
        was_dirty = st.get("dirty")
        st["dirty"] = True
        if not was_dirty and _STATUS_WAKE is not None:
            _STATUS_WAKE.set()


async def _flush_status(skey: str):
    """Push the current status text to Telegram. The single flush worker owns the
    actual edits. On RetryAfter we do NOT sleep here (that would freeze the serial
    worker — and thus every other live status — for the whole backoff window);
    instead we defer this status's next edit by pushing its clock forward and
    re-marking it dirty, so the worker keeps serving the rest meanwhile."""
    st = STATUSES.get(skey)
    if not st or not st.get("msg_id"):
        return
    st["dirty"] = False
    st["last_update_time"] = time.time()
    try:
        await APP.bot.edit_message_text(
            chat_id=ADMIN_ID, message_id=st["msg_id"],
            text=_build_status_text(st), parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("❌ Cancelar", callback_data="abort:")]]),
        )
    except BadRequest:
        pass
    except RetryAfter as e:
        # Honor the backoff Telegram actually asked for (+1s buffer). Retrying
        # *earlier* than retry_after — as a too-low cap would force — only earns a
        # longer flood ban, which is what wedges a status for minutes. Cap solely
        # as a sanity ceiling, never below what Telegram requested.
        delay = min(float(getattr(e, "retry_after", STATUS_MIN_INTERVAL)) + 1.0,
                    STATUS_RETRY_CAP)
        st["last_update_time"] = time.time() + delay  # don't retry until backoff clears
        st["dirty"] = True
    except NetworkError:
        pass


async def _relocate_status(skey: str | None):
    """After a mid-task question is answered, move the live status message below
    the answer so the chat stays chronological (otherwise the still-updating
    status sits *above* the question/answer). No-op if the task already finished
    (skey gone from STATUSES) or its status has no message yet. Sends the new
    message and repoints the updater at it *before* deleting the old one, so the
    throttled updater/heartbeat never targets a just-deleted message."""
    if not skey:
        return
    st = STATUSES.get(skey)
    if not st or not st.get("msg_id"):
        return
    old_id = st["msg_id"]
    try:
        msg = await APP.bot.send_message(
            chat_id=ADMIN_ID, text=_build_status_text(st), parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("❌ Cancelar", callback_data="abort:")]]),
        )
    except (BadRequest, RetryAfter, NetworkError):
        return  # keep the old status rather than losing it entirely
    st["msg_id"] = msg.message_id
    st["last_update_time"] = time.time()
    st["dirty"] = False
    try:
        await APP.bot.delete_message(chat_id=ADMIN_ID, message_id=old_id)
    except (BadRequest, RetryAfter, NetworkError):
        pass


def _status_intervals() -> tuple[float, float]:
    """The per-status (min, liveness) edit intervals, scaled by how many statuses
    are live right now. Telegram flood control is per *chat*, so N concurrent
    statuses must each edit N× less often to keep the aggregate rate constant and
    avoid a flood ban. With a single session this returns the base intervals."""
    n = max(1, sum(1 for st in STATUSES.values() if st and st.get("msg_id")))
    return STATUS_MIN_INTERVAL * n, STATUS_LIVENESS_INTERVAL * n


def _next_flush_delay() -> float:
    """Seconds until the soonest status needs an edit. A dirty status is due once
    the (scaled) min interval has passed since its last edit; a clean one is due at
    the (scaled) liveness interval so its elapsed clock keeps moving. The worker
    waits this long, but a fresh event can cut the wait short via the wake bell.
    Returns 0 if something is already due."""
    now = time.time()
    min_int, live_int = _status_intervals()
    deadlines = []
    for st in list(STATUSES.values()):
        if not st or not st.get("msg_id"):
            continue
        last = st.get("last_update_time", 0)
        span = min_int if st.get("dirty") else live_int
        deadlines.append(last + span)
    if not deadlines:
        return STATUS_LIVENESS_INTERVAL
    return max(0.0, min(deadlines) - now)


async def _flusher_loop():
    """Single background worker that renders all live statuses. Runs as a plain
    asyncio task (NOT job_queue — the [job-queue] extra/APScheduler may be absent,
    in which case APP.job_queue is None and nothing would ever refresh). It is
    event-driven: it sleeps exactly until the next status is actionable (or until a
    new event rings the wake bell, whichever comes first), then edits every status
    whose dirty/liveness deadline has passed. This coalesces per-token delta bursts
    into one edit per STATUS_MIN_INTERVAL while painting discrete changes promptly.
    Self-terminates when there are no live statuses left."""
    while STATUSES:
        try:
            await asyncio.wait_for(_STATUS_WAKE.wait(), timeout=_next_flush_delay())
        except asyncio.TimeoutError:
            pass
        _STATUS_WAKE.clear()
        now = time.time()
        min_int, live_int = _status_intervals()
        for skey in list(STATUSES.keys()):
            st = STATUSES.get(skey)
            if not st or not st.get("msg_id"):
                continue
            since = now - st.get("last_update_time", 0)
            due = (st.get("dirty") and since >= min_int) \
                or since >= live_int
            if due:
                try:
                    await _flush_status(skey)
                except Exception as exc:  # noqa: BLE001 — never let one bad edit kill the loop
                    logger.warning(f"status flush failed for {skey}: {exc}")


def _ensure_flusher():
    global _FLUSHER_TASK, _STATUS_WAKE
    if _STATUS_WAKE is None:
        _STATUS_WAKE = asyncio.Event()
    if _FLUSHER_TASK is None or _FLUSHER_TASK.done():
        _FLUSHER_TASK = asyncio.create_task(_flusher_loop())


def _start_status(skey: str, directory: str, msg_id: int, model: str,
                  session_label: str | None = None):
    STATUSES[skey] = {
        "msg_id": msg_id, "directory": directory, "model": model,
        "session_label": session_label,
        "state": "pending", "tool": None, "tools_seen": [], "files_edited": {},
        "reasoning_text": None, "stream_text": "", "start_time": time.time(),
        "last_update_time": time.time(), "tokens_input": 0, "tokens_output": 0,
        "context_window": None,
        "cost": 0.0, "dirty": False,
    }
    _ensure_flusher()


# --------------------------------------------------------------------------- #
# Final reply
# --------------------------------------------------------------------------- #
async def _send_reply(skey: str, directory: str, st: dict, final: dict | None,
                      cancelled: bool = False, error_msg: str | None = None):
    cwd_name = Path(directory).name or "?"
    model = st.get("model") or "?"
    effort = st.get("effort")
    elapsed = _format_elapsed(time.time() - st.get("start_time", time.time()))
    cost = (final or {}).get("cost", 0.0)
    files = st.get("files_edited", {})

    # Outcome icon — never claim success when Claude errored or was cancelled.
    is_error = bool((final or {}).get("is_error"))
    subtype = (final or {}).get("subtype") or ""
    if cancelled:
        icon = "🛑"
    elif is_error or error_msg:
        icon = "❌"
    else:
        icon = "✅"

    session_id = _resume_for(skey)
    session_title = None
    if session_id:
        meta = db.get_session_meta(session_id)
        session_title = (meta or {}).get("title")

    tok_in = st.get("tokens_input", 0)
    effort_str = f" · ⚡`{effort or 'high'}`"
    header = f"{icon} `{cwd_name}` | 🧩 `{model}`{effort_str} | ⏱ `{elapsed}`"
    if tok_in:
        ctx_str, ctx_pct = _ctx_pct(tok_in, model, st.get("context_window"))
        header += f" | {ctx_str}"
    else:
        ctx_pct = 0
    if cancelled:
        header += "\n🛑 _Cancelado por ti_"
    elif is_error:
        # Surface the CLI's structured error reason (e.g. error_max_turns).
        reason = {"error_max_turns": "límite de turnos alcanzado",
                  "error_during_execution": "error durante la ejecución"}.get(
                      subtype, subtype or "error")
        header += f"\n❌ _Claude terminó con error: {reason}_"
    elif error_msg:
        header += f"\n❌ _{error_msg[:200]}_"
    if session_title:
        truncated_title = session_title[:25] + ("..." if len(session_title) > 25 else "")
        header += f"\n📌 `{truncated_title}`"
    if ctx_pct >= 80:
        header += "\n⚠️ _Contexto casi lleno — considera abrir una sesión nueva_"
    if files:
        items = list(files.items())[:3]
        header += " 📝 " + ", ".join(f"{ico}`{rp}`" for rp, ico in items)
        if len(files) > 3:
            header += f" +{len(files)-3}"

    text = (final or {}).get("text", "") or ""
    if not text:
        # No body — still confirm the outcome so the user always knows what happened.
        tail = "_Cancelado\\._" if cancelled else (
            "_Terminó con error\\._" if (is_error or error_msg) else "_Listo\\._")
        sent = await _safe_send(f"{md2tgv2.convert(header)}\n{tail}",
                                plain_fallback=f"{header}\n(sin texto)")
        if sent:
            _track_msg(sent.message_id, skey, directory)
        return

    if len(text) > MD_FILE_THRESHOLD:
        import io
        f = io.BytesIO(text.encode("utf-8"))
        f.name = "respuesta.md"
        for attempt in range(2):
            try:
                sent = await APP.bot.send_document(ADMIN_ID, document=f,
                                                   caption=md2tgv2.convert(header),
                                                   parse_mode="MarkdownV2")
                _track_msg(sent.message_id, skey, directory)
                return
            except RetryAfter as e:
                if attempt == 0:
                    await asyncio.sleep(float(getattr(e, "retry_after", 1)) + 0.5)
                    f.seek(0)
                    continue
                logger.error("send respuesta.md giving up after RetryAfter")
            except Exception as exc:  # noqa: BLE001
                logger.error(f"send respuesta.md failed: {exc}")
                break
        # Fallback: at least notify the user the body couldn't be delivered.
        await _safe_send(
            f"{md2tgv2.convert(header)}\n⚠️ _No se pudo enviar la respuesta completa "
            f"\\(ver logs\\)\\._",
            plain_fallback=f"{header}\n⚠️ No se pudo enviar la respuesta (ver logs).")
        return

    chunks = [text[i:i+3800] for i in range(0, len(text), 3800)]
    last = None
    for i, chunk in enumerate(chunks):
        body = (f"{md2tgv2.convert(header)}\n" if i == 0 else "") + md2tgv2.convert(chunk)
        plain = (f"{header}\n" if i == 0 else "") + chunk
        sent = await _safe_send(body, plain_fallback=plain)
        if sent:
            last = sent
    if last:
        _track_msg(last.message_id, skey, directory)
    else:
        # Every chunk failed to send → don't leave the user in the dark.
        await _safe_send("⚠️ _No se pudo enviar la respuesta \\(ver logs\\)\\._",
                         plain_fallback="⚠️ No se pudo enviar la respuesta (ver logs).")


async def _finish(skey: str, directory: str, final: dict | None,
                  cancelled: bool = False, error_msg: str | None = None):
    st = STATUSES.pop(skey, None)
    RUNNING.pop(skey, None)
    try:
        db.inflight_remove(skey)
    except Exception as exc:  # noqa: BLE001
        logger.warning(f"inflight_remove failed: {exc}")
    # The flusher loop self-terminates once STATUSES is empty (no cleanup needed).
    if st and st.get("msg_id"):
        await _delete_msg(APP.bot, st["msg_id"])
    if st:
        files = st.get("files_edited", {})
        pre = st.get("pre_snapshot")
        if files:
            LAST_EDITED[skey] = dict(files)
            if pre:
                UNDO_STACK.setdefault(directory, []).append(pre)
                # A new edit invalidates redo; those snapshots are now unreachable.
                for sha in REDO_STACK.pop(directory, None) or []:
                    await asyncio.to_thread(gitops.drop_snapshot, directory, sha)
                overflow = UNDO_STACK[directory][:-50]
                if overflow:
                    for sha in overflow:
                        await asyncio.to_thread(gitops.drop_snapshot, directory, sha)
                    UNDO_STACK[directory] = UNDO_STACK[directory][-50:]
        elif pre:
            # Read-only task: the upfront snapshot is never used — drop its ref.
            await asyncio.to_thread(gitops.drop_snapshot, directory, pre)
        # A close-initiated cancel suppresses the per-task reply: the session is
        # being deleted and the close handler posts a single summary instead.
        if not st.get("suppress_reply"):
            await _send_reply(skey, directory, st, final,
                              cancelled=cancelled, error_msg=error_msg)
    await _drain_queue(skey, directory)


async def _drain_queue(skey: str, directory: str):
    q = QUEUES.get(skey)
    if not q:
        return
    item = q.popleft()
    # Delete the queue position message for this item.
    msg_id = item.get("queue_msg_id")
    if msg_id:
        try:
            await APP.bot.delete_message(ADMIN_ID, msg_id)
        except Exception:
            pass
    if not q:
        QUEUES.pop(skey, None)
    else:
        # Renumber the remaining queue messages.
        sid = _resume_for(skey)
        sk = _key(skey)
        for i, remaining in enumerate(q, start=1):
            rmsg_id = remaining.get("queue_msg_id")
            if not rmsg_id:
                continue
            try:
                await APP.bot.edit_message_text(
                    _queue_msg_text(i, remaining["directory"], remaining["model"],
                                    sid, remaining["text"]),
                    chat_id=ADMIN_ID,
                    message_id=rmsg_id,
                    parse_mode="Markdown",
                    reply_markup=InlineKeyboardMarkup([[
                        InlineKeyboardButton("❌ Cancelar cola",
                                             callback_data=f"qcancel:{sk}")]]))
            except Exception:
                pass
    await _dispatch(item["directory"], skey, item["model"], item["text"],
                    item.get("effort"))


# --------------------------------------------------------------------------- #
# Task runner
# --------------------------------------------------------------------------- #
_EDIT_ICONS = {"Write": "🆕", "Edit": "✏️", "MultiEdit": "✏️", "NotebookEdit": "📓"}


async def _run_task(skey: str, directory: str, prompt: str,
                    resume_sid: str | None, model: str, effort: str | None = None):
    st = STATUSES.get(skey)
    if st:
        st["effort"] = effort
    final = None
    cancelled = False
    error_msg = None
    title = prompt.strip().replace("\n", " ")[:40]
    can_use_tool = _can_use_tool if PERMISSION_MODE != "bypassPermissions" else None
    # Stamp this task's context so permission/question prompts can name the
    # project + session they belong to (matters in multisession).
    CURRENT_CTX.set({"directory": directory, "skey": skey, "model": model})

    _last_event_at = [time.monotonic()]

    async def _consume():
        nonlocal final, error_msg
        async for ev in cc.run(prompt, directory, model, resume_sid,
                               PERMISSION_MODE, can_use_tool, MCP_SERVER,
                               effort=effort):
            _last_event_at[0] = time.monotonic()
            t = ev["type"]
            if t == "client":
                if skey in RUNNING:
                    RUNNING[skey]["client"] = ev["client"]
            elif t == "session":
                sid = ev["session_id"]
                KNOWN_SID[skey] = sid
                db.remember_session(sid, directory, model, title)
                if st and not st.get("session_label"):
                    st["session_label"] = title
                active = db.get_active()
                if active and active.get("directory") == directory \
                        and not active.get("claude_session_id"):
                    db.update_active_session_id(sid)
            elif t == "turn_start":
                if st:
                    st["state"] = "busy"
                    _touch_status(st)
            elif t == "text":
                # Codex no longer exposes reasoning summaries, so the agent's
                # preamble messages are the only live narration available: show
                # them in the status instead of just flipping the state.
                if st:
                    st["state"] = "busy"
                    st["stream_text"] = ev["text"]
                    st["tool"] = ""
                    st["tool_arg"] = ""
                    _touch_status(st)
            elif t == "thinking":
                if st:
                    st["state"] = "thinking"
                    st["reasoning_text"] = ev["text"]
                    _touch_status(st)
            elif t == "stream_start":
                if st:
                    block = ev.get("block")
                    if block == "thinking":
                        st["state"] = "thinking"
                        st["reasoning_text"] = ""
                        _touch_status(st)
                    elif block == "text":
                        st["state"] = "busy"
                        st["stream_text"] = ""
                        st["tool"] = ""
                        st["tool_arg"] = ""
                        _touch_status(st)
            elif t == "thinking_delta":
                if st:
                    st["state"] = "thinking"
                    st["reasoning_text"] = st.get("reasoning_text", "") + ev["text"]
                    _touch_status(st)
            elif t == "text_delta":
                if st:
                    st["state"] = "busy"
                    st["stream_text"] = st.get("stream_text", "") + ev["text"]
                    _touch_status(st)
            elif t == "tool":
                if st:
                    st["state"] = "busy"
                    st["stream_text"] = ""
                    st["tool"] = ev["name"]
                    st["tool_arg"] = _tool_arg(ev["name"], ev["input"], directory)
                    if ev["name"] not in st["tools_seen"]:
                        st["tools_seen"].append(ev["name"])
                    if ev["name"] in cc.EDIT_TOOLS:
                        fp = (ev["input"].get("file_path") or ev["input"].get("path")
                              or ev["input"].get("notebook_path"))
                        if fp:
                            try:
                                relpath = str(Path(fp).relative_to(directory))
                            except ValueError:
                                relpath = fp
                            icon = _EDIT_ICONS.get(ev["name"], "✏️")
                            if relpath not in st["files_edited"]:
                                st["files_edited"][relpath] = icon
                    _touch_status(st)
            elif t == "usage":
                if st:
                    st["tokens_input"] = ev["input"]
                    st["tokens_output"] = ev["output"]
                    st["context_window"] = ev.get("context_window")
            elif t == "result":
                final = ev
                if st:
                    st["cost"] = ev.get("cost", 0.0)
            elif t == "error":
                error_msg = ev["message"]
                await _safe_send(f"❌ Error: {ev['message'][:1500]}", parse_mode=None)

    if st is not None:
        try:
            st["pre_snapshot"] = await asyncio.to_thread(gitops.snapshot, directory)
        except Exception:  # noqa: BLE001
            st["pre_snapshot"] = None

    async def _idle_watchdog():
        # Returns (fires) only when no event has arrived for TASK_TIMEOUT seconds.
        # Polls every 60 s so a genuinely active long task is never killed.
        interval = min(60, max(10, TASK_TIMEOUT // 10))
        while True:
            await asyncio.sleep(interval)
            if time.monotonic() - _last_event_at[0] >= TASK_TIMEOUT:
                return

    consume_task  = asyncio.create_task(_consume())
    watchdog_task = asyncio.create_task(_idle_watchdog())
    try:
        done, _pending = await asyncio.wait(
            [consume_task, watchdog_task],
            return_when=asyncio.FIRST_COMPLETED,
        )
        # Always tidy up the watchdog (no-op if it already finished).
        watchdog_task.cancel()
        await asyncio.gather(watchdog_task, return_exceptions=True)

        if watchdog_task in done and consume_task not in done:
            # Idle timeout: no event for TASK_TIMEOUT seconds while still running.
            consume_task.cancel()
            await asyncio.gather(consume_task, return_exceptions=True)
            last_tool = (st.get("tool") or "") if st else ""
            last_arg  = (st.get("tool_arg") or "") if st else ""
            tool_hint = (f"\n⚙️ Último: `{last_tool}" +
                         (f": {last_arg}`" if last_arg else "`")) if last_tool else ""
            logger.warning(
                f"task {skey} idle-timeout after {TASK_TIMEOUT}s — last tool: {last_tool} {last_arg}")
            error_msg = f"Sin actividad durante {TASK_TIMEOUT}s — tarea interrumpida.{tool_hint}"
            await _interrupt_client(skey)
        elif consume_task.done() and not consume_task.cancelled():
            exc = consume_task.exception()
            if exc is not None:
                raise exc
    except asyncio.CancelledError:
        # /esc or external cancel — tidy up children then swallow so _finish runs.
        consume_task.cancel()
        watchdog_task.cancel()
        await asyncio.gather(consume_task, watchdog_task, return_exceptions=True)
        logger.info(f"task {skey} cancelled")
        cancelled = True
    except Exception as exc:  # noqa: BLE001
        logger.error(f"run_task error: {exc}", exc_info=True)
        error_msg = f"Error inesperado: {exc}"
    finally:
        # Shield so a cancellation arriving during teardown can't abort the
        # final reply or the queue drain (otherwise queued prompts vanish).
        await asyncio.shield(
            _finish(skey, directory, final, cancelled=cancelled, error_msg=error_msg))


def _queue_msg_text(pos: int, directory: str, model: str, sid: str | None, text: str) -> str:
    line = _active_line(directory, sid, model)
    return (f"⏳ *Ocupado* — en cola (posición {pos})\n{line}\n"
            f"📨 _«{text.strip()[:120]}»_")


async def _dispatch(directory: str, skey: str, model: str, text: str,
                    effort: str | None = None):
    """Send a prompt: create the status message and launch the task (or queue)."""
    skey = _canonical_skey(skey)  # fold a materialized new-session id back onto its live placeholder
    if skey in STATUSES:  # busy → queue
        QUEUES.setdefault(skey, deque()).append(
            {"text": text, "directory": directory, "model": model, "effort": effort})
        pos = len(QUEUES[skey])
        sid = _resume_for(skey)
        sk = _key(skey)
        sent = await _safe_send(
            _queue_msg_text(pos, directory, model, sid, text),
            parse_mode="Markdown",
            plain_fallback=f"⏳ Ocupado — en cola (posición {pos}). "
                           f"{Path(directory).name} / {model}",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("❌ Cancelar cola", callback_data=f"qcancel:{sk}")]]))
        if sent:
            QUEUES[skey][-1]["queue_msg_id"] = sent.message_id
        return

    # Reserve the slot synchronously before any await to prevent a second
    # message slipping through the STATUSES check during the Telegram round-trip.
    STATUSES[skey] = {"state": "reserving"}

    try:
        resume_sid = _resume_for(skey)
        sess_label = None
        if resume_sid:
            s = _find_session(resume_sid, directory)
            if s:
                sess_label = _session_label(s)

        sess_line = f"\n💬 `{sess_label[:40]}`" if sess_label else ""
        sent = await APP.bot.send_message(
            ADMIN_ID,
            f"⚪ *ESPERANDO* | 📂 `{Path(directory).name}`\n"
            f"🧩 `{model}` | ⏱ `00:00`{sess_line}\n"
            f"_Iniciando codex-cli…_\n\n_Pulsa_ /esc _para cancelar_",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("❌ Cancelar", callback_data="abort:")]]))
    except Exception as exc:  # noqa: BLE001
        # Posting the status message failed (network/flood/etc.). Free the slot
        # so the session isn't wedged "busy" forever, and tell the user.
        STATUSES.pop(skey, None)
        logger.error(f"_dispatch failed to post status: {exc}", exc_info=True)
        await _safe_send(
            f"❌ No pude iniciar la tarea en `{Path(directory).name}` "
            f"\\(error de red\\)\\. Reintenta\\.",
            plain_fallback=f"❌ No pude iniciar la tarea en {Path(directory).name}. Reintenta.")
        return

    _start_status(skey, directory, sent.message_id, model, sess_label)
    _track_msg(sent.message_id, skey, directory)
    RUNNING[skey] = {"client": None, "directory": directory}
    # Record the in-flight task so a restart can detect it was interrupted.
    try:
        db.inflight_add(skey, directory, model, text, sent.message_id)
    except Exception as exc:  # noqa: BLE001
        logger.warning(f"inflight_add failed: {exc}")

    task = asyncio.create_task(_run_task(skey, directory, text, resume_sid, model, effort))
    RUNNING[skey]["task"] = task


# --------------------------------------------------------------------------- #
# Folder browser  (/open)
# --------------------------------------------------------------------------- #
PAGE = 8


def _folder_kbd(path: Path, page: int):
    try:
        entries = sorted(path.iterdir(), key=lambda p: (not p.is_dir(), p.name.lower()))
    except PermissionError:
        entries = []
    dirs = [e for e in entries if e.is_dir() and not e.name.startswith(".")]
    files = [e for e in entries if e.is_file()]
    all_e = dirs + files
    total = max(1, (len(all_e) + PAGE - 1) // PAGE)
    page = max(0, min(page, total - 1))
    chunk = all_e[page*PAGE:(page+1)*PAGE]

    pk = _key(str(path))
    btns = []
    for e in chunk:
        icon = "📁" if e.is_dir() else "📄"
        btns.append([InlineKeyboardButton(f"{icon} {e.name}",
                                          callback_data=f"ob:{_key(str(e))}:0")])
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("◀", callback_data=f"ob:{pk}:{page-1}"))
    if page < total - 1:
        nav.append(InlineKeyboardButton("▶", callback_data=f"ob:{pk}:{page+1}"))
    if nav:
        btns.append(nav)
    btns.append([InlineKeyboardButton("✅ Abrir aquí", callback_data=f"os:{pk}")])
    btns.append([InlineKeyboardButton("📁 Nueva carpeta", callback_data=f"mkdir:{pk}")])
    if path.parent != path:
        btns.append([InlineKeyboardButton("⬆ Subir", callback_data=f"ob:{_key(str(path.parent))}:0")])
    return f"📂 `{path}`  _{page+1}/{total}_", InlineKeyboardMarkup(btns)


@admin_only
async def cmd_open(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    _clear_send_mode()
    txt, kbd = _folder_kbd(WORKSPACE, 0)
    await update.message.reply_text(txt, reply_markup=kbd, parse_mode="Markdown")


async def cb_ob(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    parts = q.data.split(":")
    if len(parts) < 3:
        await _expired(q)
        return
    _, pk, pg = parts
    val = _val(int(pk))
    if val == KEY_MISSING:
        await _expired(q)
        return
    path = Path(val)
    if path.is_file():
        path = path.parent
    txt, kbd = _folder_kbd(path, int(pg))
    await q.edit_message_text(txt, reply_markup=kbd, parse_mode="Markdown")


async def cb_mkdir(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    path = _val(int(q.data.split(":")[1]))
    if path == KEY_MISSING:
        await _expired(q)
        return
    MKDIR_PENDING.clear()
    MKDIR_PENDING.update({"path": path, "msg_id": q.message.message_id,
                          "ts": time.time()})
    await q.edit_message_text(f"📁 Nueva carpeta en `{Path(path).name}`\n\nEscribe el nombre:",
                              parse_mode="Markdown")


async def cb_os(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Folder chosen → existing sessions picker or model picker."""
    q = update.callback_query
    await q.answer()
    cwd = _val(int(q.data.split(":")[1]))
    if not cwd or cwd == KEY_MISSING:
        await _expired(q)
        return
    sessions = _list_sessions(directory=cwd)
    if sessions:
        await _show_session_picker(q, cwd, sessions)
    else:
        await _show_model_picker(q, cwd)


async def _show_session_picker(q, cwd: str, sessions: list, mode: str = "activate"):
    pk = _key(cwd)
    if mode == "send":
        new_cb = f"sendnew:{pk}"
        cur_sid = (SEND_MODE.get("target") or {}).get("skey")
        title = f"📤 `{Path(cwd).name}` — {len(sessions)} sesión(es) (destino)"
        sel = lambda sid: f"sendsess:{_key(sid)}:{pk}"
        dele = lambda sid: f"senddel:{_key(sid)}:{pk}"
    else:  # "sessions" (from /sessions) or "activate" (from /open)
        new_cb = f"newsess:{pk}"
        cur_sid = (db.get_active() or {}).get("claude_session_id")
        title = f"📂 `{Path(cwd).name}` — {len(sessions)} sesión(es)"
        if cur_sid:
            active_s = _find_session(cur_sid, cwd)
            if active_s:
                title += f"\n✅ {_session_label(active_s).replace('`', chr(39)).replace('_', chr(92)+'_')[:50]}"
        sel = lambda sid: f"actsess:{_key(sid)}:{pk}"
        # :s suffix tells cb_delsess to re-render in sessions mode after delete
        dele = (lambda sid: f"delsess:{_key(sid)}:{pk}:s") if mode == "sessions" else (lambda sid: f"delsess:{_key(sid)}:{pk}")
    btns = [[InlineKeyboardButton("➕ Nueva sesión", callback_data=new_cb)]]
    for s in sessions[:10]:
        sid = s.session_id
        is_active = sid == cur_sid
        prefix = "✅ " if is_active else ""
        label = f"{prefix}{_session_label(s)[:26 if is_active else 28]}"
        btns.append([
            InlineKeyboardButton(label, callback_data=sel(sid)),
            InlineKeyboardButton("🗑", callback_data=dele(sid)),
        ])
    if mode in ("send", "sessions"):
        label_back = "🔙 Otro proyecto"
        cb_back = "sendback:" if mode == "send" else "sessback:"
        btns.append([InlineKeyboardButton(label_back, callback_data=cb_back)])
    btns.append([InlineKeyboardButton("❌ Cancelar", callback_data="cancel:")])
    await q.edit_message_text(
        title, reply_markup=InlineKeyboardMarkup(btns), parse_mode="Markdown")


async def _show_model_picker(q, cwd: str | None):
    pk = _key(cwd) if cwd else -1
    btns = _model_rows(lambda m: f"setmodel:{pk}:{_key(m)}")
    btns.append([InlineKeyboardButton("❌ Cancelar", callback_data="cancel:")])
    header = f"📂 `{Path(cwd).name}`\n" if cwd else ""
    await q.edit_message_text(f"{header}🧩 Elige modelo:",
                              reply_markup=InlineKeyboardMarkup(btns), parse_mode="Markdown")


async def cb_newsess(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    cwd = _val(int(q.data.split(":")[1]))
    if not cwd or cwd == KEY_MISSING:
        await _expired(q)
        return
    await _show_model_picker(q, cwd)


async def cb_setmodel(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Model chosen → either create a new active session (cwd given) or change /models."""
    q = update.callback_query
    await q.answer()
    parts = q.data.split(":")
    if len(parts) <= 2:
        await _expired(q)
        return
    pk = int(parts[1])
    model = _val(int(parts[2]))
    if not model or model == KEY_MISSING:  # stale → would persist an empty model
        await _expired(q)
        return

    if pk == -1:  # /models mode → change active session model
        active = db.get_active()
        if not active:
            await q.edit_message_text("⚠️ No hay sesión activa. Usa /open.")
            return
        cwd = active["directory"]
        sid = active.get("claude_session_id")
        db.set_active(cwd, sid, model, active.get("effort"))
        if sid:
            db.set_session_model(sid, model)
        # Show the whole session so it's clear *which* session got the new model.
        await q.edit_message_text(
            f"✅ *Modelo cambiado a* `{model}`\n{_active_card(cwd, sid, model, header='📍 En esta sesión:')}",
            parse_mode="Markdown")
        return

    cwd = _val(pk)
    if not cwd or cwd == KEY_MISSING:
        await _expired(q)
        return
    KNOWN_SID.pop(_skey(cwd, None), None)  # force truly new session, not a resume
    db.set_active(cwd, None, model)  # new session, materializes on first prompt
    await q.edit_message_text(
        _active_card(cwd, None, model, header="✅ *Nueva sesión activa*"),
        parse_mode="Markdown")


async def cb_actsess(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    parts = q.data.split(":")
    if len(parts) <= 2:
        await _expired(q)
        return
    vals = _vals(int(parts[1]), int(parts[2]))
    if vals is None or not vals[0] or not vals[1]:
        await _expired(q)  # stale button → don't clobber the active pointer with ""
        return
    sid, cwd = vals
    meta = db.get_session_meta(sid)
    model = (meta or {}).get("model") or cc.DEFAULT_MODEL
    effort = (meta or {}).get("effort")
    db.set_active(cwd, sid, model, effort)
    await q.edit_message_text(
        _active_card(cwd, sid, model, header="✅ *Sesión activa*"),
        parse_mode="Markdown")


async def cb_delsess(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    parts = q.data.split(":")
    if len(parts) <= 2:
        await _expired(q)
        return
    vals = _vals(int(parts[1]), int(parts[2]))
    if vals is None or not vals[0]:
        await _expired(q)
        return
    sid, cwd = vals
    from_sessions = len(parts) > 3 and parts[3] == "s"
    await _cancel_running([_skey(cwd, sid)])
    try:
                cc.delete_session(sid, directory=cwd or None)
    except Exception as exc:  # noqa: BLE001
        await q.edit_message_text(f"❌ Error: {exc}")
        return
    db.forget_session(sid)
    KNOWN_SID.pop(sid, None)  # purge mapping for this materialized session
    active = db.get_active()
    if active and active.get("claude_session_id") == sid:
        db.clear_active()
    sessions = _list_sessions(directory=cwd)
    if sessions:
        await _show_session_picker(q, cwd, sessions,
                                   mode="sessions" if from_sessions else "activate")
    else:
        KNOWN_SID.pop(_skey(cwd, None), None)  # prevent stale resume of the deleted session
        db.set_active(cwd, None, cc.DEFAULT_MODEL)
        await q.edit_message_text(
            "🗑️ *Sesión borrada* — no quedaban más en el proyecto.\n"
            + _active_card(cwd, None, cc.DEFAULT_MODEL,
                           header="🔄 *Nueva sesión activa (automática)*"),
            parse_mode="Markdown")


async def cb_cancel(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    # If this Cancel belongs to a /rename or /btw prompt, clear pending input.
    if PENDING_INPUT.get("msg_id") == q.message.message_id:
        PENDING_INPUT.clear()
    await q.edit_message_text("❌ Cancelado.")


# --------------------------------------------------------------------------- #
# /sessions /close
# --------------------------------------------------------------------------- #
def _group_by_dir(sessions: list) -> dict:
    by_dir: dict[str, list] = {}
    for s in sessions:
        d = getattr(s, "cwd", "") or ""
        if d:
            by_dir.setdefault(d, []).append(s)
    return by_dir


def _project_picker_rows(by_dir: dict, cb_prefix: str, mark_active: bool = False) -> list:
    """Build button rows for a project picker (one row per project, sorted).
    Does NOT include the Cancel row — each call site appends it as needed."""
    active_dir = (db.get_active() or {}).get("directory", "") if mark_active else ""
    rows = []
    for d in sorted(by_dir):
        mark = " ✅" if (mark_active and d == active_dir) else ""
        rows.append([InlineKeyboardButton(
            f"📂 {Path(d).name}{mark} ({len(by_dir[d])})",
            callback_data=f"{cb_prefix}:{_key(d)}")])
    return rows


@admin_only
async def cmd_sessions(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    _clear_send_mode()
    by_dir = _group_by_dir(_list_sessions())
    if not by_dir:
        await update.message.reply_text("No hay sesiones todavía. Usa /open.")
        return
    active = db.get_active() or {}
    active_dir = active.get("directory", "")
    active_sid = active.get("claude_session_id", "")

    header = ""
    if active_dir and active_sid:
        s = _find_session(active_sid, active_dir)
        label = _session_label(s).replace("`", "'").replace("_", "\\_")[:40] if s else active_sid[:8]
        dir_name = Path(active_dir).name.replace("`", "'").replace("_", "\\_")
        header = f"✅ Activa: `{dir_name}` › {label}\n\n"

    btns = _project_picker_rows(by_dir, "sesspick", mark_active=True)
    btns.append([InlineKeyboardButton("❌ Cancelar", callback_data="cancel:")])
    await update.message.reply_text(
        f"{header}¿De qué proyecto?",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(btns))


async def cb_sesspick(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    cwd = _val(int(q.data.split(":")[1]))
    if not cwd or cwd == KEY_MISSING:
        await _expired(q)
        return
    sessions = _list_sessions(directory=cwd)
    if sessions:
        await _show_session_picker(q, cwd, sessions, mode="sessions")
    else:
        await q.edit_message_text(f"No quedan sesiones en `{Path(cwd).name}`.",
                                  parse_mode="Markdown")


def _dir_of_skey(skey: str) -> str:
    """Best-effort directory for a live skey (RUNNING/STATUSES carry it; an
    un-materialized session encodes it as ``new::{dir}``)."""
    e = RUNNING.get(skey) or STATUSES.get(skey)
    if e and e.get("directory"):
        return e["directory"]
    if skey.startswith("new::"):
        return skey[len("new::"):]
    return ""


async def _cancel_running(skeys) -> int:
    """Interrupt + cancel any in-flight tasks for these skeys and drop their
    queued prompts. Called before deleting sessions so a close never leaves a
    task running (or a queued prompt that _finish would re-dispatch into a
    session we just deleted). The per-task final reply is suppressed — the close
    handler reports one summary instead. Returns how many tasks were cancelled."""
    n = 0
    for skey in set(skeys):
        # Drop queued prompts first so the shielded _finish drain finds nothing.
        q = QUEUES.pop(skey, None)
        if q:
            for item in q:
                mid = item.get("queue_msg_id")
                if mid:
                    try:
                        await APP.bot.delete_message(ADMIN_ID, mid)
                    except Exception:  # noqa: BLE001
                        pass
        st = STATUSES.get(skey)
        if st:
            st["suppress_reply"] = True
        entry = RUNNING.get(skey)
        if entry:
            await _interrupt_client(skey)
            task = entry.get("task")
            if task and not task.done():
                task.cancel()
            n += 1
    return n


@admin_only
async def cmd_close(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    by_dir = _group_by_dir(_list_sessions())
    if not by_dir:
        await update.message.reply_text("No hay proyectos con sesiones.")
        return
    total = sum(len(v) for v in by_dir.values())
    btns = _project_picker_rows(by_dir, "closedir")
    if len(by_dir) > 1:
        btns.append([InlineKeyboardButton(
            f"🗑️ Cerrar TODAS ({total})", callback_data="closeall:")])
    btns.append([InlineKeyboardButton("❌ Cancelar", callback_data="cancel:")])
    await update.message.reply_text("¿Qué proyecto cierro (borra sus sesiones)?",
                                    reply_markup=InlineKeyboardMarkup(btns))


async def cb_closeall(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Confirmation step before wiping every session across all projects."""
    q = update.callback_query
    await q.answer()
    by_dir = _group_by_dir(_list_sessions())
    total = sum(len(v) for v in by_dir.values())
    if not total:
        await q.edit_message_text("No hay sesiones que cerrar.")
        return
    await q.edit_message_text(
        f"⚠️ Vas a borrar *todas* las sesiones: {total} en {len(by_dir)} proyecto(s).\n"
        "Esto no se puede deshacer.",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton(f"🗑️ Sí, borrar las {total}", callback_data="closeallok:")],
            [InlineKeyboardButton("❌ Cancelar", callback_data="cancel:")]]))


async def cb_closeallok(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    interrupted = await _cancel_running(set(RUNNING) | set(STATUSES) | set(QUEUES))
    deleted = 0
    for s in _list_sessions():
        cwd = getattr(s, "cwd", "") or ""
        if not cwd:
            continue
        try:
            cc.delete_session(s.session_id, directory=cwd)
            db.forget_session(s.session_id)
            KNOWN_SID.pop(s.session_id, None)
            deleted += 1
        except Exception:  # noqa: BLE001
            pass
    had_active = bool(db.get_active())
    db.clear_active()
    msg = f"✅ Todas las sesiones cerradas — {deleted} borrada(s)."
    if interrupted:
        msg += f"\n🛑 {interrupted} tarea(s) en curso interrumpida(s)."
    if had_active:
        msg += "\n⚠️ _Ya no hay sesión activa._ Usa /open."
    await q.edit_message_text(msg, parse_mode="Markdown")


async def cb_closedir(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    vals = _vals(int(q.data.split(":")[1]))
    if vals is None:
        await _expired(q)
        return
    cwd = vals[0]
    # Hard guard: never operate on an empty directory. _list_sessions("")
    # returns sessions for *every* project, so a stale/empty cwd here would
    # wipe unrelated projects' sessions.
    if not cwd:
        await _expired(q)
        return
    sessions = _list_sessions(directory=cwd)
    interrupted = await _cancel_running(
        k for k in (set(RUNNING) | set(STATUSES) | set(QUEUES))
        if _dir_of_skey(k) == cwd)
    deleted = 0
    for s in sessions:
        try:
            cc.delete_session(s.session_id, directory=cwd)
            db.forget_session(s.session_id)
            KNOWN_SID.pop(s.session_id, None)
            deleted += 1
        except Exception:  # noqa: BLE001
            pass
    active = db.get_active()
    cleared_active = bool(active and active.get("directory") == cwd)
    if cleared_active:
        db.clear_active()
    msg = f"✅ `{Path(cwd).name}` cerrado — {deleted} sesión(es) borradas."
    if interrupted:
        msg += f"\n🛑 {interrupted} tarea(s) en curso interrumpida(s)."
    if cleared_active:
        msg += "\n⚠️ _Era tu proyecto activo: ya no hay sesión activa._ Usa /open."
    await q.edit_message_text(msg, parse_mode="Markdown")


# --------------------------------------------------------------------------- #
# /models  /permisos
# --------------------------------------------------------------------------- #
@admin_only
async def cmd_models(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    active = db.get_active()
    if not active:
        await update.message.reply_text("⚠️ No hay sesión activa. Usa /open.")
        return
    cur = active.get("model") or cc.DEFAULT_MODEL
    btns = _model_rows(lambda m: f"setmodel:-1:{_key(m)}", current=cur)
    btns.append([InlineKeyboardButton("🔄 Actualizar modelos", callback_data="refreshmodels:")])
    btns.append([InlineKeyboardButton("❌ Cancelar", callback_data="cancel:")])
    await update.message.reply_text(f"🧩 Modelo actual: `{_model_label(cur)}`\nElige:",
                                    reply_markup=InlineKeyboardMarkup(btns),
                                    parse_mode="Markdown")


@admin_only
async def cmd_refreshmodels(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    msg = await update.message.reply_text("🔄 Actualizando catálogo de modelos…")
    ok = await asyncio.to_thread(cc.refresh_catalog, True)
    lines = ["✅ *Modelos actualizados*" if ok
             else "⚠️ *No se pudo obtener el catálogo en vivo — usando caché/fallback*"]
    for m in cc.MODELS:
        lines.append(f"• `{m}` — {_model_label(m)}")
    try:
        await msg.edit_text("\n".join(lines), parse_mode="Markdown")
    except BadRequest:
        await msg.edit_text("\n".join(lines))


async def cb_refreshmodels(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer("Actualizando…")
    ok = await asyncio.to_thread(cc.refresh_catalog, True)
    active = db.get_active()
    cur = (active.get("model") if active else None) or cc.DEFAULT_MODEL
    btns = _model_rows(lambda m: f"setmodel:-1:{_key(m)}", current=cur)
    btns.append([InlineKeyboardButton("🔄 Actualizar modelos", callback_data="refreshmodels:")])
    btns.append([InlineKeyboardButton("❌ Cancelar", callback_data="cancel:")])
    note = "" if ok else "\n⚠️ _Catálogo en vivo no disponible — usando caché/fallback_"
    try:
        await q.edit_message_text(
            f"🧩 Modelo actual: `{_model_label(cur)}`\nElige:{note}",
            reply_markup=InlineKeyboardMarkup(btns),
            parse_mode="Markdown")
    except BadRequest:
        pass


@admin_only
async def cmd_esfuerzo(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    active = db.get_active()
    if not active:
        await update.message.reply_text("⚠️ No hay sesión activa. Usa /open.")
        return
    model = active.get("model") or cc.DEFAULT_MODEL
    cur_effort = active.get("effort")
    levels = cc.effort_levels(model)
    if not levels:
        await update.message.reply_text(
            f"⚡ El modelo `{model}` no tiene nivel de esfuerzo configurable.",
            parse_mode="Markdown")
        return
    btns = []
    for lvl in levels:
        is_cur = (lvl == cur_effort) or (cur_effort is None and lvl == "high")
        label = ("✅ " if is_cur else "") + lvl
        btns.append([InlineKeyboardButton(label, callback_data=f"seteffort:{lvl}")])
    btns.append([InlineKeyboardButton("❌ Cancelar", callback_data="cancel:")])
    cur_display = cur_effort or "high"
    await update.message.reply_text(
        f"⚡ Nivel de esfuerzo actual: `{cur_display}`\n"
        f"🧩 Modelo: `{model}`\nElige:",
        reply_markup=InlineKeyboardMarkup(btns),
        parse_mode="Markdown")


async def cb_seteffort(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    effort = q.data.split(":")[1]
    active = db.get_active()
    if not active:
        await q.edit_message_text("⚠️ No hay sesión activa. Usa /open.")
        return
    model = active.get("model") or cc.DEFAULT_MODEL
    allowed = cc.effort_levels(model)
    if effort not in allowed:
        await q.edit_message_text(
            f"⚠️ `{effort}` no está disponible para el modelo `{model}`.",
            parse_mode="Markdown")
        return
    stored = None if effort == "high" else effort
    db.update_active_effort(stored)
    sid = active.get("claude_session_id")
    if sid:
        db.set_session_effort(sid, stored)
    cwd = active["directory"]
    await q.edit_message_text(
        f"✅ *Esfuerzo cambiado a* `{effort}`\n"
        f"📂 `{Path(cwd).name}` · 🧩 `{model}`",
        parse_mode="Markdown")


async def _apply_rename(update: Update, sid: str, cwd: str, model: str | None, title: str):
    title = title[:60]
    db.set_session_title(sid, title)
    await update.message.reply_text(
        f"✅ *Sesión renombrada:* `{title}`\n📂 `{Path(cwd).name}` · 🧩 `{model or cc.DEFAULT_MODEL}`",
        parse_mode="Markdown")


async def cmd_rename(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    active = db.get_active()
    if not active:
        await update.message.reply_text("⚠️ No hay sesión activa. Usa /open.")
        return
    sid = active.get("claude_session_id")
    if not sid:
        await update.message.reply_text(
            "⚠️ La sesión aún no existe (envía un primer prompt antes de renombrarla).")
        return
    cwd = active["directory"]
    model = active.get("model")
    title = " ".join(ctx.args).strip()
    if title:
        await _apply_rename(update, sid, cwd, model, title)
        return
    # No argument → ask for it, store pending input.
    meta = db.get_session_meta(sid) or {}
    cur_title = meta.get("title") or sid[:8]
    kbd = InlineKeyboardMarkup(
        [[InlineKeyboardButton("❌ Cancelar", callback_data="cancel:")]])
    msg = await update.message.reply_text(
        f"✏️ Dime el nuevo nombre para la sesión `{cur_title}` "
        f"de `{Path(cwd).name}`.",
        reply_markup=kbd, parse_mode="Markdown")
    PENDING_INPUT.clear()
    PENDING_INPUT.update({"kind": "rename", "sid": sid, "directory": cwd,
                          "model": model, "msg_id": msg.message_id,
                          "ts": time.time()})


async def _run_btw(update: Update, question: str, directory: str, sid: str, model: str):
    status = await update.message.reply_text("💬 _Pensando \\(btw\\)…_",
                                             parse_mode="MarkdownV2")
    answer = ""
    forked_sid = None
    try:
        async for ev in cc.ask_side(question, directory, model, sid):
            t = ev["type"]
            if t == "session":
                forked_sid = ev["session_id"]
            elif t == "text":
                answer += ev["text"]
            elif t == "result":
                forked_sid = forked_sid or ev.get("session_id")
                if not answer:
                    answer = ev.get("text", "")
            elif t == "error":
                answer = answer or f"❌ {ev['message'][:800]}"
    except Exception as exc:  # noqa: BLE001
        answer = f"❌ {exc}"

    # Drop the throwaway forked session so it never clutters the pickers.
    if forked_sid and forked_sid != sid:
        try:
            cc.delete_session(forked_sid, directory=directory)
        except Exception:  # noqa: BLE001
            pass
        db.forget_session(forked_sid)

    out = "💬 *BTW* — no afecta al historial\n\n" + (answer or "(sin respuesta)")
    chunks = [out[i:i+3800] for i in range(0, len(out), 3800)] or [out]
    try:
        await status.edit_text(md2tgv2.convert(chunks[0]), parse_mode="MarkdownV2")
    except BadRequest:
        await status.edit_text(chunks[0])
    for c in chunks[1:]:
        await _safe_send(md2tgv2.convert(c), plain_fallback=c)


@admin_only
async def cmd_btw(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Side question (à la /btw): sees the active session's context, no tools,
    doesn't touch its history (forks + deletes a throwaway session)."""
    active = db.get_active()
    if not active:
        await update.message.reply_text("⚠️ No hay sesión activa. Usa /open.")
        return
    sid = active.get("claude_session_id")
    if not sid:
        await update.message.reply_text(
            "⚠️ La sesión activa aún no tiene contexto (envía un primer prompt).")
        return
    directory = active["directory"]
    model = active.get("model") or cc.DEFAULT_MODEL
    question = " ".join(ctx.args).strip()
    if question:
        await _run_btw(update, question, directory, sid, model)
        return
    # No argument → ask for it, store pending input.
    meta = db.get_session_meta(sid) or {}
    cur_title = meta.get("title") or sid[:8]
    kbd = InlineKeyboardMarkup(
        [[InlineKeyboardButton("❌ Cancelar", callback_data="cancel:")]])
    msg = await update.message.reply_text(
        f"💬 Dime tu pregunta sobre la sesión `{cur_title}` "
        f"de `{Path(directory).name}`.\n"
        "_Verá el contexto, sin herramientas, no afecta al historial._",
        reply_markup=kbd, parse_mode="Markdown")
    PENDING_INPUT.clear()
    PENDING_INPUT.update({"kind": "btw", "sid": sid, "directory": directory,
                          "model": model, "msg_id": msg.message_id,
                          "ts": time.time()})


@admin_only
async def cmd_init(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Ask Codex to initialize the project's AGENTS.md instructions."""
    active = db.get_active()
    if not active or not active.get("directory"):
        await update.message.reply_text(
            "⚠️ No hay sesión activa. Usa /open para abrir un proyecto primero.")
        return
    directory = active["directory"]
    skey = _skey(directory, active.get("claude_session_id"))
    model = active.get("model") or cc.DEFAULT_MODEL
    await _dispatch(
        directory, skey, model,
        "Analiza este proyecto y crea o actualiza AGENTS.md con instrucciones "
        "breves y útiles para futuros agentes: arquitectura, comandos de "
        "desarrollo y verificación, y convenciones importantes.",
        active.get("effort"))


@admin_only
async def cmd_tmp(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Activa una sesión temporal en /tmp/codex-tmp y la trata como una carpeta
    normal: pasa a ser la sesión activa, así que los mensajes normales le llegan
    sin tener que responder a nada. El directorio vive en /tmp, por lo que se
    borra al reiniciar el ordenador. `/tmp <texto>` despacha ya; `/tmp` a secas
    solo la deja activa."""
    SCRATCH_DIR.mkdir(parents=True, exist_ok=True)
    cwd = str(SCRATCH_DIR)
    # Resume id if the scratch session already materialized this boot (continuity
    # is bridged by KNOWN_SID), else None → a fresh session on first prompt.
    sid = _resume_for(_skey(cwd, None))
    meta = db.get_session_meta(sid) if sid else None
    model = (meta or {}).get("model") or cc.DEFAULT_MODEL
    effort = (meta or {}).get("effort")
    db.set_active(cwd, sid, model, effort)  # make it the active session
    skey = _skey(cwd, sid)
    text = " ".join(ctx.args).strip() if ctx.args else ""
    if text:
        await _dispatch(cwd, skey, model, text, effort)
        return
    await update.message.reply_text(
        _active_card(cwd, sid, model,
                     header="🧪 *Sesión temporal activa* — `/tmp/codex-tmp`")
        + "\n\n♻️ _Carpeta scratch: se borra al reiniciar el ordenador._",
        parse_mode="Markdown")


PERM_MODES = ["bypassPermissions", "workspace-write", "read-only"]


@admin_only
async def cmd_permisos(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    btns = [[InlineKeyboardButton(("✅ " if m == PERMISSION_MODE else "") + m,
                                  callback_data=f"perm:{_key(m)}")]
            for m in PERM_MODES]
    await update.message.reply_text(
        f"🔐 Modo de permisos actual: `{PERMISSION_MODE}`\n\n"
        "• `bypassPermissions` — acceso completo sin sandbox\n"
        "• `workspace-write` — solo escribe dentro del proyecto\n"
        "• `read-only` — no permite modificar archivos\n\nElige:",
        reply_markup=InlineKeyboardMarkup(btns), parse_mode="Markdown")


async def cb_perm_mode(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    global PERMISSION_MODE
    q = update.callback_query
    await q.answer()
    mode = _val(int(q.data.split(":")[1]))
    if not mode or mode == KEY_MISSING or mode not in PERM_MODES:
        await _expired(q)
        return
    PERMISSION_MODE = mode
    desc = {
        "bypassPermissions": "acceso completo sin sandbox",
        "workspace-write": "solo escribe dentro del proyecto",
        "read-only": "no permite modificar archivos",
    }.get(mode, "")
    text = (f"🔐 *Modo de permisos:* `{PERMISSION_MODE}`\n"
            f"_{desc}_\n"
            f"🌍 _Aplica a todas las sesiones (global)._")
    active = db.get_active()
    if active and active.get("directory"):
        text += "\n\n" + _active_line(active["directory"],
                                       active.get("claude_session_id"),
                                       active.get("model"))
    await q.edit_message_text(text, parse_mode="Markdown")


# --------------------------------------------------------------------------- #
# /esc
# --------------------------------------------------------------------------- #
async def _interrupt_client(skey: str) -> None:
    """Ask the running Claude client to interrupt (best-effort, never raises)."""
    entry = RUNNING.get(skey)
    if not entry:
        return
    client = entry.get("client")
    if client:
        try:
            await client.interrupt()
        except Exception:  # noqa: BLE001
            pass


async def _abort(skey: str) -> str:
    entry = RUNNING.get(skey)
    if not entry:
        return "⚠️ No hay tarea en curso."
    directory = entry.get("directory", "")
    await _interrupt_client(skey)
    task = entry.get("task")
    if task and not task.done():
        task.cancel()
    line = _active_line(directory, _resume_for(skey), None) if directory else ""
    return f"🛑 *Tarea cancelada.*\n{line}" if line else "🛑 Tarea cancelada."


@admin_only
async def cmd_esc(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    active = db.get_active()
    if not active:
        await update.message.reply_text("⚠️ No hay sesión activa.")
        return
    skey = _skey(active["directory"], active.get("claude_session_id"))
    await update.message.reply_text(await _abort(skey), parse_mode="Markdown")


async def cb_abort(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    target = MSG2SESS.get(q.message.message_id)
    skey = target["skey"] if target else None
    if not skey:
        active = db.get_active()
        skey = _skey(active["directory"], active.get("claude_session_id")) if active else None
    msg = await _abort(skey) if skey else "⚠️ No hay tarea en curso."
    try:
        await q.edit_message_text(msg, parse_mode="Markdown")
    except BadRequest:
        await APP.bot.send_message(ADMIN_ID, msg)


async def cb_qcancel(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Cancel a queued (not yet running) prompt."""
    q = update.callback_query
    await q.answer()
    parts = q.data.split(":", 1)
    skey = _val(int(parts[1])) if len(parts) == 2 and parts[1].isdigit() else None
    if not skey:
        try:
            await q.edit_message_text("❌ Cola expirada.")
        except Exception:
            pass
        return
    queue = QUEUES.get(skey)
    msg_id = q.message.message_id
    # Find the item in the queue by its message_id.
    idx = next((i for i, it in enumerate(queue or [])
                if it.get("queue_msg_id") == msg_id), None)
    if idx is None or queue is None:
        try:
            await q.edit_message_text("✅ Ya no estaba en cola.")
        except Exception:
            pass
        return
    removed_text = queue[idx].get("text", "")
    items = list(queue)
    items.pop(idx)
    queue.clear()
    queue.extend(items)
    if not queue:
        QUEUES.pop(skey, None)
    else:
        # Renumber remaining items.
        sid = _resume_for(skey)
        sk = _key(skey)
        for i, remaining in enumerate(queue, start=1):
            rmsg_id = remaining.get("queue_msg_id")
            if not rmsg_id:
                continue
            try:
                await APP.bot.edit_message_text(
                    _queue_msg_text(i, remaining["directory"], remaining["model"],
                                    sid, remaining["text"]),
                    chat_id=ADMIN_ID,
                    message_id=rmsg_id,
                    parse_mode="Markdown",
                    reply_markup=InlineKeyboardMarkup([[
                        InlineKeyboardButton("❌ Cancelar cola",
                                             callback_data=f"qcancel:{sk}")]]))
            except Exception:
                pass
    snip = removed_text.strip()[:80]
    try:
        await q.edit_message_text(f"🗑 *Cancelado de la cola.*\n📨 _{snip}_",
                                  parse_mode="Markdown")
    except Exception:
        pass


# --------------------------------------------------------------------------- #
# /cambios
# --------------------------------------------------------------------------- #
@admin_only
async def cmd_cambios(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    active = db.get_active()
    if not active or not active.get("directory"):
        await update.message.reply_text("⚠️ No hay sesión activa. Usa /open.")
        return
    skey = _skey(active["directory"], active.get("claude_session_id"))
    files = LAST_EDITED.get(skey, {})
    if not files:
        await update.message.reply_text("No hay cambios registrados para la sesión activa.")
        return
    lines = ["📝 *Archivos modificados en la última ejecución:*"]
    for rp, ico in files.items():
        lines.append(f"  {ico} `{rp}`")
    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")


# --------------------------------------------------------------------------- #
# /undo /redo /status
# --------------------------------------------------------------------------- #
async def _do_undo(directory: str, skey: str, reply) -> None:
    """Ejecuta el restore real. `reply` es callable para enviar/editar el mensaje final."""
    stack = UNDO_STACK.get(directory) or []
    if not stack:
        await reply("↩️ No hay nada que deshacer.")
        return
    cur = await asyncio.to_thread(gitops.snapshot, directory)
    target = stack.pop()
    ok, err = await asyncio.to_thread(gitops.restore, directory, target)
    if not ok:
        stack.append(target)
        await reply(f"❌ No se pudo deshacer: {err}")
        return
    # We're now AT `target`; it's off every stack, so drop its keep-ref.
    await asyncio.to_thread(gitops.drop_snapshot, directory, target)
    if cur:
        REDO_STACK.setdefault(directory, []).append(cur)
    LAST_EDITED.pop(skey, None)
    await reply(
        f"↩️ Deshecho. Quedan {len(stack)} paso(s) para deshacer, "
        f"{len(REDO_STACK.get(directory, []))} para rehacer.\n"
        "Usa /status para ver el estado o /redo para rehacer.")


@admin_only
async def cmd_undo(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    active = db.get_active()
    if not active or not active.get("directory"):
        await update.message.reply_text("⚠️ No hay sesión activa. Usa /open.")
        return
    directory = active["directory"]
    skey = _skey(directory, active.get("claude_session_id"))
    if skey in STATUSES:
        await update.message.reply_text("⏳ La sesión está ocupada. Espera a que termine.")
        return
    if not await asyncio.to_thread(gitops.is_git_repo, directory):
        await update.message.reply_text("⚠️ Este proyecto no es un repositorio git.")
        return
    stack = UNDO_STACK.get(directory) or []
    if not stack:
        await update.message.reply_text("↩️ No hay nada que deshacer.")
        return
    extra = await asyncio.to_thread(gitops.untracked_to_clean, directory)
    if extra:
        # Hay archivos no rastreados que el restore borraría; pedir confirmación.
        LIMIT = 15
        shown = extra[:LIMIT]
        lines = ["⚠️ *¿Deshacer?* Los siguientes archivos no rastreados serán *borrados*:"]
        for p in shown:
            lines.append(f"  • `{p}`")
        if len(extra) > LIMIT:
            lines.append(f"  … y {len(extra) - LIMIT} más")
        lines.append("\n¿Continuar con /undo?")
        kbd = InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ Sí, deshacer (borra esos archivos)",
                                  callback_data=f"undoyes:{_key(directory)}")],
            [InlineKeyboardButton("❌ Cancelar", callback_data="cancel:")],
        ])
        await update.message.reply_text(
            "\n".join(lines), parse_mode="Markdown", reply_markup=kbd)
        return
    await _do_undo(directory, skey, update.message.reply_text)


async def cb_undo_yes(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    directory = _val(int(q.data.split(":")[1]))
    if not directory or directory == KEY_MISSING:
        await _expired(q)
        return
    active = db.get_active()
    if not active or not active.get("directory"):
        await q.edit_message_text("⚠️ No hay sesión activa. Usa /open.")
        return
    skey = _skey(directory, active.get("claude_session_id"))
    if skey in STATUSES:
        await q.edit_message_text("⏳ La sesión está ocupada. Espera a que termine.")
        return
    if not await asyncio.to_thread(gitops.is_git_repo, directory):
        await q.edit_message_text("⚠️ Este proyecto no es un repositorio git.")
        return
    stack = UNDO_STACK.get(directory) or []
    if not stack:
        await q.edit_message_text("↩️ No hay nada que deshacer.")
        return
    await _do_undo(directory, skey, q.edit_message_text)


@admin_only
async def cmd_redo(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    active = db.get_active()
    if not active or not active.get("directory"):
        await update.message.reply_text("⚠️ No hay sesión activa. Usa /open.")
        return
    directory = active["directory"]
    skey = _skey(directory, active.get("claude_session_id"))
    if skey in STATUSES:
        await update.message.reply_text("⏳ La sesión está ocupada. Espera a que termine.")
        return
    if not await asyncio.to_thread(gitops.is_git_repo, directory):
        await update.message.reply_text("⚠️ Este proyecto no es un repositorio git.")
        return
    stack = REDO_STACK.get(directory) or []
    if not stack:
        await update.message.reply_text("↪️ No hay nada que rehacer.")
        return
    cur = await asyncio.to_thread(gitops.snapshot, directory)
    target = stack.pop()
    ok, err = await asyncio.to_thread(gitops.restore, directory, target)
    if not ok:
        stack.append(target)
        await update.message.reply_text(f"❌ No se pudo rehacer: {err}")
        return
    # We're now AT `target`; it's off every stack, so drop its keep-ref.
    await asyncio.to_thread(gitops.drop_snapshot, directory, target)
    if cur:
        UNDO_STACK.setdefault(directory, []).append(cur)
    LAST_EDITED.pop(skey, None)
    await update.message.reply_text(
        f"↪️ Rehecho. Quedan {len(UNDO_STACK.get(directory, []))} paso(s) para deshacer, "
        f"{len(stack)} para rehacer.\n"
        "Usa /status para ver el estado o /undo para deshacer.")


@admin_only
async def cmd_status(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    active = db.get_active()
    if not active or not active.get("directory"):
        await update.message.reply_text("⚠️ No hay sesión activa. Usa /open.")
        return
    directory = active["directory"]
    info = await asyncio.to_thread(gitops.repo_status, directory)
    if not info.get("is_repo"):
        await update.message.reply_text("⚠️ Este proyecto no es un repositorio git.")
        return
    branch = info.get("branch", "?")
    if info.get("clean"):
        await update.message.reply_text(
            f"✅ Working tree limpio en `{Path(directory).name}` "
            f"(rama `{branch}`). Nada que commitear.",
            parse_mode="Markdown")
        return
    total_a = info.get("total_added", 0)
    total_r = info.get("total_removed", 0)
    lines = [
        f"📊 *{Path(directory).name}* · rama `{branch}`",
        f"   +{total_a} / -{total_r} líneas en total",
        "",
    ]
    FILE_LIMIT = 40
    per_file = info.get("per_file", {})
    icon_map = {
        "created": "🆕", "modified": "✏️", "deleted": "🗑️", "renamed": "🔀",
    }
    all_files = []
    for kind in ("created", "modified", "deleted", "renamed"):
        for path in info.get(kind, []):
            all_files.append((icon_map[kind], path))
    shown = all_files[:FILE_LIMIT]
    for ico, path in shown:
        stats = per_file.get(path)
        if stats:
            lines.append(f"  {ico} `{path}`  (+{stats['added']} -{stats['removed']})")
        else:
            lines.append(f"  {ico} `{path}`")
    if len(all_files) > FILE_LIMIT:
        lines.append(f"  … y {len(all_files) - FILE_LIMIT} archivo(s) más")
    texto = "\n".join(lines)
    kbd = InlineKeyboardMarkup([[InlineKeyboardButton(
        "✅ Commit y push", callback_data=f"commitpush:{_key(directory)}")]])
    try:
        await update.message.reply_text(texto, parse_mode="Markdown", reply_markup=kbd)
    except Exception:  # noqa: BLE001
        await update.message.reply_text(texto, reply_markup=kbd)


async def cb_commitpush(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    dkey = int(q.data.split(":")[1])
    directory = _val(dkey)
    if directory == KEY_MISSING or not directory:
        await _expired(q)
        return
    kbd = InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancelar", callback_data="cancel:")]])
    msg = await q.edit_message_text(
        "✍️ Escribe el mensaje del commit (`-m`). El próximo mensaje se usará como tal.",
        parse_mode="Markdown", reply_markup=kbd)
    msg_id = msg.message_id if hasattr(msg, "message_id") else q.message.message_id
    PENDING_INPUT.clear()
    PENDING_INPUT.update({"kind": "commit", "directory": directory,
                          "msg_id": msg_id, "ts": time.time()})


# --------------------------------------------------------------------------- #
# Permission + question bridges (used in non-bypass modes / ask_user tool)
# --------------------------------------------------------------------------- #
def _ctx_line() -> str:
    """Compact 'which session is asking' line for permission/question prompts,
    read from the current task's ContextVar. Empty if unknown."""
    ctx = CURRENT_CTX.get()
    if not ctx or not ctx.get("directory"):
        return ""
    return _active_line(ctx["directory"], _resume_for(ctx.get("skey", "")),
                        ctx.get("model"))


async def _can_use_tool(tool_name: str, input_data: dict, context):
    loop = asyncio.get_event_loop()
    fut = loop.create_future()
    qid = next(_EPHEMERAL_QID)
    PENDING_PERMS[qid] = fut
    preview = str(input_data)[:120]
    btns = [
        [InlineKeyboardButton("✅ Permitir", callback_data=f"pa:{qid}:1"),
         InlineKeyboardButton("❌ Denegar", callback_data=f"pa:{qid}:0")],
    ]
    await _safe_send(
        f"🔐 *Permiso* — {_ctx_line()}\n"
        f"Codex quiere usar `{tool_name}`\n`{preview}`",
        parse_mode="Markdown",
        plain_fallback=f"🔐 Permiso: Codex quiere usar {tool_name}\n{preview}",
        reply_markup=InlineKeyboardMarkup(btns))
    try:
        allow = await asyncio.wait_for(fut, timeout=600)
    except asyncio.TimeoutError:
        return False
    finally:
        PENDING_PERMS.pop(qid, None)
    if allow:
        return True
    return False


async def cb_perm_answer(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    _, qid_k, val = q.data.split(":")
    fut = PENDING_PERMS.get(int(qid_k))
    if fut and not fut.done():
        fut.set_result(val == "1")
    await q.edit_message_text("✅ Permitido" if val == "1" else "❌ Denegado")


def _normalize_options(options) -> list[str]:
    """Options arrive as a real array (preferred) or, for older callers, a
    comma-separated string. Normalize to a clean, de-duplicated list — never
    split an array element on its internal commas (that shattered one answer
    into several buttons)."""
    if isinstance(options, str):
        raw = [o.strip() for o in options.split(",")]
    elif isinstance(options, (list, tuple)):
        raw = [str(o).strip() for o in options]
    else:
        raw = []
    seen: set[str] = set()
    opts: list[str] = []
    for o in raw:
        if o and o not in seen:
            seen.add(o)
            opts.append(o)
    return opts


# Telegram renders button labels fine well past this, but long labels wrap and
# become unreadable; when truncated, the full text is shown in the message body.
_BTN_LABEL_MAX = 55


async def _question_bridge(question: str, options) -> str:
    loop = asyncio.get_event_loop()
    fut = loop.create_future()
    qid = next(_EPHEMERAL_QID)
    opts = _normalize_options(options)
    skey = (CURRENT_CTX.get() or {}).get("skey")
    PENDING_Q[qid] = {"future": fut, "options": opts, "skey": skey}

    # One button per option, numbered so look-alikes (and truncated labels) stay
    # distinguishable. Any option too long for its label gets spelled out in full
    # in the message body, keyed by the same number.
    btns = []
    long_lines = []
    for i, o in enumerate(opts):
        n = i + 1
        if len(o) > _BTN_LABEL_MAX:
            label = f"{n}. {o[:_BTN_LABEL_MAX - 1]}…"
            long_lines.append(f"{n}. {o}")
        else:
            label = f"{n}. {o}"
        btns.append([InlineKeyboardButton(label, callback_data=f"qa:{qid}:{i}")])
    btns.append([InlineKeyboardButton("✏️ Responder por texto", callback_data=f"qc:{qid}")])

    # Plain-text context prefix (the question itself is sent as plain text to
    # avoid markdown breakage), so you know which session is asking.
    ctx = CURRENT_CTX.get()
    prefix = ""
    if ctx and ctx.get("directory"):
        proj = Path(ctx["directory"]).name
        prefix = f"❓ [{proj} · {ctx.get('model') or cc.DEFAULT_MODEL}]\n"
    body = f"{prefix}❓ {question}" if prefix else f"❓ {question}"
    if long_lines:
        body += "\n\n" + "\n".join(long_lines)
    await _safe_send(body, parse_mode=None, reply_markup=InlineKeyboardMarkup(btns))
    try:
        return await asyncio.wait_for(fut, timeout=900)
    except asyncio.TimeoutError:
        return ""
    finally:
        PENDING_Q.pop(qid, None)


async def cb_q_answer(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    _, qid_k, idx = q.data.split(":")
    data = PENDING_Q.get(int(qid_k))
    if not data:
        await q.edit_message_text("⚠️ Pregunta expirada.")
        return
    answer = data["options"][int(idx)] if int(idx) < len(data["options"]) else ""
    if not data["future"].done():
        data["future"].set_result(answer)
    await q.edit_message_text(f"✅ {answer}")
    await _relocate_status(data.get("skey"))


async def cb_q_custom(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    qid_k = int(q.data.split(":")[1])
    ctx.bot_data["q_custom_qid"] = qid_k
    await q.edit_message_text("✏️ Escribe tu respuesta (el próximo mensaje):")


# --------------------------------------------------------------------------- #
# /send mode
# --------------------------------------------------------------------------- #
def _clear_send_mode() -> bool:
    was = SEND_MODE["on"] or SEND_MODE["target"] is not None or SEND_MODE.get("oneshot", False)
    SEND_MODE.update({"on": False, "target": None, "pending_text": None,
                      "oneshot": False, "oneshot_pre": False})
    return was


@admin_only
async def cmd_multisesion(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    SEND_MODE["on"] = True
    by_dir = _group_by_dir(_list_sessions())
    btns = _project_picker_rows(by_dir, "sendpick")
    btns.append([InlineKeyboardButton("❌ Cancelar", callback_data="cancel:")])
    await update.message.reply_text(
        "🔀 *Modo multisesión activo* — preguntaré el destino en *cada* mensaje "
        "(no cambia la sesión activa). Responde a un mensaje del bot para seguir "
        "esa sesión concreta.\n\n⚠️ Sigues en multisesión hasta que pongas "
        "/exitmulti.", reply_markup=InlineKeyboardMarkup(btns),
        parse_mode="Markdown")


@admin_only
async def cmd_send(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """One-shot send: pick a session, dispatch one message, restore prior mode."""
    SEND_MODE["oneshot"] = True
    SEND_MODE["oneshot_pre"] = SEND_MODE["on"]
    text_arg = " ".join(ctx.args) if ctx.args else None
    if text_arg:
        SEND_MODE["pending_text"] = text_arg
    by_dir = _group_by_dir(_list_sessions())
    if not by_dir:
        SEND_MODE["oneshot"] = False
        await update.message.reply_text("No hay sesiones todavía. Usa /open.")
        return
    btns = _project_picker_rows(by_dir, "sendpick")
    btns.append([InlineKeyboardButton("❌ Cancelar", callback_data="cancel:")])
    await update.message.reply_text(
        "📤 *Envío único* — elige proyecto:",
        reply_markup=InlineKeyboardMarkup(btns), parse_mode="Markdown")


async def cb_sendpick(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    cwd = _val(int(q.data.split(":")[1]))
    if not cwd or cwd == KEY_MISSING:
        await _expired(q)
        return
    sessions = _list_sessions(directory=cwd)
    await _show_session_picker(q, cwd, sessions, mode="send")


async def cb_sendback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Return to the project picker from the session picker (send mode)."""
    q = update.callback_query
    await q.answer()
    by_dir = _group_by_dir(_list_sessions())
    if not by_dir:
        await q.edit_message_text("No hay sesiones todavía. Usa /open.")
        return
    btns = _project_picker_rows(by_dir, "sendpick")
    btns.append([InlineKeyboardButton("❌ Cancelar", callback_data="cancel:")])
    await q.edit_message_text(
        "📤 Elige proyecto:", reply_markup=InlineKeyboardMarkup(btns),
        parse_mode="Markdown")


async def cb_sessback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Return to the project picker from the session picker (/sessions mode)."""
    q = update.callback_query
    await q.answer()
    by_dir = _group_by_dir(_list_sessions())
    if not by_dir:
        await q.edit_message_text("No hay sesiones todavía. Usa /open.")
        return
    btns = _project_picker_rows(by_dir, "sesspick", mark_active=True)
    btns.append([InlineKeyboardButton("❌ Cancelar", callback_data="cancel:")])
    await q.edit_message_text(
        "¿De qué proyecto?", reply_markup=InlineKeyboardMarkup(btns),
        parse_mode="Markdown")


async def _send_to_target(q, cwd: str, skey: str, model: str, label: str):
    """Route one message to the chosen destination. The target is transient:
    after dispatch it is cleared so the next clean message asks again."""
    oneshot = SEND_MODE.get("oneshot", False)
    oneshot_pre = SEND_MODE.get("oneshot_pre", False)

    pending = SEND_MODE.pop("pending_text", None)
    SEND_MODE["pending_text"] = None
    if pending:
        SEND_MODE["target"] = None
        if oneshot:
            SEND_MODE["oneshot"] = False
            SEND_MODE["on"] = oneshot_pre  # restore prior mode
            suffix = ("🔀 Sigues en multisesión · /exitmulti para salir"
                      if oneshot_pre else "🔙 Volviendo a sesión normal")
        else:
            suffix = "🔀 Sigues en multisesión · /exitmulti para salir"
        try:
            await q.edit_message_text(
                f"📤 Enviando a {label}…\n_{suffix}_", parse_mode="Markdown")
        except BadRequest:
            await q.edit_message_text(f"📤 Enviando…")
        sid_for_effort = _resume_for(skey)
        effort = (db.get_session_meta(sid_for_effort) or {}).get("effort") if sid_for_effort else None
        await _dispatch(cwd, skey, model, pending, effort)
    else:
        # No text yet — hold destination; handle_text dispatches on next message.
        sid_for_effort = _resume_for(skey)
        effort = (db.get_session_meta(sid_for_effort) or {}).get("effort") if sid_for_effort else None
        target = {"skey": skey, "directory": cwd, "model": model, "effort": effort}
        if oneshot:
            target["oneshot_pre"] = oneshot_pre  # carry flag for handle_text
            SEND_MODE["oneshot"] = False          # flag consumed, info in target
        SEND_MODE["target"] = target
        if oneshot:
            suffix = ("🔀 Multisesión activa" if oneshot_pre else "🔙 Vuelve a normal tras envío")
        else:
            suffix = "🔀 Sigues en multisesión"
        try:
            await q.edit_message_text(
                f"📤 Destino: {label}\n🧩 `{model}`\nEscribe el mensaje.\n_{suffix}_",
                parse_mode="Markdown")
        except BadRequest:
            await q.edit_message_text(f"📤 Destino seleccionado. Escribe el mensaje.")


async def cb_sendsess(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    parts = q.data.split(":")
    if len(parts) <= 2:
        await _expired(q)
        return
    vals = _vals(int(parts[1]), int(parts[2]))
    if vals is None or not vals[0] or not vals[1]:
        await _expired(q)
        return
    sid, cwd = vals
    meta = db.get_session_meta(sid)
    model = (meta or {}).get("model") or cc.DEFAULT_MODEL
    s = _find_session(sid, cwd)
    sess_name = _session_label(s).replace("`", "'").replace("_", "\\_")[:35] if s else sid[:8]
    label = f"`{Path(cwd).name}` › {sess_name}"
    await _send_to_target(q, cwd, sid, model, label)


async def cb_sendnew(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """➕ Nueva sesión in send mode → pick a model for the new target session."""
    q = update.callback_query
    await q.answer()
    pk = int(q.data.split(":")[1])
    cwd = _val(pk)
    if not cwd or cwd == KEY_MISSING:
        await _expired(q)
        return
    btns = _model_rows(lambda m, _pk=pk: f"sendmodel:{_pk}:{_key(m)}")
    btns.append([InlineKeyboardButton("❌ Cancelar", callback_data="cancel:")])
    await q.edit_message_text(
        f"📤 `{Path(cwd).name}` — nueva sesión\n🧩 Elige modelo:",
        reply_markup=InlineKeyboardMarkup(btns), parse_mode="Markdown")


async def cb_sendmodel(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Model chosen for a new send-target session (materializes on first prompt)."""
    q = update.callback_query
    await q.answer()
    parts = q.data.split(":")
    if len(parts) <= 2:
        await _expired(q)
        return
    vals = _vals(int(parts[1]), int(parts[2]))
    if vals is None or not vals[0] or not vals[1]:
        await _expired(q)
        return
    cwd, model = vals
    new_skey = _skey(cwd, None)
    KNOWN_SID.pop(new_skey, None)  # force truly new session, not a resume
    await _send_to_target(q, cwd, new_skey, model,
                          f"nueva sesión en `{Path(cwd).name}`")


async def cb_senddel(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Delete a session from the send picker, then re-render it."""
    q = update.callback_query
    await q.answer()
    parts = q.data.split(":")
    if len(parts) <= 2:
        await _expired(q)
        return
    vals = _vals(int(parts[1]), int(parts[2]))
    if vals is None or not vals[0]:
        await _expired(q)
        return
    sid, cwd = vals
    try:
        cc.delete_session(sid, directory=cwd or None)
    except Exception as exc:  # noqa: BLE001
        await q.edit_message_text(f"❌ Error: {exc}")
        return
    db.forget_session(sid)
    KNOWN_SID.pop(sid, None)  # purge mapping for this materialized session
    if (SEND_MODE.get("target") or {}).get("skey") == sid:
        SEND_MODE["target"] = None
    await _show_session_picker(q, cwd, _list_sessions(directory=cwd), mode="send")


@admin_only
async def cmd_exitmulti(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not _clear_send_mode():
        await update.message.reply_text("No estabas en modo multisesión.")
        return
    active = db.get_active()
    if active and active.get("directory"):
        await update.message.reply_text(
            "✅ *Multisesión desactivada.*\n"
            + _active_card(active["directory"], active.get("claude_session_id"),
                           active.get("model"), header="↩️ *Vuelves a tu sesión activa:*"),
            parse_mode="Markdown")
    else:
        await update.message.reply_text(
            "✅ Multisesión desactivada. Sin sesión activa — usa /open.")


# --------------------------------------------------------------------------- #
# Uploads + audio
# --------------------------------------------------------------------------- #
@admin_only
async def handle_file(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    file_id = file_name = None
    if msg.document:
        file_id, file_name = msg.document.file_id, msg.document.file_name or f"doc_{int(time.time())}"
    elif msg.photo:
        file_id, file_name = msg.photo[-1].file_id, f"photo_{int(time.time())}.jpg"
    elif msg.video:
        file_id, file_name = msg.video.file_id, msg.video.file_name or f"video_{int(time.time())}.mp4"
    if not file_id:
        return
    active = db.get_active()
    if not active:
        await msg.reply_text("❌ No hay sesión activa. Usa /open.")
        return
    cwd = active["directory"]
    safe_name = Path(file_name).name or f"file_{int(time.time())}"
    TMP_DIR.mkdir(parents=True, exist_ok=True)
    save_path = TMP_DIR / safe_name
    # Anti-collision: never silently overwrite an existing file.
    if save_path.exists():
        stem, suffix = save_path.stem, save_path.suffix
        save_path = save_path.with_name(f"{stem}_{int(time.time())}{suffix}")
        safe_name = save_path.name
    try:
        tg = await ctx.bot.get_file(file_id)
        await tg.download_to_drive(save_path)
    except Exception as exc:  # noqa: BLE001
        await msg.reply_text(f"❌ Error al guardar: {exc}")
        return
    await msg.reply_text(f"✅ `{safe_name}` guardado en `/tmp/codex-bot-media`.\n"
                         f"📋 Cópialo a `{Path(cwd).name}` si quieres conservarlo.",
                         parse_mode="Markdown")


@admin_only
async def handle_audio(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    file_id = file_name = None
    if msg.audio:
        file_id, file_name = msg.audio.file_id, msg.audio.file_name or f"audio_{int(time.time())}.mp3"
    elif msg.voice:
        file_id, file_name = msg.voice.file_id, f"voice_{int(time.time())}.ogg"
    if not file_id:
        return
    TMP_DIR.mkdir(parents=True, exist_ok=True)
    tmp = TMP_DIR / file_name
    try:
        tg = await ctx.bot.get_file(file_id)
        await tg.download_to_drive(tmp)
    except Exception as exc:  # noqa: BLE001
        await msg.reply_text(f"❌ Error al descargar audio: {exc}")
        return
    if not grok_stt.is_configured():
        await msg.reply_text("⚠️ Transcripción no disponible (falta XAI_API_KEY).")
        tmp.unlink(missing_ok=True)
        return
    status = await msg.reply_text("🎙️ Transcribiendo…")
    text = await grok_stt.transcribe(str(tmp))
    tmp.unlink(missing_ok=True)
    if not text:
        await status.edit_text("⚠️ No se pudo transcribir.")
        return
    active = db.get_active()
    if not active:
        await status.edit_text(f"🎙️ Transcripción (sin sesión activa):\n\n{text}")
        return
    await status.edit_text(f"🎙️ *Transcrito → enviando:*\n\n{text}", parse_mode="Markdown")
    skey = _skey(active["directory"], active.get("claude_session_id"))
    await _dispatch(active["directory"], skey, active.get("model") or cc.DEFAULT_MODEL, text,
                    active.get("effort"))


# --------------------------------------------------------------------------- #
# Plain text → prompt
# --------------------------------------------------------------------------- #
@admin_only
async def handle_text(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    text = update.message.text

    # Pending /rename or /btw input? (only consume plain messages, not replies.)
    if PENDING_INPUT and not update.message.reply_to_message:
        if time.time() - PENDING_INPUT.get("ts", 0) > PENDING_FLOW_TTL:
            PENDING_INPUT.clear()
        else:
            pend = dict(PENDING_INPUT)
            PENDING_INPUT.clear()
            kind = pend["kind"]
            arg = text.strip()
            if not arg:
                await update.message.reply_text("❌ Entrada vacía. Cancelado.")
                return
            if kind == "rename":
                await _apply_rename(update, pend["sid"], pend["directory"],
                                    pend.get("model"), arg)
            elif kind == "btw":
                await _run_btw(update, arg, pend["directory"], pend["sid"],
                               pend.get("model") or cc.DEFAULT_MODEL)
            elif kind == "commit":
                await update.message.reply_text("⏳ Haciendo commit y push…")
                ok, summary = await asyncio.to_thread(gitops.commit_push, pend["directory"], arg)
                if ok:
                    await update.message.reply_text(f"✅ {summary}")
                else:
                    await update.message.reply_text(f"❌ {summary}")
            return

    # Pending mkdir name?
    if MKDIR_PENDING:
        # Expire stale flows so a forgotten "new folder" prompt doesn't swallow
        # a later normal message as a folder name.
        if time.time() - MKDIR_PENDING.get("ts", 0) > PENDING_FLOW_TTL:
            MKDIR_PENDING.clear()
        else:
            parent = Path(MKDIR_PENDING["path"])
            # Sanitize: only a single path component, no traversal.
            name = Path(text.strip()).name
            msg_id = MKDIR_PENDING.get("msg_id")
            MKDIR_PENDING.clear()
            if not name:
                await update.message.reply_text("❌ Nombre de carpeta no válido.")
                return
            new_dir = parent / name
            try:
                new_dir.mkdir(parents=False, exist_ok=False)
            except Exception as exc:  # noqa: BLE001
                await update.message.reply_text(f"❌ {exc}")
                return
            txt, kbd = _folder_kbd(new_dir, 0)
            try:
                await ctx.bot.edit_message_text(chat_id=ADMIN_ID, message_id=msg_id,
                                                text=txt, reply_markup=kbd, parse_mode="Markdown")
            except Exception:  # noqa: BLE001
                await update.message.reply_text(txt, reply_markup=kbd, parse_mode="Markdown")
            await update.message.reply_text(f"✅ Carpeta `{name}` creada.", parse_mode="Markdown")
            return

    # Pending custom question answer?
    if "q_custom_qid" in ctx.bot_data:
        qid_k = ctx.bot_data.pop("q_custom_qid")
        data = PENDING_Q.get(qid_k)
        if data and not data["future"].done():
            data["future"].set_result(text)
            await update.message.reply_text("✅ Respuesta enviada a Claude.")
            await _relocate_status(data.get("skey"))
            return
        # The question already resolved/expired (answered via button, timed out,
        # or you ran a command meanwhile). Don't swallow this message with a stale
        # "expiró" — drop the dead flag and route it normally to the active session.

    # Resolve target: reply > send target > active
    reply = update.message.reply_to_message
    effort = None
    if reply and reply.from_user and reply.from_user.is_bot and reply.message_id in MSG2SESS:
        tgt = MSG2SESS[reply.message_id]
        directory, skey = tgt["directory"], tgt["skey"]
        meta = db.get_session_meta(_resume_for(skey) or "")
        model = (meta or {}).get("model") or _active_model()
        effort = (meta or {}).get("effort")
    elif reply and reply.from_user and reply.from_user.is_bot and reply.message_id not in MSG2SESS:
        # Reply to a bot message whose tracking expired (MSG2SESS limit = 200).
        await update.message.reply_text(
            "⚠️ No sé a qué sesión pertenece ese mensaje (probablemente caducó el seguimiento). "
            "Reenvía el texto sin responder, o elige la sesión con /sessions.")
        return
    elif SEND_MODE["on"] and SEND_MODE["target"] is None:
        SEND_MODE["pending_text"] = text
        await cmd_multisesion(update, ctx)
        return
    elif SEND_MODE["target"]:
        t = SEND_MODE["target"]
        directory, skey, model = t["directory"], t["skey"], t["model"]
        effort = t.get("effort")
        SEND_MODE["target"] = None
        if "oneshot_pre" in t:
            SEND_MODE["on"] = t["oneshot_pre"]  # restore mode after one-shot send
    else:
        active = db.get_active()
        if not active or not active.get("directory"):
            await update.message.reply_text("❌ No hay sesión activa. Usa /open.")
            return
        directory = active["directory"]
        skey = _skey(directory, active.get("claude_session_id"))
        model = active.get("model") or cc.DEFAULT_MODEL
        effort = active.get("effort")

    await _dispatch(directory, skey, model, text, effort)


# --------------------------------------------------------------------------- #
# /start /help /restart
# --------------------------------------------------------------------------- #
HELP = (
    "*codex-bot* — codex-cli por Telegram\n\n"
    "/open — explorar carpetas y abrir/crear sesión\n"
    "/tmp [texto] — activa una sesión temporal en /tmp/codex-tmp (se borra al reiniciar el PC)\n"
    "/sessions — gestionar sesiones de un proyecto\n"
    "/models — cambiar modelo de Codex\n"
    "/refreshmodels — actualizar el catálogo de modelos\n"
    "/esfuerzo — nivel de esfuerzo disponible para el modelo\n"
    "/init — inicializar AGENTS.md del proyecto\n"
    "/rename — renombrar la sesión activa (`/rename mi nombre`)\n"
    "/btw — pregunta rápida sobre la sesión, sin tocar su historial\n"
    "/cambios — archivos modificados en la última ejecución\n"
    "/permisos — modo de permisos\n"
    "/send — envío único a sesión específica (un tiro)\n"
    "/multisesion — pregunta destino en cada mensaje\n"
    "/exitmulti — salir de multisesión\n"
    "/close — borrar sesiones de un proyecto\n"
    "/esc — cancelar la tarea en curso\n"
    "/restart — actualizar (git pull + deps) y reiniciar el bot\n\n"
    "Responde a un mensaje del bot para continuar esa sesión concreta.\n"
    "Envía audio para dictar, o archivos para guardarlos en el proyecto."
)


@admin_only
async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    active = db.get_active()
    if active:
        sid = active.get("claude_session_id")
        cwd = active["directory"]
        meta = db.get_session_meta(sid) if sid else None
        model = (meta or {}).get("model") or active.get("model") or cc.DEFAULT_MODEL
        s = _find_session(sid, cwd) if sid else None
        if s:
            head = (f"*Sesión activa*\n{_session_card(s, meta, cwd)}\n"
                    f"🔐 `{PERMISSION_MODE}`\n\n")
        else:
            label = "(nueva)" if not sid else sid[:12]
            head = (f"*Sesión activa*\n📂 `{Path(cwd).name}`\n"
                    f"🧩 `{model}` | 🔐 `{PERMISSION_MODE}`\n"
                    f"📦 `{label}`\n\n")
    else:
        head = f"⚠️ Sin sesión activa · 🔐 `{PERMISSION_MODE}`\n\n"
    await update.message.reply_text(head + HELP, parse_mode="Markdown")


@admin_only
async def cmd_help(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(HELP, parse_mode="Markdown")


async def _git_pull_and_deps() -> tuple[str, bool]:
    """Update this checkout in place before a restart. Returns (report, proceed);
    proceed=False aborts the restart so we never relaunch into a broken env (e.g.
    deps failed to install). Safe by design: `git pull --ff-only` never creates a
    merge commit nor overwrites uncommitted work — if it can't fast-forward it
    aborts and we just restart with the current code."""
    import subprocess, sys

    def _run(cmd, timeout):
        return subprocess.run(cmd, cwd=BOT_DIR, capture_output=True,
                              text=True, timeout=timeout)

    try:
        before = (await asyncio.to_thread(
            _run, ["git", "rev-parse", "HEAD"], 30)).stdout.strip()
        pull = await asyncio.to_thread(_run, ["git", "pull", "--ff-only"], 120)
    except Exception as exc:  # noqa: BLE001
        return (f"⚠️ No pude hacer pull ({exc}); reinicio con el código actual.", True)

    if pull.returncode != 0:
        tail = ((pull.stderr or pull.stdout).strip().splitlines() or ["desconocido"])[-1]
        return (f"⚠️ No pude actualizar ({tail}); reinicio con el código actual.", True)

    after = (await asyncio.to_thread(
        _run, ["git", "rev-parse", "HEAD"], 30)).stdout.strip()
    if before == after:
        return ("✅ Ya estaba al día; reinicio.", True)

    short = after[:7]
    changed = await asyncio.to_thread(
        _run, ["git", "diff", "--name-only", before, after], 30)
    if "requirements.txt" in changed.stdout.split():
        pip = await asyncio.to_thread(
            _run, [sys.executable, "-m", "pip", "install", "-r", "requirements.txt"], 300)
        if pip.returncode != 0:
            tail = (pip.stderr or pip.stdout).strip()[-300:]
            return (f"❌ Actualizado a {short} pero falló `pip install`; "
                    f"aborto el reinicio:\n{tail}", False)
        return (f"✅ Actualizado a {short} + dependencias; reinicio.", True)
    return (f"✅ Actualizado a {short}; reinicio.", True)


@admin_only
async def cmd_restart(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    import subprocess, sys
    service = "codex-bot.service"

    # Detect how the bot is running and pick the matching restart strategy.
    # Order: systemd user unit (preferred — Claude's login lives in this user's
    # ~/.claude) → systemd system unit via passwordless sudo → self re-exec.
    # We probe systemd with `cat`, which finishes *before* anything kills us (a
    # plain `restart` would get SIGTERM'd mid-run inside our own cgroup and
    # falsely report failure). `-n` on sudo so it never blocks waiting for a
    # password nobody can type from Telegram.
    def _unit_exists(cmd: list[str]) -> bool:
        try:
            return subprocess.run(cmd + ["cat", service],
                                  capture_output=True, text=True,
                                  timeout=10).returncode == 0
        except Exception:  # noqa: BLE001
            return False

    restart_cmd = None  # None ⇒ self re-exec (no systemd available)
    if _unit_exists(["systemctl", "--user"]):
        restart_cmd = ["systemd-run", "--user", "--collect",
                       "systemctl", "--user", "restart", service]
    elif _unit_exists(["sudo", "-n", "systemctl"]):
        # System unit reachable with passwordless sudo. Detach via a transient
        # *system* unit so the restart survives our own SIGTERM.
        restart_cmd = ["sudo", "-n", "systemd-run", "--collect",
                       "systemctl", "restart", service]
    # else: no systemd (macOS/launchd, ./run.sh, tmux, nohup…). Fall back to
    # replacing our own process image in place — works under any supervisor (or
    # none), since os.execv keeps the PID and just relaunches the same argv.

    # Pull latest + (re)install deps before relaunching, so /restart always comes
    # back on the newest code. Aborts the restart if deps break.
    report, proceed = await _git_pull_and_deps()
    await _safe_send(report, parse_mode=None)
    if not proceed:
        return

    msg = await update.message.reply_text("🔄 Reiniciando…")
    RESTART_FLAG.write_text(str(msg.message_id))
    # Fire-and-forget; the success message is shown by post_init via
    # RESTART_FLAG once we come back up.
    try:
        if restart_cmd is not None:
            subprocess.Popen(restart_cmd)
        else:
            # Self re-exec: replace this process with a fresh interpreter running
            # the same script. Never returns on success. sys.stdout/err are
            # flushed first so the "🔄 Reiniciando…" log line isn't lost.
            sys.stdout.flush()
            sys.stderr.flush()
            os.execv(sys.executable, [sys.executable, *sys.argv])
    except Exception as exc:  # noqa: BLE001
        RESTART_FLAG.unlink(missing_ok=True)
        await _safe_send(f"❌ No pude lanzar el reinicio: `{exc}`",
                         plain_fallback=f"❌ No pude lanzar el reinicio: {exc}")


# --------------------------------------------------------------------------- #
# App
# --------------------------------------------------------------------------- #
async def on_error(update: object, ctx: ContextTypes.DEFAULT_TYPE):
    """Global error handler: nothing should fail silently. Log with traceback
    and tell the admin what blew up (so a stuck 'loading' button always gets a
    follow-up message)."""
    err = ctx.error
    if isinstance(err, asyncio.CancelledError):
        return
    # Transient polling/network blips (httpx.ReadError, timeouts) are self-healing:
    # PTB retries getUpdates automatically. Log but don't spam the admin.
    if isinstance(err, NetworkError):
        logger.warning("Transient network error (polling): %s", err)
        return
    logger.error("Unhandled error in handler", exc_info=err)
    # Surface a short, safe summary to the admin. Never raise from here.
    try:
        where = ""
        if isinstance(update, Update):
            if update.callback_query and update.callback_query.data:
                where = f" (botón `{update.callback_query.data}`)"
            elif update.effective_message and update.effective_message.text:
                where = f" (mensaje «{update.effective_message.text[:40]}»)"
        msg = f"⚠️ Fallo interno{where}:\n`{type(err).__name__}: {str(err)[:300]}`"
        await APP.bot.send_message(ADMIN_ID, msg, parse_mode="Markdown")
    except Exception:  # noqa: BLE001
        try:
            await APP.bot.send_message(ADMIN_ID, "⚠️ Fallo interno (ver logs).")
        except Exception:  # noqa: BLE001
            pass


def main():
    global APP, KEYSTORE, KEYSTORE_REV
    db.init()
    KEYSTORE = db.load_keystore()  # restore so pre-restart buttons still resolve
    KEYSTORE_REV = {v: k for k, v in KEYSTORE.items()}
    cc.set_question_bridge(_question_bridge)

    asyncio.set_event_loop(asyncio.new_event_loop())  # Python 3.14 fix
    app = Application.builder().token(TOKEN).build()
    APP = app

    app.add_error_handler(on_error)

    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("help", cmd_help))
    app.add_handler(CommandHandler("open", cmd_open))
    app.add_handler(CommandHandler("sessions", cmd_sessions))
    app.add_handler(CommandHandler("close", cmd_close))
    app.add_handler(CommandHandler("models", cmd_models))
    app.add_handler(CommandHandler("refreshmodels", cmd_refreshmodels))
    app.add_handler(CommandHandler("esfuerzo", cmd_esfuerzo))
    app.add_handler(CommandHandler("init", cmd_init))
    app.add_handler(CommandHandler("tmp", cmd_tmp))
    app.add_handler(CommandHandler("rename", cmd_rename))
    app.add_handler(CommandHandler("btw", cmd_btw))
    app.add_handler(CommandHandler("cambios", cmd_cambios))
    app.add_handler(CommandHandler("undo", cmd_undo))
    app.add_handler(CommandHandler("redo", cmd_redo))
    app.add_handler(CommandHandler("status", cmd_status))
    app.add_handler(CommandHandler("permisos", cmd_permisos))
    app.add_handler(CommandHandler("send", cmd_send))
    app.add_handler(CommandHandler("multisesion", cmd_multisesion))
    app.add_handler(CommandHandler("exitmulti", cmd_exitmulti))
    app.add_handler(CommandHandler("esc", cmd_esc))
    app.add_handler(CommandHandler("restart", cmd_restart))
    app.add_handler(CallbackQueryHandler(cb_ob, pattern=r"^ob:"))
    app.add_handler(CallbackQueryHandler(cb_mkdir, pattern=r"^mkdir:"))
    app.add_handler(CallbackQueryHandler(cb_os, pattern=r"^os:"))
    app.add_handler(CallbackQueryHandler(cb_newsess, pattern=r"^newsess:"))
    app.add_handler(CallbackQueryHandler(cb_setmodel, pattern=r"^setmodel:"))
    app.add_handler(CallbackQueryHandler(cb_refreshmodels, pattern=r"^refreshmodels:"))
    app.add_handler(CallbackQueryHandler(cb_seteffort, pattern=r"^seteffort:"))
    app.add_handler(CallbackQueryHandler(cb_actsess, pattern=r"^actsess:"))
    app.add_handler(CallbackQueryHandler(cb_delsess, pattern=r"^delsess:"))
    app.add_handler(CallbackQueryHandler(cb_sesspick, pattern=r"^sesspick:"))
    app.add_handler(CallbackQueryHandler(cb_closedir, pattern=r"^closedir:"))
    app.add_handler(CallbackQueryHandler(cb_closeallok, pattern=r"^closeallok:"))
    app.add_handler(CallbackQueryHandler(cb_closeall, pattern=r"^closeall:"))
    app.add_handler(CallbackQueryHandler(cb_perm_mode, pattern=r"^perm:"))
    app.add_handler(CallbackQueryHandler(cb_perm_answer, pattern=r"^pa:"))
    app.add_handler(CallbackQueryHandler(cb_q_answer, pattern=r"^qa:"))
    app.add_handler(CallbackQueryHandler(cb_q_custom, pattern=r"^qc:"))
    app.add_handler(CallbackQueryHandler(cb_sendpick, pattern=r"^sendpick:"))
    app.add_handler(CallbackQueryHandler(cb_sendback, pattern=r"^sendback:"))
    app.add_handler(CallbackQueryHandler(cb_sessback, pattern=r"^sessback:"))
    app.add_handler(CallbackQueryHandler(cb_sendsess, pattern=r"^sendsess:"))
    app.add_handler(CallbackQueryHandler(cb_sendnew, pattern=r"^sendnew:"))
    app.add_handler(CallbackQueryHandler(cb_sendmodel, pattern=r"^sendmodel:"))
    app.add_handler(CallbackQueryHandler(cb_senddel, pattern=r"^senddel:"))
    app.add_handler(CallbackQueryHandler(cb_abort, pattern=r"^abort:"))
    app.add_handler(CallbackQueryHandler(cb_qcancel, pattern=r"^qcancel:"))
    app.add_handler(CallbackQueryHandler(cb_commitpush, pattern=r"^commitpush:"))
    app.add_handler(CallbackQueryHandler(cb_undo_yes, pattern=r"^undoyes:"))
    app.add_handler(CallbackQueryHandler(cb_cancel, pattern=r"^cancel:"))

    app.add_handler(MessageHandler(filters.Document.ALL | filters.PHOTO | filters.VIDEO, handle_file))
    app.add_handler(MessageHandler(filters.AUDIO | filters.VOICE, handle_audio))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))

    async def post_init(application: Application):
        # Refresh the model catalog from Anthropic's /v1/models. Non-blocking
        # for the bot: on failure we keep the cached/fallback catalog.
        try:
            cc.refresh_catalog()
            logger.info("models catalog: %s", ", ".join(cc.MODELS))
        except Exception as exc:  # noqa: BLE001
            logger.warning(f"models catalog refresh failed: {exc}")

        commands = [
            BotCommand("start", "Estado y menú"),
            BotCommand("open", "Explorar carpetas y abrir/crear sesión"),
            BotCommand("tmp", "Activa sesión temporal en /tmp/codex-tmp"),
            BotCommand("sessions", "Gestionar sesiones"),
            BotCommand("models", "Cambiar modelo"),
            BotCommand("refreshmodels", "Actualizar catálogo de modelos"),
            BotCommand("esfuerzo", "Nivel de esfuerzo del modelo"),
            BotCommand("init", "Inicializar AGENTS.md del proyecto"),
            BotCommand("rename", "Renombrar sesión activa"),
            BotCommand("btw", "Pregunta rápida (no afecta historial)"),
            BotCommand("cambios", "Archivos modificados en la última ejecución"),
            BotCommand("status", "Estado git del proyecto (+ commit y push)"),
            BotCommand("undo", "Deshacer los cambios de la última ejecución"),
            BotCommand("redo", "Rehacer lo último deshecho"),
            BotCommand("permisos", "Modo de permisos"),
            BotCommand("send", "Envío único a sesión específica"),
            BotCommand("multisesion", "Preguntar destino en cada mensaje"),
            BotCommand("exitmulti", "Salir de multisesión"),
            BotCommand("close", "Cerrar proyecto"),
            BotCommand("esc", "Cancelar tarea"),
            BotCommand("restart", "Actualizar (pull) y reiniciar"),
        ]
        await application.bot.set_my_commands(commands)
        await application.bot.set_my_commands(
            commands, scope=BotCommandScopeChat(chat_id=ADMIN_ID))
        await application.bot.set_chat_menu_button(
            chat_id=ADMIN_ID, menu_button=MenuButtonCommands())

        # Orphan recovery: any in-flight task row means a prompt was running
        # when the process died (crash, redeploy, /restart). The Claude
        # subprocess is gone and its status message is frozen — clean it up and
        # tell the user, so nothing looks "stuck working" forever.
        try:
            orphans = db.inflight_all()
        except Exception as exc:  # noqa: BLE001
            logger.warning(f"inflight_all failed: {exc}")
            orphans = []
        if orphans:
            for o in orphans:
                if o.get("msg_id"):
                    await _delete_msg(application.bot, o["msg_id"])
            try:
                db.inflight_clear()
            except Exception as exc:  # noqa: BLE001
                logger.warning(f"inflight_clear failed: {exc}")
            lines = ["⚠️ *El bot se reinició mientras trabajaba.*",
                     "Estas tareas se interrumpieron (no se perdió tu historial, "
                     "pero conviene revisarlas):"]
            for o in orphans[:10]:
                name = (Path(o.get("directory", "")).name or "?").replace("_", "\\_")
                prm = (o.get("prompt") or "").replace("\n", " ").replace("_", "\\_")[:50]
                lines.append(f"• 📂 `{name}` — _{prm}_" if prm else f"• 📂 `{name}`")
            try:
                await application.bot.send_message(
                    ADMIN_ID, "\n".join(lines), parse_mode="Markdown")
            except Exception as exc:  # noqa: BLE001
                logger.warning(f"orphan notice failed: {exc}")

        if RESTART_FLAG.exists():
            try:
                mid = int(RESTART_FLAG.read_text().strip())
                RESTART_FLAG.unlink(missing_ok=True)
            except Exception:  # noqa: BLE001
                RESTART_FLAG.unlink(missing_ok=True)
            else:
                lines = ["✅ *Bot reiniciado*"]
                active = db.get_active()
                if active and active.get("claude_session_id"):
                    sid = active["claude_session_id"]
                    cwd = active["directory"]
                    meta = db.get_session_meta(sid)
                    s = _find_session(sid, cwd)
                    if s:
                        lines.append(f"\n*Sesión activa:*\n{_session_card(s, meta, cwd)}")
                    else:
                        model = (meta or {}).get("model") or cc.DEFAULT_MODEL
                        lines.append(f"\n📂 `{Path(cwd).name}` · 🧩 `{model}`")
                elif active:
                    cwd = active.get("directory", "")
                    model = active.get("model") or cc.DEFAULT_MODEL
                    lines.append(f"\n📂 `{Path(cwd).name}` · 🧩 `{model}` · (nueva sesión)")
                try:
                    await application.bot.edit_message_text(
                        chat_id=ADMIN_ID, message_id=mid,
                        text="\n".join(lines), parse_mode="Markdown")
                except Exception as exc:  # noqa: BLE001
                    logger.warning(f"post_init edit_message_text failed: {exc}")
                    try:
                        await application.bot.edit_message_text(
                            chat_id=ADMIN_ID, message_id=mid,
                            text="\n".join(lines))
                    except Exception as exc2:  # noqa: BLE001
                        logger.warning(f"post_init edit fallback also failed: {exc2}")

    app.post_init = post_init
    logger.info("codex-bot starting (permission_mode=%s, workspace=%s)",
                PERMISSION_MODE, WORKSPACE)
    app.run_polling(drop_pending_updates=True, allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
