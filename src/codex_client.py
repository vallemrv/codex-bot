"""Async adapter between the Telegram bot and ``codex exec --json``."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path

logger = logging.getLogger(__name__)

EDIT_TOOLS = {"file_change", "apply_patch"}
MODELS = ["gpt-5.6-sol", "gpt-5.6-terra", "gpt-5.6-luna"]
DEFAULT_MODEL = "gpt-5.6-sol"
MODEL_LABELS = {
    "gpt-5.6-sol": "GPT-5.6 Sol",
    "gpt-5.6-terra": "GPT-5.6 Terra",
    "gpt-5.6-luna": "GPT-5.6 Luna",
}
# The catalog's ``context_window`` is the long-context billing threshold, not
# the ceiling: ``max_context_window`` is.  Codex only serves the bigger window
# when asked for it (see ``_command``), and it keeps
# ``effective_context_window_percent`` of it for the conversation itself.
DEFAULT_MAX_CONTEXT_WINDOW = 872_000
DEFAULT_EFFECTIVE_PERCENT = 95
DEFAULT_CONTEXT_WINDOW = DEFAULT_MAX_CONTEXT_WINDOW * DEFAULT_EFFECTIVE_PERCENT // 100
MAX_CONTEXT_WINDOWS = {model: DEFAULT_MAX_CONTEXT_WINDOW for model in MODELS}
CONTEXT_WINDOWS = {model: DEFAULT_CONTEXT_WINDOW for model in MODELS}
EFFORT_LEVELS = {
    "gpt-5.6-sol": ["low", "medium", "high", "xhigh", "max", "ultra"],
    "gpt-5.6-terra": ["low", "medium", "high", "xhigh", "max", "ultra"],
    "gpt-5.6-luna": ["low", "medium", "high", "xhigh", "max"],
}
MODELS_CACHE = Path.home() / ".codex" / "models_cache.json"
SESSIONS_DIR = Path.home() / ".codex" / "sessions"
CATALOG_TIMEOUT = 60  # seconds allowed to `codex debug models`


def cli_model(model: str | None) -> str | None:
    # "default" may remain in an older bot database; let the CLI resolve it.
    return None if not model or model == "default" else model


def context_window(model: str | None) -> int:
    """Tokens the conversation can actually use, for the status indicators."""
    return CONTEXT_WINDOWS.get(model or DEFAULT_MODEL, DEFAULT_CONTEXT_WINDOW)


def max_context_window(model: str | None) -> int:
    """Window to request from codex, before its effective-percent haircut."""
    selected = DEFAULT_MODEL if not model or model == "default" else model
    return MAX_CONTEXT_WINDOWS.get(selected, DEFAULT_MAX_CONTEXT_WINDOW)


def effort_levels(model: str | None) -> list[str]:
    selected = DEFAULT_MODEL if not model or model == "default" else model
    return EFFORT_LEVELS.get(selected, ["low", "medium", "high", "xhigh"])


def _fetch_catalog() -> bool:
    """Make codex-cli revalidate ``models_cache.json`` against the API.

    Reading the cache file is not enough: it only changes when codex-cli
    decides to refresh it.  ``codex debug models`` re-fetches the catalog
    (that is what ``--bundled`` opts out of) and rewrites the cache.
    """
    try:
        proc = subprocess.run([_codex_bin(), "debug", "models"],
                              capture_output=True, timeout=CATALOG_TIMEOUT,
                              check=False)
    except (OSError, subprocess.SubprocessError) as exc:
        logger.warning("No se pudo refrescar el catálogo de Codex: %s", exc)
        return False
    if proc.returncode != 0:
        logger.warning("`codex debug models` falló (%s): %s", proc.returncode,
                       proc.stderr.decode("utf-8", "replace").strip()[:200])
        return False
    return True


def refresh_catalog(force: bool = False) -> bool:
    """Load the visible GPT models cached by codex-cli.

    With ``force`` the catalog is re-fetched first; the return value then
    reports whether that live fetch worked, so callers can say when they are
    falling back to a stale cache.
    """
    global DEFAULT_MODEL
    live = _fetch_catalog() if force else False
    try:
        data = json.loads(MODELS_CACHE.read_text(encoding="utf-8"))
        available = [
            model for model in data.get("models", [])
            if model.get("visibility") == "list"
            and model.get("supported_in_api", True)
            and str(model.get("slug", "")).startswith("gpt-")
        ]
        available.sort(key=lambda model: model.get("priority", 10_000))
        if not available:
            return False
        MODELS[:] = [model["slug"] for model in available]
        MODEL_LABELS.clear()
        MODEL_LABELS.update({
            model["slug"]: model.get("display_name", model["slug"])
            for model in available
        })
        MAX_CONTEXT_WINDOWS.clear()
        CONTEXT_WINDOWS.clear()
        for model in available:
            ceiling = int(model.get("max_context_window")
                          or model.get("context_window")
                          or DEFAULT_MAX_CONTEXT_WINDOW)
            percent = int(model.get("effective_context_window_percent")
                          or DEFAULT_EFFECTIVE_PERCENT)
            MAX_CONTEXT_WINDOWS[model["slug"]] = ceiling
            CONTEXT_WINDOWS[model["slug"]] = ceiling * percent // 100
        EFFORT_LEVELS.clear()
        EFFORT_LEVELS.update({
            model["slug"]: [level["effort"] for level in model.get("supported_reasoning_levels", [])]
            for model in available
        })
        DEFAULT_MODEL = MODELS[0]
        return live or not force
    except (OSError, ValueError, TypeError, KeyError, json.JSONDecodeError) as exc:
        logger.warning("No se pudo cargar el catálogo de modelos de Codex: %s", exc)
        return False


def set_question_bridge(fn) -> None:
    # codex exec has no in-process tool callback bridge.
    return None


def build_mcp_server():
    return None


def _last_context_usage(session_id: str | None) -> tuple[int, int] | None:
    """Read the actual context used by the latest turn from Codex's journal.

    The ``usage`` field in a resumed ``turn.completed`` can be cumulative.  The
    journal's ``last_token_usage`` is the context of that individual turn.
    """
    if not session_id:
        return None
    try:
        for path in SESSIONS_DIR.rglob("*.jsonl"):
            with path.open(encoding="utf-8") as journal:
                if session_id not in journal.readline():
                    continue
                latest: tuple[int, int] | None = None
                for line in journal:
                    try:
                        event = json.loads(line)
                        payload = event.get("payload") or {}
                        if event.get("type") != "event_msg" or payload.get("type") != "token_count":
                            continue
                        info = payload.get("info") or {}
                        tokens = int((info.get("last_token_usage") or {}).get("input_tokens") or 0)
                        window = int(info.get("model_context_window") or 0)
                        if tokens and window:
                            latest = (tokens, window)
                    except (ValueError, TypeError, json.JSONDecodeError):
                        continue
                return latest
    except OSError as exc:
        logger.debug("No se pudo leer el contexto de la sesión %s: %s", session_id, exc)
    return None


def _codex_bin() -> str:
    configured = os.getenv("CODEX_BIN")
    if configured:
        return configured
    found = shutil.which("codex")
    if found:
        return found
    fallback = Path.home() / ".local" / "bin" / "codex"
    return str(fallback)


class CodexProcess:
    def __init__(self, process: asyncio.subprocess.Process):
        self.process = process

    async def interrupt(self) -> None:
        if self.process.returncode is None:
            self.process.terminate()
            try:
                await asyncio.wait_for(self.process.wait(), timeout=5)
            except asyncio.TimeoutError:
                self.process.kill()


def _command(cwd: str, model: str | None, resume_session_id: str | None,
             permission_mode: str, effort: str | None, ephemeral: bool = False) -> list[str]:
    cmd = [_codex_bin(), "exec"]
    if resume_session_id:
        cmd += ["resume"]
    cmd += ["--json", "--skip-git-repo-check"]
    selected = cli_model(model)
    if selected:
        cmd += ["--model", selected]
    # Without this codex defaults to the 272K billing threshold instead of the
    # model's real ceiling; it clamps anything larger to ``max_context_window``.
    cmd += ["--config", f"model_context_window={max_context_window(model)}"]
    if effort:
        cmd += ["--config", f'model_reasoning_effort="{effort}"']
    if ephemeral:
        cmd += ["--ephemeral"]
    if permission_mode == "bypassPermissions":
        cmd += ["--dangerously-bypass-approvals-and-sandbox"]
    elif permission_mode in {"plan", "read-only"}:
        cmd += ["--config", 'sandbox_mode="read-only"']
    else:
        cmd += ["--config", 'sandbox_mode="workspace-write"']
    if resume_session_id:
        cmd += [resume_session_id, "-"]
    else:
        cmd += ["--cd", cwd, "-"]
    return cmd


def _reasoning_text(item: dict) -> str:
    """Text of a reasoning item, tolerating the several shapes codex-cli uses.

    Recent versions ship reasoning as encrypted content with an empty ``summary``
    list, so this returns "" and no thinking event is emitted; the agent's
    preamble messages carry the live narration instead.
    """
    text = item.get("text")
    if isinstance(text, str) and text.strip():
        return text
    summary = item.get("summary")
    if isinstance(summary, str):
        return summary
    if isinstance(summary, list):
        parts = [part.get("text", "") if isinstance(part, dict) else str(part)
                 for part in summary]
        return "\n".join(p for p in parts if p)
    return ""


def _item_events(item: dict):
    kind = item.get("type", "")
    if kind == "agent_message" and item.get("text"):
        yield {"type": "text", "text": item["text"]}
    elif kind == "reasoning":
        text = _reasoning_text(item)
        if text:
            yield {"type": "thinking", "text": text}
    elif kind == "command_execution":
        yield {"type": "tool", "name": "Bash", "input": {"command": item.get("command", "")}}
    elif kind == "file_change":
        for change in item.get("changes") or []:
            yield {"type": "tool", "name": "file_change",
                   "input": {"file_path": change.get("path", "")}}
    elif kind in {"mcp_tool_call", "web_search", "todo_list"}:
        name = item.get("tool") or item.get("name") or kind
        yield {"type": "tool", "name": name, "input": item.get("arguments") or item}


async def run(prompt: str, cwd: str, model: str | None, resume_session_id: str | None,
              permission_mode: str, can_use_tool=None, mcp_server=None,
              effort: str | None = None, ephemeral: bool = False):
    """Run one Codex turn and yield the normalized events expected by the bot."""
    cmd = _command(cwd, model, resume_session_id, permission_mode, effort, ephemeral)
    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd, cwd=cwd, stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
            limit=10 * 1024 * 1024)
    except Exception as exc:
        yield {"type": "error", "message": f"No se pudo iniciar codex-cli: {exc}"}
        return

    client = CodexProcess(proc)
    yield {"type": "client", "client": client}
    proc.stdin.write(prompt.encode("utf-8"))
    await proc.stdin.drain()
    proc.stdin.close()
    stderr_task = asyncio.create_task(proc.stderr.read())

    final_text = ""
    live_session_id = resume_session_id
    tokens_in = tokens_out = 0
    error_message = ""
    while True:
        line = await proc.stdout.readline()
        if not line:
            break
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            continue
        etype = event.get("type", "")
        if etype == "thread.started":
            sid = event.get("thread_id")
            if sid:
                live_session_id = sid
                yield {"type": "session", "session_id": sid}
        elif etype == "turn.started":
            # The first item can take several seconds; mark the turn live so the
            # status leaves "ESPERANDO" as soon as codex accepts the prompt.
            yield {"type": "turn_start"}
        elif etype in {"item.started", "item.completed", "item.updated"}:
            item = event.get("item") or {}
            for normalized in _item_events(item):
                if normalized["type"] == "text":
                    final_text = normalized["text"]
                yield normalized
        elif etype == "turn.completed":
            usage = event.get("usage") or {}
            tokens_in = usage.get("input_tokens", 0) or 0
            tokens_out = usage.get("output_tokens", 0) or 0
            context = _last_context_usage(live_session_id)
            if context:
                tokens_in, window = context
            else:
                window = None
            yield {"type": "usage", "input": tokens_in, "output": tokens_out,
                   "context_window": window}
        elif etype in {"error", "turn.failed"}:
            error_message = (event.get("message") or
                             (event.get("error") or {}).get("message") or str(event))

    stderr = (await stderr_task).decode("utf-8", errors="replace").strip()
    returncode = await proc.wait()
    if returncode and not error_message:
        error_message = stderr or f"codex-cli terminó con código {returncode}"
    if error_message:
        yield {"type": "error", "message": error_message}
    yield {
        "type": "result", "text": final_text, "cost": 0.0,
        "input": tokens_in, "output": tokens_out,
        "session_id": resume_session_id or "", "is_error": bool(returncode or error_message),
        "subtype": "error_during_execution" if (returncode or error_message) else "success",
    }


async def ask_side(prompt: str, cwd: str, model: str | None,
                   resume_session_id: str | None):
    guidance = ("Responde brevemente y en español usando solo el contexto de la "
                "conversación. No modifiques archivos ni ejecutes comandos.\n\n" + prompt)
    async for event in run(guidance, cwd, model, resume_session_id, "plan",
                           effort=None, ephemeral=True):
        yield event


@dataclass
class Session:
    session_id: str
    cwd: str
    summary: str
    first_prompt: str
    last_modified: int
    file_size: int
    git_branch: str | None = None
    custom_title: str | None = None
    path: Path | None = None
    context_tokens: int | None = None
    context_window: int | None = None


def _session_home() -> Path:
    return Path(os.getenv("CODEX_HOME", str(Path.home() / ".codex"))) / "sessions"


def _read_session(path: Path) -> Session | None:
    sid = cwd = first_prompt = ""
    try:
        with path.open(encoding="utf-8") as handle:
            for line in handle:
                try:
                    entry = json.loads(line)
                except json.JSONDecodeError:
                    continue
                payload = entry.get("payload") or {}
                if entry.get("type") == "session_meta":
                    sid = payload.get("id", sid)
                    cwd = payload.get("cwd", cwd)
                if not first_prompt and payload.get("type") == "user_message":
                    message = payload.get("message", "")
                    first_prompt = message if isinstance(message, str) else str(message)
                if sid and cwd and first_prompt:
                    break
        if not sid or not cwd:
            return None
        stat = path.stat()
        summary = first_prompt.strip().replace("\n", " ")[:80] or sid[:8]
        return Session(sid, cwd, summary, first_prompt, int(stat.st_mtime * 1000),
                       stat.st_size, path=path)
    except OSError:
        return None


def _app_server_request(method: str, params: dict) -> dict:
    """Make one authoritative request to Codex's thread store."""
    proc = subprocess.Popen(
        [_codex_bin(), "app-server", "--stdio"],
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        text=True,
    )
    assert proc.stdin is not None and proc.stdout is not None

    def request(request_id: int, request_method: str, request_params: dict) -> dict:
        proc.stdin.write(json.dumps({
            "method": request_method, "id": request_id, "params": request_params,
        }) + "\n")
        proc.stdin.flush()
        while True:
            line = proc.stdout.readline()
            if not line:
                raise RuntimeError("codex app-server terminó sin responder")
            response = json.loads(line)
            if response.get("id") != request_id:
                continue
            if response.get("error"):
                raise RuntimeError(str(response["error"]))
            return response.get("result") or {}

    try:
        request(1, "initialize", {"clientInfo": {
            "name": "codex_telegram_bot", "title": "Codex Telegram Bot",
            "version": "1.0",
        }})
        proc.stdin.write(json.dumps({"method": "initialized", "params": {}}) + "\n")
        proc.stdin.flush()
        return request(2, method, params)
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=2)
        except subprocess.TimeoutExpired:
            proc.kill()


def _list_sessions_native(directory: str | None = None) -> list[Session]:
    sessions: list[Session] = []
    cursor = None
    while True:
        params: dict = {
            "limit": 100,
            "archived": False,
            # The bot creates threads through `codex exec`; app-server's default
            # interactive-source filter intentionally excludes them.
            "sourceKinds": ["exec"],
            "sortKey": "updated_at",
            "sortDirection": "desc",
        }
        if directory is not None:
            params["cwd"] = directory
        if cursor:
            params["cursor"] = cursor
        result = _app_server_request("thread/list", params)
        for thread in result.get("data") or []:
            path = Path(thread["path"]) if thread.get("path") else None
            size = path.stat().st_size if path and path.exists() else 0
            usage = _last_context_usage(thread.get("id"))
            sessions.append(Session(
                session_id=thread["id"], cwd=thread.get("cwd") or "",
                summary=(thread.get("preview") or "").strip().replace("\n", " ")[:80],
                first_prompt=thread.get("preview") or "",
                last_modified=int(thread.get("updatedAt", 0) * 1000),
                file_size=size,
                git_branch=(thread.get("gitInfo") or {}).get("branch"),
                custom_title=thread.get("name"), path=path,
                context_tokens=usage[0] if usage else None,
                context_window=usage[1] if usage else None,
            ))
        cursor = result.get("nextCursor")
        if not cursor:
            return sessions


def list_sessions(directory: str | None = None) -> list[Session]:
    try:
        return _list_sessions_native(directory)
    except Exception as exc:  # keep the bot usable with older Codex versions
        logger.warning("thread/list falló; usando lectura JSONL: %s", exc)
    root = _session_home()
    if not root.exists():
        return []
    sessions = []
    for path in root.rglob("*.jsonl"):
        session = _read_session(path)
        if session and (directory is None or session.cwd == directory):
            sessions.append(session)
    return sorted(sessions, key=lambda item: item.last_modified, reverse=True)


def delete_session(session_id: str, directory: str | None = None) -> None:
    # This removes the active/archived rollout, Codex state-DB metadata and
    # spawned descendants. Unlinking only the JSONL leaves ghost records.
    _app_server_request("thread/delete", {"threadId": session_id})


def set_session_name(session_id: str, name: str) -> None:
    _app_server_request("thread/name/set", {"threadId": session_id, "name": name})


refresh_catalog()
