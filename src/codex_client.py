"""Async adapter between the Telegram bot and ``codex exec --json``."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import shutil
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
DEFAULT_CONTEXT_WINDOW = 272_000
CONTEXT_WINDOWS = {model: DEFAULT_CONTEXT_WINDOW for model in MODELS}
EFFORT_LEVELS = {
    "gpt-5.6-sol": ["low", "medium", "high", "xhigh", "max", "ultra"],
    "gpt-5.6-terra": ["low", "medium", "high", "xhigh", "max", "ultra"],
    "gpt-5.6-luna": ["low", "medium", "high", "xhigh", "max"],
}
MODELS_CACHE = Path.home() / ".codex" / "models_cache.json"


def cli_model(model: str | None) -> str | None:
    # "default" may remain in an older bot database; let the CLI resolve it.
    return None if not model or model == "default" else model


def context_window(model: str | None) -> int:
    return CONTEXT_WINDOWS.get(model or DEFAULT_MODEL, DEFAULT_CONTEXT_WINDOW)


def effort_levels(model: str | None) -> list[str]:
    selected = DEFAULT_MODEL if not model or model == "default" else model
    return EFFORT_LEVELS.get(selected, ["low", "medium", "high", "xhigh"])


def refresh_catalog(force: bool = False) -> bool:
    """Load the three highest-priority visible GPT models cached by codex-cli."""
    global DEFAULT_MODEL
    try:
        data = json.loads(MODELS_CACHE.read_text(encoding="utf-8"))
        available = [
            model for model in data.get("models", [])
            if model.get("visibility") == "list"
            and model.get("supported_in_api", True)
            and str(model.get("slug", "")).startswith("gpt-")
        ]
        available.sort(key=lambda model: model.get("priority", 10_000))
        latest = available[:3]
        if not latest:
            return False
        MODELS[:] = [model["slug"] for model in latest]
        MODEL_LABELS.clear()
        MODEL_LABELS.update({
            model["slug"]: model.get("display_name", model["slug"])
            for model in latest
        })
        CONTEXT_WINDOWS.clear()
        CONTEXT_WINDOWS.update({
            model["slug"]: int(model.get("context_window") or DEFAULT_CONTEXT_WINDOW)
            for model in latest
        })
        EFFORT_LEVELS.clear()
        EFFORT_LEVELS.update({
            model["slug"]: [level["effort"] for level in model.get("supported_reasoning_levels", [])]
            for model in latest
        })
        DEFAULT_MODEL = MODELS[0]
        return True
    except (OSError, ValueError, TypeError, KeyError, json.JSONDecodeError) as exc:
        logger.warning("No se pudo cargar el catálogo de modelos de Codex: %s", exc)
        return False


def set_question_bridge(fn) -> None:
    # codex exec has no in-process tool callback bridge.
    return None


def build_mcp_server():
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


def _item_events(item: dict):
    kind = item.get("type", "")
    if kind == "agent_message" and item.get("text"):
        yield {"type": "text", "text": item["text"]}
    elif kind == "reasoning" and (item.get("text") or item.get("summary")):
        yield {"type": "thinking", "text": item.get("text") or item.get("summary")}
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
                yield {"type": "session", "session_id": sid}
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
            yield {"type": "usage", "input": tokens_in, "output": tokens_out}
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


def list_sessions(directory: str | None = None) -> list[Session]:
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
    for session in list_sessions(directory):
        if session.session_id == session_id and session.path:
            session.path.unlink(missing_ok=True)
            return
    raise FileNotFoundError(f"No se encontró la sesión Codex {session_id}")


refresh_catalog()
