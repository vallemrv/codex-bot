"""
Thin git helpers for /undo, /redo, /status.
Sync, subprocess-only — no extra deps.
"""

import os
import subprocess
import tempfile


class GitError(Exception):
    pass


def _run(args: list[str], cwd: str, env: dict | None = None,
         timeout: int = 30) -> str:
    r = subprocess.run(
        args, cwd=cwd, env=env,
        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        timeout=timeout,
    )
    if r.returncode != 0:
        raise GitError(r.stderr.decode(errors="replace").strip())
    return r.stdout.decode(errors="replace")


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #

def is_git_repo(directory: str) -> bool:
    try:
        out = _run(["git", "-C", directory, "rev-parse", "--is-inside-work-tree"],
                   cwd=directory)
        return out.strip() == "true"
    except (GitError, FileNotFoundError, subprocess.TimeoutExpired):
        return False


def repo_status(directory: str) -> dict:
    if not is_git_repo(directory):
        return {"is_repo": False}

    # Branch
    try:
        branch = _run(["git", "rev-parse", "--abbrev-ref", "HEAD"], cwd=directory).strip()
        if branch == "HEAD":
            branch = "(detached)"
    except GitError:
        branch = "(unknown)"

    # Porcelain status (NUL-delimited so paths with spaces/newlines are safe)
    try:
        raw = _run(["git", "status", "--porcelain=v1", "-z"], cwd=directory)
    except GitError:
        raw = ""

    created: list[str] = []
    modified: list[str] = []
    deleted: list[str] = []
    renamed: list[str] = []
    untracked_files: list[str] = []  # for counting lines

    # Each entry is NUL-terminated; renames carry a second NUL-delimited field
    entries = raw.split("\0")
    i = 0
    while i < len(entries):
        entry = entries[i]
        if len(entry) < 3:
            i += 1
            continue
        xy = entry[:2]
        path = entry[3:]
        x, y = xy[0], xy[1]

        if xy == "??":
            created.append(path)
            untracked_files.append(path)
        elif x == "R" or y == "R":
            # Next entry is the original path
            orig = entries[i + 1] if i + 1 < len(entries) else ""
            renamed.append(f"{orig} -> {path}")
            i += 1  # consume the extra field
        elif x in ("A",) and y in (" ", "A"):
            created.append(path)
        elif x == "D" or y == "D":
            deleted.append(path)
        else:
            # M in either column → modified
            modified.append(path)

        i += 1

    # Line counts: tracked changes vs HEAD (staged + unstaged, numstat)
    per_file: dict[str, dict] = {}

    try:
        numstat = _run(["git", "diff", "--numstat", "HEAD"], cwd=directory)
        for line in numstat.splitlines():
            parts = line.split("\t", 2)
            if len(parts) == 3:
                added_s, removed_s, fpath = parts
                if added_s == "-":  # binary file
                    continue
                per_file[fpath] = {
                    "added": int(added_s),
                    "removed": int(removed_s),
                }
    except GitError:
        pass

    # Untracked files: count their lines via numstat --no-index.
    # `diff --no-index` always exits non-zero when the files differ (they always
    # do vs /dev/null), so read stdout directly instead of raising + re-running.
    for fpath in untracked_files:
        full = os.path.join(directory, fpath)
        if not os.path.isfile(full):
            continue
        try:
            r2 = subprocess.run(
                ["git", "diff", "--numstat", "--no-index", "/dev/null", fpath],
                cwd=directory,
                stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                timeout=30,
            )
        except (subprocess.TimeoutExpired, OSError):
            continue
        out = r2.stdout.decode(errors="replace")
        for line in out.splitlines():
            parts = line.split("\t", 2)
            if len(parts) == 3 and parts[0] != "-":
                per_file[fpath] = {"added": int(parts[0]), "removed": int(parts[1])}
                break

    total_added = sum(v["added"] for v in per_file.values())
    total_removed = sum(v["removed"] for v in per_file.values())
    clean = not (created or modified or deleted or renamed)

    return {
        "is_repo": True,
        "branch": branch,
        "created": created,
        "modified": modified,
        "deleted": deleted,
        "renamed": renamed,
        "per_file": per_file,
        "total_added": total_added,
        "total_removed": total_removed,
        "clean": clean,
    }


def snapshot(directory: str) -> str | None:
    """Create a snapshot commit of the full working tree without touching the
    real index or history. Returns the commit sha, or None on failure."""
    if not is_git_repo(directory):
        return None

    # Check HEAD exists
    r = subprocess.run(
        ["git", "rev-parse", "--verify", "HEAD"],
        cwd=directory,
        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
    )
    has_head = r.returncode == 0
    head_sha = r.stdout.decode().strip() if has_head else None

    tmp_index = None
    try:
        with tempfile.NamedTemporaryFile(delete=False) as f:
            tmp_index = f.name
        os.unlink(tmp_index)  # git will create it fresh

        env = dict(os.environ, GIT_INDEX_FILE=tmp_index)

        if has_head:
            _run(["git", "read-tree", "HEAD"], cwd=directory, env=env)
        else:
            # Empty tree — just run add -A from a blank index
            _run(["git", "read-tree", "--empty"], cwd=directory, env=env)

        _run(["git", "add", "-A"], cwd=directory, env=env)
        tree = _run(["git", "write-tree"], cwd=directory, env=env).strip()

        if has_head:
            commit = _run(
                ["git", "commit-tree", tree, "-p", head_sha, "-m", "bot-snapshot"],
                cwd=directory, env=env,
            ).strip()
        else:
            commit = _run(
                ["git", "commit-tree", tree, "-m", "bot-snapshot"],
                cwd=directory, env=env,
            ).strip()

        # Keep the object reachable so GC won't collect it
        _run(["git", "update-ref", f"refs/bot-snapshots/{commit}", commit],
             cwd=directory)
        return commit

    except (GitError, subprocess.TimeoutExpired, OSError):
        return None
    finally:
        if tmp_index and os.path.exists(tmp_index):
            try:
                os.unlink(tmp_index)
            except OSError:
                pass


def untracked_to_clean(directory: str) -> list[str]:
    """Rutas untracked que un `git clean -fd` borraría (dry-run, respeta .gitignore).
    Lista vacía si no hay ninguna o si algo falla."""
    try:
        out = _run(["git", "clean", "-nd"], cwd=directory)
    except (GitError, subprocess.TimeoutExpired, OSError):
        return []
    paths = []
    for line in out.splitlines():
        if line.startswith("Would remove "):
            paths.append(line[len("Would remove "):].strip())
    return paths


def drop_snapshot(directory: str, sha: str) -> None:
    """Delete a snapshot's keep-ref so its objects become collectable by GC.
    Best-effort: a snapshot no longer referenced by any undo/redo stack would
    otherwise pin its objects under refs/bot-snapshots/ forever."""
    if not sha:
        return
    try:
        _run(["git", "update-ref", "-d", f"refs/bot-snapshots/{sha}"], cwd=directory)
    except (GitError, subprocess.TimeoutExpired, OSError):
        pass


def restore(directory: str, sha: str) -> tuple[bool, str]:
    """Restore the working tree to the state captured in snapshot sha.
    Does NOT move HEAD or alter commit history."""
    try:
        _run(["git", "read-tree", sha], cwd=directory)
        _run(["git", "checkout-index", "-f", "-a"], cwd=directory)
        _run(["git", "clean", "-fd"], cwd=directory)
        # Restore index to HEAD so 'git status' stays coherent
        try:
            _run(["git", "read-tree", "HEAD"], cwd=directory)
        except GitError:
            pass  # repo with no commits — index stays as-is
        return True, ""
    except GitError as e:
        return False, str(e)[:600]
    except (subprocess.TimeoutExpired, OSError) as e:
        return False, str(e)[:600]


def commit_push(directory: str, message: str) -> tuple[bool, str]:
    """Stage all changes, commit, then push. Returns (ok, summary_or_error).
    The commit is the critical step; a push failure with no remote still returns
    (True, summary) because the work is safely committed locally."""
    try:
        _run(["git", "add", "-A"], cwd=directory)
    except (GitError, subprocess.TimeoutExpired, OSError) as e:
        return False, str(e)[:600]

    # Check whether there is anything staged before attempting the commit,
    # so we detect "nothing to commit" independently of git's locale.
    try:
        staged = _run(["git", "diff", "--cached", "--name-only"], cwd=directory).strip()
    except (GitError, subprocess.TimeoutExpired, OSError):
        staged = ""

    if not staged:
        return False, "No hay cambios para commitear"

    try:
        _run(["git", "commit", "-m", message], cwd=directory)
    except GitError as e:
        return False, str(e)[:600]
    except (subprocess.TimeoutExpired, OSError) as e:
        return False, str(e)[:600]

    # Build a short summary
    try:
        branch = _run(["git", "rev-parse", "--abbrev-ref", "HEAD"],
                      cwd=directory).strip()
        sha_short = _run(["git", "rev-parse", "--short", "HEAD"],
                         cwd=directory).strip()
        summary = f"{branch} {sha_short}"
    except GitError:
        branch = "main"
        summary = "committed"

    # No remote configured at all → commit is safe locally, nothing to push.
    try:
        remotes = _run(["git", "remote"], cwd=directory).strip()
    except (GitError, subprocess.TimeoutExpired, OSError):
        remotes = ""
    if not remotes:
        return True, f"{summary} (commit local; sin remoto configurado)"

    # Does the branch already track an upstream?
    has_upstream = False
    try:
        _run(["git", "rev-parse", "--abbrev-ref", "--symbolic-full-name", "@{u}"],
             cwd=directory)
        has_upstream = True
    except GitError:
        pass

    try:
        if has_upstream:
            _run(["git", "push"], cwd=directory, timeout=120)
        else:
            _run(["git", "push", "-u", "origin", branch],
                 cwd=directory, timeout=120)
        return True, f"{summary} · push ok"
    except (GitError, subprocess.TimeoutExpired, OSError) as push_err:
        # The commit succeeded; surface the push failure honestly so the bot
        # can show it instead of pretending everything went fine.
        return True, f"{summary} · ⚠️ commit hecho, push FALLÓ: {str(push_err)[:400]}"
