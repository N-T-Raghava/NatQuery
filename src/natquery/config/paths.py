"""Filesystem locations and private-file helpers (0700 dirs / 0600 files)."""

from __future__ import annotations

import contextlib
import os
import re
from pathlib import Path

_NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$")


def base_dir() -> Path:
    """~/.natquery, or $NATQUERY_HOME when set (deployments / containers)."""
    env = os.environ.get("NATQUERY_HOME")
    return Path(env) if env else Path.home() / ".natquery"


def safe_name(name: str) -> str:
    """Validate a workspace / DB name used as a directory name (no traversal)."""
    name = (name or "").strip()
    if not _NAME_RE.fullmatch(name) or ".." in name:
        raise ValueError(
            "Invalid name: use 1-64 letters, digits, '_', '-' or '.', starting with a letter/digit."
        )
    return name


def ensure_private_dir(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    with contextlib.suppress(OSError):  # e.g. Windows / unsupported FS
        os.chmod(path, 0o700)
    return path


def workspace_dir(db_name: str) -> Path:
    return ensure_private_dir(base_dir() / safe_name(db_name))


def write_private(path: Path, text: str) -> None:
    """Atomically write a file readable only by the current user."""
    ensure_private_dir(path.parent)
    tmp = path.with_name(path.name + ".tmp")
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w", encoding="utf-8") as f:
        f.write(text)
    os.replace(tmp, path)
    with contextlib.suppress(OSError):
        os.chmod(path, 0o600)


def open_private_append(path: Path):
    ensure_private_dir(path.parent)
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
    return os.fdopen(fd, "a", encoding="utf-8")
