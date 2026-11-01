"""Checksums, deterministic reports, and reproducibility metadata."""

from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import importlib.metadata
import json
import os
from pathlib import Path
import platform
import subprocess
from typing import Any, Iterable, Mapping

from drivepriority import __version__


HASH_BLOCK_SIZE = 8 * 1024 * 1024


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(HASH_BLOCK_SIZE), b""):
            digest.update(block)
    return digest.hexdigest()


def sha256_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def stable_json_bytes(payload: Any) -> bytes:
    return (json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2) + "\n").encode("utf-8")


def write_json_atomic(path: Path, payload: Any) -> None:
    """Publish one JSON file atomically within its destination directory."""

    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp")
    data = stable_json_bytes(payload)
    with temporary.open("wb") as stream:
        stream.write(data)
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)


def read_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as stream:
        payload = json.load(stream)
    if not isinstance(payload, dict):
        raise ValueError(f"Expected a JSON object in {path}")
    return payload


def dependency_versions(names: Iterable[str]) -> dict[str, str]:
    versions: dict[str, str] = {}
    for name in names:
        try:
            versions[name] = importlib.metadata.version(name)
        except importlib.metadata.PackageNotFoundError:
            versions[name] = "not-installed"
    return dict(sorted(versions.items()))


def git_revision(repository: Path) -> dict[str, Any]:
    """Read the source revision without changing repository state."""

    def run(*arguments: str) -> str:
        completed = subprocess.run(
            ["git", *arguments],
            cwd=repository,
            check=True,
            capture_output=True,
            text=True,
        )
        return completed.stdout.strip()

    try:
        commit = run("rev-parse", "HEAD")
        branch = run("branch", "--show-current")
        dirty = bool(run("status", "--porcelain", "--untracked-files=normal"))
    except (OSError, subprocess.CalledProcessError):
        return {"commit": None, "branch": None, "dirty": None}
    return {"commit": commit, "branch": branch or None, "dirty": dirty}


def software_provenance(repository: Path) -> dict[str, Any]:
    return {
        "pipeline_version": __version__,
        "python": platform.python_version(),
        "python_implementation": platform.python_implementation(),
        "platform": platform.platform(),
        "dependencies": dependency_versions(("duckdb", "pyarrow", "PyYAML")),
        "git": git_revision(repository),
    }


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def output_inventory(root: Path, *, excluded_names: set[str] | None = None) -> list[dict[str, Any]]:
    excluded_names = excluded_names or set()
    inventory: list[dict[str, Any]] = []
    for path in sorted((item for item in root.rglob("*") if item.is_file()), key=lambda item: item.as_posix()):
        if path.name in excluded_names:
            continue
        relative = path.relative_to(root).as_posix()
        if relative.startswith("work/") or relative.startswith("exception_parts/"):
            continue
        inventory.append(
            {
                "path": relative,
                "bytes": path.stat().st_size,
                "sha256": sha256_file(path),
            }
        )
    return inventory


def verify_inventory(root: Path, inventory: Iterable[Mapping[str, Any]]) -> list[str]:
    errors: list[str] = []
    resolved_root = root.resolve()
    for entry in inventory:
        relative = str(entry["path"])
        candidate = (root / relative).resolve()
        if resolved_root not in candidate.parents:
            errors.append(f"Unsafe manifest path: {relative}")
            continue
        if not candidate.is_file():
            errors.append(f"Missing output file: {relative}")
            continue
        actual_size = candidate.stat().st_size
        if actual_size != int(entry["bytes"]):
            errors.append(f"Size mismatch for {relative}")
            continue
        if sha256_file(candidate) != entry["sha256"]:
            errors.append(f"SHA-256 mismatch for {relative}")
    return errors
