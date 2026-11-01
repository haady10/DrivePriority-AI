"""Checksums, deterministic reports, and reproducibility metadata."""

from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import importlib.metadata
import json
import os
from pathlib import Path, PurePosixPath
import platform
import subprocess
import sys
from typing import Any, Iterable, Mapping

from drivepriority import __version__


HASH_BLOCK_SIZE = 8 * 1024 * 1024
PIPELINE_RUNTIME_PATHS = (
    "src/drivepriority/__init__.py",
    "src/drivepriority/data/__init__.py",
    "src/drivepriority/data/ingest.py",
    "src/drivepriority/data/manifest.py",
    "src/drivepriority/data/schema.py",
    "src/drivepriority/data/validate.py",
)
REQUIREMENTS_PATH = "requirements.txt"
RECOVERY_DEPENDENCIES = ("duckdb", "pyarrow", "PyYAML")
RUNTIME_IDENTITY_VERSION = "1.0.0"


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


def runtime_environment_identity() -> dict[str, Any]:
    """Return the installed interpreter and scientific-runtime identity."""

    return {
        "identity_version": RUNTIME_IDENTITY_VERSION,
        "python_version": platform.python_version(),
        "python_implementation": platform.python_implementation(),
        "python_cache_tag": getattr(sys.implementation, "cache_tag", None),
        "dependencies": dependency_versions(RECOVERY_DEPENDENCIES),
    }


def _validate_runtime_against_requirements(
    requirements: Path, runtime_environment: Mapping[str, Any]
) -> None:
    locked: dict[str, str] = {}
    for raw_line in requirements.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "==" not in line:
            continue
        name, version = line.split("==", 1)
        locked[name.strip().lower()] = version.strip()
    installed = runtime_environment.get("dependencies")
    if not isinstance(installed, Mapping):
        raise RuntimeError("Installed scientific runtime identity is unavailable")
    mismatches: list[str] = []
    for name in RECOVERY_DEPENDENCIES:
        approved = locked.get(name.lower())
        actual = installed.get(name)
        if approved is None:
            mismatches.append(f"{name}=missing-lock")
        elif actual != approved:
            mismatches.append(f"{name}=installed-{actual}/approved-{approved}")
    if mismatches:
        raise RuntimeError(
            "Installed scientific runtime does not match requirements.txt: "
            + ", ".join(mismatches)
        )


def git_revision(
    repository: Path, *, pathspecs: Iterable[str] | None = None
) -> dict[str, Any]:
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
        status_arguments = ["status", "--porcelain", "--untracked-files=all"]
        if pathspecs is not None:
            status_arguments.extend(["--", *pathspecs])
        dirty = bool(run(*status_arguments))
    except (OSError, subprocess.CalledProcessError):
        return {"commit": None, "branch": None, "dirty": None}
    return {"commit": commit, "branch": branch or None, "dirty": dirty}


def pipeline_recovery_identity(repository: Path) -> dict[str, Any]:
    """Return the immutable code and environment-file identity used for recovery."""

    source_inventory: list[dict[str, str]] = []
    for relative in PIPELINE_RUNTIME_PATHS:
        path = repository / relative
        if not path.is_file():
            raise FileNotFoundError(f"Required pipeline source is unavailable: {relative}")
        source_inventory.append({"path": relative, "sha256": sha256_file(path)})

    requirements = repository / REQUIREMENTS_PATH
    if not requirements.is_file():
        raise FileNotFoundError(
            f"Required dependency lock file is unavailable: {REQUIREMENTS_PATH}"
        )

    scoped_revision = git_revision(
        repository,
        pathspecs=(*PIPELINE_RUNTIME_PATHS, REQUIREMENTS_PATH),
    )
    runtime_environment = runtime_environment_identity()
    _validate_runtime_against_requirements(requirements, runtime_environment)
    return {
        "pipeline_version": __version__,
        "git_commit": scoped_revision["commit"],
        "git_metadata_available": (
            scoped_revision["commit"] is not None
            and scoped_revision["dirty"] is not None
        ),
        "runtime_source_sha256": sha256_bytes(stable_json_bytes(source_inventory)),
        "requirements_sha256": sha256_file(requirements),
        "pipeline_scoped_dirty": scoped_revision["dirty"],
        "runtime_environment": runtime_environment,
        "runtime_environment_sha256": sha256_bytes(
            stable_json_bytes(runtime_environment)
        ),
    }


def software_provenance(
    repository: Path,
    *,
    recovery_identity: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    identity = dict(recovery_identity or pipeline_recovery_identity(repository))
    return {
        "pipeline_version": __version__,
        "python": platform.python_version(),
        "python_implementation": platform.python_implementation(),
        "platform": platform.platform(),
        "dependencies": dependency_versions(RECOVERY_DEPENDENCIES),
        "git": git_revision(repository),
        "runtime_environment_identity": identity.get("runtime_environment"),
        "recovery_identity": identity,
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


def verify_inventory(
    root: Path,
    inventory: Iterable[Mapping[str, Any]],
    *,
    excluded_names: set[str] | None = None,
    required_paths: set[str] | None = None,
) -> list[str]:
    errors: list[str] = []
    resolved_root = root.resolve()
    entries = list(inventory)
    if not entries:
        errors.append("Output inventory is missing or empty")
    listed_paths: set[str] = set()
    for entry in entries:
        if not isinstance(entry, Mapping):
            errors.append("Malformed output inventory entry")
            continue
        try:
            relative = str(entry["path"])
        except KeyError:
            errors.append("Output inventory entry has no path")
            continue
        parsed = PurePosixPath(relative)
        if (
            not relative
            or parsed.is_absolute()
            or "\\" in relative
            or any(part in {"", ".", ".."} for part in parsed.parts)
            or parsed.as_posix() != relative
        ):
            errors.append(f"Unsafe manifest path: {relative}")
            continue
        if relative in listed_paths:
            errors.append(f"Duplicate manifest path: {relative}")
            continue
        listed_paths.add(relative)
        candidate = (root / relative).resolve()
        if candidate == resolved_root or resolved_root not in candidate.parents:
            errors.append(f"Unsafe manifest path: {relative}")
            continue
        if not candidate.is_file():
            errors.append(f"Missing output file: {relative}")
            continue
        try:
            expected_bytes = int(entry["bytes"])
            expected_sha256 = str(entry["sha256"])
        except (KeyError, TypeError, ValueError):
            errors.append(f"Invalid manifest metadata for {relative}")
            continue
        actual_size = candidate.stat().st_size
        if actual_size != expected_bytes:
            errors.append(f"Size mismatch for {relative}")
            continue
        if sha256_file(candidate) != expected_sha256:
            errors.append(f"SHA-256 mismatch for {relative}")
    actual_paths = {
        str(entry["path"])
        for entry in output_inventory(root, excluded_names=excluded_names)
    }
    for missing in sorted(actual_paths - listed_paths):
        errors.append(f"Unlisted output artifact: {missing}")
    for unexpected in sorted(listed_paths - actual_paths):
        errors.append(f"Manifest lists non-output artifact: {unexpected}")
    for required in sorted((required_paths or set()) - listed_paths):
        errors.append(f"Required output artifact is not inventoried: {required}")
    return errors
