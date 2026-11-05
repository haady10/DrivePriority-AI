"""Simple fail-closed incremental integration for completed quarter ZIP packages."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import shutil
import stat
import uuid
import zipfile
from typing import Any, Mapping, Sequence

from drivepriority.integration.admission import _load_policy, central_ledger_approval, check_quarter
from drivepriority.integration.central_validate import validate_snapshot
from drivepriority.integration.central_writer import CentralWriteError, integrate_quarters
from drivepriority.integration.incremental import IncrementalError, incremental_update


OPERATOR_VERSION = "1.1.5"
STATE_VERSION = "1.1.0"
HEX64 = re.compile(r"[0-9a-f]{64}\Z")
GIT_COMMIT = re.compile(r"[0-9a-f]{40}\Z")
QUARTER = re.compile(r"20(?:1[6-9]|2[0-6])Q[1-4]\Z")
MAX_MEMBERS = 100_000
MAX_UNCOMPRESSED_BYTES = 2 * 1024**4
MAX_COMPRESSION_RATIO = 1_000


class OperatorError(RuntimeError):
    """Refuse an unsafe package or integration state."""


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, ValueError) as exc:
        raise OperatorError(f"Unreadable JSON evidence: {path.name} ({type(exc).__name__})") from exc
    if not isinstance(value, dict):
        raise OperatorError(f"JSON evidence is not an object: {path.name}")
    return value


def _atomic_json(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name("." + path.name + ".tmp-" + uuid.uuid4().hex)
    data = (json.dumps(value, indent=2, sort_keys=True) + "\n").encode("utf-8")
    with temporary.open("wb") as stream:
        stream.write(data)
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)


def _safe_member(info: zipfile.ZipInfo) -> PurePosixPath:
    name = info.filename.replace("\\", "/")
    member = PurePosixPath(name)
    file_type = stat.S_IFMT(info.external_attr >> 16)
    if (
        not name
        or member.is_absolute()
        or any(part in ("", ".", "..") for part in member.parts)
        or ":" in name
        or file_type == stat.S_IFLNK
    ):
        raise OperatorError(f"Unsafe ZIP member path or type: {info.filename!r}")
    return member


def _verify_zip(path: Path) -> tuple[str, list[zipfile.ZipInfo]]:
    if not path.is_file() or path.is_symlink():
        raise OperatorError(f"Input ZIP is missing or unsafe: {path}")
    package_sha = _sha256(path)
    try:
        with zipfile.ZipFile(path) as archive:
            infos = archive.infolist()
            if not infos or len(infos) > MAX_MEMBERS:
                raise OperatorError("ZIP member count is empty or exceeds the safety limit")
            total = 0
            for info in infos:
                _safe_member(info)
                total += info.file_size
                if total > MAX_UNCOMPRESSED_BYTES:
                    raise OperatorError("ZIP uncompressed size exceeds the safety limit")
                if info.file_size and info.compress_size == 0:
                    raise OperatorError("ZIP member has an impossible compression size")
                if info.compress_size and info.file_size / info.compress_size > MAX_COMPRESSION_RATIO:
                    raise OperatorError("ZIP member compression ratio exceeds the safety limit")
            bad = archive.testzip()
            if bad is not None:
                raise OperatorError(f"ZIP CRC verification failed: {bad}")
    except zipfile.BadZipFile as exc:
        raise OperatorError(f"Invalid or corrupted ZIP: {path.name}") from exc
    return package_sha, infos


def _cache_paths(work: Path, package_sha: str, token: str) -> tuple[Path, Path]:
    """Return short same-volume paths; the marker retains the complete SHA-256."""
    return work / "P" / package_sha[:20], work / "S" / token[:12]


def _extract_verified(path: Path, package_sha: str, work: Path) -> Path:
    target, staging = _cache_paths(work, package_sha, uuid.uuid4().hex)
    marker = target / ".drivepriority-package.json"
    if target.exists():
        if not marker.is_file() or _json(marker).get("package_sha256") != package_sha:
            raise OperatorError(f"Cached package identity mismatch: {package_sha}")
        expected: dict[str, tuple[int, str]] = {}
        with zipfile.ZipFile(path) as archive:
            for info in archive.infolist():
                member = _safe_member(info)
                if info.is_dir():
                    continue
                digest = hashlib.sha256()
                with archive.open(info) as stream:
                    for block in iter(lambda: stream.read(1024 * 1024), b""):
                        digest.update(block)
                expected[member.as_posix()] = (info.file_size, digest.hexdigest())
        actual = {
            item.relative_to(target).as_posix()
            for item in target.rglob("*")
            if item.is_file() and item != marker
        }
        if actual != set(expected):
            raise OperatorError(f"Cached package inventory mismatch: {package_sha}")
        for relative, (size, digest) in expected.items():
            candidate = target.joinpath(*PurePosixPath(relative).parts)
            if candidate.is_symlink() or candidate.stat().st_size != size or _sha256(candidate) != digest:
                raise OperatorError(f"Cached package content mismatch: {relative}")
        return target
    target.parent.mkdir(parents=True, exist_ok=True)
    staging.parent.mkdir(parents=True, exist_ok=True)
    staging.mkdir()
    try:
        with zipfile.ZipFile(path) as archive:
            for info in archive.infolist():
                member = _safe_member(info)
                destination = staging.joinpath(*member.parts)
                if info.is_dir():
                    destination.mkdir(parents=True, exist_ok=True)
                    continue
                destination.parent.mkdir(parents=True, exist_ok=True)
                with archive.open(info) as source, destination.open("xb") as output:
                    shutil.copyfileobj(source, output, length=1024 * 1024)
        if _sha256(path) != package_sha:
            raise OperatorError("Input ZIP changed during verified extraction")
        _atomic_json(
            staging / marker.name,
            {"version": OPERATOR_VERSION, "package_sha256": package_sha, "source_filename": path.name},
        )
        staging.rename(target)
    except BaseException:
        shutil.rmtree(staging, ignore_errors=True)
        raise
    return target


def _unique(root: Path, name: str) -> Path:
    matches = sorted(path for path in root.rglob(name) if path.is_file() and not path.is_symlink())
    if len(matches) != 1:
        raise OperatorError(f"Package must contain exactly one {name}; found {len(matches)}")
    return matches[0]


def _verify_output_inventory(root: Path, records: Any, *, excluded: set[str]) -> None:
    if not isinstance(records, list) or not records:
        raise OperatorError(f"Evidence inventory is missing under {root.name}")
    seen: set[str] = set()
    for record in records:
        if not isinstance(record, dict) or not isinstance(record.get("path"), str):
            raise OperatorError("Invalid evidence inventory record")
        relative = record["path"]
        pure = PurePosixPath(relative)
        if pure.is_absolute() or any(part in ("", ".", "..") for part in pure.parts) or "\\" in relative or ":" in relative:
            raise OperatorError("Unsafe evidence inventory path")
        if relative in seen or pure.name in excluded:
            raise OperatorError("Duplicate or invalid evidence inventory path")
        seen.add(relative)
        candidate = root.joinpath(*pure.parts)
        if not candidate.is_file() or candidate.is_symlink():
            raise OperatorError(f"Missing evidence inventory file: {relative}")
        expected_hash = record.get("sha256")
        expected_bytes = record.get("bytes")
        if (
            not isinstance(expected_hash, str)
            or not HEX64.fullmatch(expected_hash.lower())
            or _sha256(candidate) != expected_hash.lower()
            or not isinstance(expected_bytes, int)
            or expected_bytes < 0
            or candidate.stat().st_size != expected_bytes
        ):
            raise OperatorError(f"Evidence inventory mismatch: {relative}")


def _release_registry(path: Path | None) -> dict[str, Any]:
    registry = path or Path(__file__).resolve().parents[3] / "configs" / "central_release_trust.json"
    releases = _json(registry).get("releases")
    if not isinstance(releases, dict):
        raise OperatorError("Central release trust registry is invalid")
    return releases


def inspect_package(zip_path: Path, work: Path, *, release_registry: Path | None = None) -> dict[str, Any]:
    """Verify and inspect one immutable quarter package without admitting it."""
    package_sha, infos = _verify_zip(zip_path)
    extracted = _extract_verified(zip_path, package_sha, work)
    manifest_path = _unique(extracted, "source_manifest.json")
    revalidation_path = _unique(extracted, "revalidation_report.json")
    analysis_manifest_path = _unique(extracted, "analysis_manifest.json")
    anomaly_path = _unique(extracted, "anomaly_report.json")
    revalidation_manifest_path = _unique(extracted, "revalidation_manifest.json")
    manifest = _json(manifest_path)
    revalidation = _json(revalidation_path)
    analysis_manifest = _json(analysis_manifest_path)
    revalidation_manifest = _json(revalidation_manifest_path)
    quarter = manifest.get("assigned_quarter")
    if not isinstance(quarter, str) or not QUARTER.fullmatch(quarter):
        raise OperatorError("Package source manifest has an invalid or out-of-scope quarter")
    source_manifest_sha = _sha256(manifest_path)
    if revalidation.get("quarter") != quarter or revalidation.get("source_manifest_sha256") != source_manifest_sha:
        raise OperatorError("Revalidation evidence is not bound to the cleaned quarter")
    analysis_manifest_sha = _sha256(analysis_manifest_path)
    if (
        analysis_manifest.get("quarter") != quarter
        or analysis_manifest.get("source_manifest_sha256") != source_manifest_sha
        or revalidation.get("analysis_manifest_sha256") != analysis_manifest_sha
    ):
        raise OperatorError("Anomaly-analysis evidence is not bound to the cleaned quarter")
    _verify_output_inventory(analysis_manifest_path.parent, analysis_manifest.get("outputs"), excluded={"analysis_manifest.json"})
    _verify_output_inventory(revalidation_manifest_path.parent, revalidation_manifest.get("outputs"), excluded={"revalidation_manifest.json"})
    if anomaly_path.parent != analysis_manifest_path.parent:
        raise OperatorError("Anomaly report is outside its verified analysis inventory")
    source = manifest.get("source")
    source_sha = source.get("sha256") if isinstance(source, dict) else None
    revalidation_source = revalidation.get("source")
    approval = revalidation_source.get("approval_evidence") if isinstance(revalidation_source, dict) else None
    in_package_approval = (
        isinstance(source_sha, str)
        and bool(HEX64.fullmatch(source_sha.lower()))
        and isinstance(approval, dict)
        and str(approval.get("archive_sha256", "")).lower() == source_sha.lower()
        and approval.get("research_use_source_approved") is True
        and approval.get("evidence_level") in {"level2_official_download_supported", "level3_publisher_bytes_verified"}
    )
    if not in_package_approval:
        ledger_ok = (
            isinstance(source_sha, str)
            and bool(HEX64.fullmatch(source_sha.lower()))
            and central_ledger_approval(_load_policy(None), quarter, source_sha) is not None
        )
        if not ledger_ok:
            raise OperatorError("Package lacks exact-hash research-use source approval")
    software = manifest.get("software") if isinstance(manifest.get("software"), dict) else {}
    git = software.get("git") if isinstance(software.get("git"), dict) else {}
    commit = git.get("commit")
    recovery = software.get("recovery_identity") if isinstance(software.get("recovery_identity"), dict) else {}
    trust: dict[str, Any] = {
        "root": str(manifest_path.parent),
        "archive_sha256": source_sha.lower(),
        "revalidation_root": str(revalidation_path.parent),
    }
    if isinstance(commit, str) and GIT_COMMIT.fullmatch(commit.lower()):
        trust["pipeline_commit"] = commit.lower()
    else:
        runtime_hash = recovery.get("runtime_source_sha256")
        pipeline_version = str(software.get("pipeline_version", ""))
        release = _release_registry(release_registry).get(pipeline_version)
        release_hash = release.get("package_sha256") if isinstance(release, dict) else None
        if (
            not isinstance(runtime_hash, str)
            or not HEX64.fullmatch(runtime_hash.lower())
            or not isinstance(release_hash, str)
            or not HEX64.fullmatch(release_hash.lower())
        ):
            raise OperatorError(f"Gitless pipeline release is not independently pinned: {pipeline_version or 'unknown'}")
        pinned_runtime = release.get("runtime_source_sha256") if isinstance(release, dict) else None
        if pinned_runtime is not None:
            allowed = {pinned_runtime} if isinstance(pinned_runtime, str) else set(pinned_runtime or [])
            allowed = {str(item).lower() for item in allowed}
            if runtime_hash.lower() not in allowed:
                raise OperatorError(
                    f"Runtime source hash {runtime_hash.lower()} is not the one pinned for pipeline "
                    f"{pipeline_version}"
                )
        trust.update({"runtime_source_sha256": runtime_hash.lower(), "release_sha256": release_hash.lower()})
    return {
        "quarter": quarter,
        "zip_path": str(zip_path.resolve()),
        "zip_filename": zip_path.name,
        "package_sha256": package_sha,
        "package_bytes": zip_path.stat().st_size,
        "zip_members": len(infos),
        "source_manifest_sha256": source_manifest_sha,
        "revalidation_report_sha256": _sha256(revalidation_path),
        "validation_status": revalidation.get("status"),
        "trust": trust,
    }


def _load_inventory(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"version": STATE_VERSION, "current_snapshot": None, "quarters": {}}
    value = _json(path)
    if value.get("version") != STATE_VERSION or not isinstance(value.get("quarters"), dict):
        raise OperatorError("Integration inventory version or structure is invalid")
    return value


def _next_snapshot(output_root: Path) -> Path:
    indices = []
    for child in output_root.glob("Snapshot-*"):
        match = re.fullmatch(r"Snapshot-(\d{4})", child.name)
        if match:
            indices.append(int(match.group(1)))
    return output_root / f"Snapshot-{max(indices, default=0) + 1:04d}"


def _write_text_report(path: Path, report: Mapping[str, Any]) -> None:
    lines = [
        "DrivePriority integration result",
        "",
        f"Status: {report['status']}",
        f"ZIPs discovered: {report['zips_discovered']}",
        f"New quarters admitted: {len(report['newly_admitted'])}",
        f"Previously integrated: {len(report['previously_integrated'])}",
        f"Rejected or blocked: {len(report['blocked'])}",
        f"Total integrated quarters: {report['total_integrated_quarters']}",
        f"Total integrated rows: {report['total_integrated_rows']}",
        f"Output: {report.get('dataset_output') or 'No published snapshot'}",
    ]
    if report["newly_admitted"]:
        lines.extend(["", "New: " + ", ".join(report["newly_admitted"])])
    if report["blocked"]:
        lines.extend(["", "Blocked:"])
        lines.extend(f"- {item['package']}: {item['reason']}" for item in report["blocked"])
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _recover_publication(journal_path: Path, inventory_path: Path, inventory: dict[str, Any]) -> bool:
    if not journal_path.exists():
        return False
    journal = _json(journal_path)
    output = Path(str(journal.get("output", "")))
    additions = journal.get("additions")
    if not output.is_dir() or not isinstance(additions, dict):
        return False
    validation = validate_snapshot(output)
    if validation.get("status") != "PASS":
        raise OperatorError("Pending published snapshot failed recovery validation")
    central_manifest = _json(output / "central_manifest.json")
    declared = central_manifest.get("input_quarters")
    if not isinstance(declared, dict):
        raise OperatorError("Pending snapshot lacks quarter provenance")
    for quarter, item in additions.items():
        if quarter not in declared or declared[quarter].get("archive_sha256") != item.get("archive_sha256"):
            raise OperatorError("Pending snapshot does not match the durable integration journal")
        inventory["quarters"][quarter] = item
    inventory["current_snapshot"] = str(output.resolve())
    _atomic_json(inventory_path, inventory)
    journal_path.unlink()
    return True


def run_integration(
    root: Path,
    *,
    batch_size: int = 8192,
    minimum_free_bytes: int = 1024**3,
    release_registry: Path | None = None,
    _interrupt_after_snapshot: bool = False,
) -> dict[str, Any]:
    """Discover packages and publish a new immutable snapshot when eligible inputs exist."""
    root = root.resolve()
    input_root = root / "Input_Quarters"
    output_root = root / "Integrated_Data"
    reports_root = root / "Reports"
    work = root / ".Integration_Work"
    for directory in (input_root, output_root, reports_root, work):
        directory.mkdir(parents=True, exist_ok=True)
    inventory_path = reports_root / "integration_inventory.json"
    journal_path = work / "pending_publication.json"
    inventory = _load_inventory(inventory_path)
    recovered = _recover_publication(journal_path, inventory_path, inventory)
    if recovered:
        inventory = _load_inventory(inventory_path)
    zip_paths = sorted(path for path in input_root.glob("*.zip") if path.is_file() and not path.is_symlink())
    blocked: list[dict[str, str]] = []
    candidates: list[dict[str, Any]] = []
    for path in zip_paths:
        try:
            candidates.append(inspect_package(path, work, release_registry=release_registry))
        except (OperatorError, OSError, zipfile.BadZipFile) as exc:
            blocked.append({"package": path.name, "reason": str(exc)})
    by_quarter: dict[str, list[dict[str, Any]]] = {}
    for candidate in candidates:
        by_quarter.setdefault(candidate["quarter"], []).append(candidate)
    previous = dict(inventory["quarters"])
    newly_eligible: dict[str, dict[str, Any]] = {}
    additions: dict[str, dict[str, Any]] = {}
    unchanged: list[str] = []
    for quarter, items in sorted(by_quarter.items()):
        hashes = {item["package_sha256"] for item in items}
        prior = previous.get(quarter)
        if prior is not None:
            if hashes == {prior.get("package_sha256")}:
                unchanged.append(quarter)
            else:
                blocked.append({"package": quarter, "reason": "Conflicting package for an already integrated quarter"})
            continue
        if len(hashes) != 1:
            blocked.append({"package": quarter, "reason": "Multiple different ZIP hashes claim the same quarter"})
            continue
        candidate = items[0]
        decision = check_quarter(
            candidate["trust"]["root"],
            expected_quarter=quarter,
            trusted_archive_sha256=candidate["trust"]["archive_sha256"],
            trusted_pipeline_commit=candidate["trust"].get("pipeline_commit"),
            trusted_runtime_source_sha256=candidate["trust"].get("runtime_source_sha256"),
            trusted_release_sha256=candidate["trust"].get("release_sha256"),
            revalidation_root=candidate["trust"]["revalidation_root"],
        )
        if decision.get("decision") != "ADMIT":
            blocked.append({"package": candidate["zip_filename"], "reason": f"{decision.get('decision')}: {'; '.join(decision.get('reasons', []))}"})
            continue
        newly_eligible[quarter] = candidate["trust"]
        additions[quarter] = {
            "package_sha256": candidate["package_sha256"],
            "package_filename": candidate["zip_filename"],
            "archive_sha256": candidate["trust"]["archive_sha256"],
            "source_manifest_sha256": candidate["source_manifest_sha256"],
            "source_root": candidate["trust"]["root"],
            "revalidation_report": str(Path(candidate["trust"]["revalidation_root"]) / "revalidation_report.json"),
            "revalidation_report_sha256": candidate["revalidation_report_sha256"],
            "pipeline_commit": candidate["trust"].get("pipeline_commit"),
            "runtime_source_sha256": candidate["trust"].get("runtime_source_sha256"),
            "release_sha256": candidate["trust"].get("release_sha256"),
            "validation_status": candidate["validation_status"],
        }
    if newly_eligible:
        if shutil.disk_usage(output_root).free < minimum_free_bytes:
            raise OperatorError("Insufficient free disk space for a new immutable snapshot")
        output = _next_snapshot(output_root)
        for item in additions.values():
            item["integration_version"] = OPERATOR_VERSION
            item["admitted_snapshot"] = str(output.resolve())
        planned = {"version": STATE_VERSION, "output": str(output), "additions": additions}
        if journal_path.exists() and _json(journal_path) != planned:
            raise OperatorError("Pending integration journal conflicts with discovered packages")
        _atomic_json(journal_path, planned)
        current = inventory.get("current_snapshot")
        try:
            if current:
                checkpoint = output.parent / ("." + output.name + ".incremental-checkpoint")
                result = incremental_update(
                    Path(current),
                    newly_eligible,
                    output,
                    batch_size=batch_size,
                    resume=checkpoint.exists(),
                )
            else:
                result = integrate_quarters(newly_eligible, output, batch_size=batch_size)
                validation = validate_snapshot(output)
                if validation.get("status") != "PASS":
                    raise OperatorError("New snapshot failed independent validation")
        except (CentralWriteError, IncrementalError, OperatorError, OSError, ValueError):
            checkpoint = output.parent / ("." + output.name + ".incremental-checkpoint")
            if not output.exists() and not checkpoint.exists():
                journal_path.unlink(missing_ok=True)
            raise
        if _interrupt_after_snapshot:
            raise OperatorError("Controlled interruption after verified snapshot publication")
        for quarter, item in additions.items():
            inventory["quarters"][quarter] = item
        inventory["current_snapshot"] = str(output.resolve())
        _atomic_json(inventory_path, inventory)
        journal_path.unlink(missing_ok=True)
        total_rows = int(result.get("rows", 0))
    else:
        output = Path(inventory["current_snapshot"]) if inventory.get("current_snapshot") else None
        validation = validate_snapshot(output) if output else {"status": "PASS", "total_rows": 0}
        if output and validation.get("status") != "PASS":
            raise OperatorError("Existing integrated snapshot failed validation")
        total_rows = int(validation.get("total_rows", 0))
    report = {
        "version": OPERATOR_VERSION,
        "status": "completed_with_blocks" if blocked else "completed",
        "zips_discovered": len(zip_paths),
        "newly_admitted": sorted(newly_eligible),
        "previously_integrated": sorted(set(previous) | set(unchanged)),
        "blocked": blocked,
        "total_integrated_quarters": len(inventory["quarters"]),
        "total_integrated_rows": total_rows,
        "dataset_output": inventory.get("current_snapshot"),
        "recovered_pending_publication": recovered,
    }
    sequence = len(list(reports_root.glob("integration_run_*.json"))) + 1
    report_json = reports_root / f"integration_run_{sequence:04d}.json"
    _atomic_json(report_json, report)
    _write_text_report(report_json.with_suffix(".txt"), report)
    return report


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path.cwd())
    parser.add_argument("--batch-size", type=int, default=8192)
    parser.add_argument("--minimum-free-bytes", type=int, default=1024**3)
    parser.add_argument("--release-registry", type=Path)
    arguments = parser.parse_args(argv)
    try:
        report = run_integration(
            arguments.root,
            batch_size=arguments.batch_size,
            minimum_free_bytes=arguments.minimum_free_bytes,
            release_registry=arguments.release_registry,
        )
    except (OperatorError, CentralWriteError, IncrementalError, OSError, ValueError) as exc:
        print(f"ERROR: {exc}")
        return 2
    print(json.dumps(report, indent=2, sort_keys=True))
    return 1 if report["blocked"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
