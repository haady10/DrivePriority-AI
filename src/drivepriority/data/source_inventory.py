"""Evidence-first Backblaze archive inventory generation for C1.2."""

from __future__ import annotations

import argparse
import ctypes
from copy import deepcopy
from datetime import date, datetime, timezone
import json
from pathlib import Path, PurePosixPath
import re
import shutil
import time
import tracemalloc
from typing import Any, Mapping, Sequence
import uuid
import zipfile

import yaml

from drivepriority.data.ingest import (
    PipelineError,
    calendar_dates,
    inspect_archive,
    load_configuration,
    quarter_bounds,
)
from drivepriority.data.manifest import sha256_file, write_json_atomic


INVENTORY_TOOL_VERSION = "1.1.0"
OFFICIAL_LISTING_URL = (
    "https://www.backblaze.com/cloud-storage/resources/hard-drive-test-data"
)
OFFICIAL_ARCHIVE_ROOT = (
    "https://f001.backblazeb2.com/file/Backblaze-Hard-Drive-Data"
)
APPROVAL_VERSION = "2.0.0"
LEGACY_APPROVAL_VERSION = "1.0.0"
ASSIGNMENT_VERSION = "1.0.0"
SOURCE_POLICY_VERSION = "1.0.0"
OPERATORS = (
    "Haady Abdul Jabar",
    "Faheem",
    "Zahid",
    "Sulaiman",
    "Shuhail",
)
LEADER = "Haady Abdul Jabar"
DATE_MEMBER = re.compile(r"(?P<day>20\d{2}-\d{2}-\d{2})\.csv\Z", re.IGNORECASE)
SHA256 = re.compile(r"[0-9a-fA-F]{64}\Z")


def _windows_memory() -> dict[str, int | None]:
    if not hasattr(ctypes, "windll"):
        return {"total_physical_bytes": None, "available_physical_bytes": None}

    class MemoryStatus(ctypes.Structure):
        _fields_ = [
            ("length", ctypes.c_ulong),
            ("memory_load", ctypes.c_ulong),
            ("total_physical", ctypes.c_ulonglong),
            ("available_physical", ctypes.c_ulonglong),
            ("total_page_file", ctypes.c_ulonglong),
            ("available_page_file", ctypes.c_ulonglong),
            ("total_virtual", ctypes.c_ulonglong),
            ("available_virtual", ctypes.c_ulonglong),
            ("available_extended_virtual", ctypes.c_ulonglong),
        ]

    status = MemoryStatus()
    status.length = ctypes.sizeof(MemoryStatus)
    if not ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(status)):
        return {"total_physical_bytes": None, "available_physical_bytes": None}
    return {
        "total_physical_bytes": int(status.total_physical),
        "available_physical_bytes": int(status.available_physical),
    }


def supported_quarters() -> tuple[str, ...]:
    quarters: list[str] = []
    for year in range(2016, 2027):
        last = 2 if year == 2026 else 4
        quarters.extend(f"{year}Q{quarter}" for quarter in range(1, last + 1))
    return tuple(quarters)


SUPPORTED_QUARTERS = supported_quarters()


def official_archive_filename(quarter: str) -> str:
    if quarter not in SUPPORTED_QUARTERS:
        raise ValueError(f"Quarter is outside the approved C1.2 scope: {quarter}")
    return f"data_Q{quarter[-1]}_{quarter[:4]}.zip"


def official_archive_url(quarter: str) -> str:
    return f"{OFFICIAL_ARCHIVE_ROOT}/{official_archive_filename(quarter)}"


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _read_yaml(path: Path) -> dict[str, Any]:
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise PipelineError(f"Expected a YAML mapping in {path}")
    return payload


def _write_yaml_atomic(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp")
    temporary.write_text(
        yaml.safe_dump(dict(payload), sort_keys=False),
        encoding="utf-8",
    )
    temporary.replace(path)


def validate_operator(operator: str) -> str:
    if operator not in OPERATORS:
        raise PipelineError(
            f"Unknown operator {operator!r}; choose one of: {', '.join(OPERATORS)}"
        )
    return operator


def load_assignments(path: Path, operator: str) -> list[str]:
    """Return workload-planning assignments without restricting access.

    Any recognized operator may process any supported quarter.  This file is
    retained only to show the original workload plan and must never be used as
    an authorization boundary.
    """
    validate_operator(operator)
    payload = _read_yaml(path)
    if payload.get("version") != ASSIGNMENT_VERSION:
        raise PipelineError("Unsupported teammate assignment version")
    assignments = payload.get("assignments")
    if not isinstance(assignments, dict) or operator not in assignments:
        raise PipelineError(f"No planning record exists for operator {operator!r}")
    if set(assignments) != set(OPERATORS):
        raise PipelineError(
            "Planning assignments must contain exactly the five approved operators"
        )
    for owner, assigned in assignments.items():
        if not isinstance(assigned, list):
            raise PipelineError(f"Assignment for {owner} must be a quarter list")
        for quarter in assigned:
            if not isinstance(quarter, str) or quarter not in SUPPORTED_QUARTERS:
                raise PipelineError(f"Assignment for {owner} contains an invalid quarter")
    quarters = assignments[operator]
    if not isinstance(quarters, list):
        raise PipelineError(f"Planning assignment for {operator} must be a quarter list")
    if len(quarters) != len(set(quarters)):
        raise PipelineError(f"Planning assignment for {operator} contains duplicate quarters")
    return sorted(quarters)


def load_approvals(path: Path | None) -> dict[str, Any]:
    if path is None:
        return {}
    payload = _read_yaml(path)
    if payload.get("version") == LEGACY_APPROVAL_VERSION:
        approvals = payload.get("approvals")
        if not isinstance(approvals, dict):
            raise PipelineError("Source approvals must contain an approvals mapping")
        return approvals
    if payload.get("version") != APPROVAL_VERSION:
        raise PipelineError("Unsupported source approval version")
    records = payload.get("records")
    if not isinstance(records, list):
        raise PipelineError("Version 2 source approvals must contain a records list")
    approvals: dict[str, Any] = {}
    for record in records:
        if not isinstance(record, dict):
            raise PipelineError("Malformed source approval record")
        quarter = record.get("quarter")
        if not isinstance(quarter, str) or quarter not in SUPPORTED_QUARTERS:
            raise PipelineError("Source approval record has an invalid quarter")
        approvals[quarter] = record
    return approvals


def authorize_archive(
    *,
    quarter: str,
    archive: Path,
    base_config_path: Path,
    approvals_path: Path,
    approved_by: str,
    evidence_reference: str,
    source_url: str,
    approve_processing: bool,
    approve_research_use: bool = False,
    evidence_level: str = "level1_local_integrity",
    approved_missing_dates: Sequence[str] = (),
    publisher_sha256: str | None = None,
) -> dict[str, Any]:
    """Append one immutable leader decision for an exact quarter/archive hash."""
    if approved_by != LEADER:
        raise PipelineError(f"Only {LEADER} may create processing approvals")
    if not evidence_reference.strip() or not source_url.strip():
        raise PipelineError("Evidence reference and actual source URL are required")
    if evidence_level not in {
        "level1_local_integrity",
        "level2_official_download_supported",
        "level3_publisher_bytes_verified",
    }:
        raise PipelineError("Unsupported source evidence level")
    if evidence_level == "level1_local_integrity" and approve_research_use:
        raise PipelineError("Local integrity alone cannot approve official-source research use")
    configuration, _ = load_configuration(base_config_path)
    scan = scan_archive(quarter, archive.resolve(), configuration, source_url=source_url)
    archive_sha = str(scan["archive"]["sha256"]).lower()
    if evidence_level == "level3_publisher_bytes_verified":
        if not isinstance(publisher_sha256, str) or publisher_sha256.lower() != archive_sha:
            raise PipelineError(
                "Level 3 requires a publisher-issued SHA-256 matching the archive"
            )
    existing: dict[str, Any]
    if approvals_path.exists():
        existing = _read_yaml(approvals_path)
        if existing.get("version") != APPROVAL_VERSION or not isinstance(
            existing.get("records"), list
        ):
            raise PipelineError(
                "Leader approvals must use the version 2 append-only record format"
            )
    else:
        existing = {"version": APPROVAL_VERSION, "records": []}
    records = list(existing["records"])
    for prior in records:
        if (
            isinstance(prior, dict)
            and prior.get("quarter") == quarter
            and str(prior.get("archive_sha256", "")).lower() == archive_sha
            and prior.get("decision") == "authorize_cleaning"
            and approve_processing
            and prior.get("evidence_level") == evidence_level
            and prior.get("research_use_source_approved")
            == bool(approve_research_use)
        ):
            raise PipelineError(
                "This exact quarter/archive is already authorized; existing approval is reusable"
            )
    authenticity_status = "unverified"
    if approve_research_use and evidence_level == "level2_official_download_supported":
        authenticity_status = "manual_review_approved"
    elif approve_research_use and evidence_level == "level3_publisher_bytes_verified":
        authenticity_status = "publisher_checksum_verified"
    record = {
        "approval_id": f"approval-{uuid.uuid4().hex}",
        "record_version": APPROVAL_VERSION,
        "source_policy_version": SOURCE_POLICY_VERSION,
        "quarter": quarter,
        "decision": "authorize_cleaning" if approve_processing else "do_not_process",
        "archive_sha256": archive_sha,
        "archive_bytes": scan["archive"]["bytes"],
        "archive_filename": scan["archive"]["filename"],
        "reviewed_by": approved_by,
        "reviewed_at": _utc_now(),
        "evidence_reference": evidence_reference.strip(),
        "declared_source_url": source_url.strip(),
        "evidence_level": evidence_level,
        "authenticity_status": authenticity_status,
        "processing_authorization": bool(approve_processing),
        "research_use_source_approved": bool(approve_research_use),
        "archive_integrity_reviewed": True,
        "date_coverage_reviewed": True,
        "official_url_reviewed": evidence_level != "level1_local_integrity",
        "approved_missing_dates": sorted(set(approved_missing_dates)),
        "publisher_sha256": publisher_sha256.lower() if publisher_sha256 else None,
        "remaining_limitations": (
            []
            if evidence_level == "level3_publisher_bytes_verified"
            else ["Publisher-issued checksum unavailable or not independently verified"]
        ),
        "scan_summary": {
            "zip_crc_verified": scan["archive"]["zip_crc_verified"],
            "archive_file_count": scan["archive"]["archive_file_count"],
            "data_csv_member_count": scan["archive"]["data_csv_member_count"],
            "missing_dates": scan["calendar"]["missing_dates"],
            "unexpected_dates": scan["calendar"]["unexpected_dates"],
        },
    }
    # Validate the record against the same admission logic before publication.
    _approval_for_scan(quarter, scan, record)
    records.append(record)
    _write_yaml_atomic(
        approvals_path,
        {"version": APPROVAL_VERSION, "records": records},
    )
    return {"record": record, "scan": scan, "approvals_path": str(approvals_path.resolve())}


def scan_archive(
    quarter: str,
    archive: Path,
    configuration: Mapping[str, Any],
    *,
    source_url: str | None = None,
    download_date: str | None = None,
    capture_method: str = "local_archive_inspection",
    evidence_reference: str | None = None,
) -> dict[str, Any]:
    expected_filename = official_archive_filename(quarter)
    if archive.name != expected_filename:
        raise PipelineError(
            f"Archive filename for {quarter} must be {expected_filename}, got {archive.name}"
        )
    before = archive.stat()
    digest = sha256_file(archive)
    after = archive.stat()
    if (before.st_size, before.st_mtime_ns) != (after.st_size, after.st_mtime_ns):
        raise PipelineError(f"Archive changed while hashing: {archive}")

    with zipfile.ZipFile(archive, "r") as source:
        crc_failure = source.testzip()
    if crc_failure is not None:
        raise PipelineError(f"ZIP CRC verification failed for member {crc_failure}")

    members, inventory = inspect_archive(archive, configuration)
    member_dates: list[str] = []
    unrecognized_csv_members: list[str] = []
    for member in members:
        name = PurePosixPath(member.info.filename).name
        match = DATE_MEMBER.fullmatch(name)
        if match is None:
            unrecognized_csv_members.append(member.info.filename)
            continue
        try:
            parsed = date.fromisoformat(match.group("day")).isoformat()
        except ValueError:
            unrecognized_csv_members.append(member.info.filename)
            continue
        member_dates.append(parsed)

    start, end = quarter_bounds(quarter)
    expected_dates = calendar_dates(start, end)
    observed_dates = sorted(set(member_dates))
    duplicate_dates = sorted(
        day for day in set(member_dates) if member_dates.count(day) > 1
    )
    missing_dates = sorted(set(expected_dates) - set(observed_dates))
    unexpected_dates = sorted(set(observed_dates) - set(expected_dates))
    return {
        "tool_version": INVENTORY_TOOL_VERSION,
        "quarter": quarter,
        "official_reference": {
            "listing_url": OFFICIAL_LISTING_URL,
            "archive_url": source_url or official_archive_url(quarter),
            "archive_filename": expected_filename,
            "reference_status": (
                "operator_declared_official_download_url"
                if source_url
                else "configured_backblaze_download_reference"
            ),
            "publisher_checksum_available": False,
        },
        "archive": {
            "path": str(archive.resolve()),
            "filename": archive.name,
            "bytes": before.st_size,
            "sha256": digest,
            "zip_crc_verified": True,
            "archive_file_count": inventory["archive_file_count"],
            "data_csv_member_count": inventory["member_count"],
            "excluded_metadata_member_count": inventory[
                "excluded_metadata_member_count"
            ],
            "non_csv_member_count": inventory["non_csv_member_count"],
            "selection_accounting_complete": inventory[
                "selection_accounting_complete"
            ],
        },
        "calendar": {
            "expected_dates": expected_dates,
            "observed_member_dates": observed_dates,
            "missing_dates": missing_dates,
            "unexpected_dates": unexpected_dates,
            "duplicate_dates": duplicate_dates,
            "unrecognized_csv_members": sorted(unrecognized_csv_members),
            "coverage_complete": not (
                missing_dates
                or unexpected_dates
                or duplicate_dates
                or unrecognized_csv_members
            ),
        },
        "integrity_status": "verified_local_bytes",
        "authenticity_status": "unverified_publisher_bytes",
        "source_declaration": {
            "download_date": download_date,
            "capture_method": capture_method,
            "evidence_reference": evidence_reference,
            "launcher_observed_download": False,
        },
        "observed_at": _utc_now(),
    }


def resource_check(
    quarter: str,
    archive: Path,
    configuration: Mapping[str, Any],
    *,
    source_url: str | None = None,
) -> dict[str, Any]:
    """Measure bounded archive inspection without running ingestion."""
    archive = archive.resolve()
    disk_before = shutil.disk_usage(archive.parent)
    tracemalloc.start()
    started = time.perf_counter()
    scan = scan_archive(quarter, archive, configuration, source_url=source_url)
    elapsed = time.perf_counter() - started
    _current, peak_python = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    disk_after = shutil.disk_usage(archive.parent)
    memory = _windows_memory()
    return {
        "resource_check_version": "1.0.0",
        "quarter": quarter,
        "archive_sha256": scan["archive"]["sha256"],
        "archive_bytes": scan["archive"]["bytes"],
        "operation": "read_only_hash_crc_member_and_calendar_scan",
        "elapsed_seconds": round(elapsed, 6),
        "peak_python_tracemalloc_bytes": peak_python,
        "disk_free_before_bytes": disk_before.free,
        "disk_free_after_bytes": disk_after.free,
        "disk_growth_observed_bytes": max(0, disk_before.free - disk_after.free),
        "system_memory": memory,
        "bounded_processing_demonstrated": True,
        "full_ingestion_executed": False,
        "duckdb_spill_measured": False,
        "eight_gb_suitability_proven": False,
        "limitations": [
            "tracemalloc excludes native-library allocations",
            "read-only inspection is not a full-quarter ingestion RAM benchmark",
            "DuckDB spill is not exercised by archive inspection",
        ],
        "scan_summary": {
            "zip_crc_verified": scan["archive"]["zip_crc_verified"],
            "archive_file_count": scan["archive"]["archive_file_count"],
            "data_csv_member_count": scan["archive"]["data_csv_member_count"],
            "coverage_complete": scan["calendar"]["coverage_complete"],
        },
        "measured_at": _utc_now(),
    }


def _approval_for_scan(
    quarter: str,
    scan: Mapping[str, Any],
    raw: Any,
) -> dict[str, Any]:
    if raw is None:
        return {
            "processing_authorized": False,
            "authenticity_status": "unverified",
            "approved_missing_dates": [],
            "reason": "no_manual_approval_record",
        }
    if not isinstance(raw, dict):
        raise PipelineError(f"Approval for {quarter} must be a mapping")
    required = ("decision", "reviewed_by", "reviewed_at", "evidence_reference")
    if any(not isinstance(raw.get(name), str) or not raw[name].strip() for name in required):
        raise PipelineError(f"Approval for {quarter} lacks required review evidence")
    observed_sha = str(scan["archive"]["sha256"]).lower()
    if str(raw.get("archive_sha256", "")).lower() != observed_sha:
        raise PipelineError(f"Approval archive SHA-256 mismatch for {quarter}")
    if raw["decision"] not in {"authorize_cleaning", "do_not_process"}:
        raise PipelineError(f"Invalid processing decision for {quarter}")
    approved_gaps = raw.get("approved_missing_dates", [])
    if not isinstance(approved_gaps, list) or not all(
        isinstance(item, str) for item in approved_gaps
    ):
        raise PipelineError(f"Approval gaps for {quarter} must be a date list")
    missing_dates = set(scan["calendar"]["missing_dates"])
    if not set(approved_gaps).issubset(missing_dates):
        raise PipelineError(f"Approval for {quarter} includes a gap not observed in the archive")
    authenticity_status = raw.get("authenticity_status", "unverified")
    if authenticity_status not in {
        "unverified",
        "manual_review_approved",
        "publisher_checksum_verified",
    }:
        raise PipelineError(f"Invalid authenticity status for {quarter}")
    if authenticity_status != "unverified":
        if raw["decision"] != "authorize_cleaning":
            raise PipelineError(
                f"Authenticity cannot be approved for a rejected archive: {quarter}"
            )
        if raw.get("archive_integrity_reviewed") is not True:
            raise PipelineError(f"Archive integrity review is incomplete for {quarter}")
        if raw.get("date_coverage_reviewed") is not True:
            raise PipelineError(f"Date coverage review is incomplete for {quarter}")
        if raw.get("official_url_reviewed") is not True:
            raise PipelineError(f"Official URL review is incomplete for {quarter}")
    evidence_level = raw.get("evidence_level", "legacy_unspecified")
    if evidence_level not in {
        "legacy_unspecified",
        "level1_local_integrity",
        "level2_official_download_supported",
        "level3_publisher_bytes_verified",
    }:
        raise PipelineError(f"Invalid source evidence level for {quarter}")
    if (
        authenticity_status == "publisher_checksum_verified"
        and evidence_level != "legacy_unspecified"
        and evidence_level != "level3_publisher_bytes_verified"
    ):
        raise PipelineError("Publisher-byte verification requires Level 3 evidence")
    return {
        "processing_authorized": raw["decision"] == "authorize_cleaning",
        "authenticity_status": authenticity_status,
        "approved_missing_dates": sorted(approved_gaps),
        "reviewed_by": raw["reviewed_by"],
        "reviewed_at": raw["reviewed_at"],
        "evidence_reference": raw["evidence_reference"],
        "approval_id": raw.get("approval_id"),
        "source_policy_version": raw.get("source_policy_version"),
        "evidence_level": evidence_level,
        "declared_source_url": raw.get("declared_source_url"),
        "remaining_limitations": raw.get("remaining_limitations", []),
        "reason": raw.get("reason"),
    }


def build_quarter_configuration(
    base: Mapping[str, Any],
    scan: Mapping[str, Any],
    approval: Mapping[str, Any],
) -> dict[str, Any]:
    quarter = str(scan["quarter"])
    archive = scan["archive"]
    official = scan["official_reference"]
    calendar = scan["calendar"]
    authenticity_status = str(approval["authenticity_status"])
    authenticity_approved = authenticity_status != "unverified"
    configuration = deepcopy(dict(base))
    configuration["source_inventory"] = {
        "status": "verified",
        "provenance_reference": official["listing_url"],
        "verified_by": "DrivePriority C1.2 source inventory tool",
        "verified_at": scan["observed_at"],
        "quarters": {
            quarter: {
                "status": "approved",
                "canonical_source_url": official["archive_url"],
                "original_filename": archive["filename"],
                "expected_bytes": archive["bytes"] if authenticity_approved else None,
                "expected_sha256": archive["sha256"] if authenticity_approved else None,
                "authenticity_evidence": (
                    {
                        "status": authenticity_status,
                        "reviewed_by": approval.get("reviewed_by"),
                        "reviewed_at": approval.get("reviewed_at"),
                        "evidence_reference": approval.get("evidence_reference"),
                        "approved_sha256": archive["sha256"],
                        "evidence_level": approval.get("evidence_level"),
                        "source_policy_version": approval.get(
                            "source_policy_version"
                        ),
                        "approval_id": approval.get("approval_id"),
                        "remaining_limitations": approval.get(
                            "remaining_limitations", []
                        ),
                    }
                    if authenticity_approved
                    else {"status": "unverified"}
                ),
                "observed_bytes": archive["bytes"],
                "observed_sha256": archive["sha256"],
                "observation_evidence": {
                    "method": "local SHA-256, complete ZIP CRC, member and date inspection",
                    "observed_at": scan["observed_at"],
                    "evidence_reference": str(scan["official_reference"]["listing_url"]),
                    "archive_file_count": archive["archive_file_count"],
                    "data_csv_member_count": archive["data_csv_member_count"],
                    "excluded_metadata_member_count": archive[
                        "excluded_metadata_member_count"
                    ],
                    "publisher_checksum_available": official[
                        "publisher_checksum_available"
                    ],
                    "source_declaration": scan.get("source_declaration"),
                },
                "expected_dates": calendar["expected_dates"],
                "approved_missing_dates": list(approval["approved_missing_dates"]),
            }
        },
    }
    return configuration


def generate_inventory(
    *,
    operator: str | None = None,
    teammate: str | None = None,
    assignments_path: Path | None = None,
    requested_quarters: Sequence[str] | None = None,
    archive_directory: Path,
    evidence_directory: Path,
    base_config_path: Path,
    approvals_path: Path | None = None,
) -> dict[str, Any]:
    actual_operator = validate_operator(operator or teammate or "")
    planned = (
        load_assignments(assignments_path, actual_operator)
        if assignments_path is not None
        else []
    )
    quarters = list(requested_quarters) if requested_quarters is not None else planned
    if not quarters:
        raise PipelineError("Select at least one quarter to inspect or process")
    if any(quarter not in SUPPORTED_QUARTERS for quarter in quarters):
        raise PipelineError("Requested quarter is outside 2016Q1 through 2026Q2")
    if len(quarters) != len(set(quarters)):
        raise PipelineError("Requested quarter list contains duplicates")
    quarters = sorted(quarters)
    approvals = load_approvals(approvals_path)
    if evidence_directory.exists():
        raise PipelineError(f"Evidence directory already exists: {evidence_directory}")
    evidence_directory.mkdir(parents=True)
    base_configuration, _config_hash = load_configuration(base_config_path)
    records: list[dict[str, Any]] = []
    plan: list[dict[str, Any]] = []
    for quarter in quarters:
        archive = archive_directory / official_archive_filename(quarter)
        if not archive.is_file():
            records.append(
                {
                    "quarter": quarter,
                    "scan_status": "missing",
                    "expected_filename": archive.name,
                }
            )
            plan.append(
                {
                    "quarter": quarter,
                    "processing_authorized": False,
                    "reason": "selected_archive_missing",
                }
            )
            continue
        selected_approval = approvals.get(quarter)
        declared_url = (
            selected_approval.get("declared_source_url")
            if isinstance(selected_approval, dict)
            else None
        )
        scan = scan_archive(
            quarter,
            archive,
            base_configuration,
            source_url=declared_url,
            capture_method="launcher_local_archive_inspection",
            evidence_reference=(
                selected_approval.get("evidence_reference")
                if isinstance(selected_approval, dict)
                else None
            ),
        )
        approval = _approval_for_scan(quarter, scan, selected_approval)
        configuration = build_quarter_configuration(base_configuration, scan, approval)
        config_path = evidence_directory / "configs" / f"{quarter}.yaml"
        _write_yaml_atomic(config_path, configuration)
        record = {
            "quarter": quarter,
            "scan_status": "verified",
            "scan": scan,
            "approval": approval,
            "configuration_path": str(config_path.resolve()),
        }
        records.append(record)
        remaining_gaps = sorted(
            set(scan["calendar"]["missing_dates"])
            - set(approval["approved_missing_dates"])
        )
        authorized = bool(approval["processing_authorized"])
        plan.append(
            {
                "quarter": quarter,
                "archive_path": str(archive.resolve()),
                "source_url": official_archive_url(quarter),
                "configuration_path": str(config_path.resolve()),
                "processing_authorized": authorized,
                "expected_validation_status": (
                    "requires_review"
                    if approval["authenticity_status"] == "unverified" or remaining_gaps
                    else "eligible_for_pipeline_validation"
                ),
                "unapproved_missing_dates": remaining_gaps,
                "reason": approval.get("reason"),
                "operator": actual_operator,
                "original_planned_operators": [],
            }
        )
        if assignments_path is not None:
            assignments_payload = _read_yaml(assignments_path).get("assignments", {})
            plan[-1]["original_planned_operators"] = sorted(
                name
                for name, values in assignments_payload.items()
                if isinstance(values, list) and quarter in values
            )
    report = {
        "tool_version": INVENTORY_TOOL_VERSION,
        "operator": actual_operator,
        "operator_selection_is_traceability_not_authentication": True,
        "planned_quarters": planned,
        "selected_quarters": quarters,
        "generated_at": _utc_now(),
        "records": records,
    }
    execution = {
        "tool_version": INVENTORY_TOOL_VERSION,
        "operator": actual_operator,
        "generated_at": report["generated_at"],
        "quarters": plan,
    }
    write_json_atomic(evidence_directory / "source_inventory_report.json", report)
    write_json_atomic(evidence_directory / "execution_plan.json", execution)
    return execution


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--operator", required=True, choices=OPERATORS)
    parser.add_argument("--quarter", action="append", required=True)
    parser.add_argument("--assignments", type=Path)
    parser.add_argument("--archive-dir", type=Path, required=True)
    parser.add_argument("--evidence-dir", type=Path, required=True)
    parser.add_argument("--base-config", type=Path, required=True)
    parser.add_argument("--approvals", type=Path)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    arguments = build_parser().parse_args(argv)
    try:
        result = generate_inventory(
            operator=arguments.operator,
            assignments_path=arguments.assignments,
            requested_quarters=arguments.quarter,
            archive_directory=arguments.archive_dir,
            evidence_directory=arguments.evidence_dir,
            base_config_path=arguments.base_config,
            approvals_path=arguments.approvals,
        )
    except (PipelineError, OSError, ValueError, zipfile.BadZipFile) as exc:
        raise SystemExit(f"C1.2 source inventory failed: {exc}") from exc
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
