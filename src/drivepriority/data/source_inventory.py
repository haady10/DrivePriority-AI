"""Evidence-first Backblaze archive inventory generation for C1.2."""

from __future__ import annotations

import argparse
from copy import deepcopy
from datetime import date, datetime, timezone
import json
from pathlib import Path, PurePosixPath
import re
from typing import Any, Mapping, Sequence
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


INVENTORY_TOOL_VERSION = "1.0.0"
OFFICIAL_LISTING_URL = (
    "https://www.backblaze.com/cloud-storage/resources/hard-drive-test-data"
)
OFFICIAL_ARCHIVE_ROOT = (
    "https://f001.backblazeb2.com/file/Backblaze-Hard-Drive-Data"
)
APPROVAL_VERSION = "1.0.0"
ASSIGNMENT_VERSION = "1.0.0"
DATE_MEMBER = re.compile(r"(?P<day>20\d{2}-\d{2}-\d{2})\.csv\Z", re.IGNORECASE)
SHA256 = re.compile(r"[0-9a-fA-F]{64}\Z")


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


def load_assignments(path: Path, teammate: str) -> list[str]:
    payload = _read_yaml(path)
    if payload.get("version") != ASSIGNMENT_VERSION:
        raise PipelineError("Unsupported teammate assignment version")
    assignments = payload.get("assignments")
    if not isinstance(assignments, dict) or teammate not in assignments:
        raise PipelineError(f"No assignment record exists for teammate {teammate!r}")
    permitted_teammates = {"Faheem", "Zahid", "Sulaiman", "Shuhail"}
    if set(assignments) != permitted_teammates:
        raise PipelineError(
            "Assignments must contain exactly Faheem, Zahid, Sulaiman, and Shuhail"
        )
    owners: dict[str, str] = {}
    for owner, assigned in assignments.items():
        if not isinstance(assigned, list):
            raise PipelineError(f"Assignment for {owner} must be a quarter list")
        for quarter in assigned:
            if not isinstance(quarter, str) or quarter not in SUPPORTED_QUARTERS:
                raise PipelineError(f"Assignment for {owner} contains an invalid quarter")
            if quarter in owners:
                raise PipelineError(
                    f"Quarter {quarter} is assigned to both {owners[quarter]} and {owner}"
                )
            owners[quarter] = owner
    quarters = assignments[teammate]
    if not isinstance(quarters, list) or not quarters:
        raise PipelineError(
            f"No quarters are assigned to {teammate}; research-lead assignment is required"
        )
    if len(quarters) != len(set(quarters)):
        raise PipelineError(f"Assignment for {teammate} contains duplicate quarters")
    return sorted(quarters)


def load_approvals(path: Path | None) -> dict[str, Any]:
    if path is None:
        return {}
    payload = _read_yaml(path)
    if payload.get("version") != APPROVAL_VERSION:
        raise PipelineError("Unsupported source approval version")
    approvals = payload.get("approvals")
    if not isinstance(approvals, dict):
        raise PipelineError("Source approvals must contain an approvals mapping")
    return approvals


def scan_archive(
    quarter: str,
    archive: Path,
    configuration: Mapping[str, Any],
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
            "archive_url": official_archive_url(quarter),
            "archive_filename": expected_filename,
            "reference_status": "quarter_and_download_link_listed_by_backblaze",
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
        "observed_at": _utc_now(),
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
    return {
        "processing_authorized": raw["decision"] == "authorize_cleaning",
        "authenticity_status": authenticity_status,
        "approved_missing_dates": sorted(approved_gaps),
        "reviewed_by": raw["reviewed_by"],
        "reviewed_at": raw["reviewed_at"],
        "evidence_reference": raw["evidence_reference"],
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
                },
                "expected_dates": calendar["expected_dates"],
                "approved_missing_dates": list(approval["approved_missing_dates"]),
            }
        },
    }
    return configuration


def generate_inventory(
    *,
    teammate: str,
    assignments_path: Path,
    archive_directory: Path,
    evidence_directory: Path,
    base_config_path: Path,
    approvals_path: Path | None = None,
) -> dict[str, Any]:
    quarters = load_assignments(assignments_path, teammate)
    approvals = load_approvals(approvals_path)
    unknown_approvals = sorted(set(approvals) - set(quarters))
    if unknown_approvals:
        raise PipelineError(
            "Approval file contains unassigned quarters: " + ", ".join(unknown_approvals)
        )
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
                    "reason": "assigned_archive_missing",
                }
            )
            continue
        scan = scan_archive(quarter, archive, base_configuration)
        approval = _approval_for_scan(quarter, scan, approvals.get(quarter))
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
            }
        )
    report = {
        "tool_version": INVENTORY_TOOL_VERSION,
        "teammate": teammate,
        "assigned_quarters": quarters,
        "generated_at": _utc_now(),
        "records": records,
    }
    execution = {
        "tool_version": INVENTORY_TOOL_VERSION,
        "teammate": teammate,
        "generated_at": report["generated_at"],
        "quarters": plan,
    }
    write_json_atomic(evidence_directory / "source_inventory_report.json", report)
    write_json_atomic(evidence_directory / "execution_plan.json", execution)
    return execution


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--teammate", required=True)
    parser.add_argument("--assignments", type=Path, required=True)
    parser.add_argument("--archive-dir", type=Path, required=True)
    parser.add_argument("--evidence-dir", type=Path, required=True)
    parser.add_argument("--base-config", type=Path, required=True)
    parser.add_argument("--approvals", type=Path)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    arguments = build_parser().parse_args(argv)
    try:
        result = generate_inventory(
            teammate=arguments.teammate,
            assignments_path=arguments.assignments,
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
