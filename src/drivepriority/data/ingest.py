"""Command-line ingestion of one existing local Backblaze quarterly archive."""

from __future__ import annotations

import argparse
import csv
from dataclasses import dataclass
from datetime import date, timedelta
import io
import json
import math
import os
from pathlib import Path
import re
import shutil
import sys
from typing import Any, Mapping, Sequence
from urllib.parse import urlparse
import zipfile

import pyarrow as pa
import pyarrow.parquet as pq
import yaml

from drivepriority.data import SCHEMA_VERSION
from drivepriority.data.manifest import (
    output_inventory,
    pipeline_recovery_identity,
    read_json,
    sha256_bytes,
    sha256_file,
    software_provenance,
    stable_json_bytes,
    utc_now,
    verify_inventory,
    write_json_atomic,
)
from drivepriority.data.schema import (
    ConversionIssue,
    HeaderAnalysis,
    SchemaError,
    analyze_headers,
    canonical_arrow_schema,
    exception_arrow_schema,
    parse_iso_date,
    raw_record_evidence,
    union_smart_columns,
)
from drivepriority.data.validate import (
    DuplicateTracker,
    QualityStats,
    ValidationError,
    convert_record,
    validate_accounting,
)


QUARTER_PATTERN = re.compile(r"^(?P<year>20\d{2})Q(?P<quarter>[1-4])$")
FINAL_REPORT_NAMES = {
    "source_manifest.json",
    "schema_report.json",
    "quality_report.json",
    "validation_results.json",
    "exceptions.parquet",
}
RUNTIME_NAMES = {"run_state.json", "state.duckdb", "state.duckdb.wal"}


class PipelineError(RuntimeError):
    """Raised for a safe, actionable pipeline failure."""


class IngestionInterrupted(PipelineError):
    """Controlled interruption used to verify recovery behavior."""


@dataclass(frozen=True)
class MemberPlan:
    archive_index: int
    info: zipfile.ZipInfo
    analysis: HeaderAnalysis


def quarter_bounds(quarter: str) -> tuple[date, date]:
    match = QUARTER_PATTERN.fullmatch(quarter)
    if not match:
        raise PipelineError("Quarter must use YYYYQ1, YYYYQ2, YYYYQ3, or YYYYQ4")
    year = int(match.group("year"))
    quarter_number = int(match.group("quarter"))
    start_month = 1 + (quarter_number - 1) * 3
    start = date(year, start_month, 1)
    if quarter_number == 4:
        following = date(year + 1, 1, 1)
    else:
        following = date(year, start_month + 3, 1)
    return start, following - timedelta(days=1)


def calendar_dates(start: date, end: date) -> list[str]:
    days = (end - start).days
    return [(start + timedelta(days=offset)).isoformat() for offset in range(days + 1)]


def _positive_integer(value: Any, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise PipelineError(f"Configuration {label} must be a positive integer")
    return value


def _validate_source_inventory_configuration(payload: Mapping[str, Any]) -> None:
    inventory = payload.get("source_inventory")
    if not isinstance(inventory, dict):
        raise PipelineError("Configuration source_inventory must be a mapping")
    status = inventory.get("status")
    if status not in {"unavailable", "verified"}:
        raise PipelineError(
            "Configuration source_inventory.status must be 'unavailable' or 'verified'"
        )
    quarters = inventory.get("quarters")
    if not isinstance(quarters, dict):
        raise PipelineError("Configuration source_inventory.quarters must be a mapping")
    if status == "unavailable":
        if quarters:
            raise PipelineError(
                "Unavailable source inventory cannot contain purported approved quarter entries"
            )
        return

    for field_name in ("provenance_reference", "verified_by", "verified_at"):
        value = inventory.get(field_name)
        if not isinstance(value, str) or not value.strip():
            raise PipelineError(
                f"Verified source inventory requires non-empty {field_name}"
            )

    for quarter, entry in quarters.items():
        if not isinstance(quarter, str) or not QUARTER_PATTERN.fullmatch(quarter):
            raise PipelineError(f"Invalid source inventory quarter key: {quarter!r}")
        if not isinstance(entry, dict) or entry.get("status") != "approved":
            raise PipelineError(
                f"Source inventory entry {quarter} must be an approved mapping"
            )
        canonical_url = entry.get("canonical_source_url")
        if not isinstance(canonical_url, str):
            raise PipelineError(
                f"Source inventory entry {quarter} requires canonical_source_url"
            )
        _validate_source_url(canonical_url)
        filename = entry.get("original_filename")
        if (
            not isinstance(filename, str)
            or not filename
            or Path(filename).name != filename
        ):
            raise PipelineError(
                f"Source inventory entry {quarter} requires a safe original_filename"
            )
        expected_bytes = entry.get("expected_bytes")
        if expected_bytes is not None:
            _positive_integer(
                expected_bytes,
                f"source_inventory.quarters.{quarter}.expected_bytes",
            )
        expected_sha256 = entry.get("expected_sha256")
        if expected_sha256 is not None and not (
            isinstance(expected_sha256, str)
            and re.fullmatch(r"[0-9a-fA-F]{64}", expected_sha256)
        ):
            raise PipelineError(
                f"Source inventory entry {quarter} expected_sha256 must be null or 64 hexadecimal characters"
            )

        expected_dates = entry.get("expected_dates")
        approved_gaps = entry.get("approved_missing_dates")
        if not isinstance(expected_dates, list) or not all(
            isinstance(value, str) for value in expected_dates
        ):
            raise PipelineError(
                f"Source inventory entry {quarter} expected_dates must be a date list"
            )
        if not isinstance(approved_gaps, list) or not all(
            isinstance(value, str) for value in approved_gaps
        ):
            raise PipelineError(
                f"Source inventory entry {quarter} approved_missing_dates must be a date list"
            )
        if len(expected_dates) != len(set(expected_dates)) or len(approved_gaps) != len(
            set(approved_gaps)
        ):
            raise PipelineError(
                f"Source inventory entry {quarter} contains duplicate calendar dates"
            )
        start, end = quarter_bounds(quarter)
        quarter_calendar = set(calendar_dates(start, end))
        try:
            parsed_expected = {date.fromisoformat(value).isoformat() for value in expected_dates}
            parsed_gaps = {date.fromisoformat(value).isoformat() for value in approved_gaps}
        except ValueError as exc:
            raise PipelineError(
                f"Source inventory entry {quarter} contains an invalid ISO date"
            ) from exc
        if parsed_expected != quarter_calendar:
            raise PipelineError(
                f"Source inventory entry {quarter} expected_dates must enumerate the complete quarter calendar"
            )
        if not parsed_gaps.issubset(quarter_calendar):
            raise PipelineError(
                f"Source inventory entry {quarter} approved gaps must fall inside the quarter"
            )


def load_configuration(path: Path) -> tuple[dict[str, Any], str]:
    raw = path.read_bytes()
    payload = yaml.safe_load(raw)
    if not isinstance(payload, dict):
        raise PipelineError("Ingestion configuration must be a YAML mapping")
    if payload.get("schema", {}).get("version") != SCHEMA_VERSION:
        raise PipelineError(
            f"Configuration schema.version must be {SCHEMA_VERSION!r}"
        )
    validation = payload.get("validation", {})
    if not isinstance(validation, dict):
        raise PipelineError("Configuration validation must be a mapping")
    frozen_safeguards = {
        "require_dates_within_quarter": True,
        "preserve_source_duplicates": True,
        "require_complete_row_accounting": True,
        "require_nonempty_output": True,
        "require_checkpoint_integrity": True,
        "require_source_inventory_review": True,
        "resolve_final_device_identity": False,
        "generate_future_labels": False,
    }
    for name, required_value in frozen_safeguards.items():
        if validation.get(name) is not required_value:
            raise PipelineError(
                f"Configuration validation.{name} must remain {required_value!r} in quarterly ingestion"
            )
    processing = payload.get("processing")
    if not isinstance(processing, dict):
        raise PipelineError("Configuration processing must be a mapping")
    for name in (
        "batch_rows",
        "parquet_row_group_rows",
        "worker_threads",
        "maximum_archive_members",
        "maximum_member_uncompressed_bytes",
        "maximum_archive_uncompressed_bytes",
        "minimum_free_space_bytes",
    ):
        _positive_integer(processing.get(name), f"processing.{name}")
    encoding = processing.get("encoding")
    if not isinstance(encoding, str) or not encoding.strip():
        raise PipelineError("Configuration processing.encoding must be a non-empty string")
    for name, minimum in (
        ("maximum_compression_ratio", 0.0),
        ("working_space_multiplier", 1.0),
    ):
        value = processing.get(name)
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise PipelineError(f"Configuration processing.{name} must be numeric")
        numeric = float(value)
        if not math.isfinite(numeric) or numeric <= minimum:
            comparator = "greater than 1" if minimum == 1.0 else "positive"
            raise PipelineError(
                f"Configuration processing.{name} must be finite and {comparator}"
            )

    parquet = payload.get("parquet")
    if not isinstance(parquet, dict):
        raise PipelineError("Configuration parquet must be a mapping")
    compression = parquet.get("compression")
    if not isinstance(compression, str) or not compression.strip():
        raise PipelineError("Configuration parquet.compression must be a non-empty string")
    compression = compression.lower()
    if compression not in {"none", "uncompressed"}:
        try:
            available = pa.Codec.is_available(compression)
        except ValueError as exc:
            raise PipelineError(
                f"Unsupported Parquet compression codec: {compression}"
            ) from exc
        if not available:
            raise PipelineError(f"Parquet compression codec is unavailable: {compression}")
    compression_level = parquet.get("compression_level")
    if (
        isinstance(compression_level, bool)
        or not isinstance(compression_level, int)
        or compression_level < 0
    ):
        raise PipelineError("Configuration parquet.compression_level must be a non-negative integer")

    recovery = payload.get("recovery")
    if not isinstance(recovery, dict):
        raise PipelineError("Configuration recovery must be a mapping")
    staging_suffix = recovery.get("staging_suffix")
    if not isinstance(staging_suffix, str) or not re.fullmatch(
        r"\.[A-Za-z0-9][A-Za-z0-9._-]{0,63}", staging_suffix
    ):
        raise PipelineError(
            "Configuration recovery.staging_suffix must be a safe dot-prefixed filename suffix"
        )
    if recovery.get("completed_output_policy") != "verify":
        raise PipelineError(
            "Configuration recovery.completed_output_policy must remain 'verify'"
        )

    _validate_source_inventory_configuration(payload)
    return payload, sha256_bytes(raw)


def _validate_source_url(source_url: str) -> None:
    parsed = urlparse(source_url)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise PipelineError("--source-url must be an absolute HTTP or HTTPS provenance URL")


def _quarter_within_scope(config: Mapping[str, Any], start: date, end: date) -> None:
    schema_config = config["schema"]
    allowed_start = date.fromisoformat(str(schema_config["allowed_start_date"]))
    allowed_end = date.fromisoformat(str(schema_config["allowed_end_date"]))
    if start < allowed_start or end > allowed_end:
        raise PipelineError(
            f"Assigned quarter falls outside approved scope {allowed_start} through {allowed_end}"
        )


def inspect_archive(
    archive: Path, config: Mapping[str, Any]
) -> tuple[list[MemberPlan], dict[str, Any]]:
    """Read the ZIP inventory and every CSV header without extracting members."""

    processing = config["processing"]
    maximum_member_bytes = int(processing["maximum_member_uncompressed_bytes"])
    maximum_archive_bytes = int(processing["maximum_archive_uncompressed_bytes"])
    maximum_members = int(processing["maximum_archive_members"])
    maximum_ratio = float(processing["maximum_compression_ratio"])
    encoding = str(processing.get("encoding", "utf-8-sig"))

    members: list[MemberPlan] = []
    with zipfile.ZipFile(archive, "r") as source:
        file_infos = [info for info in source.infolist() if not info.is_dir()]
        if len(file_infos) > maximum_members:
            raise PipelineError(
                f"Archive contains {len(file_infos)} files, exceeding the configured limit {maximum_members}"
            )
        total_uncompressed_bytes = sum(info.file_size for info in file_infos)
        if total_uncompressed_bytes > maximum_archive_bytes:
            raise PipelineError(
                "Archive uncompressed size exceeds the configured total expansion limit"
            )
        csv_infos = [
            (index, info)
            for index, info in enumerate(source.infolist())
            if not info.is_dir() and info.filename.lower().endswith(".csv")
        ]
        if not csv_infos:
            raise PipelineError("Archive contains no CSV members")
        names = [info.filename for _, info in csv_infos]
        if len(names) != len(set(names)):
            raise PipelineError("Archive contains duplicate CSV member names")

        for archive_index, info in sorted(csv_infos, key=lambda item: (item[1].filename.casefold(), item[0])):
            if info.file_size > maximum_member_bytes:
                raise PipelineError(f"Member exceeds configured size limit: {info.filename}")
            ratio = info.file_size / max(info.compress_size, 1)
            if ratio > maximum_ratio:
                raise PipelineError(
                    f"Member compression ratio {ratio:.1f} exceeds limit for {info.filename}"
                )
            try:
                with source.open(info, "r") as binary:
                    with io.TextIOWrapper(binary, encoding=encoding, newline="") as text:
                        reader = csv.reader(text, strict=True)
                        headers = next(reader)
            except StopIteration as exc:
                raise PipelineError(f"CSV member is empty: {info.filename}") from exc
            except (UnicodeError, csv.Error, OSError) as exc:
                raise PipelineError(f"Cannot read header for {info.filename}: {exc}") from exc
            try:
                analysis = analyze_headers(headers)
            except SchemaError as exc:
                raise PipelineError(f"Invalid header in {info.filename}: {exc}") from exc
            if analysis.missing_required_columns:
                missing = ", ".join(analysis.missing_required_columns)
                raise PipelineError(
                    f"CSV member {info.filename} is missing required columns: {missing}"
                )
            members.append(MemberPlan(archive_index, info, analysis))

    inventory = {
        "member_count": len(members),
        "archive_file_count": len(file_infos),
        "total_uncompressed_bytes": total_uncompressed_bytes,
        "members": [
            {
                "archive_index": member.archive_index,
                "filename": member.info.filename,
                "compressed_bytes": member.info.compress_size,
                "uncompressed_bytes": member.info.file_size,
                "crc32": f"{member.info.CRC:08x}",
                "header_signature": member.analysis.signature,
                "original_headers": list(member.analysis.original_headers),
                "normalized_headers": list(member.analysis.canonical_headers),
            }
            for member in members
        ],
    }
    return members, inventory


def _assess_source_inventory(
    config: Mapping[str, Any],
    *,
    quarter: str,
    archive: Path,
    archive_sha256: str,
    source_url: str,
) -> dict[str, Any]:
    inventory = config["source_inventory"]
    provenance = {
        "status": inventory["status"],
        "provenance_reference": inventory.get("provenance_reference"),
        "verified_by": inventory.get("verified_by"),
        "verified_at": inventory.get("verified_at"),
    }
    entry = inventory["quarters"].get(quarter)
    if inventory["status"] != "verified":
        return {
            "status": "requires_review",
            "provenance": provenance,
            "quarter_entry_status": "unavailable",
            "authenticity_verified": False,
            "trusted_gap_evidence": False,
            "approved_missing_dates": [],
            "review_reasons": ["approved_source_inventory_unavailable"],
            "checks": [],
        }
    if entry is None:
        return {
            "status": "requires_review",
            "provenance": provenance,
            "quarter_entry_status": "missing",
            "authenticity_verified": False,
            "trusted_gap_evidence": False,
            "approved_missing_dates": [],
            "review_reasons": ["approved_source_inventory_entry_missing"],
            "checks": [],
        }

    checks: list[dict[str, Any]] = []

    def require_match(name: str, expected: Any, observed: Any) -> None:
        passed = expected == observed
        checks.append({"name": name, "expected": expected, "observed": observed, "passed": passed})
        if not passed:
            raise PipelineError(
                f"Approved source inventory mismatch for {quarter}: {name}"
            )

    require_match("canonical_source_url", entry["canonical_source_url"], source_url)
    require_match("original_filename", entry["original_filename"], archive.name)
    if entry.get("expected_bytes") is not None:
        require_match("archive_bytes", int(entry["expected_bytes"]), archive.stat().st_size)
    if entry.get("expected_sha256") is not None:
        require_match(
            "archive_sha256",
            str(entry["expected_sha256"]).lower(),
            archive_sha256.lower(),
        )

    review_reasons: list[str] = []
    authenticity_verified = entry.get("expected_sha256") is not None
    if not authenticity_verified:
        review_reasons.append("independent_archive_checksum_unavailable")
    return {
        "status": "requires_review" if review_reasons else "verified",
        "provenance": provenance,
        "quarter_entry_status": "approved",
        "authenticity_verified": authenticity_verified,
        "trusted_gap_evidence": True,
        "expected_dates": list(entry["expected_dates"]),
        "approved_missing_dates": sorted(entry["approved_missing_dates"]),
        "review_reasons": review_reasons,
        "checks": checks,
    }


def _disk_space_preflight(
    output_parent: Path,
    archive_inventory: Mapping[str, Any],
    config: Mapping[str, Any],
) -> dict[str, int | float]:
    processing = config["processing"]
    multiplier = float(processing["working_space_multiplier"])
    estimated = math.ceil(int(archive_inventory["total_uncompressed_bytes"]) * multiplier)
    required = max(int(processing["minimum_free_space_bytes"]), estimated)
    available = shutil.disk_usage(output_parent).free
    if available < required:
        raise PipelineError(
            f"Insufficient free disk space: requires at least {required} bytes, found {available}"
        )
    return {
        "available_bytes": available,
        "required_bytes": required,
        "minimum_free_space_bytes": int(processing["minimum_free_space_bytes"]),
        "working_space_multiplier": multiplier,
    }


def _state_identity(
    archive_sha256: str,
    config_sha256: str,
    quarter: str,
    source_url: str,
    pipeline_identity: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "archive_sha256": archive_sha256,
        "config_sha256": config_sha256,
        "quarter": quarter,
        "source_url": source_url,
        "schema_version": SCHEMA_VERSION,
        "pipeline": dict(pipeline_identity),
    }


def _validate_state(staging: Path, identity: Mapping[str, Any]) -> dict[str, Any]:
    state_path = staging / "run_state.json"
    if not state_path.is_file():
        raise PipelineError("Staging directory has no verifiable run_state.json")
    state = read_json(state_path)
    if state.get("identity") != dict(identity):
        raise PipelineError(
            "Staging state belongs to a different archive, quarter, URL, configuration, "
            "or pipeline identity"
        )
    return state


def _safe_remove_tree(path: Path, boundary: Path) -> None:
    resolved = path.resolve()
    resolved_boundary = boundary.resolve()
    if resolved == resolved_boundary or resolved_boundary not in resolved.parents:
        raise PipelineError(f"Refusing unsafe recursive removal: {resolved}")
    if path.exists():
        shutil.rmtree(path)


def _prepare_staging(
    output: Path,
    staging: Path,
    identity: Mapping[str, Any],
    *,
    resume: bool,
    restart: bool,
) -> tuple[dict[str, Any], bool]:
    if output.exists():
        return {}, False
    if staging.exists():
        state = _validate_state(staging, identity)
        if restart:
            _safe_remove_tree(staging, staging.parent)
        elif not resume:
            raise PipelineError("Matching staging output exists; use --resume or --restart")
        else:
            return state, True
    staging.mkdir(parents=True, exist_ok=False)
    state = {"identity": dict(identity), "started_at": utc_now(), "status": "processing"}
    write_json_atomic(staging / "run_state.json", state)
    return state, False


def _verify_existing_output(output: Path, identity: Mapping[str, Any]) -> dict[str, Any]:
    manifest_path = output / "source_manifest.json"
    if not manifest_path.is_file():
        raise PipelineError("Output exists but has no source_manifest.json; refusing overwrite")
    manifest = read_json(manifest_path)
    if manifest.get("identity") != dict(identity) or manifest.get("status") != "complete":
        raise PipelineError(
            "Existing output does not match this archive, quarter, URL, configuration, "
            "and pipeline identity"
        )
    errors = verify_inventory(output, manifest.get("outputs", []))
    if errors:
        raise PipelineError("Existing output verification failed: " + "; ".join(errors))
    return {"status": "verified_existing", "output": str(output), "manifest": manifest}


def _parquet_schema_fingerprint(path: Path) -> str:
    schema = pq.ParquetFile(path).schema_arrow
    return sha256_bytes(schema.serialize().to_pybytes())


def _checkpoint_artifacts(staging: Path, relative_paths: Sequence[str]) -> list[dict[str, Any]]:
    artifacts: list[dict[str, Any]] = []
    for relative in sorted(relative_paths):
        path = staging / relative
        parquet_file = pq.ParquetFile(path)
        artifacts.append(
            {
                "path": relative,
                "bytes": path.stat().st_size,
                "sha256": sha256_file(path),
                "rows": parquet_file.metadata.num_rows,
                "schema_fingerprint": sha256_bytes(
                    parquet_file.schema_arrow.serialize().to_pybytes()
                ),
            }
        )
    return artifacts


def _verify_completed_checkpoints(
    staging: Path, completed: Mapping[int, Mapping[str, Any]]
) -> None:
    errors: list[str] = []
    resolved_staging = staging.resolve()
    for member_index in sorted(completed):
        files = completed[member_index].get("files", {})
        artifacts = files.get("artifacts")
        relative_paths = files.get("relative_paths")
        if not isinstance(artifacts, list) or not isinstance(relative_paths, list):
            errors.append(f"member {member_index}: missing checkpoint artifact inventory")
            continue

        artifact_paths = [str(item.get("path")) for item in artifacts if isinstance(item, dict)]
        if sorted(artifact_paths) != sorted(str(path) for path in relative_paths):
            errors.append(f"member {member_index}: checkpoint artifact path inventory mismatch")
            continue

        for artifact in artifacts:
            if not isinstance(artifact, dict):
                errors.append(f"member {member_index}: malformed checkpoint artifact entry")
                continue
            relative = str(artifact.get("path", ""))
            candidate = (staging / relative).resolve()
            if resolved_staging not in candidate.parents:
                errors.append(f"member {member_index}: unsafe checkpoint path {relative}")
                continue
            if not candidate.is_file():
                errors.append(f"member {member_index}: missing checkpoint artifact {relative}")
                continue
            try:
                if candidate.stat().st_size != int(artifact["bytes"]):
                    errors.append(f"member {member_index}: size mismatch for {relative}")
                    continue
                if sha256_file(candidate) != artifact["sha256"]:
                    errors.append(f"member {member_index}: SHA-256 mismatch for {relative}")
                    continue
                parquet_file = pq.ParquetFile(candidate)
                if parquet_file.metadata.num_rows != int(artifact["rows"]):
                    errors.append(f"member {member_index}: row count mismatch for {relative}")
                    continue
                if _parquet_schema_fingerprint(candidate) != artifact["schema_fingerprint"]:
                    errors.append(f"member {member_index}: schema mismatch for {relative}")
            except (KeyError, TypeError, ValueError, OSError, pa.ArrowException) as exc:
                errors.append(
                    f"member {member_index}: invalid checkpoint metadata for {relative}: {exc}"
                )

    if errors:
        raise PipelineError(
            "Completed checkpoint verification failed; use --restart to replay staged work: "
            + "; ".join(errors)
        )


def _cleanup_incomplete_files(staging: Path, completed: Mapping[int, Mapping[str, Any]]) -> None:
    allowed = {
        relative
        for member in completed.values()
        for relative in member["files"]["relative_paths"]
    }
    for directory_name in ("data", "exception_parts"):
        directory = staging / directory_name
        if directory.exists():
            for file_path in directory.rglob("*.parquet"):
                relative = file_path.relative_to(staging).as_posix()
                if relative not in allowed:
                    file_path.unlink()
    work = staging / "work"
    if work.exists():
        _safe_remove_tree(work, staging)
    for name in FINAL_REPORT_NAMES:
        candidate = staging / name
        if candidate.exists():
            candidate.unlink()


def _parquet_write_options(config: Mapping[str, Any]) -> dict[str, Any]:
    parquet = config["parquet"]
    compression = str(parquet.get("compression", "zstd")).lower()
    options: dict[str, Any] = {
        "compression": None if compression in {"none", "uncompressed"} else compression,
        "write_statistics": bool(parquet.get("write_statistics", True)),
        "use_dictionary": bool(parquet.get("dictionary_encode_strings", True)),
    }
    if options["compression"] is not None:
        options["compression_level"] = int(parquet.get("compression_level", 3))
    return options


def _write_parquet(path: Path, table: pa.Table, config: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(
        table,
        path,
        row_group_size=int(config["processing"].get("parquet_row_group_rows", 32768)),
        **_parquet_write_options(config),
    )


def _malformed_issue(
    member: MemberPlan,
    values: Sequence[str],
    *,
    quarter: str,
    record_number: int,
    line_number: int,
) -> ConversionIssue:
    raw_json, raw_sha = raw_record_evidence(member.analysis.original_headers, values)
    return ConversionIssue(
        source_quarter=quarter,
        source_file=member.info.filename,
        source_record_number=record_number,
        source_line_number=line_number,
        serial_number=None,
        date_token=None,
        column_name=None,
        original_header=None,
        raw_value=None,
        exception_code="malformed_column_count",
        severity="error",
        row_disposition="quarantined",
        message=(
            f"Expected {len(member.analysis.original_headers)} columns but received {len(values)}"
        ),
        raw_record_json=raw_json,
        raw_record_sha256=raw_sha,
    )


def _move_member_outputs(work_member: Path, staging: Path, quarter: str) -> list[str]:
    relative_paths: list[str] = []
    destinations = (
        # Use a non-Hive directory name because source_quarter is deliberately
        # retained inside every Parquet file. This avoids duplicate partition
        # field inference when a consumer opens an individual file or dataset.
        (work_member / "data", staging / "data" / quarter),
        (work_member / "exceptions", staging / "exception_parts"),
    )
    for source_directory, destination_directory in destinations:
        if not source_directory.exists():
            continue
        destination_directory.mkdir(parents=True, exist_ok=True)
        for source in sorted(source_directory.glob("*.parquet")):
            destination = destination_directory / source.name
            if destination.exists():
                raise PipelineError(f"Refusing to overwrite member output {destination}")
            os.replace(source, destination)
            relative_paths.append(destination.relative_to(staging).as_posix())
    return sorted(relative_paths)


def _process_member(
    archive: Path,
    member: MemberPlan,
    *,
    member_sequence: int,
    quarter: str,
    quarter_start: date,
    quarter_end: date,
    smart_columns: Sequence[str],
    unexpected_smart_columns: Sequence[str],
    output_schema: pa.Schema,
    staging: Path,
    tracker: DuplicateTracker,
    config: Mapping[str, Any],
    interrupt_after_rows: int | None,
    rows_seen_before_member: int,
) -> tuple[QualityStats, list[str], int]:
    work_member = staging / "work" / f"member-{member_sequence:05d}"
    if work_member.exists():
        _safe_remove_tree(work_member, staging)
    (work_member / "data").mkdir(parents=True)
    (work_member / "exceptions").mkdir(parents=True)

    missing_tokens = {str(value) for value in config["schema"].get("missing_tokens", [""])}
    batch_rows = int(config["processing"].get("batch_rows", 32768))
    encoding = str(config["processing"].get("encoding", "utf-8-sig"))
    exception_schema = exception_arrow_schema()
    fingerprint_fields = [
        name for name in output_schema.names if name not in {"source_quarter", "source_file", "schema_version"}
    ]
    stats = QualityStats()
    pending_entries: list[tuple[Mapping[str, Any], int, int]] = []
    pending_issues: list[ConversionIssue] = []
    source_rows_in_batch = 0
    part_number = 0
    rows_seen = rows_seen_before_member

    def flush() -> None:
        nonlocal part_number, source_rows_in_batch
        duplicate_issues = tracker.classify(pending_entries, fingerprint_fields)
        for issue in duplicate_issues:
            stats.record_issue(issue)
        pending_issues.extend(duplicate_issues)

        if pending_entries:
            table = pa.Table.from_pylist(
                [
                    {name: entry[0].get(name) for name in output_schema.names}
                    for entry in pending_entries
                ],
                schema=output_schema,
            )
            _write_parquet(
                work_member / "data" / f"part-m{member_sequence:05d}-b{part_number:06d}.parquet",
                table,
                config,
            )
        if pending_issues:
            exception_table = pa.Table.from_pylist(
                [issue.to_dict() for issue in pending_issues], schema=exception_schema
            )
            _write_parquet(
                work_member / "exceptions" / f"part-m{member_sequence:05d}-b{part_number:06d}.parquet",
                exception_table,
                config,
            )
        pending_entries.clear()
        pending_issues.clear()
        source_rows_in_batch = 0
        part_number += 1

    tracker.begin_member()
    try:
        with zipfile.ZipFile(archive, "r") as source:
            with source.open(member.info, "r") as binary:
                with io.TextIOWrapper(binary, encoding=encoding, newline="") as text:
                    reader = csv.reader(text, strict=True)
                    actual_header = next(reader)
                    if tuple(actual_header) != member.analysis.original_headers:
                        raise PipelineError(f"Header changed between inspection and processing: {member.info.filename}")
                    for source_record_number, values in enumerate(reader, start=1):
                        rows_seen += 1
                        stats.input_rows += 1
                        source_rows_in_batch += 1
                        if interrupt_after_rows is not None and rows_seen >= interrupt_after_rows:
                            raise IngestionInterrupted("Controlled interruption during member processing")

                        if len(values) != len(member.analysis.original_headers):
                            issue = _malformed_issue(
                                member,
                                values,
                                quarter=quarter,
                                record_number=source_record_number,
                                line_number=reader.line_num,
                            )
                            pending_issues.append(issue)
                            stats.record_issue(issue)
                            stats.malformed_rows += 1
                            stats.quarantined_rows += 1
                        else:
                            index = member.analysis.index_by_canonical
                            date_position = index.get("date")
                            failure_position = index.get("failure")
                            source_date: date | None = None
                            if date_position is not None:
                                try:
                                    source_date = parse_iso_date(values[date_position])
                                    key = source_date.isoformat()
                                    stats.daily_source_rows[key] = stats.daily_source_rows.get(key, 0) + 1
                                except ValueError:
                                    pass
                            if failure_position is not None and values[failure_position].strip() in {"0", "1"}:
                                flag = values[failure_position].strip()
                                stats.failure_flag_counts[flag] = stats.failure_flag_counts.get(flag, 0) + 1
                                if flag == "1" and source_date is not None:
                                    key = source_date.isoformat()
                                    stats.daily_failure_flags[key] = stats.daily_failure_flags.get(key, 0) + 1

                            row, issues, smart_states = convert_record(
                                values,
                                member.analysis,
                                source_quarter=quarter,
                                source_file=member.info.filename,
                                source_record_number=source_record_number,
                                source_line_number=reader.line_num,
                                quarter_start=quarter_start,
                                quarter_end=quarter_end,
                                missing_tokens=missing_tokens,
                            )
                            for field_name, state in smart_states.items():
                                stats.record_smart_state(field_name, state)
                            for issue in issues:
                                stats.record_issue(issue)
                            pending_issues.extend(issues)
                            if row is None:
                                stats.quarantined_rows += 1
                            else:
                                stats.observe_output(row, member.analysis.smart_columns)
                                raw_json, raw_sha = raw_record_evidence(
                                    member.analysis.original_headers, values
                                )
                                identity_tokens = [
                                    values[index[name]] if name in index else None
                                    for name in ("serial_number", "model", "manufacturer", "capacity_bytes")
                                ]
                                identity_json = json.dumps(
                                    identity_tokens,
                                    ensure_ascii=False,
                                    separators=(",", ":"),
                                )
                                row["_source_raw_record_json"] = raw_json
                                row["_source_raw_record_sha256"] = raw_sha
                                row["_source_identity_signature"] = sha256_bytes(
                                    identity_json.encode("utf-8")
                                )
                                pending_entries.append((row, source_record_number, reader.line_num))

                        if source_rows_in_batch >= batch_rows:
                            flush()
        if source_rows_in_batch or pending_entries or pending_issues:
            flush()

        absent = set(smart_columns) - set(member.analysis.smart_columns)
        for field_name in absent:
            stats.smart_absent_by_field[field_name] = stats.input_rows

        relative_paths = _move_member_outputs(work_member, staging, quarter)
        file_details = {
            "relative_paths": relative_paths,
            "artifacts": _checkpoint_artifacts(staging, relative_paths),
        }
        tracker.complete_member(member_sequence, stats, file_details)
        if work_member.exists():
            _safe_remove_tree(work_member, staging)
        return stats, relative_paths, rows_seen
    except csv.Error as exc:
        tracker.rollback_member()
        raise PipelineError(
            f"CSV parser failed in {member.info.filename} near physical line {getattr(reader, 'line_num', '?')}: {exc}"
        ) from exc
    except Exception:
        tracker.rollback_member()
        raise


def _compact_exceptions(staging: Path, config: Mapping[str, Any]) -> int:
    parts_root = staging / "exception_parts"
    parts = sorted(parts_root.glob("*.parquet")) if parts_root.exists() else []
    destination = staging / "exceptions.parquet"
    if destination.exists():
        destination.unlink()
    if not parts:
        return 0
    writer = pq.ParquetWriter(destination, exception_arrow_schema(), **_parquet_write_options(config))
    rows = 0
    try:
        for part in parts:
            table = pq.read_table(part)
            writer.write_table(table)
            rows += table.num_rows
    finally:
        writer.close()
    return rows


def _publish_completed_staging(
    staging: Path, output: Path, identity: Mapping[str, Any]
) -> dict[str, Any] | None:
    """Finish an already validated staging result after a late interruption."""

    manifest_path = staging / "source_manifest.json"
    if not manifest_path.is_file():
        return None
    manifest = read_json(manifest_path)
    if manifest.get("status") != "complete" or manifest.get("identity") != dict(identity):
        return None
    errors = verify_inventory(staging, manifest.get("outputs", []))
    if errors:
        raise PipelineError("Completed staging verification failed: " + "; ".join(errors))
    for runtime_name in RUNTIME_NAMES:
        runtime_path = staging / runtime_name
        if runtime_path.exists():
            runtime_path.unlink()
    for internal_name in ("work", "exception_parts"):
        internal_path = staging / internal_name
        if internal_path.exists():
            _safe_remove_tree(internal_path, staging)
    os.replace(staging, output)
    processing = manifest["processing"]
    return {
        "status": "complete",
        "output": str(output),
        "input_rows": processing["input_rows"],
        "output_rows": processing["output_rows"],
        "quarantined_rows": processing["quarantined_rows"],
        "validation_status": manifest["validation"]["status"],
    }


def _aggregate_completed(completed: Mapping[int, Mapping[str, Any]]) -> QualityStats:
    aggregate = QualityStats()
    for member_index in sorted(completed):
        aggregate.merge(QualityStats.from_dict(completed[member_index]["stats"]))
    return aggregate


def _schema_report(
    members: Sequence[MemberPlan],
    output_schema: pa.Schema,
    smart_columns: Sequence[str],
    unexpected_smart_columns: Sequence[str],
    stats: QualityStats,
) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "canonical_fields": [
            {"name": field.name, "type": str(field.type), "nullable": field.nullable}
            for field in output_schema
        ],
        "smart_numeric_contract": {
            "type": "decimal128(38, 0)",
            "minimum": str(-(10**38 - 1)),
            "maximum": str(10**38 - 1),
            "fractional_or_scientific_tokens": "exception evidence; canonical value null",
        },
        "smart_columns": list(smart_columns),
        "unexpected_smart_string_columns": list(unexpected_smart_columns),
        "members": [
            {
                "filename": member.info.filename,
                **member.analysis.to_report(),
                "source_physical_types": {
                    original: "CSV text token" for original in member.analysis.original_headers
                },
            }
            for member in members
        ],
        "model_smart_column_availability": {
            model: sorted(columns) for model, columns in sorted(stats.model_columns_present.items())
        },
        "model_smart_non_null_counts": stats.to_dict()["model_smart_non_null"],
        "identity_scope": (
            "Raw identifiers are preserved. Stable device_key construction and cross-quarter identity "
            "resolution are central integration responsibilities."
        ),
    }


def _calendar_coverage(
    stats: QualityStats,
    quarter_start: date,
    quarter_end: date,
    source_assessment: Mapping[str, Any],
) -> dict[str, Any]:
    expected_dates = list(
        source_assessment.get("expected_dates", calendar_dates(quarter_start, quarter_end))
    )
    observed_dates = sorted(
        value for value in stats.daily_source_rows if quarter_start.isoformat() <= value <= quarter_end.isoformat()
    )
    missing_dates = sorted(set(expected_dates) - set(observed_dates))
    approved_candidates = (
        set(source_assessment.get("approved_missing_dates", []))
        if source_assessment.get("trusted_gap_evidence")
        else set()
    )
    approved_missing_dates = sorted(set(missing_dates) & approved_candidates)
    unexplained_missing_dates = sorted(set(missing_dates) - approved_candidates)
    outside_dates = sorted(
        value for value in stats.daily_source_rows if value < quarter_start.isoformat() or value > quarter_end.isoformat()
    )
    if unexplained_missing_dates:
        coverage_status = "requires_review"
    elif approved_missing_dates:
        coverage_status = "complete_with_approved_gaps"
    else:
        coverage_status = "complete"
    return {
        "status": coverage_status,
        "expected_start": quarter_start.isoformat(),
        "expected_end": quarter_end.isoformat(),
        "expected_day_count": len(expected_dates),
        "observed_dates": observed_dates,
        "missing_source_dates": missing_dates,
        "approved_missing_source_dates": approved_missing_dates,
        "unexplained_missing_source_dates": unexplained_missing_dates,
        "outside_assigned_quarter_dates": outside_dates,
        "daily_source_rows": dict(sorted(stats.daily_source_rows.items())),
        "daily_failure_flags": dict(sorted(stats.daily_failure_flags.items())),
        "fabricated_rows": 0,
    }


def _quality_report(
    stats: QualityStats,
    smart_columns: Sequence[str],
    calendar_coverage: Mapping[str, Any],
    source_assessment: Mapping[str, Any],
) -> dict[str, Any]:
    missing_canonical = {
        field_name: stats.output_rows - stats.non_null_by_field.get(field_name, 0)
        for field_name in smart_columns
    }
    return {
        "schema_version": SCHEMA_VERSION,
        "row_accounting": validate_accounting(stats),
        "counts": stats.to_dict(),
        "smart_source_states": {
            field_name: {
                "absent_column_rows": stats.smart_absent_by_field.get(field_name, 0),
                "present_blank_rows": stats.smart_blank_by_field.get(field_name, 0),
                "invalid_token_rows": stats.smart_invalid_by_field.get(field_name, 0),
                "genuine_zero_rows": stats.smart_zero_by_field.get(field_name, 0),
                "canonical_null_rows": missing_canonical[field_name],
            }
            for field_name in smart_columns
        },
        "calendar_coverage": dict(calendar_coverage),
        "source_authenticity": dict(source_assessment),
        "failure_flags": {
            "preserved_valid_source_counts": dict(sorted(stats.failure_flag_counts.items())),
            "interpretation": "Original recorded 0/1 flags; no physical-failure inference performed",
        },
        "central_integration_boundary": {
            "performed_here": [
                "per-quarter source verification",
                "within-quarter duplicate and candidate collision audit",
                "typed quarterly Parquet and exception evidence",
            ],
            "deferred_centrally": [
                "stable device_key resolution",
                "cross-quarter duplicate reconciliation",
                "chronological drive-history stitching",
                "future-label construction and horizon completeness",
            ],
        },
    }


def _validation_results(
    stats: QualityStats,
    output_schema: pa.Schema,
    staging: Path,
    member_count: int,
    completed_count: int,
    calendar_coverage: Mapping[str, Any],
    source_assessment: Mapping[str, Any],
) -> dict[str, Any]:
    data_files = sorted((staging / "data").rglob("*.parquet")) if (staging / "data").exists() else []
    parquet_rows = 0
    schemas_match = True
    readable = True
    for path in data_files:
        try:
            parquet_file = pq.ParquetFile(path)
            parquet_rows += parquet_file.metadata.num_rows
            schemas_match = schemas_match and parquet_file.schema_arrow.equals(output_schema, check_metadata=True)
        except Exception:
            readable = False
            schemas_match = False
    checks = [
        {"name": "all_csv_members_completed", "passed": completed_count == member_count},
        validate_accounting(stats),
        {
            "name": "nonempty_retained_output",
            "passed": stats.output_rows > 0,
            "retained_output_rows": stats.output_rows,
        },
        {"name": "parquet_files_readable", "passed": readable},
        {"name": "canonical_schema_matches", "passed": schemas_match},
        {
            "name": "parquet_row_count_matches",
            "passed": parquet_rows == stats.output_rows,
            "parquet_rows": parquet_rows,
            "reported_output_rows": stats.output_rows,
        },
    ]
    structural_pass = all(check["passed"] for check in checks)
    review_reasons = list(source_assessment.get("review_reasons", []))
    if calendar_coverage.get("unexplained_missing_source_dates"):
        review_reasons.append("unexplained_calendar_coverage_gaps")
    if stats.conflicting_duplicates:
        review_reasons.append("conflicting_duplicate_records")
    if stats.serial_identity_collisions:
        review_reasons.append("serial_identity_collisions")
    if stats.issue_severity_counts.get("error", 0):
        review_reasons.append("error_severity_exception_evidence")
    review_reasons = sorted(set(review_reasons))

    warning_reasons: list[str] = []
    if stats.issue_codes:
        warning_reasons.append("quality_exception_evidence")
    if calendar_coverage.get("approved_missing_source_dates"):
        warning_reasons.append("approved_calendar_gaps_present")
    warning_reasons = sorted(set(warning_reasons))

    if not structural_pass:
        status = "failed"
    elif review_reasons:
        status = "requires_review"
    elif warning_reasons:
        status = "passed_with_warnings"
    else:
        status = "passed"
    return {
        "schema_version": SCHEMA_VERSION,
        "status": status,
        "outcome_order": ["failed", "requires_review", "passed_with_warnings", "passed"],
        "checks": checks,
        "review_reasons": review_reasons,
        "warning_reasons": warning_reasons,
        "central_integration_admission": {
            "eligible": status in {"passed", "passed_with_warnings"},
            "requires_explicit_review": status == "requires_review",
        },
        "quality_findings_do_not_resolve_identity": True,
        "deterministic_for_identical_inputs": True,
    }


def run_ingestion(
    *,
    config_path: Path,
    quarter: str,
    archive: Path,
    output: Path,
    source_url: str,
    dry_run: bool = False,
    resume: bool = False,
    restart: bool = False,
    _interrupt_after_rows: int | None = None,
    _interrupt_before_publish: bool = False,
) -> dict[str, Any]:
    """Ingest one archive; internal interruption control exists only for synthetic recovery tests."""

    if sum(bool(value) for value in (dry_run, resume, restart)) > 1:
        raise PipelineError("--dry-run, --resume, and --restart are mutually exclusive")
    config_path = config_path.resolve()
    archive = archive.resolve()
    output = output.resolve()
    if not config_path.is_file():
        raise PipelineError(f"Configuration does not exist: {config_path}")
    if not archive.is_file():
        raise PipelineError(f"Archive does not exist: {archive}")
    _validate_source_url(source_url)
    config, config_sha256 = load_configuration(config_path)
    start, end = quarter_bounds(quarter)
    _quarter_within_scope(config, start, end)
    archive_sha256 = sha256_file(archive)
    repository = Path(__file__).resolve().parents[3]
    try:
        pipeline_identity = pipeline_recovery_identity(repository)
    except OSError as exc:
        raise PipelineError(f"Cannot establish pipeline recovery identity: {exc}") from exc
    members, archive_inventory = inspect_archive(archive, config)
    source_assessment = _assess_source_inventory(
        config,
        quarter=quarter,
        archive=archive,
        archive_sha256=archive_sha256,
        source_url=source_url,
    )
    smart_columns, unexpected_smart_columns = union_smart_columns(
        member.analysis for member in members
    )
    output_schema = canonical_arrow_schema(smart_columns, unexpected_smart_columns)
    identity = _state_identity(
        archive_sha256,
        config_sha256,
        quarter,
        source_url,
        pipeline_identity,
    )

    if dry_run:
        return {
            "status": "dry_run",
            "identity": identity,
            "archive": str(archive),
            "output": str(output),
            "member_count": len(members),
            "smart_column_count": len(smart_columns),
            "source_inventory": source_assessment,
            "intended_operations": [
                "stream local CSV members in bounded batches",
                "write staged partitioned Parquet and audit reports",
                "validate and atomically publish output",
            ],
        }
    if output.exists():
        return _verify_existing_output(output, identity)
    output.parent.mkdir(parents=True, exist_ok=True)
    disk_preflight = _disk_space_preflight(output.parent, archive_inventory, config)
    staging_suffix = str(config["recovery"].get("staging_suffix", ".staging"))
    staging = output.with_name(output.name + staging_suffix)
    state, was_resumed = _prepare_staging(
        output, staging, identity, resume=resume, restart=restart
    )
    if was_resumed:
        completed_result = _publish_completed_staging(staging, output, identity)
        if completed_result is not None:
            return completed_result

    tracker = DuplicateTracker(
        staging / "state.duckdb",
        memory_limit=str(config["processing"].get("duckdb_memory_limit", "1GB")),
        threads=int(config["processing"].get("worker_threads", 2)),
    )
    try:
        completed = tracker.completed_members()
        _verify_completed_checkpoints(staging, completed)
        _cleanup_incomplete_files(staging, completed)
        rows_seen = sum(int(member["stats"].get("input_rows", 0)) for member in completed.values())
        with zipfile.ZipFile(archive, "r") as source:
            bad_member = source.testzip()
            if bad_member is not None:
                raise PipelineError(f"ZIP CRC validation failed for {bad_member}")

        for member_sequence, member in enumerate(members):
            if member_sequence in completed:
                continue
            _process_member(
                archive,
                member,
                member_sequence=member_sequence,
                quarter=quarter,
                quarter_start=start,
                quarter_end=end,
                smart_columns=smart_columns,
                unexpected_smart_columns=unexpected_smart_columns,
                output_schema=output_schema,
                staging=staging,
                tracker=tracker,
                config=config,
                interrupt_after_rows=_interrupt_after_rows,
                rows_seen_before_member=rows_seen,
            )
            completed = tracker.completed_members()
            rows_seen = sum(int(item["stats"].get("input_rows", 0)) for item in completed.values())

        completed = tracker.completed_members()
        stats = _aggregate_completed(completed)
        exception_rows = _compact_exceptions(staging, config)
        schema_report = _schema_report(
            members, output_schema, smart_columns, unexpected_smart_columns, stats
        )
        calendar_coverage = _calendar_coverage(
            stats,
            start,
            end,
            source_assessment,
        )
        quality_report = _quality_report(
            stats,
            smart_columns,
            calendar_coverage,
            source_assessment,
        )
        validation = _validation_results(
            stats,
            output_schema,
            staging,
            len(members),
            len(completed),
            calendar_coverage,
            source_assessment,
        )
        write_json_atomic(staging / "schema_report.json", schema_report)
        write_json_atomic(staging / "quality_report.json", quality_report)
        write_json_atomic(staging / "validation_results.json", validation)

        if validation["status"] == "failed":
            failed_manifest = {
                "manifest_version": "1.0.0",
                "schema_version": SCHEMA_VERSION,
                "status": "failed",
                "identity": identity,
                "validation": validation,
                "completed_at": utc_now(),
            }
            write_json_atomic(staging / "source_manifest.json", failed_manifest)
            raise ValidationError("Structural validation failed; output remains in staging")

        member_reports = []
        for sequence, member in enumerate(members):
            member_stats = QualityStats.from_dict(completed[sequence]["stats"])
            member_reports.append(
                {
                    "processing_sequence": sequence,
                    "archive_index": member.archive_index,
                    "filename": member.info.filename,
                    "compressed_bytes": member.info.compress_size,
                    "uncompressed_bytes": member.info.file_size,
                    "crc32": f"{member.info.CRC:08x}",
                    "crc_verified": True,
                    "header_signature": member.analysis.signature,
                    "original_headers": list(member.analysis.original_headers),
                    "observed_dates": sorted(member_stats.daily_source_rows),
                    "input_rows": member_stats.input_rows,
                    "output_rows": member_stats.output_rows,
                    "quarantined_rows": member_stats.quarantined_rows,
                }
            )

        inventory = output_inventory(
            staging,
            excluded_names=RUNTIME_NAMES | {"source_manifest.json"},
        )
        for entry in inventory:
            if entry["path"].endswith(".parquet"):
                entry["rows"] = pq.ParquetFile(staging / entry["path"]).metadata.num_rows
        manifest = {
            "manifest_version": "1.0.0",
            "schema_version": SCHEMA_VERSION,
            "status": "complete",
            "identity": identity,
            "source": {
                "url": source_url,
                "local_path": str(archive),
                "original_filename": archive.name,
                "bytes": archive.stat().st_size,
                "sha256": archive_sha256,
                "acquisition_status": "provided_local_existing_archive",
                "download_performed_by_pipeline": False,
                "member_count": archive_inventory["member_count"],
                "members": member_reports,
                "approved_inventory_evidence": source_assessment,
            },
            "assigned_quarter": quarter,
            "quarter_start": start.isoformat(),
            "quarter_end": end.isoformat(),
            "configuration": {
                "path": str(config_path),
                "sha256": config_sha256,
                "content": config,
            },
            "software": software_provenance(
                repository,
                recovery_identity=pipeline_identity,
            ),
            "processing": {
                "started_at": state["started_at"],
                "completed_at": utc_now(),
                "resumed": was_resumed,
                "input_rows": stats.input_rows,
                "output_rows": stats.output_rows,
                "quarantined_rows": stats.quarantined_rows,
                "exception_rows": exception_rows,
                "disk_space_preflight": disk_preflight,
            },
            "validation": validation,
            "outputs": inventory,
            "central_integration_required": True,
        }
        write_json_atomic(staging / "source_manifest.json", manifest)
        if _interrupt_before_publish:
            raise IngestionInterrupted("Controlled interruption after validation and before publication")
    finally:
        tracker.close()

    for runtime_name in RUNTIME_NAMES:
        runtime_path = staging / runtime_name
        if runtime_path.exists():
            runtime_path.unlink()
    work = staging / "work"
    if work.exists():
        _safe_remove_tree(work, staging)
    exception_parts = staging / "exception_parts"
    if exception_parts.exists():
        _safe_remove_tree(exception_parts, staging)
    os.replace(staging, output)
    return {
        "status": "complete",
        "output": str(output),
        "input_rows": stats.input_rows,
        "output_rows": stats.output_rows,
        "quarantined_rows": stats.quarantined_rows,
        "validation_status": validation["status"],
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Ingest one existing local Backblaze Drive Stats quarterly ZIP archive"
    )
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--quarter", required=True, help="Assigned quarter in YYYYQn form")
    parser.add_argument("--archive", type=Path, required=True, help="Existing local ZIP archive")
    parser.add_argument("--output", type=Path, required=True, help="Final immutable output directory")
    parser.add_argument("--source-url", required=True, help="Official source URL recorded as provenance only")
    action = parser.add_mutually_exclusive_group()
    action.add_argument("--dry-run", action="store_true")
    action.add_argument("--resume", action="store_true")
    action.add_argument("--restart", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    arguments = build_parser().parse_args(argv)
    try:
        result = run_ingestion(
            config_path=arguments.config,
            quarter=arguments.quarter,
            archive=arguments.archive,
            output=arguments.output,
            source_url=arguments.source_url,
            dry_run=arguments.dry_run,
            resume=arguments.resume,
            restart=arguments.restart,
        )
    except (PipelineError, ValidationError, OSError, zipfile.BadZipFile) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
