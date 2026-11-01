"""Command-line ingestion of one existing local Backblaze quarterly archive."""

from __future__ import annotations

import argparse
import codecs
from collections import Counter
import csv
from dataclasses import dataclass, replace
from datetime import date, timedelta
from decimal import Decimal
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

from drivepriority.data import (
    SCHEMA_CONTRACT_VERSION,
    SCHEMA_FINGERPRINT_METHOD,
    SCHEMA_VERSION,
)
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
    normalize_header,
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
MANIFEST_VERSION = "1.1.0"
PUBLICATION_MARKER = "publication_ready.json"
RUNTIME_NAMES = {
    "run_state.json",
    "state.duckdb",
    "state.duckdb.wal",
    "processing_failure.json",
}
REPRODUCIBILITY_METHOD_VERSION = "1.0.0"
MISSING_VALUE_CONTRACT_VERSION = "1.0.0"
IMMUTABLE_MISSING_TOKENS = [""]
DUCKDB_MEMORY_LIMIT_PATTERN = re.compile(
    r"^[1-9][0-9]*(?:\.[0-9]+)?(?:B|KB|MB|GB|TB|KiB|MiB|GiB|TiB)$"
)


class PipelineError(RuntimeError):
    """Raised for a safe, actionable pipeline failure."""


class IngestionInterrupted(PipelineError):
    """Controlled interruption used to verify recovery behavior."""


def _reproducibility_evidence() -> dict[str, Any]:
    return {
        "method_version": REPRODUCIBILITY_METHOD_VERSION,
        "scope": "semantic_equivalence_for_matching_provenance_identity",
        "semantic_comparison_includes": [
            "canonical row values and Arrow types",
            "Arrow and Parquet schemas including metadata",
            "schema, quality, and validation reports",
            "source-member and exception evidence inventories",
            "failure counts and source-to-output accounting",
        ],
        "intentionally_variable_metadata": [
            "processing timestamps",
            "available disk space at preflight",
            "absolute local paths",
            "host platform and dependency metadata when environments differ",
        ],
        "byte_for_byte_file_equality_claimed": False,
        "limitations": [
            "Parquet physical bytes can vary with library, codec, platform, or writer metadata",
            "semantic comparison does not replace manifest checksum verification for one published run",
            "real historical archives require separate acceptance testing",
        ],
    }


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
    schema = payload.get("schema")
    if not isinstance(schema, dict):
        raise PipelineError("Configuration schema must be a mapping")
    if schema.get("version") != SCHEMA_VERSION:
        raise PipelineError(
            f"Configuration schema.version must be {SCHEMA_VERSION!r}"
        )
    if schema.get("missing_value_contract_version") != MISSING_VALUE_CONTRACT_VERSION:
        raise PipelineError(
            "Configuration schema.missing_value_contract_version must be "
            f"{MISSING_VALUE_CONTRACT_VERSION!r}"
        )
    if schema.get("missing_tokens") != IMMUTABLE_MISSING_TOKENS:
        raise PipelineError(
            "Configuration schema.missing_tokens must be exactly ['']; literal zero and "
            "all nonempty tokens are scientific source values"
        )
    for field, required in (
        ("allowed_start_date", "2016-01-01"),
        ("allowed_end_date", "2026-06-30"),
    ):
        if schema.get(field) != required:
            raise PipelineError(f"Configuration schema.{field} must remain {required!r}")
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
        "canonical_batch_bytes",
        "exception_batch_rows",
        "exception_batch_bytes",
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
    try:
        canonical_encoding = codecs.lookup(encoding).name
    except LookupError as exc:
        raise PipelineError("Configuration processing.encoding is not a known codec") from exc
    if canonical_encoding != codecs.lookup("utf-8-sig").name or encoding.lower().replace("_", "-") not in {
        "utf-8-sig",
        "utf8-sig",
    }:
        raise PipelineError(
            "Configuration processing.encoding must remain 'utf-8-sig' for the quarterly contract"
        )
    memory_limit = processing.get("duckdb_memory_limit")
    if not isinstance(memory_limit, str) or not DUCKDB_MEMORY_LIMIT_PATTERN.fullmatch(
        memory_limit
    ):
        raise PipelineError(
            "Configuration processing.duckdb_memory_limit must be a positive explicit byte unit"
        )
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
    if compression in {"none", "uncompressed"}:
        if compression_level != 0:
            raise PipelineError(
                "Configuration parquet.compression_level must be 0 for uncompressed output"
            )
    else:
        try:
            supports_level = pa.Codec.supports_compression_level(compression)
            if supports_level:
                maximum_level = pa.Codec.maximum_compression_level(compression)
                if compression_level > maximum_level:
                    raise ValueError(
                        f"compression level exceeds codec maximum {maximum_level}"
                    )
            elif compression_level != 0:
                raise ValueError("codec does not accept an explicit compression level")
            pa.Codec(compression, compression_level=compression_level)
        except (ValueError, OSError, pa.ArrowException) as exc:
            raise PipelineError(
                "Configuration parquet.compression_level is invalid for the selected codec"
            ) from exc
    for field in ("write_statistics", "dictionary_encode_strings"):
        if type(parquet.get(field)) is not bool:
            raise PipelineError(f"Configuration parquet.{field} must be a boolean")

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
                    header_bytes = binary.readline()
                    if not header_bytes:
                        raise StopIteration
                    header_text = header_bytes.decode(encoding)
                    headers = next(csv.reader([header_text], strict=True))
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


def _stable_archive_identity(archive: Path) -> dict[str, Any]:
    """Hash a source only when its file metadata stays stable throughout the read."""

    before = archive.stat()
    digest = sha256_file(archive)
    after = archive.stat()
    stable_fields = ("st_size", "st_mtime_ns")
    if any(getattr(before, name) != getattr(after, name) for name in stable_fields):
        raise PipelineError("Source ZIP changed while its identity was being calculated")
    return {
        "bytes": before.st_size,
        "sha256": digest,
    }


def _verify_archive_unchanged(
    archive: Path, expected: Mapping[str, Any], *, phase: str
) -> None:
    try:
        observed = _stable_archive_identity(archive)
    except OSError as exc:
        raise PipelineError(f"Source ZIP is unavailable during {phase}") from exc
    if observed != dict(expected):
        raise PipelineError(
            f"Source ZIP identity changed during {phase}; refusing publication"
        )


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


def _validate_publication_marker(
    staging: Path, identity: Mapping[str, Any]
) -> tuple[dict[str, Any], dict[str, Any]]:
    marker_path = staging / PUBLICATION_MARKER
    manifest_path = staging / "source_manifest.json"
    if not marker_path.is_file() or not manifest_path.is_file():
        raise PipelineError(
            "Staging has neither processing state nor a verifiable publication marker"
        )
    marker = read_json(marker_path)
    manifest = read_json(manifest_path)
    if (
        marker.get("marker_version") != "1.0.0"
        or marker.get("identity") != dict(identity)
        or manifest.get("identity") != dict(identity)
        or manifest.get("status") != "complete"
        or marker.get("manifest_sha256") != sha256_file(manifest_path)
    ):
        raise PipelineError("Completed staging publication marker is invalid")
    state = {
        "identity": dict(identity),
        "started_at": manifest.get("processing", {}).get("started_at"),
        "status": "publication_ready",
    }
    return state, manifest


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
        if (staging / "run_state.json").is_file():
            state = _validate_state(staging, identity)
        else:
            state, _manifest = _validate_publication_marker(staging, identity)
        if restart:
            _safe_remove_tree(staging, staging.parent)
        elif not resume:
            raise PipelineError("Matching staging output exists; use --resume or --restart")
        else:
            if state.get("status") == "failed" or (
                staging / "processing_failure.json"
            ).exists():
                raise PipelineError(
                    "Staging contains a parser or decoding failure; inspect "
                    "processing_failure.json and use --restart to replay safely"
                )
            return state, True
    staging.mkdir(parents=True, exist_ok=False)
    state = {"identity": dict(identity), "started_at": utc_now(), "status": "processing"}
    write_json_atomic(staging / "run_state.json", state)
    return state, False


def _write_publication_marker(
    staging: Path, identity: Mapping[str, Any]
) -> None:
    manifest_path = staging / "source_manifest.json"
    write_json_atomic(
        staging / PUBLICATION_MARKER,
        {
            "marker_version": "1.0.0",
            "status": "publication_ready",
            "identity": dict(identity),
            "manifest_sha256": sha256_file(manifest_path),
            "recorded_at": utc_now(),
        },
    )


def _arrow_schema_fingerprint(schema: pa.Schema) -> str:
    return sha256_bytes(schema.serialize().to_pybytes())


def _field_inventory(schema: pa.Schema) -> list[dict[str, Any]]:
    inventory: list[dict[str, Any]] = []
    for field in schema:
        entry: dict[str, Any] = {
            "name": field.name,
            "arrow_type": str(field.type),
            "nullable": field.nullable,
        }
        if pa.types.is_decimal(field.type):
            entry["decimal_precision"] = field.type.precision
            entry["decimal_scale"] = field.type.scale
        inventory.append(entry)
    return inventory


def _published_schema_evidence(root: Path) -> tuple[dict[str, Any], pa.Schema | None]:
    data_files = sorted((root / "data").rglob("*.parquet")) if (root / "data").exists() else []
    schemas: list[pa.Schema] = []
    files: list[dict[str, Any]] = []
    errors: list[str] = []
    for path in data_files:
        relative = path.relative_to(root).as_posix()
        try:
            parquet_file = pq.ParquetFile(path)
            schema = parquet_file.schema_arrow
            schemas.append(schema)
            files.append(
                {
                    "path": relative,
                    "rows": parquet_file.metadata.num_rows,
                    "schema_fingerprint": _arrow_schema_fingerprint(schema),
                }
            )
        except (OSError, pa.ArrowException) as exc:
            errors.append(f"{relative}: {type(exc).__name__}")

    canonical_schema = schemas[0] if schemas else None
    schemas_consistent = bool(schemas) and not errors and all(
        schema.equals(canonical_schema, check_metadata=True) for schema in schemas
    )
    metadata = {
        key.decode("utf-8", errors="replace"): value.decode("utf-8", errors="replace")
        for key, value in (canonical_schema.metadata or {}).items()
    } if canonical_schema is not None else {}
    evidence = {
        "schema_contract_version": SCHEMA_CONTRACT_VERSION,
        "fingerprint_method": SCHEMA_FINGERPRINT_METHOD,
        "fingerprint_source": "Arrow schema read back from staged or published canonical Parquet",
        "canonical_schema_fingerprint": (
            _arrow_schema_fingerprint(canonical_schema)
            if canonical_schema is not None
            else None
        ),
        "canonical_columns": (
            _field_inventory(canonical_schema) if canonical_schema is not None else []
        ),
        "arrow_schema_metadata": metadata,
        "data_file_schemas": files,
        "all_data_file_schemas_consistent": schemas_consistent,
        "read_errors": errors,
    }
    return evidence, canonical_schema


def _quarterly_schema_contract(
    root: Path,
    expected_schema: pa.Schema,
    smart_columns: Sequence[str],
    unexpected_smart_columns: Sequence[str],
) -> dict[str, Any]:
    evidence, actual_schema = _published_schema_evidence(root)
    evidence.update(
        {
            "recognized_numeric_smart_fields": list(smart_columns),
            "unexpected_smart_extension_fields": list(unexpected_smart_columns),
            "matches_planned_quarterly_union": (
                actual_schema is not None
                and actual_schema.equals(expected_schema, check_metadata=True)
            ),
        }
    )
    return evidence


def _verify_published_schema_contract(
    root: Path, recorded_contract: Mapping[str, Any] | None
) -> list[str]:
    if not isinstance(recorded_contract, Mapping):
        return ["missing quarterly schema contract"]
    current, _schema = _published_schema_evidence(root)
    errors: list[str] = []
    for field_name in (
        "schema_contract_version",
        "fingerprint_method",
        "canonical_schema_fingerprint",
        "canonical_columns",
        "arrow_schema_metadata",
        "data_file_schemas",
        "all_data_file_schemas_consistent",
    ):
        if recorded_contract.get(field_name) != current.get(field_name):
            errors.append(f"quarterly schema contract mismatch: {field_name}")
    if current["read_errors"]:
        errors.extend(f"published schema read failure: {item}" for item in current["read_errors"])
    if not current["all_data_file_schemas_consistent"]:
        errors.append("published canonical Parquet schemas are inconsistent")
    for report_name in ("schema_report.json", "validation_results.json"):
        report_path = root / report_name
        try:
            report_contract = read_json(report_path).get("schema_contract")
        except (OSError, ValueError, json.JSONDecodeError) as exc:
            errors.append(
                f"cannot verify schema contract in {report_name}: {type(exc).__name__}"
            )
            continue
        if report_contract != dict(recorded_contract):
            errors.append(f"quarterly schema contract mismatch: {report_name}")
    return errors


def _verify_completed_manifest(
    root: Path, manifest: Mapping[str, Any], identity: Mapping[str, Any]
) -> list[str]:
    errors: list[str] = []
    if manifest.get("manifest_version") != MANIFEST_VERSION:
        errors.append("unsupported or missing manifest version")
    if manifest.get("status") != "complete":
        errors.append("manifest status is not complete")
    if manifest.get("identity") != dict(identity):
        errors.append("manifest identity mismatch")
    outputs = manifest.get("outputs")
    if not isinstance(outputs, list):
        return [*errors, "manifest outputs must be a nonempty list"]
    required_paths = {
        "schema_report.json",
        "quality_report.json",
        "validation_results.json",
    }
    data_paths = {
        str(entry.get("path"))
        for entry in outputs
        if isinstance(entry, Mapping)
        and str(entry.get("path", "")).startswith("data/")
        and str(entry.get("path", "")).endswith(".parquet")
    }
    if not data_paths:
        errors.append("manifest inventories no canonical Parquet data")
    required_paths.update(data_paths)
    exception_rows = manifest.get("processing", {}).get("exception_rows")
    if isinstance(exception_rows, bool) or not isinstance(exception_rows, int):
        errors.append("manifest processing.exception_rows is invalid")
    elif exception_rows > 0:
        required_paths.add("exceptions.parquet")
    elif any(
        isinstance(entry, Mapping) and entry.get("path") == "exceptions.parquet"
        for entry in outputs
    ):
        errors.append("manifest inventories exceptions.parquet with zero exception rows")
    errors.extend(
        verify_inventory(
            root,
            outputs,
            excluded_names=RUNTIME_NAMES
            | {"source_manifest.json", PUBLICATION_MARKER},
            required_paths=required_paths,
        )
    )
    errors.extend(
        _verify_published_schema_contract(root, manifest.get("schema_contract"))
    )
    try:
        _state, marker_manifest = _validate_publication_marker(root, identity)
        if marker_manifest != dict(manifest):
            errors.append("publication marker manifest content mismatch")
    except (PipelineError, OSError, ValueError, json.JSONDecodeError) as exc:
        errors.append(f"publication marker verification failed: {exc}")
    return errors


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
    errors = _verify_completed_manifest(output, manifest, identity)
    if errors:
        raise PipelineError("Existing output verification failed: " + "; ".join(errors))
    return {"status": "verified_existing", "output": str(output), "manifest": manifest}


def _parquet_schema_fingerprint(path: Path) -> str:
    schema = pq.ParquetFile(path).schema_arrow
    return _arrow_schema_fingerprint(schema)


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
                "schema_fingerprint": _arrow_schema_fingerprint(
                    parquet_file.schema_arrow
                ),
            }
        )
    return artifacts


def _checkpoint_evidence_summary(
    staging: Path, relative_paths: Sequence[str]
) -> dict[str, Any]:
    data_rows = 0
    exception_rows = 0
    issue_codes: Counter[str] = Counter()
    quarantined_evidence_ids: set[str] = set()
    all_evidence_ids: set[str] = set()
    raw_copy_counts: Counter[str] = Counter()
    missing_evidence_links = 0
    field_issue_rows = 0
    for relative in sorted(relative_paths):
        path = staging / relative
        parquet_file = pq.ParquetFile(path)
        if relative.startswith("data/"):
            data_rows += parquet_file.metadata.num_rows
            continue
        if not relative.startswith("exception_parts/"):
            continue
        for batch in parquet_file.iter_batches(
            batch_size=8192,
            columns=[
                "exception_code",
                "row_disposition",
                "row_evidence_id",
                "raw_record_json",
                "column_name",
            ],
        ):
            for row in batch.to_pylist():
                exception_rows += 1
                issue_codes[str(row["exception_code"])] += 1
                evidence_id = row.get("row_evidence_id")
                if not evidence_id:
                    missing_evidence_links += 1
                    continue
                all_evidence_ids.add(str(evidence_id))
                if row.get("row_disposition") == "quarantined":
                    quarantined_evidence_ids.add(str(evidence_id))
                if row.get("raw_record_json") is not None:
                    raw_copy_counts[str(evidence_id)] += 1
                if row.get("column_name") is not None:
                    field_issue_rows += 1
    duplicate_or_missing_raw_evidence = sum(
        raw_copy_counts.get(evidence_id, 0) != 1
        for evidence_id in all_evidence_ids
    )
    return {
        "data_rows": data_rows,
        "exception_rows": exception_rows,
        "issue_code_counts": dict(sorted(issue_codes.items())),
        "quarantined_evidence_records": len(quarantined_evidence_ids),
        "linked_evidence_records": len(all_evidence_ids),
        "missing_evidence_links": missing_evidence_links,
        "raw_evidence_copy_violations": duplicate_or_missing_raw_evidence,
        "field_issue_rows": field_issue_rows,
    }


def _verify_completed_checkpoints(
    staging: Path, completed: Mapping[int, Mapping[str, Any]]
) -> None:
    errors: list[str] = []
    resolved_staging = staging.resolve()
    for member_index in sorted(completed):
        stats = completed[member_index].get("stats", {})
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

        member_error_count = len(errors)
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
        if len(errors) != member_error_count:
            continue
        try:
            actual_summary = _checkpoint_evidence_summary(staging, relative_paths)
        except (OSError, ValueError, pa.ArrowException) as exc:
            errors.append(
                f"member {member_index}: cannot reconcile checkpoint evidence: {exc}"
            )
            continue
        if files.get("evidence_summary") != actual_summary:
            errors.append(f"member {member_index}: checkpoint evidence summary mismatch")
        expected_issue_codes = {
            str(code): int(count)
            for code, count in stats.get("issue_codes", {}).items()
        }
        if actual_summary["data_rows"] != int(stats.get("output_rows", -1)):
            errors.append(f"member {member_index}: canonical checkpoint row mismatch")
        if actual_summary["issue_code_counts"] != expected_issue_codes:
            errors.append(f"member {member_index}: exception checkpoint count mismatch")
        if actual_summary["quarantined_evidence_records"] != int(
            stats.get("quarantined_rows", -1)
        ):
            errors.append(f"member {member_index}: quarantine evidence count mismatch")
        if actual_summary["missing_evidence_links"]:
            errors.append(f"member {member_index}: exception evidence link is missing")
        if actual_summary["raw_evidence_copy_violations"]:
            errors.append(f"member {member_index}: raw-row evidence linkage is inconsistent")

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


def _row_evidence_id(issue: ConversionIssue) -> str | None:
    if issue.raw_record_sha256 is None:
        return None
    payload = {
        "source_quarter": issue.source_quarter,
        "source_file": issue.source_file,
        "source_record_number": issue.source_record_number,
        "source_line_number": issue.source_line_number,
        "raw_record_sha256": issue.raw_record_sha256,
    }
    return sha256_bytes(stable_json_bytes(payload))


def _estimated_issue_bytes(issue: ConversionIssue) -> int:
    """Conservatively estimate buffered Python evidence without discarding it."""

    return 256 + sum(
        len(value.encode("utf-8"))
        for value in issue.to_dict().values()
        if isinstance(value, str)
    )


def _estimated_canonical_entry_bytes(row: Mapping[str, Any]) -> int:
    """Estimate buffered canonical and provenance payload bytes conservatively."""

    total = 512
    for value in row.values():
        if isinstance(value, str):
            total += len(value.encode("utf-8")) + 64
        elif isinstance(value, Decimal):
            total += len(str(value)) + 64
        else:
            total += 64
    return total


def _sanitized_failure_description(exc: BaseException, limit: int = 500) -> str:
    if isinstance(exc, UnicodeError):
        description = (
            f"codec={getattr(exc, 'encoding', 'unknown')}; "
            f"start={getattr(exc, 'start', 'unknown')}; "
            f"end={getattr(exc, 'end', 'unknown')}; "
            f"reason={getattr(exc, 'reason', type(exc).__name__)}"
        )
    else:
        description = str(exc)
    description = " ".join(description.split())
    if len(description) > limit:
        description = description[: limit - 3] + "..."
    return description or type(exc).__name__


def _write_processing_failure(
    staging: Path,
    *,
    archive_sha256: str,
    member: MemberPlan,
    member_sequence: int,
    exc: BaseException,
    source_record_number: int | None,
    source_line_number: int | None,
    approximate_byte_position: int | None,
    rows_seen: int,
    completed_member_indexes: Sequence[int],
) -> None:
    state_path = staging / "run_state.json"
    state = read_json(state_path)
    recorded_at = utc_now()
    report = {
        "report_version": "1.0.0",
        "status": "processing_failed",
        "recorded_at": recorded_at,
        "archive_sha256": archive_sha256,
        "identity": state.get("identity"),
        "source_member": {
            "filename": member.info.filename,
            "archive_index": member.archive_index,
            "processing_sequence": member_sequence,
        },
        "position": {
            "source_record_number": source_record_number,
            "approximate_line_number": source_line_number,
            "approximate_uncompressed_byte_position": approximate_byte_position,
            "rows_seen_in_run": rows_seen,
        },
        "exception": {
            "type": type(exc).__name__,
            "description": _sanitized_failure_description(exc),
        },
        "recovery": {
            "state_before_failure": state.get("status"),
            "state_after_failure": "failed",
            "completed_member_indexes": sorted(completed_member_indexes),
            "resume_allowed": False,
            "restart_required": True,
        },
    }
    write_json_atomic(staging / "processing_failure.json", report)
    state.update(
        {
            "status": "failed",
            "failed_at": recorded_at,
            "failure_report": "processing_failure.json",
        }
    )
    write_json_atomic(state_path, state)


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
    archive_sha256: str,
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
    canonical_batch_bytes = int(config["processing"]["canonical_batch_bytes"])
    exception_batch_rows = int(config["processing"]["exception_batch_rows"])
    exception_batch_bytes = int(config["processing"]["exception_batch_bytes"])
    encoding = str(config["processing"].get("encoding", "utf-8-sig"))
    exception_schema = exception_arrow_schema()
    fingerprint_fields = [
        name for name in output_schema.names if name not in {"source_quarter", "source_file", "schema_version"}
    ]
    stats = QualityStats()
    pending_entries: list[tuple[Mapping[str, Any], int, int]] = []
    pending_entry_bytes = 0
    pending_issues: list[ConversionIssue] = []
    pending_issue_bytes = 0
    written_evidence_ids: set[str] = set()
    source_rows_in_batch = 0
    data_part_number = 0
    exception_part_number = 0
    rows_seen = rows_seen_before_member
    current_record_number: int | None = None
    current_line_number: int | None = None
    approximate_byte_position: int | None = None
    reader: Any = None

    def flush_exceptions() -> None:
        nonlocal exception_part_number, pending_issue_bytes
        if not pending_issues:
            return
        exception_table = pa.Table.from_pylist(
            [issue.to_dict() for issue in pending_issues], schema=exception_schema
        )
        _write_parquet(
            work_member
            / "exceptions"
            / f"part-m{member_sequence:05d}-e{exception_part_number:06d}.parquet",
            exception_table,
            config,
        )
        pending_issues.clear()
        pending_issue_bytes = 0
        exception_part_number += 1

    def queue_issue(issue: ConversionIssue) -> None:
        nonlocal pending_issue_bytes
        evidence_id = issue.row_evidence_id or _row_evidence_id(issue)
        issue = replace(issue, row_evidence_id=evidence_id)
        if evidence_id is not None and issue.raw_record_json is not None:
            if evidence_id in written_evidence_ids:
                issue = replace(issue, raw_record_json=None)
            else:
                written_evidence_ids.add(evidence_id)
        estimated_bytes = _estimated_issue_bytes(issue)
        if pending_issues and (
            len(pending_issues) >= exception_batch_rows
            or pending_issue_bytes + estimated_bytes > exception_batch_bytes
        ):
            flush_exceptions()
        pending_issues.append(issue)
        pending_issue_bytes += estimated_bytes
        if (
            len(pending_issues) >= exception_batch_rows
            or pending_issue_bytes >= exception_batch_bytes
        ):
            flush_exceptions()

    def flush_data() -> None:
        nonlocal data_part_number, source_rows_in_batch, pending_entry_bytes
        duplicate_issues = tracker.classify(pending_entries, fingerprint_fields)
        for issue in duplicate_issues:
            stats.record_issue(issue)
            queue_issue(issue)

        if pending_entries:
            table = pa.Table.from_pylist(
                [
                    {name: entry[0].get(name) for name in output_schema.names}
                    for entry in pending_entries
                ],
                schema=output_schema,
            )
            _write_parquet(
                work_member
                / "data"
                / f"part-m{member_sequence:05d}-b{data_part_number:06d}.parquet",
                table,
                config,
            )
        pending_entries.clear()
        pending_entry_bytes = 0
        written_evidence_ids.clear()
        source_rows_in_batch = 0
        data_part_number += 1

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
                        current_record_number = source_record_number
                        current_line_number = reader.line_num
                        try:
                            approximate_byte_position = binary.tell()
                        except (OSError, ValueError):
                            approximate_byte_position = None
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
                            queue_issue(issue)
                            stats.record_issue(issue)
                            stats.malformed_rows += 1
                            stats.quarantined_rows += 1
                            malformed_index = member.analysis.index_by_canonical
                            date_position = malformed_index.get("date")
                            failure_position = malformed_index.get("failure")
                            malformed_date: date | None = None
                            if date_position is not None and date_position < len(values):
                                try:
                                    malformed_date = parse_iso_date(values[date_position])
                                except ValueError:
                                    pass
                            malformed_flag = (
                                values[failure_position].strip()
                                if failure_position is not None
                                and failure_position < len(values)
                                and values[failure_position].strip() in {"0", "1"}
                                else None
                            )
                            stats.record_malformed_failure_evidence(
                                malformed_flag, malformed_date
                            )
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
                            source_flag: str | None = None
                            if failure_position is not None and values[failure_position].strip() in {"0", "1"}:
                                source_flag = values[failure_position].strip()
                                stats.record_failure_flag(
                                    source_flag,
                                    source_date,
                                    disposition="source",
                                )

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
                            model_position = index.get("model")
                            model_token = (
                                values[model_position] if model_position is not None else None
                            )
                            model_key = (
                                "<MISSING>"
                                if model_token is None or model_token.strip() in missing_tokens
                                else model_token
                            )
                            member_extensions = set(member.analysis.unexpected_smart_columns)
                            for field_name, state in smart_states.items():
                                if field_name in member_extensions:
                                    stats.record_extension_state(
                                        field_name,
                                        state,
                                        source_quarter=quarter,
                                        source_file=member.info.filename,
                                        model=model_key,
                                    )
                                else:
                                    stats.record_smart_state(
                                        field_name,
                                        state,
                                        source_quarter=quarter,
                                        source_file=member.info.filename,
                                        model=model_key,
                                    )
                            for field_name in (
                                set(unexpected_smart_columns) - member_extensions
                            ):
                                stats.record_extension_state(
                                    field_name,
                                    "absent",
                                    source_quarter=quarter,
                                    source_file=member.info.filename,
                                    model=model_key,
                                )
                            for issue in issues:
                                stats.record_issue(issue)
                                queue_issue(issue)
                            if row is None:
                                stats.quarantined_rows += 1
                                if source_flag is not None:
                                    stats.record_failure_flag(
                                        source_flag,
                                        source_date,
                                        disposition="quarantined",
                                    )
                            else:
                                if source_flag is not None:
                                    stats.record_failure_flag(
                                        source_flag,
                                        source_date,
                                        disposition="retained",
                                    )
                                stats.observe_output(row, member.analysis.smart_columns)
                                raw_json, raw_sha = raw_record_evidence(
                                    member.analysis.original_headers, values
                                )
                                identity_tokens = [
                                    row.get(name)
                                    for name in ("model", "manufacturer", "capacity_bytes")
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
                                pending_entry_bytes += _estimated_canonical_entry_bytes(row)

                        if (
                            source_rows_in_batch >= batch_rows
                            or pending_entry_bytes >= canonical_batch_bytes
                        ):
                            flush_data()
        if source_rows_in_batch or pending_entries:
            flush_data()
        flush_exceptions()

        absent = set(smart_columns) - set(member.analysis.smart_columns)
        for field_name in absent:
            stats.smart_absent_by_field[field_name] = stats.input_rows

        relative_paths = _move_member_outputs(work_member, staging, quarter)
        file_details = {
            "relative_paths": relative_paths,
            "artifacts": _checkpoint_artifacts(staging, relative_paths),
            "evidence_summary": _checkpoint_evidence_summary(
                staging, relative_paths
            ),
        }
        tracker.complete_member(member_sequence, stats, file_details)
        if work_member.exists():
            _safe_remove_tree(work_member, staging)
        return stats, relative_paths, rows_seen
    except (csv.Error, UnicodeError) as exc:
        tracker.rollback_member()
        if reader is not None:
            current_line_number = getattr(reader, "line_num", current_line_number)
        _write_processing_failure(
            staging,
            archive_sha256=archive_sha256,
            member=member,
            member_sequence=member_sequence,
            exc=exc,
            source_record_number=current_record_number,
            source_line_number=current_line_number,
            approximate_byte_position=approximate_byte_position,
            rows_seen=rows_seen,
            completed_member_indexes=list(tracker.completed_members()),
        )
        raise PipelineError(
            f"CSV parser or decoding failed in {member.info.filename}; inspect "
            "processing_failure.json and use --restart"
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
            parquet_file = pq.ParquetFile(part)
            for batch in parquet_file.iter_batches(
                batch_size=int(config["processing"]["exception_batch_rows"])
            ):
                writer.write_batch(batch)
                rows += batch.num_rows
    finally:
        writer.close()
    return rows


def _finalize_publication(
    staging: Path,
    output: Path,
    *,
    interrupt_after_cleanup: str | None = None,
) -> None:
    for runtime_name in sorted(RUNTIME_NAMES):
        runtime_path = staging / runtime_name
        if runtime_path.exists():
            runtime_path.unlink()
        if interrupt_after_cleanup == runtime_name:
            raise IngestionInterrupted(
                f"Controlled interruption after publication cleanup of {runtime_name}"
            )
    for internal_name in ("work", "exception_parts"):
        internal_path = staging / internal_name
        if internal_path.exists():
            _safe_remove_tree(internal_path, staging)
        if interrupt_after_cleanup == internal_name:
            raise IngestionInterrupted(
                f"Controlled interruption after publication cleanup of {internal_name}"
            )
    os.replace(staging, output)


def _publish_completed_staging(
    staging: Path,
    output: Path,
    identity: Mapping[str, Any],
    *,
    interrupt_after_cleanup: str | None = None,
) -> dict[str, Any] | None:
    """Finish an already validated staging result after a late interruption."""

    manifest_path = staging / "source_manifest.json"
    if not manifest_path.is_file():
        return None
    manifest = read_json(manifest_path)
    if manifest.get("status") != "complete" or manifest.get("identity") != dict(identity):
        return None
    errors = _verify_completed_manifest(staging, manifest, identity)
    if errors:
        raise PipelineError("Completed staging verification failed: " + "; ".join(errors))
    _finalize_publication(
        staging,
        output,
        interrupt_after_cleanup=interrupt_after_cleanup,
    )
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
    schema_contract: Mapping[str, Any],
    smart_columns: Sequence[str],
    unexpected_smart_columns: Sequence[str],
    stats: QualityStats,
) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "schema_contract_version": SCHEMA_CONTRACT_VERSION,
        "canonical_schema_fingerprint": schema_contract[
            "canonical_schema_fingerprint"
        ],
        "canonical_fields": list(schema_contract["canonical_columns"]),
        "schema_contract": dict(schema_contract),
        "smart_numeric_contract": {
            "type": "decimal128(38, 0)",
            "minimum": str(-(10**38 - 1)),
            "maximum": str(10**38 - 1),
            "fractional_or_scientific_tokens": "exception evidence; canonical value null",
        },
        "smart_columns": list(smart_columns),
        "unexpected_smart_string_columns": list(unexpected_smart_columns),
        "unexpected_smart_contract": {
            "canonical_type": "string",
            "preservation": "original non-missing source token",
            "interpretation": (
                "Unvalidated extension; not treated as a numeric SMART measurement without "
                "a separately approved explicit contract"
            ),
        },
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
        "source_column_availability_and_drift": {
            "distinct_header_signatures": sorted(
                {member.analysis.signature for member in members}
            ),
            "schema_drift_observed": len(
                {member.analysis.signature for member in members}
            ) > 1,
            "members": [
                {
                    "filename": member.info.filename,
                    "header_signature": member.analysis.signature,
                    "recognized_numeric_smart_fields_present": list(
                        member.analysis.smart_columns
                    ),
                    "recognized_numeric_smart_fields_absent_from_member": sorted(
                        set(smart_columns) - set(member.analysis.smart_columns)
                    ),
                    "unexpected_smart_extension_fields_present": list(
                        member.analysis.unexpected_smart_columns
                    ),
                    "unexpected_smart_extension_fields_absent_from_member": sorted(
                        set(unexpected_smart_columns)
                        - set(member.analysis.unexpected_smart_columns)
                    ),
                    "unknown_source_fields": list(member.analysis.unknown_columns),
                    "missing_required_fields": list(
                        member.analysis.missing_required_columns
                    ),
                }
                for member in members
            ],
        },
        "model_smart_column_availability": {
            model: sorted(columns) for model, columns in sorted(stats.model_columns_present.items())
        },
        "model_smart_non_null_counts": stats.to_dict()["model_smart_non_null"],
        "identity_scope": (
            "Raw identifiers are preserved. Stable device_key construction and cross-quarter identity "
            "resolution are central integration responsibilities."
        ),
        "historical_compatibility_evidence": {
            "status": "contract_and_synthetic_fixture_coverage_only",
            "synthetic_fixtures_are_authentic_records": False,
            "covered_variability": [
                "optional manufacturer column",
                "SMART column presence and absence across members",
                "case, spacing, underscore, and UTF-8 BOM header forms",
                "blank, signed, unusual, and invalid SMART tokens",
                "nested and date-oriented archive member names",
                "assigned-quarter boundary dates through 2026-06-30",
            ],
            "outstanding_evidence": [
                "acceptance results for independently verified real archives from every historical period",
                "measured peak RAM on representative real quarterly archives",
            ],
        },
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
    extension_canonical_nulls = {
        field_name: stats.output_rows - stats.non_null_by_field.get(field_name, 0)
        for field_name in stats.extension_state_counts
    }
    failure_flags = sorted(
        set(stats.failure_flag_counts)
        | set(stats.retained_failure_flag_counts)
        | set(stats.quarantined_failure_flag_counts)
    )
    failure_reconciliation = {
        flag: {
            "valid_source": stats.failure_flag_counts.get(flag, 0),
            "retained_canonical": stats.retained_failure_flag_counts.get(flag, 0),
            "quarantine_evidence": stats.quarantined_failure_flag_counts.get(flag, 0),
            "passed": stats.failure_flag_counts.get(flag, 0)
            == stats.retained_failure_flag_counts.get(flag, 0)
            + stats.quarantined_failure_flag_counts.get(flag, 0),
        }
        for flag in failure_flags
    }
    daily_dates = sorted(
        set(stats.daily_source_failure_flag_counts)
        | set(stats.daily_retained_failure_flag_counts)
        | set(stats.daily_quarantined_failure_flag_counts)
    )
    daily_reconciliation = {
        observed_date: {
            flag: {
                "valid_source": stats.daily_source_failure_flag_counts.get(
                    observed_date, {}
                ).get(flag, 0),
                "retained_canonical": stats.daily_retained_failure_flag_counts.get(
                    observed_date, {}
                ).get(flag, 0),
                "quarantine_evidence": stats.daily_quarantined_failure_flag_counts.get(
                    observed_date, {}
                ).get(flag, 0),
                "passed": stats.daily_source_failure_flag_counts.get(
                    observed_date, {}
                ).get(flag, 0)
                == stats.daily_retained_failure_flag_counts.get(observed_date, {}).get(
                    flag, 0
                )
                + stats.daily_quarantined_failure_flag_counts.get(
                    observed_date, {}
                ).get(flag, 0),
            }
            for flag in sorted(
                set(stats.daily_source_failure_flag_counts.get(observed_date, {}))
                | set(stats.daily_retained_failure_flag_counts.get(observed_date, {}))
                | set(stats.daily_quarantined_failure_flag_counts.get(observed_date, {}))
            )
        }
        for observed_date in daily_dates
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
                "suspicious_negative_rows": stats.smart_negative_by_field.get(
                    field_name, 0
                ),
                "canonical_null_rows": missing_canonical[field_name],
            }
            for field_name in smart_columns
        },
        "negative_smart_values": {
            "by_quarter": QualityStats._sorted_nested(stats.smart_negative_by_quarter),
            "by_member": QualityStats._sorted_nested(stats.smart_negative_by_member),
            "by_model": QualityStats._sorted_nested(stats.smart_negative_by_model),
            "by_field": dict(sorted(stats.smart_negative_by_field.items())),
            "interpretation": (
                "Signed values are preserved exactly as decimal128(38,0) and flagged as "
                "suspicious; no correction or universal SMART interpretation is applied"
            ),
        },
        "unexpected_smart_extension_states": {
            "by_field": {
                field_name: {
                    "absent_column_rows": states.get("absent", 0),
                    "present_blank_rows": states.get("blank", 0),
                    "literal_zero_rows": states.get("zero", 0),
                    "other_nonempty_rows": states.get("value", 0),
                    "canonical_null_rows": extension_canonical_nulls[field_name],
                }
                for field_name, states in sorted(stats.extension_state_counts.items())
            },
            "by_quarter": QualityStats._sorted_state_dimensions(
                stats.extension_states_by_quarter
            ),
            "by_member": QualityStats._sorted_state_dimensions(
                stats.extension_states_by_member
            ),
            "by_model": QualityStats._sorted_state_dimensions(
                stats.extension_states_by_model
            ),
            "interpretation": (
                "Unexpected smart_* extensions are source strings only and are not "
                "validated numeric measurements"
            ),
        },
        "calendar_coverage": dict(calendar_coverage),
        "source_authenticity": dict(source_assessment),
        "failure_flags": {
            "preserved_valid_source_counts": dict(sorted(stats.failure_flag_counts.items())),
            "valid_source_counts": dict(sorted(stats.failure_flag_counts.items())),
            "retained_canonical_counts": dict(
                sorted(stats.retained_failure_flag_counts.items())
            ),
            "quarantine_evidence_counts": dict(
                sorted(stats.quarantined_failure_flag_counts.items())
            ),
            "daily_valid_source_counts": QualityStats._sorted_nested(
                stats.daily_source_failure_flag_counts
            ),
            "daily_retained_canonical_counts": QualityStats._sorted_nested(
                stats.daily_retained_failure_flag_counts
            ),
            "daily_quarantine_evidence_counts": QualityStats._sorted_nested(
                stats.daily_quarantined_failure_flag_counts
            ),
            "source_to_output_reconciliation": {
                "by_flag": failure_reconciliation,
                "by_date": daily_reconciliation,
                "passed": all(
                    item["passed"] for item in failure_reconciliation.values()
                )
                and all(
                    item["passed"]
                    for date_items in daily_reconciliation.values()
                    for item in date_items.values()
                ),
            },
            "malformed_width_evidence": {
                "safely_identifiable_flag_counts": dict(
                    sorted(stats.malformed_identifiable_failure_flag_counts.items())
                ),
                "daily_safely_identifiable_flag_counts": QualityStats._sorted_nested(
                    stats.daily_malformed_identifiable_failure_flag_counts
                ),
                "unclassifiable_failure_rows": (
                    stats.malformed_unclassifiable_failure_rows
                ),
                "included_in_valid_source_or_output_counts": False,
                "accounting_scope_complete": (
                    stats.malformed_unclassifiable_failure_rows == 0
                ),
                "interpretation": (
                    "Malformed-width records remain quarantine evidence. Readable positional "
                    "flags are disclosed separately and never treated as retained drive days."
                ),
            },
            "first_failure_event_selection_performed": False,
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


def _readback_failure_evidence(
    staging: Path,
) -> dict[str, Any]:
    """Decode every Parquet data page and independently reconstruct failure counts."""

    retained: Counter[str] = Counter()
    retained_daily: dict[str, Counter[str]] = {}
    quarantined: Counter[str] = Counter()
    quarantined_daily: dict[str, Counter[str]] = {}
    decoded_rows = 0
    errors: list[str] = []

    def record(
        counts: Counter[str], daily: dict[str, Counter[str]], flag: str, day: str | None
    ) -> None:
        counts[flag] += 1
        if day is not None:
            daily.setdefault(day, Counter())[flag] += 1

    data_files = (
        sorted((staging / "data").rglob("*.parquet"))
        if (staging / "data").exists()
        else []
    )
    for path in data_files:
        try:
            parquet_file = pq.ParquetFile(path)
            for batch in parquet_file.iter_batches(batch_size=8192):
                decoded_rows += batch.num_rows
                failures = batch.column(batch.schema.get_field_index("failure")).to_pylist()
                dates = batch.column(batch.schema.get_field_index("date")).to_pylist()
                for flag, observed_date in zip(failures, dates, strict=True):
                    record(
                        retained,
                        retained_daily,
                        str(flag),
                        observed_date.isoformat() if observed_date is not None else None,
                    )
        except Exception as exc:
            errors.append(f"{path.relative_to(staging).as_posix()}: {type(exc).__name__}")

    exceptions_path = staging / "exceptions.parquet"
    seen_quarantined_evidence: set[str] = set()
    if exceptions_path.is_file():
        try:
            parquet_file = pq.ParquetFile(exceptions_path)
            for batch in parquet_file.iter_batches(batch_size=8192):
                rows = batch.to_pylist()
                for row in rows:
                    if row.get("row_disposition") != "quarantined":
                        continue
                    evidence_id = row.get("row_evidence_id")
                    raw_json = row.get("raw_record_json")
                    if (
                        not isinstance(evidence_id, str)
                        or evidence_id in seen_quarantined_evidence
                        or not isinstance(raw_json, str)
                    ):
                        continue
                    seen_quarantined_evidence.add(evidence_id)
                    payload = json.loads(raw_json)
                    if not isinstance(payload, dict) or "headers" in payload:
                        continue
                    normalized = {normalize_header(str(key)): value for key, value in payload.items()}
                    raw_flag = normalized.get("failure")
                    flag = raw_flag.strip() if isinstance(raw_flag, str) else None
                    if flag not in {"0", "1"}:
                        continue
                    raw_date = normalized.get("date")
                    observed_date: str | None = None
                    if isinstance(raw_date, str):
                        try:
                            observed_date = parse_iso_date(raw_date).isoformat()
                        except ValueError:
                            pass
                    record(quarantined, quarantined_daily, flag, observed_date)
        except Exception as exc:
            errors.append(
                f"exceptions.parquet: {type(exc).__name__}"
            )

    def plain_nested(values: Mapping[str, Counter[str]]) -> dict[str, dict[str, int]]:
        return {
            day: dict(sorted(counts.items()))
            for day, counts in sorted(values.items())
        }

    return {
        "decoded_canonical_rows": decoded_rows,
        "retained_counts": dict(sorted(retained.items())),
        "retained_daily_counts": plain_nested(retained_daily),
        "quarantined_counts": dict(sorted(quarantined.items())),
        "quarantined_daily_counts": plain_nested(quarantined_daily),
        "errors": errors,
    }


def _validation_results(
    stats: QualityStats,
    output_schema: pa.Schema,
    schema_contract: Mapping[str, Any],
    staging: Path,
    member_count: int,
    completed_count: int,
    calendar_coverage: Mapping[str, Any],
    source_assessment: Mapping[str, Any],
) -> dict[str, Any]:
    data_files = sorted((staging / "data").rglob("*.parquet")) if (staging / "data").exists() else []
    readback = _readback_failure_evidence(staging)
    parquet_rows = int(readback["decoded_canonical_rows"])
    schemas_match = True
    readable = True
    for path in data_files:
        try:
            parquet_file = pq.ParquetFile(path)
            schemas_match = schemas_match and parquet_file.schema_arrow.equals(output_schema, check_metadata=True)
        except Exception:
            readable = False
            schemas_match = False
    if readback["errors"]:
        readable = False
    failure_flags = (
        set(stats.failure_flag_counts)
        | set(stats.retained_failure_flag_counts)
        | set(stats.quarantined_failure_flag_counts)
    )
    failure_totals_reconcile = all(
        stats.failure_flag_counts.get(flag, 0)
        == stats.retained_failure_flag_counts.get(flag, 0)
        + stats.quarantined_failure_flag_counts.get(flag, 0)
        for flag in failure_flags
    )
    failure_dates = (
        set(stats.daily_source_failure_flag_counts)
        | set(stats.daily_retained_failure_flag_counts)
        | set(stats.daily_quarantined_failure_flag_counts)
    )
    failure_daily_reconcile = all(
        stats.daily_source_failure_flag_counts.get(observed_date, {}).get(flag, 0)
        == stats.daily_retained_failure_flag_counts.get(observed_date, {}).get(flag, 0)
        + stats.daily_quarantined_failure_flag_counts.get(observed_date, {}).get(flag, 0)
        for observed_date in failure_dates
        for flag in (
            set(stats.daily_source_failure_flag_counts.get(observed_date, {}))
            | set(stats.daily_retained_failure_flag_counts.get(observed_date, {}))
            | set(stats.daily_quarantined_failure_flag_counts.get(observed_date, {}))
        )
    )
    independent_failure_reconcile = (
        readback["retained_counts"]
        == dict(sorted(stats.retained_failure_flag_counts.items()))
        and readback["quarantined_counts"]
        == dict(sorted(stats.quarantined_failure_flag_counts.items()))
        and readback["retained_daily_counts"]
        == QualityStats._sorted_nested(stats.daily_retained_failure_flag_counts)
        and readback["quarantined_daily_counts"]
        == QualityStats._sorted_nested(stats.daily_quarantined_failure_flag_counts)
    )
    checks = [
        {"name": "all_csv_members_completed", "passed": completed_count == member_count},
        validate_accounting(stats),
        {
            "name": "nonempty_retained_output",
            "passed": stats.output_rows > 0,
            "retained_output_rows": stats.output_rows,
        },
        {
            "name": "parquet_files_readable",
            "passed": readable,
            "bounded_full_page_readback_errors": readback["errors"],
        },
        {"name": "canonical_schema_matches", "passed": schemas_match},
        {
            "name": "parquet_row_count_matches",
            "passed": parquet_rows == stats.output_rows,
            "parquet_rows": parquet_rows,
            "reported_output_rows": stats.output_rows,
        },
        {
            "name": "failure_flag_source_to_output_reconciliation",
            "passed": (
                failure_totals_reconcile
                and failure_daily_reconcile
                and independent_failure_reconcile
            ),
            "total_counts_passed": failure_totals_reconcile,
            "daily_counts_passed": failure_daily_reconcile,
            "independent_parquet_and_evidence_counts_passed": (
                independent_failure_reconcile
            ),
            "readback": readback,
        },
        {
            "name": "published_quarterly_schema_contract",
            "passed": (
                schema_contract.get("canonical_schema_fingerprint") is not None
                and schema_contract.get("all_data_file_schemas_consistent") is True
                and schema_contract.get("matches_planned_quarterly_union") is True
                and schema_contract.get("read_errors") == []
                and schema_contract.get("arrow_schema_metadata", {}).get(
                    "drivepriority.schema_version"
                )
                == SCHEMA_VERSION
                and schema_contract.get("arrow_schema_metadata", {}).get(
                    "drivepriority.schema_contract_version"
                )
                == SCHEMA_CONTRACT_VERSION
            ),
            "schema_contract_version": SCHEMA_CONTRACT_VERSION,
            "canonical_schema_fingerprint": schema_contract.get(
                "canonical_schema_fingerprint"
            ),
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
        "schema_contract": dict(schema_contract),
        "reproducibility": _reproducibility_evidence(),
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
    _interrupt_publication_after_cleanup: str | None = None,
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
    source_snapshot = _stable_archive_identity(archive)
    archive_sha256 = str(source_snapshot["sha256"])
    repository = Path(__file__).resolve().parents[3]
    try:
        pipeline_identity = pipeline_recovery_identity(repository)
    except OSError as exc:
        raise PipelineError(f"Cannot establish pipeline recovery identity: {exc}") from exc
    members, archive_inventory = inspect_archive(archive, config)
    _verify_archive_unchanged(archive, source_snapshot, phase="archive inspection")
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
        _verify_archive_unchanged(archive, source_snapshot, phase="dry-run verification")
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
        completed_result = _publish_completed_staging(
            staging,
            output,
            identity,
            interrupt_after_cleanup=_interrupt_publication_after_cleanup,
        )
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
                archive_sha256=archive_sha256,
                interrupt_after_rows=_interrupt_after_rows,
                rows_seen_before_member=rows_seen,
            )
            completed = tracker.completed_members()
            rows_seen = sum(int(item["stats"].get("input_rows", 0)) for item in completed.values())

        completed = tracker.completed_members()
        stats = _aggregate_completed(completed)
        _verify_archive_unchanged(archive, source_snapshot, phase="member processing")
        exception_rows = _compact_exceptions(staging, config)
        schema_contract = _quarterly_schema_contract(
            staging,
            output_schema,
            smart_columns,
            unexpected_smart_columns,
        )
        schema_report = _schema_report(
            members,
            schema_contract,
            smart_columns,
            unexpected_smart_columns,
            stats,
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
            schema_contract,
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
                "manifest_version": MANIFEST_VERSION,
                "schema_version": SCHEMA_VERSION,
                "status": "failed",
                "identity": identity,
                "schema_contract": schema_contract,
                "validation": validation,
                "completed_at": utc_now(),
            }
            write_json_atomic(staging / "source_manifest.json", failed_manifest)
            raise ValidationError("Structural validation failed; output remains in staging")

        _verify_archive_unchanged(archive, source_snapshot, phase="pre-publication validation")

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
            "manifest_version": MANIFEST_VERSION,
            "schema_version": SCHEMA_VERSION,
            "status": "complete",
            "identity": identity,
            "schema_contract": schema_contract,
            "source": {
                "url": source_url,
                "local_path": str(archive),
                "original_filename": archive.name,
                "bytes": source_snapshot["bytes"],
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
            "reproducibility": _reproducibility_evidence(),
            "outputs": inventory,
            "central_integration_required": True,
        }
        _verify_archive_unchanged(archive, source_snapshot, phase="manifest publication")
        write_json_atomic(staging / "source_manifest.json", manifest)
        _verify_archive_unchanged(archive, source_snapshot, phase="publication marker creation")
        _write_publication_marker(staging, identity)
        if _interrupt_before_publish:
            raise IngestionInterrupted("Controlled interruption after validation and before publication")
    finally:
        tracker.close()

    _verify_archive_unchanged(archive, source_snapshot, phase="atomic publication")
    _finalize_publication(
        staging,
        output,
        interrupt_after_cleanup=_interrupt_publication_after_cleanup,
    )
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
