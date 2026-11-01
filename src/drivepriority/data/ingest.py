"""Command-line ingestion of one existing local Backblaze quarterly archive."""

from __future__ import annotations

import argparse
import csv
from dataclasses import dataclass
from datetime import date, timedelta
import io
import json
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
    frozen_safeguards = {
        "preserve_source_duplicates": True,
        "resolve_final_device_identity": False,
        "generate_future_labels": False,
    }
    for name, required_value in frozen_safeguards.items():
        if validation.get(name) is not required_value:
            raise PipelineError(
                f"Configuration validation.{name} must remain {required_value!r} in quarterly ingestion"
            )
    if int(payload.get("processing", {}).get("batch_rows", 0)) <= 0:
        raise PipelineError("Configuration processing.batch_rows must be positive")
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
    maximum_member_bytes = processing.get("maximum_member_uncompressed_bytes")
    maximum_ratio = float(processing.get("maximum_compression_ratio", 200))
    encoding = str(processing.get("encoding", "utf-8-sig"))

    members: list[MemberPlan] = []
    with zipfile.ZipFile(archive, "r") as source:
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
            if maximum_member_bytes is not None and info.file_size > int(maximum_member_bytes):
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
            members.append(MemberPlan(archive_index, info, analysis))

    inventory = {
        "member_count": len(members),
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
    return {
        "compression": str(parquet.get("compression", "zstd")),
        "compression_level": int(parquet.get("compression_level", 3)),
        "write_statistics": bool(parquet.get("write_statistics", True)),
        "use_dictionary": bool(parquet.get("dictionary_encode_strings", True)),
    }


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


def _quality_report(
    stats: QualityStats,
    smart_columns: Sequence[str],
    quarter_start: date,
    quarter_end: date,
) -> dict[str, Any]:
    expected_dates = calendar_dates(quarter_start, quarter_end)
    observed_dates = sorted(
        value for value in stats.daily_source_rows if quarter_start.isoformat() <= value <= quarter_end.isoformat()
    )
    missing_dates = sorted(set(expected_dates) - set(observed_dates))
    outside_dates = sorted(
        value for value in stats.daily_source_rows if value < quarter_start.isoformat() or value > quarter_end.isoformat()
    )
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
        "calendar_coverage": {
            "expected_start": quarter_start.isoformat(),
            "expected_end": quarter_end.isoformat(),
            "expected_day_count": len(expected_dates),
            "observed_dates": observed_dates,
            "missing_source_dates": missing_dates,
            "outside_assigned_quarter_dates": outside_dates,
            "daily_source_rows": dict(sorted(stats.daily_source_rows.items())),
            "daily_failure_flags": dict(sorted(stats.daily_failure_flags.items())),
        },
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
    has_quality_findings = bool(stats.issue_codes)
    status = "failed" if not structural_pass else ("passed_with_warnings" if has_quality_findings else "passed")
    return {
        "schema_version": SCHEMA_VERSION,
        "status": status,
        "checks": checks,
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
            "intended_operations": [
                "stream local CSV members in bounded batches",
                "write staged partitioned Parquet and audit reports",
                "validate and atomically publish output",
            ],
        }
    if output.exists():
        return _verify_existing_output(output, identity)
    output.parent.mkdir(parents=True, exist_ok=True)
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
        quality_report = _quality_report(stats, smart_columns, start, end)
        validation = _validation_results(
            stats, output_schema, staging, len(members), len(completed)
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
