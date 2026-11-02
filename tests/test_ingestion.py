from __future__ import annotations

import csv
from datetime import date, timedelta
from decimal import Decimal
import hashlib
import io
import json
from pathlib import Path
from types import SimpleNamespace
import zipfile

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import yaml

import drivepriority.data.ingest as ingest_module
from drivepriority import __version__
from drivepriority.data.ingest import (
    IngestionInterrupted,
    PipelineError,
    load_configuration,
    run_ingestion,
)
from drivepriority.data.manifest import (
    PIPELINE_RUNTIME_PATHS,
    REQUIREMENTS_PATH,
    pipeline_recovery_identity,
)
from drivepriority.data.validate import DuplicateTracker, ValidationError


CONFIG_SOURCE = Path(__file__).resolve().parents[1] / "configs" / "ingestion.yaml"
SOURCE_URL = "https://www.backblaze.com/cloud-storage/resources/hard-drive-test-data"
BASE_HEADER = [
    "date",
    "serial_number",
    "model",
    "manufacturer",
    "capacity_bytes",
    "failure",
]


def write_test_config(
    tmp_path: Path,
    *,
    batch_rows: int = 2,
    canonical_batch_bytes: int = 67108864,
    exception_batch_rows: int = 8192,
    exception_batch_bytes: int = 16777216,
) -> Path:
    config = yaml.safe_load(CONFIG_SOURCE.read_text(encoding="utf-8"))
    config["processing"]["batch_rows"] = batch_rows
    config["processing"]["canonical_batch_bytes"] = canonical_batch_bytes
    config["processing"]["exception_batch_rows"] = exception_batch_rows
    config["processing"]["exception_batch_bytes"] = exception_batch_bytes
    config["processing"]["parquet_row_group_rows"] = batch_rows
    path = tmp_path / "ingestion.yaml"
    path.write_text(yaml.safe_dump(config, sort_keys=False), encoding="utf-8")
    return path


def csv_text(rows: list[list[str]]) -> str:
    stream = io.StringIO(newline="")
    writer = csv.writer(stream, lineterminator="\n")
    writer.writerows(rows)
    return stream.getvalue()


def write_archive(path: Path, members: dict[str, list[list[str]]]) -> Path:
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for name, rows in members.items():
            archive.writestr(name, csv_text(rows))
    return path


def write_raw_archive(path: Path, members: dict[str, bytes]) -> Path:
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for name, payload in members.items():
            archive.writestr(name, payload)
    return path


def appledouble_metadata(payload: bytes = b"synthetic metadata") -> bytes:
    header = (
        b"\x00\x05\x16\x07"
        b"\x00\x02\x00\x00"
        b"Mac OS X        "
        b"\x00\x01"
    )
    offset = len(header) + 12
    entry = (
        (9).to_bytes(4, "big")
        + offset.to_bytes(4, "big")
        + len(payload).to_bytes(4, "big")
    )
    return header + entry + payload


def read_json(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


def read_drive_data(output: Path) -> pa.Table:
    files = sorted((output / "data").rglob("*.parquet"))
    assert files
    return pa.concat_tables([pq.read_table(path) for path in files])


def run(tmp_path: Path, archive: Path, output_name: str = "output", **kwargs):
    return run_ingestion(
        config_path=write_test_config(tmp_path),
        quarter="2016Q1",
        archive=archive,
        output=tmp_path / output_name,
        source_url=SOURCE_URL,
        **kwargs,
    )


def normal_archive(tmp_path: Path) -> Path:
    return write_archive(
        tmp_path / "2016Q1.zip",
        {
            "2016-01-01.csv": [
                [*BASE_HEADER, "smart_5_raw", "smart_198_raw"],
                ["2016-01-01", "SN-1", "Model A", "Vendor A", "1000", "0", "", "0"],
                [
                    "2016-01-02",
                    "SN-1",
                    "Model A",
                    "Vendor A",
                    "1000",
                    "1",
                    "-1",
                    str(10**38 - 1),
                ],
            ],
            "2016-01-03.csv": [
                [*BASE_HEADER, "smart_5_raw", "smart_9_normalized", "smart_vendor_blob"],
                ["2016-01-03", "SN-2", "Model B", "Vendor B", "2000", "0", "0", "100", "opaque"],
            ],
        },
    )


def q1_2016_dates() -> list[str]:
    start = date(2016, 1, 1)
    end = date(2016, 3, 31)
    return [
        (start + timedelta(days=offset)).isoformat()
        for offset in range((end - start).days + 1)
    ]


def write_approved_config(
    tmp_path: Path,
    archive: Path,
    *,
    approved_missing_dates: list[str] | None = None,
    include_checksum: bool = True,
    batch_rows: int = 32,
) -> Path:
    path = write_test_config(tmp_path, batch_rows=batch_rows)
    config = yaml.safe_load(path.read_text(encoding="utf-8"))
    config["source_inventory"] = {
        "status": "verified",
        "provenance_reference": "synthetic-test-inventory-v1",
        "verified_by": "automated-test-fixture",
        "verified_at": "2026-11-01T00:00:00Z",
        "quarters": {
            "2016Q1": {
                "status": "approved",
                "canonical_source_url": SOURCE_URL,
                "original_filename": archive.name,
                "expected_bytes": archive.stat().st_size,
                "expected_sha256": (
                    hashlib.sha256(archive.read_bytes()).hexdigest()
                    if include_checksum
                    else None
                ),
                "expected_dates": q1_2016_dates(),
                "approved_missing_dates": approved_missing_dates or [],
            }
        },
    }
    path.write_text(yaml.safe_dump(config, sort_keys=False), encoding="utf-8")
    return path


def complete_quarter_archive(
    tmp_path: Path,
    *,
    omitted_dates: set[str] | None = None,
    extra_rows: list[list[str]] | None = None,
    smart_value: str = "1",
) -> Path:
    omitted_dates = omitted_dates or set()
    rows = [[*BASE_HEADER, "smart_5_raw"]]
    rows.extend(
        [day, f"SN-{day}", "Model", "Vendor", "1000", "0", smart_value]
        for day in q1_2016_dates()
        if day not in omitted_dates
    )
    rows.extend(extra_rows or [])
    return write_archive(tmp_path / "2016Q1-complete.zip", {"quarter.csv": rows})


def fake_pipeline_identity(
    commit: str = "a" * 40, *, duckdb_version: str = "2.0.0"
) -> dict[str, object]:
    runtime_environment = {
        "identity_version": "1.0.0",
        "python_version": "3.11.9",
        "python_implementation": "CPython",
        "python_cache_tag": "cpython-311",
        "dependencies": {
            "PyYAML": "6.0.3",
            "duckdb": duckdb_version,
            "pyarrow": "23.0.1",
        },
    }
    return {
        "pipeline_version": __version__,
        "git_commit": commit,
        "git_metadata_available": True,
        "runtime_source_sha256": "b" * 64,
        "requirements_sha256": "c" * 64,
        "pipeline_scoped_dirty": False,
        "runtime_environment": runtime_environment,
        "runtime_environment_sha256": hashlib.sha256(
            (json.dumps(runtime_environment, sort_keys=True, indent=2) + "\n").encode("utf-8")
        ).hexdigest(),
    }


def write_identity_repository(tmp_path: Path) -> Path:
    repository = tmp_path / "identity-repository"
    for relative in PIPELINE_RUNTIME_PATHS:
        path = repository / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(f"# {relative}\n", encoding="utf-8")
    requirements = repository / REQUIREMENTS_PATH
    requirements.write_text(
        "duckdb==2.0.0\npyarrow==23.0.1\nPyYAML==6.0.3\n", encoding="utf-8"
    )
    return repository


def interrupted_checkpoint(tmp_path: Path) -> tuple[Path, Path, Path, Path]:
    archive = normal_archive(tmp_path)
    config = write_test_config(tmp_path, batch_rows=1)
    output = tmp_path / "checkpoint-output"
    with pytest.raises(IngestionInterrupted):
        run_ingestion(
            config_path=config,
            quarter="2016Q1",
            archive=archive,
            output=output,
            source_url=SOURCE_URL,
            _interrupt_after_rows=3,
        )
    staging = output.with_name(output.name + ".staging")
    return archive, config, output, staging


def checkpoint_files(staging: Path) -> dict[str, object]:
    connection = duckdb.connect(str(staging / "state.duckdb"))
    try:
        row = connection.execute(
            "SELECT files_json FROM completed_members ORDER BY member_index LIMIT 1"
        ).fetchone()
    finally:
        connection.close()
    assert row is not None
    return json.loads(row[0])


def replace_checkpoint_files(staging: Path, files: dict[str, object]) -> None:
    connection = duckdb.connect(str(staging / "state.duckdb"))
    try:
        connection.execute(
            "UPDATE completed_members SET files_json = ? WHERE member_index = 0",
            [json.dumps(files, sort_keys=True, separators=(",", ":"))],
        )
    finally:
        connection.close()


def resume_checkpoint(archive: Path, config: Path, output: Path) -> dict[str, object]:
    return run_ingestion(
        config_path=config,
        quarter="2016Q1",
        archive=archive,
        output=output,
        source_url=SOURCE_URL,
        resume=True,
    )


def test_normal_archive_ingestion_produces_reproducible_submission(tmp_path: Path) -> None:
    archive = normal_archive(tmp_path)
    result = run(tmp_path, archive)
    output = tmp_path / "output"
    table = read_drive_data(output)

    assert result == {
        "status": "complete",
        "output": str(output.resolve()),
        "input_rows": 3,
        "output_rows": 3,
        "quarantined_rows": 0,
        "validation_status": "requires_review",
    }
    assert table.num_rows == 3
    assert table.schema.field("smart_5_raw").type == pa.decimal128(38, 0)
    assert table.column("failure").to_pylist() == [0, 1, 0]
    assert table.column("smart_5_raw").to_pylist() == [None, -1, 0]
    assert table.column("smart_198_raw").to_pylist() == [0, 10**38 - 1, None]
    assert table.column("smart_vendor_blob").to_pylist() == [None, None, "opaque"]
    assert set(table.column("source_quarter").to_pylist()) == {"2016Q1"}
    exceptions = pq.read_table(output / "exceptions.parquet")
    assert exceptions.column("exception_code").to_pylist() == [
        "suspicious_negative_smart_value"
    ]

    quality = read_json(output / "quality_report.json")
    assert quality["smart_source_states"]["smart_5_raw"] == {
        "absent_column_rows": 0,
        "present_blank_rows": 1,
        "invalid_token_rows": 0,
        "genuine_zero_rows": 1,
        "suspicious_negative_rows": 1,
        "canonical_null_rows": 1,
    }
    assert quality["smart_source_states"]["smart_198_raw"]["absent_column_rows"] == 1
    assert quality["smart_source_states"]["smart_5_raw"]["suspicious_negative_rows"] == 1
    assert quality["failure_flags"]["preserved_valid_source_counts"] == {"0": 2, "1": 1}
    assert quality["calendar_coverage"]["observed_dates"] == [
        "2016-01-01",
        "2016-01-02",
        "2016-01-03",
    ]
    assert "2016-01-04" in quality["calendar_coverage"]["missing_source_dates"]

    manifest = read_json(output / "source_manifest.json")
    assert manifest["source"]["url"] == SOURCE_URL
    assert manifest["source"]["sha256"] == hashlib.sha256(archive.read_bytes()).hexdigest()
    assert manifest["manifest_version"] == "1.2.0"
    assert manifest["software"]["pipeline_version"] == "0.1.3"
    assert manifest["identity"]["pipeline"] == manifest["software"]["recovery_identity"]
    assert manifest["identity"]["pipeline"] == pipeline_recovery_identity(
        Path(__file__).resolve().parents[1]
    )
    assert manifest["identity"]["pipeline"]["git_commit"]
    assert len(manifest["identity"]["pipeline"]["runtime_source_sha256"]) == 64
    assert len(manifest["identity"]["pipeline"]["requirements_sha256"]) == 64
    assert isinstance(manifest["identity"]["pipeline"]["pipeline_scoped_dirty"], bool)
    assert manifest["configuration"]["sha256"]
    assert all(entry["sha256"] for entry in manifest["outputs"])
    assert manifest["central_integration_required"] is True


def test_dry_run_reads_and_hashes_but_creates_no_output(tmp_path: Path) -> None:
    archive = normal_archive(tmp_path)
    output = tmp_path / "never-created"
    config = write_test_config(tmp_path)

    result = run_ingestion(
        config_path=config,
        quarter="2016Q1",
        archive=archive,
        output=output,
        source_url=SOURCE_URL,
        dry_run=True,
    )

    assert result["status"] == "dry_run"
    assert result["member_count"] == 2
    assert not output.exists()
    assert not output.with_name(output.name + ".staging").exists()


def test_verified_appledouble_metadata_is_excluded_and_inventoried(
    tmp_path: Path,
) -> None:
    archive = write_raw_archive(
        tmp_path / "appledouble.zip",
        {
            "2016-01-01.csv": csv_text(
                [BASE_HEADER, ["2016-01-01", "SN-1", "Model", "Vendor", "1000", "0"]]
            ).encode("utf-8"),
            "__MACOSX/._2016-01-01.csv": appledouble_metadata(),
            "README.txt": b"synthetic mixed-member fixture",
        },
    )
    config = write_test_config(tmp_path)
    output = tmp_path / "appledouble-output"

    dry_run = run_ingestion(
        config_path=config,
        quarter="2016Q1",
        archive=archive,
        output=output,
        source_url=SOURCE_URL,
        dry_run=True,
    )
    selection = dry_run["archive_member_selection"]
    assert dry_run["member_count"] == 1
    assert selection["contract_version"] == "1.0.0"
    assert selection["archive_file_count"] == 3
    assert selection["data_csv_member_count"] == 1
    assert selection["excluded_metadata_member_count"] == 1
    assert selection["non_csv_member_count"] == 1
    assert selection["accounting_complete"] is True
    excluded = selection["excluded_metadata_members"][0]
    assert excluded["filename"] == "__MACOSX/._2016-01-01.csv"
    assert excluded["reason_code"] == "verified_appledouble_metadata"
    assert excluded["evidence"]["magic_hex"] == "00051607"
    assert excluded["evidence"]["version_hex"] == "00020000"
    assert excluded["crc_verified"] is True
    assert len(excluded["sha256"]) == 64
    assert selection["non_csv_members"][0]["filename"] == "README.txt"

    run_ingestion(
        config_path=config,
        quarter="2016Q1",
        archive=archive,
        output=output,
        source_url=SOURCE_URL,
    )
    manifest_selection = read_json(output / "source_manifest.json")["source"][
        "archive_member_selection"
    ]
    assert manifest_selection == selection
    assert read_drive_data(output).num_rows == 1


def test_appledouble_looking_binary_without_signature_is_not_excluded(
    tmp_path: Path,
) -> None:
    archive = write_raw_archive(
        tmp_path / "deceptive-appledouble.zip",
        {
            "2016-01-01.csv": csv_text(
                [BASE_HEADER, ["2016-01-01", "SN-1", "Model", "Vendor", "1000", "0"]]
            ).encode("utf-8"),
            "__MACOSX/._2016-01-02.csv": b"\xffnot verified AppleDouble metadata",
        },
    )

    with pytest.raises(PipelineError, match="Cannot read header.*__MACOSX"):
        run_ingestion(
            config_path=write_test_config(tmp_path),
            quarter="2016Q1",
            archive=archive,
            output=tmp_path / "deceptive-output",
            source_url=SOURCE_URL,
            dry_run=True,
        )


@pytest.mark.parametrize(
    "member_name",
    ["__MACOSX/._legitimate.csv", "__MACOSX/legitimate.csv"],
)
def test_legitimate_csv_under_macosx_is_not_excluded(
    tmp_path: Path, member_name: str
) -> None:
    payload = csv_text(
        [BASE_HEADER, ["2016-01-01", "SN-1", "Model", "Vendor", "1000", "0"]]
    ).encode("utf-8")
    archive = write_raw_archive(tmp_path / "legitimate-macosx.csv.zip", {member_name: payload})

    result = run_ingestion(
        config_path=write_test_config(tmp_path),
        quarter="2016Q1",
        archive=archive,
        output=tmp_path / "legitimate-output",
        source_url=SOURCE_URL,
        dry_run=True,
    )

    assert result["member_count"] == 1
    assert result["archive_member_selection"]["excluded_metadata_member_count"] == 0
    assert result["archive_member_selection"]["accounting_complete"] is True


def test_appledouble_signature_outside_top_level_macosx_is_not_excluded(
    tmp_path: Path,
) -> None:
    archive = write_raw_archive(
        tmp_path / "misplaced-appledouble.zip",
        {"nested/__MACOSX/._2016-01-01.csv": appledouble_metadata()},
    )

    with pytest.raises(PipelineError, match="missing required columns|Invalid header"):
        run_ingestion(
            config_path=write_test_config(tmp_path),
            quarter="2016Q1",
            archive=archive,
            output=tmp_path / "misplaced-output",
            source_url=SOURCE_URL,
            dry_run=True,
        )


def test_quarterly_configuration_cannot_enable_future_labels(tmp_path: Path) -> None:
    config_path = write_test_config(tmp_path)
    config = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    config["validation"]["generate_future_labels"] = True
    config_path.write_text(yaml.safe_dump(config, sort_keys=False), encoding="utf-8")

    with pytest.raises(PipelineError, match="generate_future_labels"):
        load_configuration(config_path)


def test_invalid_smart_token_is_retained_as_exception_evidence(tmp_path: Path) -> None:
    archive = write_archive(
        tmp_path / "invalid-smart.zip",
        {
            "day.csv": [
                [*BASE_HEADER, "smart_5_raw"],
                ["2016-01-01", "SN-1", "Model A", "Vendor", "1000", "0", "1e6"],
            ]
        },
    )

    result = run(tmp_path, archive)
    output = tmp_path / "output"
    data = read_drive_data(output)
    exceptions = pq.read_table(output / "exceptions.parquet")

    assert result["validation_status"] == "requires_review"
    assert data.column("smart_5_raw").to_pylist() == [None]
    assert exceptions.column("exception_code").to_pylist() == ["invalid_smart_value"]
    assert exceptions.column("raw_value").to_pylist() == ["1e6"]
    assert "1e6" in exceptions.column("raw_record_json").to_pylist()[0]
    quality = read_json(output / "quality_report.json")
    assert quality["smart_source_states"]["smart_5_raw"]["invalid_token_rows"] == 1


def test_bounded_batches_write_multiple_parquet_parts(tmp_path: Path) -> None:
    header = [*BASE_HEADER, "smart_5_raw"]
    rows = [
        [f"2016-01-0{day}", f"SN-{day}", "Model", "Vendor", "1000", "0", str(day)]
        for day in range(1, 6)
    ]
    archive = write_archive(tmp_path / "batched.zip", {"days.csv": [header, *rows]})
    config = write_test_config(tmp_path, batch_rows=2)
    output = tmp_path / "batched-output"

    run_ingestion(
        config_path=config,
        quarter="2016Q1",
        archive=archive,
        output=output,
        source_url=SOURCE_URL,
    )

    parts = sorted((output / "data" / "2016Q1").glob("*.parquet"))
    assert len(parts) == 3
    assert sum(pq.ParquetFile(path).metadata.num_rows for path in parts) == 5


def test_invalid_required_value_is_losslessly_quarantined_and_accounted(tmp_path: Path) -> None:
    archive = write_archive(
        tmp_path / "invalid-required.zip",
        {
            "day.csv": [
                [*BASE_HEADER, "smart_5_raw"],
                ["2016-01-01", "SN-1", "Model A", "Vendor", "1000", "2", "5"],
            ]
        },
    )

    with pytest.raises(ValidationError, match="Structural validation failed"):
        run(tmp_path, archive)
    output = tmp_path / "output"
    staging = output.with_name(output.name + ".staging")
    quality = read_json(staging / "quality_report.json")
    exceptions = pq.read_table(staging / "exceptions.parquet")

    assert not output.exists()
    assert read_json(staging / "validation_results.json")["status"] == "failed"
    assert quality["counts"]["input_rows"] == 1
    assert quality["counts"]["output_rows"] == 0
    assert quality["counts"]["quarantined_rows"] == 1
    assert quality["row_accounting"]["passed"]
    assert exceptions.column("exception_code").to_pylist() == ["invalid_failure_flag"]
    assert exceptions.column("raw_record_json").to_pylist()[0]


def test_malformed_column_count_and_out_of_quarter_date_are_audited(tmp_path: Path) -> None:
    archive = write_archive(
        tmp_path / "quality.zip",
        {
            "day.csv": [
                [*BASE_HEADER, "smart_5_raw"],
                ["2016-01-01", "SN-1", "Model A", "Vendor", "1000", "0", "5", "extra"],
                ["2016-04-01", "SN-2", "Model A", "Vendor", "1000", "0", "5"],
            ]
        },
    )

    with pytest.raises(ValidationError, match="Structural validation failed"):
        run(tmp_path, archive)
    output = tmp_path / "output"
    staging = output.with_name(output.name + ".staging")
    quality = read_json(staging / "quality_report.json")
    codes = set(pq.read_table(staging / "exceptions.parquet").column("exception_code").to_pylist())

    assert not output.exists()
    assert quality["counts"]["quarantined_rows"] == 2
    assert codes == {"malformed_column_count", "date_outside_assigned_quarter"}
    assert quality["counts"]["malformed_rows"] == 1
    assert quality["calendar_coverage"]["outside_assigned_quarter_dates"] == ["2016-04-01"]


def test_duplicate_audit_does_not_resolve_final_identity(tmp_path: Path) -> None:
    header = [*BASE_HEADER, "smart_5_raw"]
    archive = write_archive(
        tmp_path / "duplicates.zip",
        {
            "a.csv": [header, ["2016-01-01", "SN-X", "Model A", "Vendor", "1000", "0", "1"]],
            "b.csv": [
                header,
                ["2016-01-01", "SN-X", "Model A", "Vendor", "1000", "0", "1"],
                ["2016-01-01", "SN-X", "Model A", "Vendor", "1000", "0", "2"],
                ["2016-01-01", "SN-X", "Model B", "Vendor", "2000", "0", "3"],
            ],
        },
    )

    result = run(tmp_path, archive)
    output = tmp_path / "output"
    exceptions = pq.read_table(output / "exceptions.parquet")
    quality = read_json(output / "quality_report.json")

    assert read_drive_data(output).num_rows == 4
    assert exceptions.column("exception_code").to_pylist() == [
        "exact_duplicate",
        "conflicting_duplicate",
        "serial_identity_collision",
    ]
    assert quality["central_integration_boundary"]["deferred_centrally"]
    assert quality["counts"]["serial_identity_collisions"] == 1
    validation = read_json(output / "validation_results.json")
    assert result["validation_status"] == "requires_review"
    assert "serial_identity_collisions" in validation["review_reasons"]


def test_interrupted_member_resumes_without_touching_unrelated_files(tmp_path: Path) -> None:
    archive = normal_archive(tmp_path)
    config = write_test_config(tmp_path, batch_rows=1)
    output = tmp_path / "recoverable"
    sentinel = tmp_path / "unrelated.txt"
    sentinel.write_text("preserve me", encoding="utf-8")
    sentinel_hash = hashlib.sha256(sentinel.read_bytes()).hexdigest()

    with pytest.raises(IngestionInterrupted):
        run_ingestion(
            config_path=config,
            quarter="2016Q1",
            archive=archive,
            output=output,
            source_url=SOURCE_URL,
            _interrupt_after_rows=3,
        )

    assert not output.exists()
    staging = output.with_name(output.name + ".staging")
    assert staging.is_dir()
    files = checkpoint_files(staging)
    artifacts = files["artifacts"]
    assert artifacts
    assert set(artifacts[0]) == {
        "path",
        "bytes",
        "sha256",
        "rows",
        "schema_fingerprint",
    }
    assert hashlib.sha256(sentinel.read_bytes()).hexdigest() == sentinel_hash

    result = run_ingestion(
        config_path=config,
        quarter="2016Q1",
        archive=archive,
        output=output,
        source_url=SOURCE_URL,
        resume=True,
    )

    assert result["status"] == "complete"
    assert read_drive_data(output).num_rows == 3
    assert hashlib.sha256(sentinel.read_bytes()).hexdigest() == sentinel_hash


def test_mismatched_resume_is_refused_and_restart_is_scoped(tmp_path: Path) -> None:
    archive = normal_archive(tmp_path)
    config = write_test_config(tmp_path, batch_rows=1)
    output = tmp_path / "recoverable"
    sentinel = tmp_path / "sibling.bin"
    sentinel.write_bytes(b"do not touch")

    with pytest.raises(IngestionInterrupted):
        run_ingestion(
            config_path=config,
            quarter="2016Q1",
            archive=archive,
            output=output,
            source_url=SOURCE_URL,
            _interrupt_after_rows=1,
        )
    with pytest.raises(PipelineError, match="different archive.*pipeline identity"):
        run_ingestion(
            config_path=config,
            quarter="2016Q1",
            archive=archive,
            output=output,
            source_url="https://example.invalid/different.zip",
            resume=True,
        )

    result = run_ingestion(
        config_path=config,
        quarter="2016Q1",
        archive=archive,
        output=output,
        source_url=SOURCE_URL,
        restart=True,
    )
    assert result["status"] == "complete"
    assert sentinel.read_bytes() == b"do not touch"


def test_resume_after_validated_prepublication_interruption_preserves_exceptions(
    tmp_path: Path,
) -> None:
    archive = write_archive(
        tmp_path / "late-interrupt.zip",
        {
            "day.csv": [
                [*BASE_HEADER, "smart_5_raw"],
                ["2016-01-01", "SN-1", "Model", "Vendor", "1000", "0", "invalid"],
            ]
        },
    )
    config = write_test_config(tmp_path)
    output = tmp_path / "late-output"
    sentinel = tmp_path / "late-sibling.txt"
    sentinel.write_text("unchanged", encoding="utf-8")

    with pytest.raises(IngestionInterrupted, match="before publication"):
        run_ingestion(
            config_path=config,
            quarter="2016Q1",
            archive=archive,
            output=output,
            source_url=SOURCE_URL,
            _interrupt_before_publish=True,
        )

    staging = output.with_name(output.name + ".staging")
    assert (staging / "source_manifest.json").is_file()
    result = run_ingestion(
        config_path=config,
        quarter="2016Q1",
        archive=archive,
        output=output,
        source_url=SOURCE_URL,
        resume=True,
    )

    assert result["status"] == "complete"
    assert pq.read_table(output / "exceptions.parquet").num_rows == 1
    assert sentinel.read_text(encoding="utf-8") == "unchanged"
    assert not staging.exists()


def test_completed_rerun_verifies_instead_of_overwriting(tmp_path: Path) -> None:
    archive = normal_archive(tmp_path)
    config = write_test_config(tmp_path)
    output = tmp_path / "immutable"
    arguments = {
        "config_path": config,
        "quarter": "2016Q1",
        "archive": archive,
        "output": output,
        "source_url": SOURCE_URL,
    }
    run_ingestion(**arguments)
    manifest_hash = hashlib.sha256((output / "source_manifest.json").read_bytes()).hexdigest()

    result = run_ingestion(**arguments)

    assert result["status"] == "verified_existing"
    assert hashlib.sha256((output / "source_manifest.json").read_bytes()).hexdigest() == manifest_hash


def test_changed_pipeline_revision_refuses_resume(tmp_path: Path, monkeypatch) -> None:
    archive = normal_archive(tmp_path)
    config = write_test_config(tmp_path, batch_rows=1)
    output = tmp_path / "revision-resume"
    first_identity = fake_pipeline_identity("1" * 40)
    monkeypatch.setattr(
        "drivepriority.data.ingest.pipeline_recovery_identity",
        lambda _repository: first_identity,
    )
    with pytest.raises(IngestionInterrupted):
        run_ingestion(
            config_path=config,
            quarter="2016Q1",
            archive=archive,
            output=output,
            source_url=SOURCE_URL,
            _interrupt_after_rows=3,
        )

    monkeypatch.setattr(
        "drivepriority.data.ingest.pipeline_recovery_identity",
        lambda _repository: fake_pipeline_identity("2" * 40),
    )
    with pytest.raises(PipelineError, match="pipeline identity"):
        resume_checkpoint(archive, config, output)
    assert not output.exists()


def test_changed_pipeline_revision_refuses_completed_output_reuse(
    tmp_path: Path, monkeypatch
) -> None:
    archive = normal_archive(tmp_path)
    config = write_test_config(tmp_path)
    output = tmp_path / "revision-complete"
    monkeypatch.setattr(
        "drivepriority.data.ingest.pipeline_recovery_identity",
        lambda _repository: fake_pipeline_identity("1" * 40),
    )
    run_ingestion(
        config_path=config,
        quarter="2016Q1",
        archive=archive,
        output=output,
        source_url=SOURCE_URL,
    )

    monkeypatch.setattr(
        "drivepriority.data.ingest.pipeline_recovery_identity",
        lambda _repository: fake_pipeline_identity("2" * 40),
    )
    with pytest.raises(PipelineError, match="pipeline identity"):
        run_ingestion(
            config_path=config,
            quarter="2016Q1",
            archive=archive,
            output=output,
            source_url=SOURCE_URL,
        )


def test_runtime_source_change_changes_recovery_identity(tmp_path: Path) -> None:
    repository = write_identity_repository(tmp_path)
    before = pipeline_recovery_identity(repository)
    runtime_source = repository / PIPELINE_RUNTIME_PATHS[-1]
    runtime_source.write_text("# changed runtime source\n", encoding="utf-8")
    after = pipeline_recovery_identity(repository)

    assert before["runtime_source_sha256"] != after["runtime_source_sha256"]
    assert before["requirements_sha256"] == after["requirements_sha256"]
    assert before["git_commit"] is None
    assert before["pipeline_scoped_dirty"] is None
    assert before["git_metadata_available"] is False


def test_requirements_change_changes_recovery_identity(tmp_path: Path) -> None:
    repository = write_identity_repository(tmp_path)
    before = pipeline_recovery_identity(repository)
    requirements = repository / REQUIREMENTS_PATH
    requirements.write_text(
        requirements.read_text(encoding="utf-8") + "# reviewed lock annotation\n",
        encoding="utf-8",
    )
    after = pipeline_recovery_identity(repository)

    assert before["requirements_sha256"] != after["requirements_sha256"]
    assert before["runtime_source_sha256"] == after["runtime_source_sha256"]


def test_installed_runtime_change_changes_recovery_identity(
    tmp_path: Path, monkeypatch
) -> None:
    repository = write_identity_repository(tmp_path)
    first = fake_pipeline_identity()["runtime_environment"]
    second = fake_pipeline_identity(duckdb_version="2.0.1")["runtime_environment"]
    monkeypatch.setattr(
        "drivepriority.data.manifest.runtime_environment_identity", lambda: first
    )
    before = pipeline_recovery_identity(repository)
    monkeypatch.setattr(
        "drivepriority.data.manifest.runtime_environment_identity", lambda: second
    )
    assert before["runtime_environment"] == first
    with pytest.raises(RuntimeError, match="does not match requirements"):
        pipeline_recovery_identity(repository)


def test_installed_runtime_change_refuses_checkpoint_resume(
    tmp_path: Path, monkeypatch
) -> None:
    archive = normal_archive(tmp_path)
    config = write_test_config(tmp_path, batch_rows=1)
    output = tmp_path / "runtime-change-resume"
    monkeypatch.setattr(
        "drivepriority.data.ingest.pipeline_recovery_identity",
        lambda _repository: fake_pipeline_identity(duckdb_version="2.0.0"),
    )
    with pytest.raises(IngestionInterrupted):
        run_ingestion(
            config_path=config,
            quarter="2016Q1",
            archive=archive,
            output=output,
            source_url=SOURCE_URL,
            _interrupt_after_rows=3,
        )
    monkeypatch.setattr(
        "drivepriority.data.ingest.pipeline_recovery_identity",
        lambda _repository: fake_pipeline_identity(duckdb_version="2.0.1"),
    )

    with pytest.raises(PipelineError, match="pipeline identity"):
        run_ingestion(
            config_path=config,
            quarter="2016Q1",
            archive=archive,
            output=output,
            source_url=SOURCE_URL,
            resume=True,
        )


def test_installed_runtime_change_refuses_completed_output_reuse(
    tmp_path: Path, monkeypatch
) -> None:
    archive = normal_archive(tmp_path)
    config = write_test_config(tmp_path)
    output = tmp_path / "runtime-change-complete"
    monkeypatch.setattr(
        "drivepriority.data.ingest.pipeline_recovery_identity",
        lambda _repository: fake_pipeline_identity(duckdb_version="2.0.0"),
    )
    run_ingestion(
        config_path=config,
        quarter="2016Q1",
        archive=archive,
        output=output,
        source_url=SOURCE_URL,
    )
    monkeypatch.setattr(
        "drivepriority.data.ingest.pipeline_recovery_identity",
        lambda _repository: fake_pipeline_identity(duckdb_version="2.0.1"),
    )

    with pytest.raises(PipelineError, match="pipeline identity"):
        run_ingestion(
            config_path=config,
            quarter="2016Q1",
            archive=archive,
            output=output,
            source_url=SOURCE_URL,
        )


def test_unrelated_untracked_file_does_not_change_recovery_identity(
    tmp_path: Path, monkeypatch
) -> None:
    repository = write_identity_repository(tmp_path)
    observed_pathspecs: list[tuple[str, ...]] = []

    def fixed_revision(_repository: Path, *, pathspecs=None):
        observed_pathspecs.append(tuple(pathspecs or ()))
        return {"commit": "a" * 40, "branch": "test", "dirty": False}

    monkeypatch.setattr("drivepriority.data.manifest.git_revision", fixed_revision)
    before = pipeline_recovery_identity(repository)
    unrelated = repository / "docs" / "codex_connection_test.md"
    unrelated.parent.mkdir(parents=True)
    unrelated.write_text("untracked and unrelated\n", encoding="utf-8")
    after = pipeline_recovery_identity(repository)

    assert before == after
    assert all("docs/codex_connection_test.md" not in paths for paths in observed_pathspecs)


def test_tampered_readable_checkpoint_is_not_recertified(tmp_path: Path) -> None:
    archive, config, output, staging = interrupted_checkpoint(tmp_path)
    files = checkpoint_files(staging)
    artifact = files["artifacts"][0]
    target = staging / artifact["path"]
    original_sha256 = artifact["sha256"]
    table = pq.read_table(target)
    failure_index = table.schema.get_field_index("failure")
    replacement = pa.array(
        [1 if value == 0 else value for value in table.column("failure").to_pylist()],
        type=table.schema.field("failure").type,
    )
    pq.write_table(table.set_column(failure_index, "failure", replacement), target)
    artifact["bytes"] = target.stat().st_size
    replace_checkpoint_files(staging, files)

    with pytest.raises(PipelineError, match="--restart.*SHA-256 mismatch"):
        resume_checkpoint(archive, config, output)
    assert checkpoint_files(staging)["artifacts"][0]["sha256"] == original_sha256
    assert not output.exists()


def test_missing_checkpoint_artifact_requires_restart(tmp_path: Path) -> None:
    archive, config, output, staging = interrupted_checkpoint(tmp_path)
    files = checkpoint_files(staging)
    target = staging / files["artifacts"][0]["path"]
    target.unlink()

    with pytest.raises(PipelineError, match="--restart.*missing checkpoint artifact"):
        resume_checkpoint(archive, config, output)
    assert not output.exists()


@pytest.mark.parametrize(
    ("field", "message"),
    [
        ("bytes", "size mismatch"),
        ("sha256", "SHA-256 mismatch"),
        ("schema_fingerprint", "schema mismatch"),
        ("rows", "row count mismatch"),
    ],
)
def test_inconsistent_checkpoint_metadata_requires_restart(
    tmp_path: Path, field: str, message: str
) -> None:
    archive, config, output, staging = interrupted_checkpoint(tmp_path)
    files = checkpoint_files(staging)
    artifact = files["artifacts"][0]
    if field in {"bytes", "rows"}:
        artifact[field] = int(artifact[field]) + 1
    else:
        artifact[field] = "0" * 64
    replace_checkpoint_files(staging, files)

    with pytest.raises(PipelineError, match="--restart.*" + message):
        resume_checkpoint(archive, config, output)
    assert not output.exists()


def test_validation_results_are_deterministic_for_identical_inputs(tmp_path: Path) -> None:
    archive = normal_archive(tmp_path)
    config = write_test_config(tmp_path)
    first = tmp_path / "first"
    second = tmp_path / "second"

    for output in (first, second):
        run_ingestion(
            config_path=config,
            quarter="2016Q1",
            archive=archive,
            output=output,
            source_url=SOURCE_URL,
        )

    assert (first / "validation_results.json").read_bytes() == (
        second / "validation_results.json"
    ).read_bytes()
    validation = read_json(first / "validation_results.json")
    assert "deterministic_for_identical_inputs" not in validation
    assert validation["reproducibility"]["method_version"] == "1.0.0"
    assert validation["reproducibility"]["byte_for_byte_file_equality_claimed"] is False
    assert not {
        "label",
        "feature",
        "prediction",
        "risk_score",
    }.intersection(read_drive_data(first).schema.names)


def test_independent_runs_match_semantically_across_data_reports_and_evidence(
    tmp_path: Path,
) -> None:
    archive = normal_archive(tmp_path)
    config = write_test_config(tmp_path, batch_rows=1, exception_batch_rows=1)
    outputs = [tmp_path / "semantic-first", tmp_path / "semantic-second"]
    for output in outputs:
        run_ingestion(
            config_path=config,
            quarter="2016Q1",
            archive=archive,
            output=output,
            source_url=SOURCE_URL,
        )

    first_data = read_drive_data(outputs[0])
    second_data = read_drive_data(outputs[1])
    assert first_data.schema.equals(second_data.schema, check_metadata=True)
    assert first_data.to_pylist() == second_data.to_pylist()

    first_parts = sorted((outputs[0] / "data").rglob("*.parquet"))
    second_parts = sorted((outputs[1] / "data").rglob("*.parquet"))
    assert [path.relative_to(outputs[0]).as_posix() for path in first_parts] == [
        path.relative_to(outputs[1]).as_posix() for path in second_parts
    ]
    for first_part, second_part in zip(first_parts, second_parts, strict=True):
        assert pq.ParquetFile(first_part).schema_arrow.equals(
            pq.ParquetFile(second_part).schema_arrow,
            check_metadata=True,
        )

    first_exceptions = pq.read_table(outputs[0] / "exceptions.parquet")
    second_exceptions = pq.read_table(outputs[1] / "exceptions.parquet")
    assert first_exceptions.schema.equals(second_exceptions.schema, check_metadata=True)
    assert first_exceptions.to_pylist() == second_exceptions.to_pylist()

    for report_name in (
        "schema_report.json",
        "quality_report.json",
        "validation_results.json",
    ):
        assert read_json(outputs[0] / report_name) == read_json(outputs[1] / report_name)

    manifests = [read_json(output / "source_manifest.json") for output in outputs]
    source_fields = (
        "original_filename",
        "bytes",
        "sha256",
        "member_count",
        "members",
        "approved_inventory_evidence",
    )
    assert {name: manifests[0]["source"][name] for name in source_fields} == {
        name: manifests[1]["source"][name] for name in source_fields
    }
    semantic_inventories = [
        sorted(
            (entry["path"], entry.get("rows"))
            for entry in manifest["outputs"]
        )
        for manifest in manifests
    ]
    assert semantic_inventories[0] == semantic_inventories[1]
    first_quality = read_json(outputs[0] / "quality_report.json")
    second_quality = read_json(outputs[1] / "quality_report.json")
    assert first_quality["failure_flags"] == second_quality["failure_flags"]
    assert first_quality["row_accounting"] == second_quality["row_accounting"]
    assert manifests[0]["reproducibility"] == manifests[1]["reproducibility"]
    assert manifests[0]["reproducibility"]["intentionally_variable_metadata"]


def test_source_or_configuration_change_updates_provenance_identity(tmp_path: Path) -> None:
    first_archive = write_archive(
        tmp_path / "source-a.zip",
        {"day.csv": [[*BASE_HEADER, "smart_5_raw"], ["2016-01-01", "SN", "M", "V", "1", "0", "1"]]},
    )
    second_archive = write_archive(
        tmp_path / "source-b.zip",
        {"day.csv": [[*BASE_HEADER, "smart_5_raw"], ["2016-01-01", "SN", "M", "V", "1", "0", "2"]]},
    )
    first_config = write_test_config(tmp_path, batch_rows=2)
    second_config = tmp_path / "ingestion-changed.yaml"
    changed = yaml.safe_load(first_config.read_text(encoding="utf-8"))
    changed["processing"]["batch_rows"] = 3
    second_config.write_text(yaml.safe_dump(changed, sort_keys=False), encoding="utf-8")

    def identity(archive: Path, config: Path) -> dict[str, object]:
        return run_ingestion(
            config_path=config,
            quarter="2016Q1",
            archive=archive,
            output=tmp_path / f"unused-{archive.stem}-{config.stem}",
            source_url=SOURCE_URL,
            dry_run=True,
        )["identity"]

    baseline = identity(first_archive, first_config)
    source_changed = identity(second_archive, first_config)
    config_changed = identity(first_archive, second_config)
    assert baseline["archive_sha256"] != source_changed["archive_sha256"]
    assert baseline["config_sha256"] != config_changed["config_sha256"]
    assert baseline["pipeline"] == source_changed["pipeline"] == config_changed["pipeline"]


def test_corrupt_zip_and_header_collision_fail_before_final_publication(tmp_path: Path) -> None:
    config = write_test_config(tmp_path)
    corrupt = tmp_path / "corrupt.zip"
    corrupt.write_bytes(b"not a zip archive")
    with pytest.raises(zipfile.BadZipFile):
        run_ingestion(
            config_path=config,
            quarter="2016Q1",
            archive=corrupt,
            output=tmp_path / "bad-output",
            source_url=SOURCE_URL,
        )

    collision = write_archive(
        tmp_path / "collision.zip",
        {
            "day.csv": [
                [*BASE_HEADER, "SMART 5 RAW", "smart_5_raw"],
                ["2016-01-01", "SN", "M", "V", "1", "0", "1", "1"],
            ]
        },
    )
    with pytest.raises(PipelineError, match="collision"):
        run_ingestion(
            config_path=config,
            quarter="2016Q1",
            archive=collision,
            output=tmp_path / "collision-output",
            source_url=SOURCE_URL,
        )
    assert not (tmp_path / "collision-output").exists()


@pytest.mark.parametrize("missing_field", ["date", "serial_number", "model", "capacity_bytes", "failure"])
def test_missing_required_header_is_rejected_before_staging(
    tmp_path: Path, missing_field: str
) -> None:
    field_index = BASE_HEADER.index(missing_field)
    header = [value for index, value in enumerate(BASE_HEADER) if index != field_index]
    row = [
        value
        for index, value in enumerate(
            ["2016-01-01", "SN-1", "Model", "Vendor", "1000", "0"]
        )
        if index != field_index
    ]
    archive = write_archive(tmp_path / "missing-header.zip", {"day.csv": [header, row]})
    output = tmp_path / "missing-header-output"

    with pytest.raises(PipelineError, match=f"missing required columns: {missing_field}"):
        run_ingestion(
            config_path=write_test_config(tmp_path),
            quarter="2016Q1",
            archive=archive,
            output=output,
            source_url=SOURCE_URL,
        )
    assert not output.exists()
    assert not output.with_name(output.name + ".staging").exists()


def test_empty_archive_is_rejected_before_staging(tmp_path: Path) -> None:
    archive = tmp_path / "empty.zip"
    with zipfile.ZipFile(archive, "w"):
        pass
    output = tmp_path / "empty-output"

    with pytest.raises(PipelineError, match="no CSV members"):
        run_ingestion(
            config_path=write_test_config(tmp_path),
            quarter="2016Q1",
            archive=archive,
            output=output,
            source_url=SOURCE_URL,
        )
    assert not output.exists()


def test_header_only_archive_fails_nonempty_output_check(tmp_path: Path) -> None:
    archive = write_archive(
        tmp_path / "header-only.zip",
        {"day.csv": [[*BASE_HEADER, "smart_5_raw"]]},
    )
    output = tmp_path / "header-only-output"

    with pytest.raises(ValidationError, match="Structural validation failed"):
        run_ingestion(
            config_path=write_test_config(tmp_path),
            quarter="2016Q1",
            archive=archive,
            output=output,
            source_url=SOURCE_URL,
        )

    staging = output.with_name(output.name + ".staging")
    validation = read_json(staging / "validation_results.json")
    nonempty = next(check for check in validation["checks"] if check["name"] == "nonempty_retained_output")
    assert validation["status"] == "failed"
    assert not nonempty["passed"]
    assert not output.exists()


def test_verified_inventory_and_complete_calendar_pass(tmp_path: Path) -> None:
    archive = complete_quarter_archive(tmp_path)
    config = write_approved_config(tmp_path, archive)
    output = tmp_path / "trusted-complete"

    result = run_ingestion(
        config_path=config,
        quarter="2016Q1",
        archive=archive,
        output=output,
        source_url=SOURCE_URL,
    )

    validation = read_json(output / "validation_results.json")
    manifest = read_json(output / "source_manifest.json")
    quality = read_json(output / "quality_report.json")
    assert result["validation_status"] == "passed"
    assert validation["status"] == "passed"
    assert validation["outcome_order"] == [
        "failed",
        "requires_review",
        "passed_with_warnings",
        "passed",
    ]
    assert validation["central_integration_admission"]["eligible"]
    assert quality["calendar_coverage"]["status"] == "complete"
    assert quality["calendar_coverage"]["fabricated_rows"] == 0
    evidence = manifest["source"]["approved_inventory_evidence"]
    assert evidence["status"] == "verified"
    assert evidence["authenticity_verified"]
    assert evidence["provenance"]["provenance_reference"] == "synthetic-test-inventory-v1"


def test_documented_approved_calendar_gap_is_warning_not_review(tmp_path: Path) -> None:
    missing_day = "2016-02-29"
    archive = complete_quarter_archive(tmp_path, omitted_dates={missing_day})
    config = write_approved_config(
        tmp_path,
        archive,
        approved_missing_dates=[missing_day],
    )
    output = tmp_path / "approved-gap"

    result = run_ingestion(
        config_path=config,
        quarter="2016Q1",
        archive=archive,
        output=output,
        source_url=SOURCE_URL,
    )

    coverage = read_json(output / "quality_report.json")["calendar_coverage"]
    validation = read_json(output / "validation_results.json")
    assert result["validation_status"] == "passed_with_warnings"
    assert coverage["approved_missing_source_dates"] == [missing_day]
    assert coverage["unexplained_missing_source_dates"] == []
    assert coverage["fabricated_rows"] == 0
    assert validation["central_integration_admission"]["eligible"]


def test_unexplained_calendar_gap_requires_review_without_fabrication(tmp_path: Path) -> None:
    missing_day = "2016-02-29"
    archive = complete_quarter_archive(tmp_path, omitted_dates={missing_day})
    config = write_approved_config(tmp_path, archive)
    output = tmp_path / "unexplained-gap"

    result = run_ingestion(
        config_path=config,
        quarter="2016Q1",
        archive=archive,
        output=output,
        source_url=SOURCE_URL,
    )

    coverage = read_json(output / "quality_report.json")["calendar_coverage"]
    validation = read_json(output / "validation_results.json")
    assert result["validation_status"] == "requires_review"
    assert coverage["unexplained_missing_source_dates"] == [missing_day]
    assert read_drive_data(output).num_rows == len(q1_2016_dates()) - 1
    assert "unexplained_calendar_coverage_gaps" in validation["review_reasons"]
    assert not validation["central_integration_admission"]["eligible"]


def test_unavailable_inventory_requires_review_even_with_complete_calendar(tmp_path: Path) -> None:
    archive = complete_quarter_archive(tmp_path)
    output = tmp_path / "inventory-unavailable"

    result = run_ingestion(
        config_path=write_test_config(tmp_path),
        quarter="2016Q1",
        archive=archive,
        output=output,
        source_url=SOURCE_URL,
    )

    validation = read_json(output / "validation_results.json")
    evidence = read_json(output / "source_manifest.json")["source"]["approved_inventory_evidence"]
    assert result["validation_status"] == "requires_review"
    assert evidence["quarter_entry_status"] == "unavailable"
    assert "approved_source_inventory_unavailable" in validation["review_reasons"]


def test_verified_inventory_without_independent_checksum_requires_review(tmp_path: Path) -> None:
    archive = complete_quarter_archive(tmp_path)
    config = write_approved_config(tmp_path, archive, include_checksum=False)
    output = tmp_path / "checksum-unavailable"

    result = run_ingestion(
        config_path=config,
        quarter="2016Q1",
        archive=archive,
        output=output,
        source_url=SOURCE_URL,
    )

    validation = read_json(output / "validation_results.json")
    assert result["validation_status"] == "requires_review"
    assert "independent_archive_checksum_unavailable" in validation["review_reasons"]


def test_verified_inventory_missing_quarter_entry_requires_review(tmp_path: Path) -> None:
    archive = complete_quarter_archive(tmp_path)
    config_path = write_test_config(tmp_path)
    config = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    config["source_inventory"] = {
        "status": "verified",
        "provenance_reference": "synthetic-test-inventory-v1",
        "verified_by": "automated-test-fixture",
        "verified_at": "2026-11-01T00:00:00Z",
        "quarters": {},
    }
    config_path.write_text(yaml.safe_dump(config, sort_keys=False), encoding="utf-8")
    output = tmp_path / "inventory-entry-missing"

    result = run_ingestion(
        config_path=config_path,
        quarter="2016Q1",
        archive=archive,
        output=output,
        source_url=SOURCE_URL,
    )

    validation = read_json(output / "validation_results.json")
    assert result["validation_status"] == "requires_review"
    assert "approved_source_inventory_entry_missing" in validation["review_reasons"]


def test_unverified_inventory_cannot_claim_approved_gap(tmp_path: Path) -> None:
    config_path = write_test_config(tmp_path)
    config = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    config["source_inventory"]["quarters"] = {
        "2016Q1": {"approved_missing_dates": ["2016-01-01"]}
    }
    config_path.write_text(yaml.safe_dump(config, sort_keys=False), encoding="utf-8")

    with pytest.raises(PipelineError, match="cannot contain purported approved"):
        load_configuration(config_path)


def test_approved_gap_outside_quarter_is_rejected(tmp_path: Path) -> None:
    archive = complete_quarter_archive(tmp_path)
    config_path = write_approved_config(tmp_path, archive)
    config = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    config["source_inventory"]["quarters"]["2016Q1"]["approved_missing_dates"] = [
        "2016-04-01"
    ]
    config_path.write_text(yaml.safe_dump(config, sort_keys=False), encoding="utf-8")

    with pytest.raises(PipelineError, match="approved gaps must fall inside the quarter"):
        load_configuration(config_path)


@pytest.mark.parametrize(
    "evidence_field",
    ["canonical_source_url", "original_filename", "expected_bytes", "expected_sha256"],
)
def test_trusted_inventory_mismatch_is_rejected(
    tmp_path: Path, evidence_field: str
) -> None:
    archive = complete_quarter_archive(tmp_path)
    config_path = write_approved_config(tmp_path, archive)
    config = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    entry = config["source_inventory"]["quarters"]["2016Q1"]
    replacements = {
        "canonical_source_url": "https://example.invalid/other.zip",
        "original_filename": "other.zip",
        "expected_bytes": archive.stat().st_size + 1,
        "expected_sha256": "0" * 64,
    }
    entry[evidence_field] = replacements[evidence_field]
    config_path.write_text(yaml.safe_dump(config, sort_keys=False), encoding="utf-8")
    output = tmp_path / f"mismatch-{evidence_field}"

    with pytest.raises(PipelineError, match=f"inventory mismatch.*{evidence_field.replace('expected_', 'archive_') if evidence_field.startswith('expected_') else evidence_field}"):
        run_ingestion(
            config_path=config_path,
            quarter="2016Q1",
            archive=archive,
            output=output,
            source_url=SOURCE_URL,
        )
    assert not output.exists()


def test_warning_only_evidence_produces_passed_with_warnings(tmp_path: Path) -> None:
    archive = complete_quarter_archive(tmp_path, smart_value="invalid")
    config = write_approved_config(tmp_path, archive)
    output = tmp_path / "warning-only"

    result = run_ingestion(
        config_path=config,
        quarter="2016Q1",
        archive=archive,
        output=output,
        source_url=SOURCE_URL,
    )

    validation = read_json(output / "validation_results.json")
    assert result["validation_status"] == "passed_with_warnings"
    assert validation["review_reasons"] == []
    assert validation["warning_reasons"] == ["quality_exception_evidence"]


def test_error_evidence_requires_review(tmp_path: Path) -> None:
    archive = complete_quarter_archive(
        tmp_path,
        extra_rows=[["2016-01-01", "BAD", "Model", "Vendor", "1000", "2", "1"]],
    )
    config = write_approved_config(tmp_path, archive)
    output = tmp_path / "error-evidence"

    result = run_ingestion(
        config_path=config,
        quarter="2016Q1",
        archive=archive,
        output=output,
        source_url=SOURCE_URL,
    )

    validation = read_json(output / "validation_results.json")
    assert result["validation_status"] == "requires_review"
    assert "error_severity_exception_evidence" in validation["review_reasons"]
    assert read_json(output / "quality_report.json")["counts"]["issue_severity_counts"]["error"] == 1


def test_conflicting_duplicate_requires_review(tmp_path: Path) -> None:
    archive = complete_quarter_archive(
        tmp_path,
        extra_rows=[
            ["2016-01-01", "SN-2016-01-01", "Model", "Vendor", "1000", "0", "2"]
        ],
    )
    config = write_approved_config(tmp_path, archive)
    output = tmp_path / "conflicting-duplicate"

    result = run_ingestion(
        config_path=config,
        quarter="2016Q1",
        archive=archive,
        output=output,
        source_url=SOURCE_URL,
    )

    validation = read_json(output / "validation_results.json")
    assert result["validation_status"] == "requires_review"
    assert "conflicting_duplicate_records" in validation["review_reasons"]


@pytest.mark.parametrize(
    ("section", "field", "value"),
    [
        ("processing", "batch_rows", 0),
        ("processing", "canonical_batch_bytes", 0),
        ("processing", "exception_batch_rows", 0),
        ("processing", "exception_batch_bytes", 0),
        ("processing", "parquet_row_group_rows", 0),
        ("processing", "maximum_archive_members", None),
        ("processing", "maximum_member_uncompressed_bytes", 0),
        ("processing", "maximum_archive_uncompressed_bytes", 0),
        ("processing", "minimum_free_space_bytes", 0),
        ("processing", "worker_threads", 0),
        ("processing", "maximum_compression_ratio", 0),
        ("processing", "working_space_multiplier", 1.0),
        ("processing", "encoding", "utf-16"),
        ("processing", "encoding", "not-a-codec"),
        ("processing", "duckdb_memory_limit", "1GB'; DROP TABLE records;--"),
        ("parquet", "compression", "not-a-codec"),
        ("parquet", "compression_level", -1),
        ("parquet", "compression_level", 999999),
        ("parquet", "write_statistics", 1),
        ("parquet", "dictionary_encode_strings", "true"),
        ("recovery", "staging_suffix", "../unsafe"),
        ("recovery", "completed_output_policy", "overwrite"),
        ("schema", "missing_value_contract_version", "0.0.0"),
        ("schema", "missing_tokens", ["", "0"]),
        ("schema", "missing_tokens", ""),
        ("schema", "allowed_start_date", "2015-01-01"),
        ("schema", "allowed_end_date", "2026-12-31"),
        ("validation", "require_dates_within_quarter", False),
        ("validation", "require_complete_row_accounting", False),
        ("validation", "require_nonempty_output", False),
        ("validation", "require_checkpoint_integrity", False),
        ("validation", "require_source_inventory_review", False),
    ],
)
def test_unsafe_configuration_values_are_rejected(
    tmp_path: Path, section: str, field: str, value
) -> None:
    config_path = write_test_config(tmp_path)
    config = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    config[section][field] = value
    config_path.write_text(yaml.safe_dump(config, sort_keys=False), encoding="utf-8")

    with pytest.raises(PipelineError, match=field):
        load_configuration(config_path)


@pytest.mark.parametrize(
    "section", ["schema", "processing", "parquet", "validation", "recovery"]
)
def test_configuration_sections_must_be_mappings(tmp_path: Path, section: str) -> None:
    config_path = write_test_config(tmp_path)
    config = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    config[section] = []
    config_path.write_text(yaml.safe_dump(config, sort_keys=False), encoding="utf-8")

    with pytest.raises(PipelineError, match=section):
        load_configuration(config_path)


@pytest.mark.parametrize(
    ("limit_field", "limit_value", "message"),
    [
        ("maximum_archive_members", 1, "exceeding the configured limit"),
        ("maximum_member_uncompressed_bytes", 1, "Member exceeds configured size limit"),
        ("maximum_archive_uncompressed_bytes", 1, "total expansion limit"),
    ],
)
def test_zip_safety_limits_are_enforced(
    tmp_path: Path, limit_field: str, limit_value: int, message: str
) -> None:
    archive = normal_archive(tmp_path)
    config_path = write_test_config(tmp_path)
    config = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    config["processing"][limit_field] = limit_value
    config_path.write_text(yaml.safe_dump(config, sort_keys=False), encoding="utf-8")

    with pytest.raises(PipelineError, match=message):
        run_ingestion(
            config_path=config_path,
            quarter="2016Q1",
            archive=archive,
            output=tmp_path / f"limit-{limit_field}",
            source_url=SOURCE_URL,
        )


def test_insufficient_disk_space_fails_before_staging(tmp_path: Path, monkeypatch) -> None:
    archive = normal_archive(tmp_path)
    output = tmp_path / "low-disk-output"
    monkeypatch.setattr(
        "drivepriority.data.ingest.shutil.disk_usage",
        lambda _path: SimpleNamespace(free=0),
    )

    with pytest.raises(PipelineError, match="Insufficient free disk space"):
        run_ingestion(
            config_path=write_test_config(tmp_path),
            quarter="2016Q1",
            archive=archive,
            output=output,
            source_url=SOURCE_URL,
        )
    assert not output.exists()
    assert not output.with_name(output.name + ".staging").exists()


def run_identity_collision_case(
    tmp_path: Path,
    *,
    mode: str,
    changed_field: str = "model",
) -> tuple[Path, dict, pa.Table]:
    header = [*BASE_HEADER, "smart_5_raw"]
    first = ["2016-01-01", "SERIAL-X", "Model A", "Vendor", "1000", "0", "1"]
    second = ["2016-02-01", "SERIAL-X", "Model A", "Vendor", "1000", "0", "2"]
    second[BASE_HEADER.index(changed_field)] = {
        "model": "Model B",
        "manufacturer": "Other Vendor",
        "capacity_bytes": "2000",
    }[changed_field]
    if mode == "cross_member":
        members = {"a.csv": [header, first], "b.csv": [header, second]}
        batch_rows = 10
    else:
        members = {"days.csv": [header, first, second]}
        batch_rows = 1 if mode == "cross_batch" else 10
    archive = write_archive(tmp_path / f"identity-{mode}-{changed_field}.zip", members)
    output = tmp_path / f"identity-{mode}-{changed_field}"
    result = run_ingestion(
        config_path=write_test_config(tmp_path, batch_rows=batch_rows),
        quarter="2016Q1",
        archive=archive,
        output=output,
        source_url=SOURCE_URL,
    )
    return output, result, read_drive_data(output)


@pytest.mark.parametrize("mode", ["same_batch", "cross_batch", "cross_member"])
def test_cross_date_serial_identity_collision_across_processing_boundaries(
    tmp_path: Path, mode: str
) -> None:
    output, result, data = run_identity_collision_case(tmp_path, mode=mode)
    exceptions = pq.read_table(output / "exceptions.parquet")
    collision_indexes = [
        index
        for index, code in enumerate(exceptions.column("exception_code").to_pylist())
        if code == "serial_identity_collision"
    ]

    assert data.num_rows == 2
    assert data.column("serial_number").to_pylist() == ["SERIAL-X", "SERIAL-X"]
    assert len(collision_indexes) == 1
    index = collision_indexes[0]
    assert exceptions.column("first_seen_date")[index].as_py() == "2016-01-01"
    assert exceptions.column("last_seen_date")[index].as_py() == "2016-02-01"
    assert exceptions.column("first_seen_source_file")[index].as_py() in {
        "a.csv",
        "days.csv",
    }
    assert result["validation_status"] == "requires_review"
    assert "serial_identity_collisions" in read_json(
        output / "validation_results.json"
    )["review_reasons"]


@pytest.mark.parametrize("changed_field", ["model", "manufacturer", "capacity_bytes"])
def test_serial_identity_signature_audits_each_hardware_field(
    tmp_path: Path, changed_field: str
) -> None:
    output, _result, data = run_identity_collision_case(
        tmp_path,
        mode="same_batch",
        changed_field=changed_field,
    )
    codes = pq.read_table(output / "exceptions.parquet").column(
        "exception_code"
    ).to_pylist()

    assert data.num_rows == 2
    assert codes == ["serial_identity_collision"]


def test_blank_and_absent_manufacturer_do_not_change_identity_and_serial_is_preserved(
    tmp_path: Path,
) -> None:
    serial = "  SERIAL-ORIGINAL  "
    with_manufacturer = [*BASE_HEADER, "smart_5_raw"]
    without_manufacturer = [
        "date",
        "serial_number",
        "model",
        "capacity_bytes",
        "failure",
        "smart_5_raw",
    ]
    archive = write_archive(
        tmp_path / "manufacturer-missing.zip",
        {
            "a.csv": [
                with_manufacturer,
                ["2016-01-01", serial, "Model", "", "1000", "0", "1"],
            ],
            "b.csv": [
                without_manufacturer,
                ["2016-02-01", serial, "Model", "1000", "0", "2"],
            ],
        },
    )
    output = tmp_path / "manufacturer-missing-output"

    run_ingestion(
        config_path=write_test_config(tmp_path),
        quarter="2016Q1",
        archive=archive,
        output=output,
        source_url=SOURCE_URL,
    )

    data = read_drive_data(output)
    assert data.column("serial_number").to_pylist() == [serial, serial]
    assert data.column("manufacturer").to_pylist() == [None, None]
    assert not (output / "exceptions.parquet").exists()


def test_negative_smart_values_are_exact_traceable_and_aggregated(tmp_path: Path) -> None:
    minimum = str(-(10**38 - 1))
    archive = write_archive(
        tmp_path / "negative-smart.zip",
        {
            "a.csv": [
                [*BASE_HEADER, "smart_5_raw"],
                ["2016-01-01", "SN-1", "Model A", "Vendor", "1000", "0", "-1"],
                ["2016-01-02", "SN-2", "Model A", "Vendor", "1000", "0", minimum],
                ["2016-01-03", "SN-3", "Model B", "Vendor", "1000", "0", "-999"],
                ["2016-01-04", "SN-4", "Model B", "Vendor", "1000", "0", "0"],
                ["2016-01-05", "SN-5", "Model B", "Vendor", "1000", "0", ""],
                ["2016-01-06", "SN-6", "Model B", "Vendor", "1000", "0", "invalid"],
            ],
            "b.csv": [
                BASE_HEADER,
                ["2016-01-07", "SN-7", "Model C", "Vendor", "1000", "0"],
            ],
        },
    )
    output = tmp_path / "negative-smart-output"

    run_ingestion(
        config_path=write_test_config(tmp_path),
        quarter="2016Q1",
        archive=archive,
        output=output,
        source_url=SOURCE_URL,
    )

    values = read_drive_data(output).column("smart_5_raw").to_pylist()
    assert values[:3] == [-1, -(10**38 - 1), -999]
    assert values[3:] == [0, None, None, None]
    exceptions = pq.read_table(output / "exceptions.parquet")
    codes = exceptions.column("exception_code").to_pylist()
    assert codes.count("suspicious_negative_smart_value") == 3
    assert set(
        value
        for code, value in zip(codes, exceptions.column("raw_value").to_pylist())
        if code == "suspicious_negative_smart_value"
    ) == {"-1", minimum, "-999"}
    quality = read_json(output / "quality_report.json")
    states = quality["smart_source_states"]["smart_5_raw"]
    assert states == {
        "absent_column_rows": 1,
        "present_blank_rows": 1,
        "invalid_token_rows": 1,
        "genuine_zero_rows": 1,
        "suspicious_negative_rows": 3,
        "canonical_null_rows": 3,
    }
    negative = quality["negative_smart_values"]
    assert negative["by_quarter"]["2016Q1"]["smart_5_raw"] == 3
    assert negative["by_member"]["a.csv"]["smart_5_raw"] == 3
    assert negative["by_model"]["Model A"]["smart_5_raw"] == 2
    assert negative["by_model"]["Model B"]["smart_5_raw"] == 1


def test_failure_flags_reconcile_source_retained_and_quarantined_rows(
    tmp_path: Path,
) -> None:
    archive = write_archive(
        tmp_path / "failure-accounting.zip",
        {
            "days.csv": [
                BASE_HEADER,
                ["2016-01-01", "SN-1", "Model", "Vendor", "1000", "1"],
                ["2016-01-01", "SN-2", "Model", "Vendor", "1000", "0"],
                ["2016-01-02", "", "Model", "Vendor", "1000", "1"],
                ["2016-01-02", "", "Model", "Vendor", "1000", "0"],
                ["2016-01-03", "SN-3", "Model", "Vendor", "1000", "1"],
            ]
        },
    )
    output = tmp_path / "failure-accounting-output"

    run_ingestion(
        config_path=write_test_config(tmp_path),
        quarter="2016Q1",
        archive=archive,
        output=output,
        source_url=SOURCE_URL,
    )

    assert read_drive_data(output).column("failure").to_pylist() == [1, 0, 1]
    failure = read_json(output / "quality_report.json")["failure_flags"]
    assert failure["valid_source_counts"] == {"0": 2, "1": 3}
    assert failure["retained_canonical_counts"] == {"0": 1, "1": 2}
    assert failure["quarantine_evidence_counts"] == {"0": 1, "1": 1}
    assert failure["daily_valid_source_counts"]["2016-01-02"] == {"0": 1, "1": 1}
    assert failure["daily_retained_canonical_counts"]["2016-01-01"] == {
        "0": 1,
        "1": 1,
    }
    assert failure["daily_quarantine_evidence_counts"]["2016-01-02"] == {
        "0": 1,
        "1": 1,
    }
    assert failure["source_to_output_reconciliation"]["passed"]
    assert not failure["first_failure_event_selection_performed"]
    reconciliation_check = next(
        check
        for check in read_json(output / "validation_results.json")["checks"]
        if check["name"] == "failure_flag_source_to_output_reconciliation"
    )
    assert reconciliation_check["passed"] is True
    assert reconciliation_check["total_counts_passed"] is True
    assert reconciliation_check["daily_counts_passed"] is True
    assert reconciliation_check["independent_parquet_and_evidence_counts_passed"] is True
    assert reconciliation_check["readback"]["retained_counts"] == {"0": 1, "1": 2}
    assert reconciliation_check["readback"]["quarantined_counts"] == {"0": 1, "1": 1}


def test_unexpected_smart_extension_states_are_audited_without_numeric_interpretation(
    tmp_path: Path,
) -> None:
    archive = write_archive(
        tmp_path / "smart-extension.zip",
        {
            "a.csv": [
                [*BASE_HEADER, "smart_vendor_blob"],
                ["2016-01-01", "SN-1", "Model A", "Vendor", "1000", "0", ""],
                ["2016-01-02", "SN-2", "Model A", "Vendor", "1000", "0", "0"],
                ["2016-01-03", "SN-3", "Model C", "Vendor", "1000", "0", "opaque"],
            ],
            "b.csv": [
                BASE_HEADER,
                ["2016-01-04", "SN-4", "Model B", "Vendor", "1000", "0"],
            ],
        },
    )
    output = tmp_path / "smart-extension-output"

    run_ingestion(
        config_path=write_test_config(tmp_path),
        quarter="2016Q1",
        archive=archive,
        output=output,
        source_url=SOURCE_URL,
    )

    data = read_drive_data(output)
    assert data.schema.field("smart_vendor_blob").type == pa.string()
    assert data.column("smart_vendor_blob").to_pylist() == [None, "0", "opaque", None]
    quality = read_json(output / "quality_report.json")
    extension = quality["unexpected_smart_extension_states"]
    assert extension["by_field"]["smart_vendor_blob"] == {
        "absent_column_rows": 1,
        "present_blank_rows": 1,
        "literal_zero_rows": 1,
        "other_nonempty_rows": 1,
        "canonical_null_rows": 2,
    }
    assert extension["by_member"]["a.csv"]["smart_vendor_blob"] == {
        "blank": 1,
        "value": 1,
        "zero": 1,
    }
    assert extension["by_member"]["b.csv"]["smart_vendor_blob"] == {"absent": 1}
    assert extension["by_model"]["Model B"]["smart_vendor_blob"] == {"absent": 1}
    assert extension["by_quarter"]["2016Q1"]["smart_vendor_blob"] == {
        "absent": 1,
        "blank": 1,
        "value": 1,
        "zero": 1,
    }
    contract = read_json(output / "schema_report.json")["unexpected_smart_contract"]
    assert contract["canonical_type"] == "string"
    assert "not treated as a numeric" in contract["interpretation"]


def _wide_invalid_archive(tmp_path: Path, field_count: int = 20) -> Path:
    smart_fields = [f"smart_{index}_raw" for index in range(1, field_count + 1)]
    return write_archive(
        tmp_path / "wide-invalid.zip",
        {
            "wide.csv": [
                [*BASE_HEADER, *smart_fields],
                [
                    "2016-01-01",
                    "SN-WIDE",
                    "Model Wide",
                    "Vendor",
                    "1000",
                    "0",
                    *[f"invalid-{index}" for index in range(field_count)],
                ],
            ]
        },
    )


def test_wide_invalid_row_stores_raw_evidence_once_and_links_every_field(
    tmp_path: Path,
) -> None:
    output = tmp_path / "wide-output"
    run_ingestion(
        config_path=write_test_config(tmp_path),
        quarter="2016Q1",
        archive=_wide_invalid_archive(tmp_path),
        output=output,
        source_url=SOURCE_URL,
    )

    exceptions = pq.read_table(output / "exceptions.parquet")
    assert exceptions.num_rows == 20
    evidence_ids = exceptions.column("row_evidence_id").to_pylist()
    assert len(set(evidence_ids)) == 1
    assert evidence_ids[0] is not None and len(evidence_ids[0]) == 64
    raw_rows = exceptions.column("raw_record_json").to_pylist()
    assert sum(value is not None for value in raw_rows) == 1
    assert set(exceptions.column("column_name").to_pylist()) == {
        f"smart_{index}_raw" for index in range(1, 21)
    }
    assert all(exceptions.column("raw_record_sha256").to_pylist())


def test_exception_row_limit_flushes_independently_from_source_batches(tmp_path: Path) -> None:
    archive = _wide_invalid_archive(tmp_path, field_count=10)
    output = tmp_path / "exception-row-batches"
    with pytest.raises(IngestionInterrupted):
        run_ingestion(
            config_path=write_test_config(
                tmp_path,
                batch_rows=100,
                exception_batch_rows=3,
                exception_batch_bytes=10_000_000,
            ),
            quarter="2016Q1",
            archive=archive,
            output=output,
            source_url=SOURCE_URL,
            _interrupt_before_publish=True,
        )

    parts = sorted((tmp_path / "exception-row-batches.staging" / "exception_parts").glob("*.parquet"))
    assert len(parts) == 4
    assert [pq.ParquetFile(path).metadata.num_rows for path in parts] == [3, 3, 3, 1]


def test_exception_byte_limit_flushes_without_truncating_evidence(tmp_path: Path) -> None:
    archive = _wide_invalid_archive(tmp_path, field_count=8)
    output = tmp_path / "exception-byte-batches"
    with pytest.raises(IngestionInterrupted):
        run_ingestion(
            config_path=write_test_config(
                tmp_path,
                batch_rows=100,
                exception_batch_rows=1000,
                exception_batch_bytes=700,
            ),
            quarter="2016Q1",
            archive=archive,
            output=output,
            source_url=SOURCE_URL,
            _interrupt_before_publish=True,
        )

    staging = tmp_path / "exception-byte-batches.staging"
    parts = sorted((staging / "exception_parts").glob("*.parquet"))
    assert len(parts) > 1
    compacted = pq.read_table(staging / "exceptions.parquet")
    assert compacted.num_rows == 8
    assert set(compacted.column("raw_value").to_pylist()) == {
        f"invalid-{index}" for index in range(8)
    }


def test_exception_compaction_streams_parquet_batches(tmp_path: Path, monkeypatch) -> None:
    archive = _wide_invalid_archive(tmp_path, field_count=8)
    output = tmp_path / "streamed-exceptions"

    def forbid_whole_table_read(*_args, **_kwargs):
        raise AssertionError("whole-part read_table must not be used during compaction")

    with monkeypatch.context() as context:
        context.setattr("drivepriority.data.ingest.pq.read_table", forbid_whole_table_read)
        run_ingestion(
            config_path=write_test_config(
                tmp_path, exception_batch_rows=2, exception_batch_bytes=10_000_000
            ),
            quarter="2016Q1",
            archive=archive,
            output=output,
            source_url=SOURCE_URL,
        )

    assert pq.read_table(output / "exceptions.parquet").num_rows == 8


@pytest.mark.parametrize(
    ("archive_name", "payload", "exception_type"),
    [
        (
            "malformed-quoting.zip",
            (",".join(BASE_HEADER) + "\n").encode("utf-8")
            + b'2016-01-01,SN-1,"unterminated,Vendor,1000,0\n',
            "Error",
        ),
        (
            "invalid-utf8.zip",
            (",".join(BASE_HEADER) + "\n").encode("utf-8")
            + b"2016-01-01,SN-1,Model,Vendor,1000,0,\xff\n",
            "UnicodeDecodeError",
        ),
    ],
)
def test_parser_and_decoding_failures_leave_atomic_restart_only_evidence(
    tmp_path: Path, archive_name: str, payload: bytes, exception_type: str
) -> None:
    archive = write_raw_archive(tmp_path / archive_name, {"bad.csv": payload})
    output = tmp_path / f"{archive_name}-output"

    with pytest.raises(PipelineError, match="processing_failure.json"):
        run_ingestion(
            config_path=write_test_config(tmp_path),
            quarter="2016Q1",
            archive=archive,
            output=output,
            source_url=SOURCE_URL,
        )

    staging = output.with_name(output.name + ".staging")
    report = read_json(staging / "processing_failure.json")
    assert not output.exists()
    assert report["archive_sha256"] == hashlib.sha256(archive.read_bytes()).hexdigest()
    assert report["source_member"]["filename"] == "bad.csv"
    assert report["exception"]["type"] == exception_type
    assert report["report_version"] == "1.1.0"
    assert report["position"]["last_successful_source_record_number"] is None
    assert report["position"]["attempted_source_record_number"] == 1
    assert report["position"]["attempted_record_number_is_exact"] is True
    assert report["position"]["line_number_is_approximate"] is True
    assert report["position"]["byte_position_is_approximate"] is True
    assert report["recovery"]["restart_required"] is True
    assert report["recovery"]["resume_allowed"] is False
    assert report["identity"] == read_json(staging / "run_state.json")["identity"]
    assert read_json(staging / "run_state.json")["status"] == "failed"
    assert not list(staging.glob("*.tmp"))


def test_parser_failure_distinguishes_attempted_from_last_successful_record(
    tmp_path: Path,
) -> None:
    payload = (
        (",".join(BASE_HEADER) + "\n").encode("utf-8")
        + b"2016-01-01,SN-1,Model,Vendor,1000,0\n"
        + b'2016-01-02,SN-2,"unterminated,Vendor,1000,0\n'
    )
    archive = write_raw_archive(tmp_path / "second-record-parser-error.zip", {"bad.csv": payload})
    output = tmp_path / "second-record-parser-error-output"

    with pytest.raises(PipelineError, match="processing_failure.json"):
        run_ingestion(
            config_path=write_test_config(tmp_path),
            quarter="2016Q1",
            archive=archive,
            output=output,
            source_url=SOURCE_URL,
        )

    position = read_json(
        output.with_name(output.name + ".staging") / "processing_failure.json"
    )["position"]
    assert position["last_successful_source_record_number"] == 1
    assert position["attempted_source_record_number"] == 2
    assert position["rows_seen_in_run"] == 1
    assert "source_record_number" not in position


def test_failed_parser_run_refuses_resume_and_restart_is_scoped(tmp_path: Path) -> None:
    payload = (",".join(BASE_HEADER) + "\n").encode("utf-8") + b'"unterminated\n'
    archive = write_raw_archive(tmp_path / "restart-bad.zip", {"bad.csv": payload})
    config = write_test_config(tmp_path)
    output = tmp_path / "restart-output"
    sibling = tmp_path / "unrelated.txt"
    sibling.write_text("preserve", encoding="utf-8")

    with pytest.raises(PipelineError, match="processing_failure.json"):
        run_ingestion(
            config_path=config,
            quarter="2016Q1",
            archive=archive,
            output=output,
            source_url=SOURCE_URL,
        )
    staging = tmp_path / "restart-output.staging"
    marker = staging / "old-run-marker.txt"
    marker.write_text("discard on explicit restart", encoding="utf-8")

    with pytest.raises(PipelineError, match="use --restart"):
        run_ingestion(
            config_path=config,
            quarter="2016Q1",
            archive=archive,
            output=output,
            source_url=SOURCE_URL,
            resume=True,
        )
    assert marker.exists()

    with pytest.raises(PipelineError, match="processing_failure.json"):
        run_ingestion(
            config_path=config,
            quarter="2016Q1",
            archive=archive,
            output=output,
            source_url=SOURCE_URL,
            restart=True,
        )
    assert not marker.exists()
    assert sibling.read_text(encoding="utf-8") == "preserve"
    assert (staging / "processing_failure.json").is_file()


def test_completed_exception_outputs_resume_with_revised_evidence_schema(tmp_path: Path) -> None:
    archive = _wide_invalid_archive(tmp_path, field_count=6)
    config = write_test_config(tmp_path, exception_batch_rows=2)
    output = tmp_path / "exception-resume-output"
    with pytest.raises(IngestionInterrupted):
        run_ingestion(
            config_path=config,
            quarter="2016Q1",
            archive=archive,
            output=output,
            source_url=SOURCE_URL,
            _interrupt_before_publish=True,
        )

    result = run_ingestion(
        config_path=config,
        quarter="2016Q1",
        archive=archive,
        output=output,
        source_url=SOURCE_URL,
        resume=True,
    )
    exceptions = pq.read_table(output / "exceptions.parquet")
    assert result["status"] == "complete"
    assert exceptions.num_rows == 6
    assert sum(value is not None for value in exceptions.column("raw_record_json").to_pylist()) == 1
    assert len(set(exceptions.column("row_evidence_id").to_pylist())) == 1


SYNTHETIC_HISTORICAL_LAYOUTS = [
    pytest.param(
        {
            "quarter": "2016Q1",
            "archive_name": "data_Q1_2016.zip",
            "members": [
                (
                    "data_Q1_2016/2016-01-01.csv",
                    [
                        [
                            "Date",
                            "Serial Number",
                            "Model",
                            "Capacity Bytes",
                            "Failure",
                            "SMART 1 Normalized",
                            "SMART 1 Raw",
                        ],
                        ["2016-01-01", "SYN-2016", "Legacy", "1000", "0", "100", "1"],
                    ],
                    False,
                )
            ],
            "expected": {
                "manufacturer": [None],
                "smart_1_normalized": [Decimal(100)],
                "smart_1_raw": [Decimal(1)],
            },
            "exception_codes": [],
        },
        id="synthetic-2016-layout",
    ),
    pytest.param(
        {
            "quarter": "2017Q4",
            "archive_name": "2017-Q4-drive-stats.zip",
            "members": [
                (
                    "2017-12-31.csv",
                    [
                        [*BASE_HEADER[:3], "capacity_bytes", "failure", "smart_5_raw", "smart_9_raw"],
                        ["2017-12-31", "SYN-2017", "Legacy", "2000", "0", "", "0007"],
                    ],
                    True,
                )
            ],
            "expected": {
                "manufacturer": [None],
                "smart_5_raw": [None],
                "smart_9_raw": [Decimal(7)],
            },
            "exception_codes": [],
        },
        id="synthetic-2017-bom-layout",
    ),
    pytest.param(
        {
            "quarter": "2018Q1",
            "archive_name": "2018-transition.zip",
            "members": [
                (
                    "transition/2018-03-31.csv",
                    [
                        [*BASE_HEADER[:3], "capacity_bytes", "failure", "smart_188_raw", "smart_190_normalized"],
                        ["2018-03-31", "SYN-2018", "Transition", "3000", "1", "+7", "0"],
                    ],
                    False,
                )
            ],
            "expected": {
                "manufacturer": [None],
                "smart_188_raw": [Decimal(7)],
                "smart_190_normalized": [Decimal(0)],
            },
            "exception_codes": [],
        },
        id="synthetic-2018-transition-layout",
    ),
    pytest.param(
        {
            "quarter": "2021Q3",
            "archive_name": "middle-period-2021Q3.zip",
            "members": [
                (
                    "data_Q3_2021/day_2021-07-01.csv",
                    [
                        [*BASE_HEADER, "smart_197_raw", "smart_vendor_blob"],
                        ["2021-07-01", "SYN-2021", "Middle", "Vendor", "4000", "0", "N/A", "opaque"],
                    ],
                    False,
                )
            ],
            "expected": {
                "manufacturer": ["Vendor"],
                "smart_197_raw": [None],
                "smart_vendor_blob": ["opaque"],
            },
            "exception_codes": ["invalid_smart_value"],
        },
        id="synthetic-middle-period-layout",
    ),
    pytest.param(
        {
            "quarter": "2025Q4",
            "archive_name": "2025-Q4-synthetic-layout.zip",
            "members": [
                (
                    "Q4_2025/2025-10-01.csv",
                    [
                        [*BASE_HEADER, "smart_5_raw"],
                        ["2025-10-01", "SYN-2025-A", "Recent", "Vendor", "5000", "0", "1"],
                    ],
                    False,
                ),
                (
                    "Q4_2025/2025-12-31.csv",
                    [
                        [*BASE_HEADER, "smart_198_raw"],
                        ["2025-12-31", "SYN-2025-B", "Recent", "Vendor", "5000", "0", "2"],
                    ],
                    False,
                ),
            ],
            "expected": {
                "smart_5_raw": [Decimal(1), None],
                "smart_198_raw": [None, Decimal(2)],
            },
            "absent_rows": {"smart_5_raw": 1, "smart_198_raw": 1},
            "exception_codes": [],
        },
        id="synthetic-2025-schema-drift-layout",
    ),
    pytest.param(
        {
            "quarter": "2026Q2",
            "archive_name": "2026-Q2-through-june.zip",
            "members": [
                (
                    "2026/Q2/2026-06-30.csv",
                    [
                        [
                            "DATE",
                            "SERIAL NUMBER",
                            "MODEL",
                            "MANUFACTURER",
                            "CAPACITY BYTES",
                            "FAILURE",
                            "SMART 250 RAW",
                        ],
                        ["2026-06-30", "SYN-2026", "Current", "Vendor", "6000", "0", "-1"],
                    ],
                    False,
                )
            ],
            "expected": {"smart_250_raw": [Decimal(-1)]},
            "exception_codes": ["suspicious_negative_smart_value"],
        },
        id="synthetic-2026-june-boundary-layout",
    ),
]


@pytest.mark.parametrize("case", SYNTHETIC_HISTORICAL_LAYOUTS)
def test_synthetic_historical_layout_contracts(tmp_path: Path, case: dict[str, object]) -> None:
    encoded_members: dict[str, bytes] = {}
    for member_name, rows, with_bom in case["members"]:
        text = csv_text(rows)
        encoded_members[member_name] = (("\ufeff" if with_bom else "") + text).encode("utf-8")
    archive = write_raw_archive(tmp_path / case["archive_name"], encoded_members)
    output = tmp_path / f"historical-{case['quarter']}"

    run_ingestion(
        config_path=write_test_config(tmp_path),
        quarter=case["quarter"],
        archive=archive,
        output=output,
        source_url=SOURCE_URL,
    )

    data = read_drive_data(output)
    for field_name, expected_values in case["expected"].items():
        assert data.column(field_name).to_pylist() == expected_values
    assert data.column("source_file").to_pylist() == sorted(encoded_members)

    schema_report = read_json(output / "schema_report.json")
    compatibility = schema_report["historical_compatibility_evidence"]
    assert compatibility["status"] == "contract_and_synthetic_fixture_coverage_only"
    assert compatibility["synthetic_fixtures_are_authentic_records"] is False
    assert compatibility["outstanding_evidence"]
    assert [member["filename"] for member in schema_report["members"]] == sorted(encoded_members)

    quality = read_json(output / "quality_report.json")
    for field_name, absent_rows in case.get("absent_rows", {}).items():
        assert quality["smart_source_states"][field_name]["absent_column_rows"] == absent_rows
    exception_path = output / "exceptions.parquet"
    if case["exception_codes"]:
        assert exception_path.is_file()
        assert pq.read_table(exception_path).column("exception_code").to_pylist() == case[
            "exception_codes"
        ]
    else:
        assert not exception_path.exists()


@pytest.mark.parametrize(
    ("quarter", "members", "expected_numeric", "expected_extensions"),
    [
        pytest.param(
            "2016Q1",
            {
                "synthetic-a.csv": [
                    [*BASE_HEADER, "smart_5_raw", "smart_vendor_blob"],
                    ["2016-01-01", "SYN-A", "Model", "Vendor", "1000", "0", "1", "opaque"],
                ]
            },
            ["smart_5_raw"],
            ["smart_vendor_blob"],
            id="synthetic-quarter-schema-a",
        ),
        pytest.param(
            "2025Q4",
            {
                "synthetic-b1.csv": [
                    [*BASE_HEADER, "smart_9_normalized"],
                    ["2025-10-01", "SYN-B1", "Model", "Vendor", "2000", "0", "100"],
                ],
                "synthetic-b2.csv": [
                    [*BASE_HEADER, "smart_198_raw"],
                    ["2025-12-31", "SYN-B2", "Model", "Vendor", "2000", "0", "2"],
                ],
            },
            ["smart_9_normalized", "smart_198_raw"],
            [],
            id="synthetic-quarter-schema-b",
        ),
    ],
)
def test_quarterly_schema_contract_uses_actual_published_parquet_schema(
    tmp_path: Path,
    quarter: str,
    members: dict[str, list[list[str]]],
    expected_numeric: list[str],
    expected_extensions: list[str],
) -> None:
    archive = write_archive(tmp_path / f"{quarter}-synthetic-contract.zip", members)
    output = tmp_path / f"{quarter}-schema-contract-output"
    run_ingestion(
        config_path=write_test_config(tmp_path),
        quarter=quarter,
        archive=archive,
        output=output,
        source_url=SOURCE_URL,
    )

    data_files = sorted((output / "data").rglob("*.parquet"))
    actual_schema = pq.ParquetFile(data_files[0]).schema_arrow
    actual_fingerprint = hashlib.sha256(actual_schema.serialize().to_pybytes()).hexdigest()
    assert all(
        pq.ParquetFile(path).schema_arrow.equals(actual_schema, check_metadata=True)
        for path in data_files
    )
    assert actual_schema.metadata[b"drivepriority.schema_version"] == b"1.1.0"
    assert actual_schema.metadata[b"drivepriority.schema_contract_version"] == b"1.0.0"

    schema_report = read_json(output / "schema_report.json")
    validation = read_json(output / "validation_results.json")
    manifest = read_json(output / "source_manifest.json")
    contract = schema_report["schema_contract"]
    assert contract == validation["schema_contract"] == manifest["schema_contract"]
    assert contract["canonical_schema_fingerprint"] == actual_fingerprint
    assert contract["schema_contract_version"] == "1.0.0"
    assert contract["fingerprint_method"] == "sha256_arrow_schema_ipc_v1"
    assert contract["recognized_numeric_smart_fields"] == expected_numeric
    assert contract["unexpected_smart_extension_fields"] == expected_extensions
    assert [field["name"] for field in contract["canonical_columns"]] == actual_schema.names
    decimal_fields = {
        field["name"]: (field["decimal_precision"], field["decimal_scale"])
        for field in contract["canonical_columns"]
        if "decimal_precision" in field
    }
    assert decimal_fields == {field: (38, 0) for field in expected_numeric}
    assert contract["all_data_file_schemas_consistent"] is True
    assert contract["matches_planned_quarterly_union"] is True
    contract_check = next(
        check
        for check in validation["checks"]
        if check["name"] == "published_quarterly_schema_contract"
    )
    assert contract_check["passed"] is True
    drift = schema_report["source_column_availability_and_drift"]
    assert drift["schema_drift_observed"] is (len(members) > 1)
    assert len(drift["members"]) == len(members)


def test_quarterly_schema_contract_survives_checkpoint_resume(tmp_path: Path) -> None:
    archive = write_archive(
        tmp_path / "synthetic-schema-resume.zip",
        {
            "a.csv": [
                [*BASE_HEADER, "smart_5_raw"],
                ["2016-01-01", "SYN-1", "Model", "Vendor", "1000", "0", "1"],
            ],
            "b.csv": [
                [*BASE_HEADER, "smart_198_raw"],
                ["2016-01-02", "SYN-2", "Model", "Vendor", "1000", "0", "2"],
            ],
        },
    )
    config = write_test_config(tmp_path, batch_rows=1)
    output = tmp_path / "schema-resume-output"
    with pytest.raises(IngestionInterrupted):
        run_ingestion(
            config_path=config,
            quarter="2016Q1",
            archive=archive,
            output=output,
            source_url=SOURCE_URL,
            _interrupt_after_rows=2,
        )

    staging = tmp_path / "schema-resume-output.staging"
    checkpoint = checkpoint_files(staging)
    for artifact in checkpoint["artifacts"]:
        path = staging / artifact["path"]
        actual = hashlib.sha256(
            pq.ParquetFile(path).schema_arrow.serialize().to_pybytes()
        ).hexdigest()
        assert artifact["schema_fingerprint"] == actual

    result = run_ingestion(
        config_path=config,
        quarter="2016Q1",
        archive=archive,
        output=output,
        source_url=SOURCE_URL,
        resume=True,
    )
    assert result["status"] == "complete"
    manifest = read_json(output / "source_manifest.json")
    assert manifest["schema_contract"]["recognized_numeric_smart_fields"] == [
        "smart_5_raw",
        "smart_198_raw",
    ]
    assert manifest["schema_contract"]["all_data_file_schemas_consistent"] is True


def test_completed_output_reuse_rejects_schema_contract_fingerprint_mismatch(
    tmp_path: Path,
) -> None:
    archive = normal_archive(tmp_path)
    config = write_test_config(tmp_path)
    output = tmp_path / "schema-contract-tamper"
    run_ingestion(
        config_path=config,
        quarter="2016Q1",
        archive=archive,
        output=output,
        source_url=SOURCE_URL,
    )
    manifest_path = output / "source_manifest.json"
    manifest = read_json(manifest_path)
    manifest["schema_contract"]["canonical_schema_fingerprint"] = "0" * 64
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    with pytest.raises(PipelineError, match="schema contract mismatch"):
        run_ingestion(
            config_path=config,
            quarter="2016Q1",
            archive=archive,
            output=output,
            source_url=SOURCE_URL,
        )


def test_publication_cleanup_interruption_resumes_without_run_state(tmp_path: Path) -> None:
    archive = normal_archive(tmp_path)
    config = write_test_config(tmp_path)
    output = tmp_path / "cleanup-interruption-output"
    with pytest.raises(IngestionInterrupted, match="run_state.json"):
        run_ingestion(
            config_path=config,
            quarter="2016Q1",
            archive=archive,
            output=output,
            source_url=SOURCE_URL,
            _interrupt_publication_after_cleanup="run_state.json",
        )

    staging = tmp_path / "cleanup-interruption-output.staging"
    assert not (staging / "run_state.json").exists()
    assert (staging / "publication_ready.json").is_file()
    assert not output.exists()

    result = run_ingestion(
        config_path=config,
        quarter="2016Q1",
        archive=archive,
        output=output,
        source_url=SOURCE_URL,
        resume=True,
    )
    assert result["status"] == "complete"
    assert (output / "publication_ready.json").is_file()


def test_publication_rename_failure_is_recoverable(tmp_path: Path, monkeypatch) -> None:
    archive = normal_archive(tmp_path)
    config = write_test_config(tmp_path)
    output = (tmp_path / "rename-failure-output").resolve()
    staging = output.with_name(output.name + ".staging")
    real_replace = __import__("os").replace
    failed = False

    def fail_publication_rename(source, destination):
        nonlocal failed
        if Path(source).resolve() == staging and Path(destination).resolve() == output and not failed:
            failed = True
            raise OSError("synthetic publication rename failure")
        return real_replace(source, destination)

    with monkeypatch.context() as context:
        context.setattr("drivepriority.data.ingest.os.replace", fail_publication_rename)
        with pytest.raises(OSError, match="synthetic publication rename failure"):
            run_ingestion(
                config_path=config,
                quarter="2016Q1",
                archive=archive,
                output=output,
                source_url=SOURCE_URL,
            )

    assert staging.is_dir()
    assert (staging / "publication_ready.json").is_file()
    assert not (staging / "run_state.json").exists()
    result = run_ingestion(
        config_path=config,
        quarter="2016Q1",
        archive=archive,
        output=output,
        source_url=SOURCE_URL,
        resume=True,
    )
    assert result["status"] == "complete"


def test_checkpoint_rejects_omitted_exception_artifact_before_cleanup(tmp_path: Path) -> None:
    archive, config, output, staging = interrupted_checkpoint(tmp_path)
    files = checkpoint_files(staging)
    exception_paths = [
        path for path in files["relative_paths"] if path.startswith("exception_parts/")
    ]
    assert exception_paths
    omitted = exception_paths[0]
    files["relative_paths"] = [path for path in files["relative_paths"] if path != omitted]
    files["artifacts"] = [
        artifact for artifact in files["artifacts"] if artifact["path"] != omitted
    ]
    replace_checkpoint_files(staging, files)

    with pytest.raises(PipelineError, match="exception checkpoint count mismatch"):
        resume_checkpoint(archive, config, output)
    assert (staging / omitted).is_file()


def test_checkpoint_rejects_inconsistent_exception_evidence_summary(tmp_path: Path) -> None:
    archive, config, output, staging = interrupted_checkpoint(tmp_path)
    files = checkpoint_files(staging)
    files["evidence_summary"]["exception_rows"] += 1
    replace_checkpoint_files(staging, files)

    with pytest.raises(PipelineError, match="evidence summary mismatch"):
        resume_checkpoint(archive, config, output)


@pytest.mark.parametrize("mutation", ["empty_inventory", "missing_quality", "unlisted_file"])
def test_completed_output_requires_exhaustive_manifest_inventory(
    tmp_path: Path, mutation: str
) -> None:
    archive = normal_archive(tmp_path)
    config = write_test_config(tmp_path)
    output = tmp_path / f"manifest-{mutation}"
    run_ingestion(
        config_path=config,
        quarter="2016Q1",
        archive=archive,
        output=output,
        source_url=SOURCE_URL,
    )
    manifest_path = output / "source_manifest.json"
    manifest = read_json(manifest_path)
    if mutation == "empty_inventory":
        manifest["outputs"] = []
    elif mutation == "missing_quality":
        manifest["outputs"] = [
            entry for entry in manifest["outputs"] if entry["path"] != "quality_report.json"
        ]
    else:
        (output / "unexpected.parquet").write_bytes(b"not a real artifact")
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    with pytest.raises(PipelineError, match="Existing output verification failed"):
        run_ingestion(
            config_path=config,
            quarter="2016Q1",
            archive=archive,
            output=output,
            source_url=SOURCE_URL,
        )


def test_unknown_source_values_are_preserved_as_linked_review_evidence(
    tmp_path: Path,
) -> None:
    archive = write_archive(
        tmp_path / "unknown-source-field.zip",
        {
            "day.csv": [
                [*BASE_HEADER, "Rack Location"],
                ["2016-01-01", "SN-1", "Model", "Vendor", "1000", "0", "Rack-A/07"],
            ]
        },
    )
    output = tmp_path / "unknown-source-field-output"

    result = run_ingestion(
        config_path=write_test_config(tmp_path),
        quarter="2016Q1",
        archive=archive,
        output=output,
        source_url=SOURCE_URL,
    )

    exceptions = pq.read_table(output / "exceptions.parquet").to_pylist()
    unsupported = [
        row for row in exceptions if row["exception_code"] == "unsupported_source_field"
    ]
    assert result["validation_status"] == "requires_review"
    assert len(unsupported) == 1
    assert unsupported[0]["column_name"] == "rack_location"
    assert unsupported[0]["original_header"] == "Rack Location"
    assert unsupported[0]["raw_value"] == "Rack-A/07"
    assert unsupported[0]["source_record_number"] == 1
    assert json.loads(unsupported[0]["raw_record_json"])["Rack Location"] == "Rack-A/07"
    assert "rack_location" not in read_drive_data(output).schema.names


def test_malformed_width_failure_flags_are_disclosed_outside_drive_day_counts(
    tmp_path: Path,
) -> None:
    archive = write_archive(
        tmp_path / "malformed-width-failures.zip",
        {
            "day.csv": [
                BASE_HEADER,
                ["2016-01-01", "SN-1", "Model", "Vendor", "1000", "0"],
                ["2016-01-02", "SN-2", "Model", "Vendor", "1000", "1", "extra"],
                ["2016-01-03", "SN-3", "Model", "Vendor", "1000"],
            ]
        },
    )
    output = tmp_path / "malformed-width-failures-output"

    run_ingestion(
        config_path=write_test_config(tmp_path),
        quarter="2016Q1",
        archive=archive,
        output=output,
        source_url=SOURCE_URL,
    )

    failure = read_json(output / "quality_report.json")["failure_flags"]
    malformed = failure["malformed_width_evidence"]
    assert failure["valid_source_counts"] == {"0": 1}
    assert failure["retained_canonical_counts"] == {"0": 1}
    assert failure["quarantine_evidence_counts"] == {}
    assert malformed["safely_identifiable_flag_counts"] == {"1": 1}
    assert malformed["daily_safely_identifiable_flag_counts"] == {
        "2016-01-02": {"1": 1}
    }
    assert malformed["unclassifiable_failure_rows"] == 1
    assert malformed["included_in_valid_source_or_output_counts"] is False
    assert malformed["accounting_scope_complete"] is False


def test_corrupted_parquet_data_pages_fail_bounded_readback_validation(
    tmp_path: Path, monkeypatch
) -> None:
    archive = normal_archive(tmp_path)
    output = tmp_path / "corrupt-data-page-output"
    original_compact = ingest_module._compact_exceptions

    def compact_then_corrupt(staging: Path, config) -> int:
        rows = original_compact(staging, config)
        target = next((staging / "data").rglob("*.parquet"))
        parquet_file = pq.ParquetFile(target)
        failure_index = parquet_file.schema_arrow.get_field_index("failure")
        column = parquet_file.metadata.row_group(0).column(failure_index)
        start = column.dictionary_page_offset
        if start is None or start < 0:
            start = column.data_page_offset
        payload = bytearray(target.read_bytes())
        corrupt_start = int(start) + 4
        corrupt_end = min(
            corrupt_start + max(16, int(column.total_compressed_size) // 2),
            int(start) + int(column.total_compressed_size),
        )
        payload[corrupt_start:corrupt_end] = b"\xff" * (corrupt_end - corrupt_start)
        target.write_bytes(payload)
        assert pq.ParquetFile(target).metadata.num_rows > 0
        return rows

    monkeypatch.setattr(ingest_module, "_compact_exceptions", compact_then_corrupt)
    with pytest.raises(ValidationError, match="Structural validation failed"):
        run_ingestion(
            config_path=write_test_config(tmp_path),
            quarter="2016Q1",
            archive=archive,
            output=output,
            source_url=SOURCE_URL,
        )

    staging = output.with_name(output.name + ".staging")
    check = next(
        item
        for item in read_json(staging / "validation_results.json")["checks"]
        if item["name"] == "parquet_files_readable"
    )
    assert check["passed"] is False
    assert check["bounded_full_page_readback_errors"]
    assert not output.exists()


def test_source_zip_replacement_during_processing_refuses_publication(
    tmp_path: Path, monkeypatch
) -> None:
    archive = complete_quarter_archive(tmp_path)
    output = tmp_path / "source-replacement-output"
    original_process_member = ingest_module._process_member

    def process_then_replace(*args, **kwargs):
        result = original_process_member(*args, **kwargs)
        write_archive(
            archive,
            {
                "replacement.csv": [
                    BASE_HEADER,
                    ["2016-01-01", "REPLACED", "Other", "Vendor", "1", "0"],
                ]
            },
        )
        return result

    monkeypatch.setattr(ingest_module, "_process_member", process_then_replace)
    with pytest.raises(PipelineError, match="Source ZIP identity changed"):
        run_ingestion(
            config_path=write_test_config(tmp_path),
            quarter="2016Q1",
            archive=archive,
            output=output,
            source_url=SOURCE_URL,
        )
    assert not output.exists()
    assert output.with_name(output.name + ".staging").exists()


def test_structurally_valid_zip_with_corrupt_crc_is_rejected(tmp_path: Path) -> None:
    archive = tmp_path / "bad-crc.zip"
    payload = (
        ",".join(BASE_HEADER) + "\n2016-01-01,SN-1,Model,Vendor,1000,0\n"
    ).encode("utf-8")
    with zipfile.ZipFile(archive, "w", compression=zipfile.ZIP_STORED) as target:
        target.writestr("day.csv", payload)
    raw = bytearray(archive.read_bytes())
    position = raw.index(b"SN-1")
    raw[position] ^= 0x01
    archive.write_bytes(raw)

    with pytest.raises((PipelineError, zipfile.BadZipFile)):
        run_ingestion(
            config_path=write_test_config(tmp_path),
            quarter="2016Q1",
            archive=archive,
            output=tmp_path / "bad-crc-output",
            source_url=SOURCE_URL,
        )
    assert not (tmp_path / "bad-crc-output").exists()


def test_canonical_buffer_flushes_at_estimated_byte_limit_without_data_loss(
    tmp_path: Path,
) -> None:
    values = ["x" * 800, "y" * 800, "z" * 800]
    archive = write_archive(
        tmp_path / "canonical-byte-limit.zip",
        {
            "day.csv": [
                [*BASE_HEADER, "smart_vendor_blob"],
                *[
                    [
                        f"2016-01-0{index}",
                        f"SN-{index}",
                        "Model",
                        "Vendor",
                        "1000",
                        "0",
                        value,
                    ]
                    for index, value in enumerate(values, start=1)
                ],
            ]
        },
    )
    output = tmp_path / "canonical-byte-limit-output"

    run_ingestion(
        config_path=write_test_config(
            tmp_path, batch_rows=1000, canonical_batch_bytes=1000
        ),
        quarter="2016Q1",
        archive=archive,
        output=output,
        source_url=SOURCE_URL,
    )

    parts = sorted((output / "data").rglob("*.parquet"))
    assert len(parts) == 3
    assert read_drive_data(output).column("smart_vendor_blob").to_pylist() == values


def test_large_same_serial_identity_group_avoids_pairwise_candidate_expansion(
    tmp_path: Path,
) -> None:
    row_count = 10_000
    start = date(1990, 1, 1)
    tracker = DuplicateTracker(tmp_path / "large-collision-state.duckdb")
    entries = []
    for ordinal in range(row_count):
        observed_date = start + timedelta(days=ordinal)
        model = "Model-A" if ordinal % 2 == 0 else "Model-B"
        entries.append(
            (
                {
                    "date": observed_date,
                    "serial_number": "SAME-SERIAL",
                    "model": model,
                    "manufacturer": "Vendor",
                    "capacity_bytes": 1000,
                    "failure": 0,
                    "source_quarter": "synthetic-stress",
                    "source_file": "large.csv",
                    "schema_version": "1.1.0",
                },
                ordinal + 1,
                ordinal + 2,
            )
        )

    try:
        tracker.begin_member()
        issues = tracker.classify(
            entries,
            (
                "date",
                "serial_number",
                "model",
                "manufacturer",
                "capacity_bytes",
                "failure",
            ),
        )
    finally:
        tracker.close()

    collisions = [
        issue for issue in issues if issue.exception_code == "serial_identity_collision"
    ]
    assert len(collisions) == row_count - 1
    assert collisions[0].source_record_number == 2
    assert collisions[-1].source_record_number == row_count


def test_checkpoint_severity_tamper_cannot_downgrade_review_status(
    tmp_path: Path,
) -> None:
    archive = write_archive(
        tmp_path / "severity-checkpoint.zip",
        {
            "a.csv": [
                [*BASE_HEADER, "unsupported_field"],
                ["2016-01-01", "SN-1", "Model", "Vendor", "1000", "0", "evidence"],
            ],
            "b.csv": [
                BASE_HEADER,
                ["2016-01-02", "SN-2", "Model", "Vendor", "1000", "0"],
            ],
        },
    )
    config = write_test_config(tmp_path, batch_rows=1)
    output = tmp_path / "severity-checkpoint-output"
    with pytest.raises(IngestionInterrupted):
        run_ingestion(
            config_path=config,
            quarter="2016Q1",
            archive=archive,
            output=output,
            source_url=SOURCE_URL,
            _interrupt_after_rows=2,
        )
    staging = output.with_name(output.name + ".staging")
    connection = duckdb.connect(str(staging / "state.duckdb"))
    try:
        row = connection.execute(
            "SELECT stats_json FROM completed_members WHERE member_index = 0"
        ).fetchone()
        assert row is not None
        stats = json.loads(row[0])
        assert stats["issue_severity_counts"] == {"error": 1}
        stats["issue_severity_counts"] = {}
        connection.execute(
            "UPDATE completed_members SET stats_json = ? WHERE member_index = 0",
            [json.dumps(stats, sort_keys=True, separators=(",", ":"))],
        )
    finally:
        connection.close()

    with pytest.raises(PipelineError, match="exception severity count mismatch"):
        resume_checkpoint(archive, config, output)
    assert not output.exists()


def test_legitimate_headers_column_does_not_hide_quarantined_failure_evidence(
    tmp_path: Path,
) -> None:
    archive = write_archive(
        tmp_path / "headers-source-column.zip",
        {
            "day.csv": [
                [*BASE_HEADER, "headers"],
                ["2016-01-01", "", "Model", "Vendor", "1000", "1", "legitimate"],
                ["2016-01-02", "SN-2", "Model", "Vendor", "1000", "0", "also-source"],
            ]
        },
    )
    output = tmp_path / "headers-source-column-output"

    result = run_ingestion(
        config_path=write_test_config(tmp_path),
        quarter="2016Q1",
        archive=archive,
        output=output,
        source_url=SOURCE_URL,
    )

    assert result["validation_status"] == "requires_review"
    validation = read_json(output / "validation_results.json")
    reconciliation = next(
        check
        for check in validation["checks"]
        if check["name"] == "failure_flag_source_to_output_reconciliation"
    )
    assert reconciliation["passed"] is True
    assert reconciliation["readback"]["quarantined_counts"] == {"1": 1}
    exception_rows = pq.read_table(output / "exceptions.parquet").to_pylist()
    raw_row = next(row for row in exception_rows if row["raw_record_json"] is not None)
    assert json.loads(raw_row["raw_record_json"])["headers"] == "legitimate"


def test_exception_heavy_evidence_reconciliation_remains_complete(
    tmp_path: Path,
) -> None:
    quarantine_rows = 2_000
    rows = [BASE_HEADER]
    rows.extend(
        [
            "2016-01-01",
            "",
            "Model",
            hashlib.sha256(f"vendor-{index}".encode("ascii")).hexdigest(),
            "1000",
            str(index % 2),
        ]
        for index in range(quarantine_rows)
    )
    rows.append(["2016-01-02", "SN-OK", "Model", "Vendor", "1000", "0"])
    archive = write_archive(tmp_path / "exception-heavy.zip", {"day.csv": rows})
    output = tmp_path / "exception-heavy-output"

    result = run_ingestion(
        config_path=write_test_config(
            tmp_path,
            batch_rows=257,
            exception_batch_rows=31,
            exception_batch_bytes=4096,
        ),
        quarter="2016Q1",
        archive=archive,
        output=output,
        source_url=SOURCE_URL,
    )

    assert result["validation_status"] == "requires_review"
    quality = read_json(output / "quality_report.json")
    assert quality["counts"]["quarantined_rows"] == quarantine_rows
    assert quality["counts"]["issue_severity_counts"] == {"error": quarantine_rows}
    validation = read_json(output / "validation_results.json")
    reconciliation = next(
        check
        for check in validation["checks"]
        if check["name"] == "failure_flag_source_to_output_reconciliation"
    )
    assert reconciliation["passed"] is True
    assert reconciliation["readback"]["quarantined_counts"] == {
        "0": quarantine_rows // 2,
        "1": quarantine_rows // 2,
    }


def test_complete_manifest_without_marker_resumes_verified_publication(
    tmp_path: Path, monkeypatch
) -> None:
    archive = normal_archive(tmp_path)
    config = write_test_config(tmp_path)
    output = tmp_path / "pre-marker-interruption-output"
    with pytest.raises(IngestionInterrupted, match="before publication marker"):
        run_ingestion(
            config_path=config,
            quarter="2016Q1",
            archive=archive,
            output=output,
            source_url=SOURCE_URL,
            _interrupt_after_complete_manifest=True,
        )

    staging = output.with_name(output.name + ".staging")
    assert (staging / "run_state.json").is_file()
    assert (staging / "source_manifest.json").is_file()
    assert not (staging / "publication_ready.json").exists()
    manifest_sha256 = hashlib.sha256(
        (staging / "source_manifest.json").read_bytes()
    ).hexdigest()
    data_hashes = {
        path.relative_to(staging).as_posix(): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in (staging / "data").rglob("*.parquet")
    }

    monkeypatch.setattr(
        ingest_module,
        "_process_member",
        lambda *args, **kwargs: pytest.fail("verified publication recovery reprocessed a member"),
    )
    result = run_ingestion(
        config_path=config,
        quarter="2016Q1",
        archive=archive,
        output=output,
        source_url=SOURCE_URL,
        resume=True,
    )

    assert result["status"] == "complete"
    assert (output / "publication_ready.json").is_file()
    assert hashlib.sha256((output / "source_manifest.json").read_bytes()).hexdigest() == manifest_sha256
    assert {
        path.relative_to(output).as_posix(): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in (output / "data").rglob("*.parquet")
    } == data_hashes


def test_complete_manifest_without_marker_rejects_modified_report(
    tmp_path: Path,
) -> None:
    archive = normal_archive(tmp_path)
    config = write_test_config(tmp_path)
    output = tmp_path / "pre-marker-tamper-output"
    with pytest.raises(IngestionInterrupted):
        run_ingestion(
            config_path=config,
            quarter="2016Q1",
            archive=archive,
            output=output,
            source_url=SOURCE_URL,
            _interrupt_after_complete_manifest=True,
        )
    staging = output.with_name(output.name + ".staging")
    quality_path = staging / "quality_report.json"
    quality = read_json(quality_path)
    quality["counts"]["input_rows"] += 1
    quality_path.write_text(json.dumps(quality), encoding="utf-8")

    with pytest.raises(
        PipelineError,
        match=(
            "Complete pre-marker staging verification failed.*"
            "(?:Size mismatch|SHA-256 mismatch)"
        ),
    ):
        run_ingestion(
            config_path=config,
            quarter="2016Q1",
            archive=archive,
            output=output,
            source_url=SOURCE_URL,
            resume=True,
        )
    assert not output.exists()
    assert not (staging / "publication_ready.json").exists()


@pytest.mark.parametrize("recovery_mode", ["marker_present", "pre_marker"])
def test_recovered_publication_rejects_source_zip_changed_after_initial_check(
    tmp_path: Path, monkeypatch, recovery_mode: str
) -> None:
    archive = normal_archive(tmp_path)
    config = write_test_config(tmp_path)
    output = tmp_path / f"{recovery_mode}-source-change-output"
    interruption = (
        {"_interrupt_before_publish": True}
        if recovery_mode == "marker_present"
        else {"_interrupt_after_complete_manifest": True}
    )
    with pytest.raises(IngestionInterrupted):
        run_ingestion(
            config_path=config,
            quarter="2016Q1",
            archive=archive,
            output=output,
            source_url=SOURCE_URL,
            **interruption,
        )

    real_verify_manifest = ingest_module._verify_completed_manifest
    source_replaced = False

    def verify_manifest_then_replace_source(*args, **kwargs):
        nonlocal source_replaced
        errors = real_verify_manifest(*args, **kwargs)
        if not source_replaced:
            write_archive(
                archive,
                {
                    "replacement.csv": [
                        BASE_HEADER,
                        ["2016-01-01", "REPLACED", "Other", "Vendor", "1", "0"],
                    ]
                },
            )
            source_replaced = True
        return errors

    monkeypatch.setattr(
        ingest_module,
        "_verify_completed_manifest",
        verify_manifest_then_replace_source,
    )
    with pytest.raises(
        PipelineError,
        match="Source ZIP identity changed during final publication",
    ):
        run_ingestion(
            config_path=config,
            quarter="2016Q1",
            archive=archive,
            output=output,
            source_url=SOURCE_URL,
            resume=True,
        )

    assert source_replaced is True
    assert not output.exists()
    assert output.with_name(output.name + ".staging").is_dir()


@pytest.mark.parametrize("publication_route", ["normal", "marker_present", "pre_marker"])
def test_final_source_integrity_gate_allows_unchanged_archive(
    tmp_path: Path, monkeypatch, publication_route: str
) -> None:
    archive = normal_archive(tmp_path)
    config = write_test_config(tmp_path)
    output = tmp_path / f"{publication_route}-source-gate-output"
    if publication_route != "normal":
        interruption = (
            {"_interrupt_before_publish": True}
            if publication_route == "marker_present"
            else {"_interrupt_after_complete_manifest": True}
        )
        with pytest.raises(IngestionInterrupted):
            run_ingestion(
                config_path=config,
                quarter="2016Q1",
                archive=archive,
                output=output,
                source_url=SOURCE_URL,
                **interruption,
            )

    real_verify_archive = ingest_module._verify_archive_unchanged
    phases: list[str] = []

    def record_archive_verification(*args, phase: str, **kwargs):
        phases.append(phase)
        return real_verify_archive(*args, phase=phase, **kwargs)

    monkeypatch.setattr(
        ingest_module,
        "_verify_archive_unchanged",
        record_archive_verification,
    )
    result = run_ingestion(
        config_path=config,
        quarter="2016Q1",
        archive=archive,
        output=output,
        source_url=SOURCE_URL,
        resume=publication_route != "normal",
    )

    assert result["status"] == "complete"
    assert phases[-1] == "final publication"
    assert output.is_dir()
