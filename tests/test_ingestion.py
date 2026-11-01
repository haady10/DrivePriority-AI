from __future__ import annotations

import csv
from datetime import date, timedelta
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
from drivepriority.data.validate import ValidationError


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


def write_test_config(tmp_path: Path, *, batch_rows: int = 2) -> Path:
    config = yaml.safe_load(CONFIG_SOURCE.read_text(encoding="utf-8"))
    config["processing"]["batch_rows"] = batch_rows
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


def fake_pipeline_identity(commit: str = "a" * 40) -> dict[str, object]:
    return {
        "pipeline_version": __version__,
        "git_commit": commit,
        "git_metadata_available": True,
        "runtime_source_sha256": "b" * 64,
        "requirements_sha256": "c" * 64,
        "pipeline_scoped_dirty": False,
    }


def write_identity_repository(tmp_path: Path) -> Path:
    repository = tmp_path / "identity-repository"
    for relative in PIPELINE_RUNTIME_PATHS:
        path = repository / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(f"# {relative}\n", encoding="utf-8")
    requirements = repository / REQUIREMENTS_PATH
    requirements.write_text("pyarrow==23.0.1\n", encoding="utf-8")
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
    assert manifest["software"]["pipeline_version"] == "0.1.1"
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
    (repository / REQUIREMENTS_PATH).write_text("pyarrow==23.0.2\n", encoding="utf-8")
    after = pipeline_recovery_identity(repository)

    assert before["requirements_sha256"] != after["requirements_sha256"]
    assert before["runtime_source_sha256"] == after["runtime_source_sha256"]


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
    assert not {
        "label",
        "feature",
        "prediction",
        "risk_score",
    }.intersection(read_drive_data(first).schema.names)


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
        ("processing", "parquet_row_group_rows", 0),
        ("processing", "maximum_archive_members", None),
        ("processing", "maximum_member_uncompressed_bytes", 0),
        ("processing", "maximum_archive_uncompressed_bytes", 0),
        ("processing", "minimum_free_space_bytes", 0),
        ("processing", "worker_threads", 0),
        ("processing", "maximum_compression_ratio", 0),
        ("processing", "working_space_multiplier", 1.0),
        ("parquet", "compression", "not-a-codec"),
        ("parquet", "compression_level", -1),
        ("recovery", "staging_suffix", "../unsafe"),
        ("recovery", "completed_output_policy", "overwrite"),
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
    assert reconciliation_check == {
        "name": "failure_flag_source_to_output_reconciliation",
        "passed": True,
        "total_counts_passed": True,
        "daily_counts_passed": True,
    }


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
