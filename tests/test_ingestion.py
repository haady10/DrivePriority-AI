from __future__ import annotations

import csv
import hashlib
import io
import json
from pathlib import Path
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
        "validation_status": "passed",
    }
    assert table.num_rows == 3
    assert table.schema.field("smart_5_raw").type == pa.decimal128(38, 0)
    assert table.column("failure").to_pylist() == [0, 1, 0]
    assert table.column("smart_5_raw").to_pylist() == [None, -1, 0]
    assert table.column("smart_198_raw").to_pylist() == [0, 10**38 - 1, None]
    assert table.column("smart_vendor_blob").to_pylist() == [None, None, "opaque"]
    assert set(table.column("source_quarter").to_pylist()) == {"2016Q1"}
    assert not (output / "exceptions.parquet").exists()

    quality = read_json(output / "quality_report.json")
    assert quality["smart_source_states"]["smart_5_raw"] == {
        "absent_column_rows": 0,
        "present_blank_rows": 1,
        "invalid_token_rows": 0,
        "genuine_zero_rows": 1,
        "canonical_null_rows": 1,
    }
    assert quality["smart_source_states"]["smart_198_raw"]["absent_column_rows"] == 1
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

    assert result["validation_status"] == "passed_with_warnings"
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

    result = run(tmp_path, archive)
    output = tmp_path / "output"
    quality = read_json(output / "quality_report.json")
    exceptions = pq.read_table(output / "exceptions.parquet")

    assert result["input_rows"] == 1
    assert result["output_rows"] == 0
    assert result["quarantined_rows"] == 1
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

    result = run(tmp_path, archive)
    output = tmp_path / "output"
    quality = read_json(output / "quality_report.json")
    codes = set(pq.read_table(output / "exceptions.parquet").column("exception_code").to_pylist())

    assert result["quarantined_rows"] == 2
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

    run(tmp_path, archive)
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
