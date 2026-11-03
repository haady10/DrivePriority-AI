from __future__ import annotations

import csv
import io
import json
from pathlib import Path
import zipfile

import pyarrow.parquet as pq
import pytest
import yaml

from drivepriority.data.analyze import AnalysisError, analyze_quarter
from drivepriority.data.ingest import run_ingestion
from drivepriority.data.revalidate import RevalidationError, revalidate_quarter


CONFIG = Path(__file__).resolve().parents[1] / "configs" / "ingestion.yaml"
SOURCE_URL = "https://www.backblaze.com/cloud-storage/resources/hard-drive-test-data"
HEADER = [
    "date",
    "serial_number",
    "model",
    "manufacturer",
    "capacity_bytes",
    "failure",
]


def _fixture(tmp_path: Path, rows: list[list[str]]) -> Path:
    stream = io.StringIO(newline="")
    csv.writer(stream, lineterminator="\n").writerows([HEADER, *rows])
    archive = tmp_path / "source.zip"
    with zipfile.ZipFile(archive, "w", compression=zipfile.ZIP_DEFLATED) as target:
        target.writestr("day.csv", stream.getvalue())
    config = yaml.safe_load(CONFIG.read_text(encoding="utf-8"))
    config["processing"]["batch_rows"] = 2
    config["processing"]["parquet_row_group_rows"] = 2
    config["source_inventory"] = {
        "status": "unavailable",
        "provenance_reference": None,
        "verified_by": None,
        "verified_at": None,
        "quarters": {},
    }
    config_path = tmp_path / "config.yaml"
    config_path.write_text(yaml.safe_dump(config, sort_keys=False), encoding="utf-8")
    output = tmp_path / "cleaned"
    run_ingestion(
        config_path=config_path,
        quarter="2016Q1",
        archive=archive,
        output=output,
        source_url=SOURCE_URL,
    )
    return output


def test_anomaly_analysis_groups_identity_evidence_without_rewriting_rows(
    tmp_path: Path,
) -> None:
    cleaned = _fixture(
        tmp_path,
        [
            ["2016-01-01", "SN-A", "ST 1000", "Vendor", "1000", "0"],
            ["2016-01-02", "SN-A", " st   1000 ", "Vendor", "-1", "0"],
            ["2016-01-01", "SN-B", "Model A", "Vendor", "1000", "0"],
            ["2016-01-02", "SN-B", "Model B", "Vendor", "2000", "0"],
        ],
    )
    before = sorted(path.read_bytes() for path in (cleaned / "data").rglob("*.parquet"))

    report = analyze_quarter(cleaned, tmp_path / "analysis", memory_limit="128MB")

    assert report["identity_analysis"]["affected_serials"] == 2
    assert report["identity_analysis"]["model_formatting_serials"] == 1
    assert report["identity_analysis"]["known_unknown_capacity_serials"] == 1
    assert report["identity_analysis"]["unresolved_serials"] == 1
    groups = pq.read_table(tmp_path / "analysis" / "collision_groups.parquet").to_pylist()
    by_serial = {row["serial_number"]: row for row in groups}
    assert by_serial["SN-A"]["unresolved_identity_ambiguity"] is False
    assert by_serial["SN-B"]["unresolved_identity_ambiguity"] is True
    after = sorted(path.read_bytes() for path in (cleaned / "data").rglob("*.parquet"))
    assert before == after


def test_analysis_rejects_tampered_cleaned_output(tmp_path: Path) -> None:
    cleaned = _fixture(
        tmp_path,
        [["2016-01-01", "SN-1", "Model", "Vendor", "1000", "0"]],
    )
    target = next((cleaned / "data").rglob("*.parquet"))
    target.write_bytes(target.read_bytes() + b"tamper")

    with pytest.raises(AnalysisError, match="integrity failed"):
        analyze_quarter(cleaned, tmp_path / "analysis")


def test_revalidation_is_immutable_and_keeps_unresolved_cases_blocked(
    tmp_path: Path,
) -> None:
    cleaned = _fixture(
        tmp_path,
        [
            ["2016-01-01", "SN-1", "Model A", "Vendor", "1000", "0"],
            ["2016-01-02", "SN-1", "Model B", "Vendor", "2000", "0"],
        ],
    )
    analysis = tmp_path / "analysis"
    analyze_quarter(cleaned, analysis)

    report = revalidate_quarter(cleaned, analysis, tmp_path / "revalidation")

    assert report["status"] == "requires_review"
    assert "unresolved_identity_ambiguity" in report["review_reasons"]
    assert (tmp_path / "revalidation" / "revalidation_report.json").is_file()
    with pytest.raises(RevalidationError, match="already exists"):
        revalidate_quarter(cleaned, analysis, tmp_path / "revalidation")


def test_resolution_evidence_is_bound_to_source_manifest(tmp_path: Path) -> None:
    cleaned = _fixture(
        tmp_path,
        [
            ["2016-01-01", "SN-1", "Model A", "Vendor", "1000", "0"],
            ["2016-01-02", "SN-1", "Model B", "Vendor", "2000", "0"],
        ],
    )
    analysis = tmp_path / "analysis"
    analyze_quarter(cleaned, analysis)
    evidence = tmp_path / "resolution.json"
    evidence.write_text(
        json.dumps(
            {
                "quarter": "2016Q1",
                "source_manifest_sha256": "0" * 64,
                "approved_serials": ["SN-1"],
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(RevalidationError, match="source identity mismatch"):
        revalidate_quarter(
            cleaned,
            analysis,
            tmp_path / "revalidation",
            resolution_evidence=evidence,
        )
