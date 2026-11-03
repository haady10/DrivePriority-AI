from __future__ import annotations

import csv
from datetime import date, timedelta
import hashlib
import io
import json
from pathlib import Path
import zipfile

import pytest
import yaml

from drivepriority.data.ingest import PipelineError
from drivepriority.data.source_inventory import (
    OPERATORS,
    authorize_archive,
    generate_inventory,
    official_archive_filename,
    resource_check,
    validate_operator,
)
from drivepriority.data.ingest import load_configuration
from drivepriority.data.workflow import _submissions, main as workflow_main


ROOT = Path(__file__).resolve().parents[1]
BASE_CONFIG = ROOT / "configs" / "ingestion.yaml"
OFFICIAL_URL = "https://f001.backblazeb2.com/file/Backblaze-Hard-Drive-Data/data_Q1_2016.zip"


def _archive(directory: Path) -> Path:
    path = directory / official_archive_filename("2016Q1")
    start = date(2016, 1, 1)
    end = date(2016, 4, 1)
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as target:
        for offset in range((end - start).days):
            day = (start + timedelta(days=offset)).isoformat()
            stream = io.StringIO(newline="")
            writer = csv.writer(stream, lineterminator="\n")
            writer.writerow(
                ["date", "serial_number", "model", "manufacturer", "capacity_bytes", "failure"]
            )
            writer.writerow([day, f"SN-{day}", "Model", "Vendor", "1000", "0"])
            target.writestr(f"{day}.csv", stream.getvalue())
    return path


def _planning(path: Path) -> Path:
    path.write_text(
        yaml.safe_dump(
            {
                "version": "1.0.0",
                "assignments": {name: [] for name in OPERATORS},
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    return path


@pytest.mark.parametrize("operator", OPERATORS)
def test_all_five_operators_can_select_unassigned_quarter(tmp_path: Path, operator: str) -> None:
    raw = tmp_path / "raw"
    raw.mkdir()
    _archive(raw)
    plan = generate_inventory(
        operator=operator,
        assignments_path=_planning(tmp_path / "planning.yaml"),
        requested_quarters=["2016Q1"],
        archive_directory=raw,
        evidence_directory=tmp_path / f"evidence-{operator.replace(' ', '-')}",
        base_config_path=BASE_CONFIG,
    )
    assert plan["operator"] == operator
    assert plan["quarters"][0]["quarter"] == "2016Q1"


def test_unknown_operator_is_rejected() -> None:
    with pytest.raises(PipelineError, match="Unknown operator"):
        validate_operator("Unknown Person")


def test_leader_authorization_is_append_only_reusable_and_hash_bound(tmp_path: Path) -> None:
    raw = tmp_path / "raw"
    raw.mkdir()
    archive = _archive(raw)
    approvals = tmp_path / "approvals.yaml"
    result = authorize_archive(
        quarter="2016Q1",
        archive=archive,
        base_config_path=BASE_CONFIG,
        approvals_path=approvals,
        approved_by="Haady Abdul Jabar",
        evidence_reference="synthetic local integrity review",
        source_url=OFFICIAL_URL,
        approve_processing=True,
    )
    assert result["record"]["processing_authorization"] is True
    stored = yaml.safe_load(approvals.read_text(encoding="utf-8"))
    assert stored["version"] == "2.0.0"
    assert len(stored["records"]) == 1

    plan = generate_inventory(
        operator="Shuhail",
        requested_quarters=["2016Q1"],
        archive_directory=raw,
        evidence_directory=tmp_path / "evidence",
        base_config_path=BASE_CONFIG,
        approvals_path=approvals,
    )
    assert plan["quarters"][0]["processing_authorized"] is True

    with zipfile.ZipFile(archive, "a") as target:
        target.writestr("unexpected.txt", "changed archive bytes")
    with pytest.raises(PipelineError, match="Approval archive SHA-256 mismatch"):
        generate_inventory(
            operator="Faheem",
            requested_quarters=["2016Q1"],
            archive_directory=raw,
            evidence_directory=tmp_path / "changed-evidence",
            base_config_path=BASE_CONFIG,
            approvals_path=approvals,
        )


def test_research_use_approval_records_level_without_claiming_level_three(tmp_path: Path) -> None:
    raw = tmp_path / "raw"
    raw.mkdir()
    archive = _archive(raw)
    result = authorize_archive(
        quarter="2016Q1",
        archive=archive,
        base_config_path=BASE_CONFIG,
        approvals_path=tmp_path / "approvals.yaml",
        approved_by="Haady Abdul Jabar",
        evidence_reference="synthetic official-download declaration review",
        source_url=OFFICIAL_URL,
        approve_processing=True,
        approve_research_use=True,
        evidence_level="level2_official_download_supported",
    )
    record = result["record"]
    assert record["authenticity_status"] == "manual_review_approved"
    assert record["evidence_level"] == "level2_official_download_supported"
    assert record["publisher_sha256"] is None
    assert record["remaining_limitations"]

    with pytest.raises(PipelineError, match="Only Haady Abdul Jabar"):
        authorize_archive(
            quarter="2016Q1",
            archive=archive,
            base_config_path=BASE_CONFIG,
            approvals_path=tmp_path / "unauthorized-actor.yaml",
            approved_by="Faheem",
            evidence_reference="synthetic",
            source_url=OFFICIAL_URL,
            approve_processing=True,
        )

    with pytest.raises(PipelineError, match="publisher-issued SHA-256"):
        authorize_archive(
            quarter="2016Q1",
            archive=archive,
            base_config_path=BASE_CONFIG,
            approvals_path=tmp_path / "level3.yaml",
            approved_by="Haady Abdul Jabar",
            evidence_reference="no real publisher checksum",
            source_url=OFFICIAL_URL,
            approve_processing=True,
            approve_research_use=True,
            evidence_level="level3_publisher_bytes_verified",
        )


def test_resource_check_is_read_only_and_does_not_claim_eight_gb(tmp_path: Path) -> None:
    raw = tmp_path / "raw"
    raw.mkdir()
    archive = _archive(raw)
    before = hashlib.sha256(archive.read_bytes()).hexdigest()
    configuration, _ = load_configuration(BASE_CONFIG)
    report = resource_check("2016Q1", archive, configuration, source_url=OFFICIAL_URL)
    assert hashlib.sha256(archive.read_bytes()).hexdigest() == before
    assert report["bounded_processing_demonstrated"] is True
    assert report["full_ingestion_executed"] is False
    assert report["eight_gb_suitability_proven"] is False


def test_inspect_writes_machine_and_human_evidence(tmp_path: Path) -> None:
    raw = tmp_path / "raw"
    raw.mkdir()
    archive = _archive(raw)
    report = tmp_path / "evidence" / "source.json"
    assert workflow_main(
        [
            "inspect",
            "--operator",
            "Zahid",
            "--quarter",
            "2016Q1",
            "--archive",
            str(archive),
            "--source-url",
            OFFICIAL_URL,
            "--report",
            str(report),
        ]
    ) == 0
    assert json.loads(report.read_text(encoding="utf-8"))["operator"] == "Zahid"
    assert "ZIP CRC: PASS" in report.with_suffix(".txt").read_text(encoding="utf-8")


def test_duplicate_submission_detection_distinguishes_conflicting_sources(tmp_path: Path) -> None:
    for name, digest in (("run-a", "a" * 64), ("run-b", "b" * 64)):
        root = tmp_path / name
        root.mkdir()
        (root / "source_manifest.json").write_text(
            json.dumps(
                {
                    "assigned_quarter": "2016Q1",
                    "source": {"sha256": digest},
                    "software": {"pipeline_version": "0.3.0"},
                }
            ),
            encoding="utf-8",
        )
    report = _submissions(tmp_path, "2016Q1")
    assert report["submission_count"] == 2
    assert report["classification"] == "conflicting_source_identity"
    assert report["central_action"] == "block_conflicting_quarter_versions"
