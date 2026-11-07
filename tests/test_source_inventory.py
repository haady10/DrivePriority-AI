from __future__ import annotations

import csv
from datetime import date, timedelta
import hashlib
import io
from pathlib import Path
import zipfile

import pytest
import yaml

from drivepriority.data.ingest import PipelineError, load_configuration
from drivepriority.data.source_inventory import (
    SUPPORTED_QUARTERS,
    generate_inventory,
    official_archive_filename,
    official_archive_url,
    scan_archive,
)


ROOT = Path(__file__).resolve().parents[1]
BASE_CONFIG = ROOT / "configs" / "ingestion.yaml"
HEADER = [
    "date",
    "serial_number",
    "model",
    "manufacturer",
    "capacity_bytes",
    "failure",
    "smart_5_raw",
]


def _csv(day: str, smart: str = "0") -> bytes:
    stream = io.StringIO(newline="")
    writer = csv.writer(stream, lineterminator="\n")
    writer.writerow(HEADER)
    writer.writerow([day, f"SN-{day}", "Model", "Vendor", "1000", "0", smart])
    return stream.getvalue().encode("utf-8")


def _quarter_days() -> list[str]:
    start = date(2016, 1, 1)
    end = date(2016, 3, 31)
    return [
        (start + timedelta(days=offset)).isoformat()
        for offset in range((end - start).days + 1)
    ]


def _archive(directory: Path, *, omit: set[str] | None = None) -> Path:
    omit = omit or set()
    path = directory / official_archive_filename("2016Q1")
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for day in _quarter_days():
            if day not in omit:
                archive.writestr(f"{day}.csv", _csv(day, "-1" if day.endswith("02") else "0"))
    return path


def _assignments(path: Path) -> Path:
    path.write_text(
        yaml.safe_dump(
            {
                "version": "1.0.0",
                "assignments": {
                    "Faheem": ["2016Q1"],
                    "Zahid": [],
                    "Sulaiman": [],
                    "Shuhail": [],
                },
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    return path


def _approval(path: Path, archive: Path, **updates) -> Path:
    entry = {
        "decision": "authorize_cleaning",
        "archive_sha256": hashlib.sha256(archive.read_bytes()).hexdigest(),
        "reviewed_by": "synthetic-reviewer",
        "reviewed_at": "2026-11-03T00:00:00Z",
        "evidence_reference": "synthetic-review-record",
        "authenticity_status": "unverified",
        "archive_integrity_reviewed": True,
        "date_coverage_reviewed": True,
        "official_url_reviewed": True,
        "approved_missing_dates": [],
        "reason": "synthetic launcher test",
    }
    entry.update(updates)
    path.write_text(
        yaml.safe_dump(
            {"version": "1.0.0", "approvals": {"2016Q1": entry}},
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    return path


def test_supported_quarter_and_official_url_contract() -> None:
    assert len(SUPPORTED_QUARTERS) == 42
    assert SUPPORTED_QUARTERS[0] == "2016Q1"
    assert SUPPORTED_QUARTERS[-1] == "2026Q2"
    assert official_archive_url("2026Q2").endswith("/data_Q2_2026.zip")
    with pytest.raises(ValueError):
        official_archive_url("2026Q3")


def test_duplicate_cross_teammate_assignment_is_rejected(tmp_path: Path) -> None:
    assignments = tmp_path / "assignments.yaml"
    assignments.write_text(
        yaml.safe_dump(
            {
                "version": "1.0.0",
                "assignments": {
                    "Faheem": ["2016Q1"],
                    "Zahid": ["2016Q1"],
                    "Sulaiman": [],
                    "Shuhail": [],
                },
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    with pytest.raises(PipelineError, match="assigned to both"):
        generate_inventory(
            teammate="Faheem",
            assignments_path=assignments,
            archive_directory=tmp_path,
            evidence_directory=tmp_path / "evidence",
            base_config_path=BASE_CONFIG,
        )


def test_scan_records_integrity_and_complete_calendar(tmp_path: Path) -> None:
    archive = _archive(tmp_path)
    configuration, _ = load_configuration(BASE_CONFIG)

    report = scan_archive("2016Q1", archive, configuration)

    assert report["archive"]["sha256"] == hashlib.sha256(archive.read_bytes()).hexdigest()
    assert report["archive"]["zip_crc_verified"] is True
    assert report["archive"]["data_csv_member_count"] == 91
    assert report["calendar"]["coverage_complete"] is True
    assert report["authenticity_status"] == "unverified_publisher_bytes"
    assert report["official_reference"]["publisher_checksum_available"] is False


def test_unapproved_scan_generates_evidence_but_not_execution(tmp_path: Path) -> None:
    archive_directory = tmp_path / "archives"
    archive_directory.mkdir()
    _archive(archive_directory)
    evidence = tmp_path / "evidence"

    plan = generate_inventory(
        teammate="Faheem",
        assignments_path=_assignments(tmp_path / "assignments.yaml"),
        archive_directory=archive_directory,
        evidence_directory=evidence,
        base_config_path=BASE_CONFIG,
    )

    quarter = plan["quarters"][0]
    assert quarter["processing_authorized"] is False
    assert quarter["expected_validation_status"] == "requires_review"
    generated, _ = load_configuration(Path(quarter["configuration_path"]))
    entry = generated["source_inventory"]["quarters"]["2016Q1"]
    assert entry["expected_sha256"] is None
    assert entry["authenticity_evidence"]["status"] == "unverified"


def test_manual_processing_approval_does_not_promote_authenticity(tmp_path: Path) -> None:
    archive_directory = tmp_path / "archives"
    archive_directory.mkdir()
    archive = _archive(archive_directory)
    evidence = tmp_path / "evidence"

    plan = generate_inventory(
        teammate="Faheem",
        assignments_path=_assignments(tmp_path / "assignments.yaml"),
        archive_directory=archive_directory,
        evidence_directory=evidence,
        base_config_path=BASE_CONFIG,
        approvals_path=_approval(tmp_path / "approvals.yaml", archive),
    )

    quarter = plan["quarters"][0]
    assert quarter["processing_authorized"] is True
    assert quarter["expected_validation_status"] == "requires_review"
    generated, _ = load_configuration(Path(quarter["configuration_path"]))
    entry = generated["source_inventory"]["quarters"]["2016Q1"]
    assert entry["expected_sha256"] is None
    assert entry["observed_sha256"] == hashlib.sha256(archive.read_bytes()).hexdigest()


def test_completed_manual_authenticity_review_is_explicit(tmp_path: Path) -> None:
    archive_directory = tmp_path / "archives"
    archive_directory.mkdir()
    archive = _archive(archive_directory)
    evidence = tmp_path / "evidence"
    approval = _approval(
        tmp_path / "approvals.yaml",
        archive,
        authenticity_status="manual_review_approved",
    )

    plan = generate_inventory(
        teammate="Faheem",
        assignments_path=_assignments(tmp_path / "assignments.yaml"),
        archive_directory=archive_directory,
        evidence_directory=evidence,
        base_config_path=BASE_CONFIG,
        approvals_path=approval,
    )

    generated, _ = load_configuration(
        Path(plan["quarters"][0]["configuration_path"])
    )
    entry = generated["source_inventory"]["quarters"]["2016Q1"]
    assert entry["expected_sha256"] == hashlib.sha256(archive.read_bytes()).hexdigest()
    assert entry["authenticity_evidence"]["status"] == "manual_review_approved"


def test_approval_hash_mismatch_fails_closed(tmp_path: Path) -> None:
    archive_directory = tmp_path / "archives"
    archive_directory.mkdir()
    archive = _archive(archive_directory)

    with pytest.raises(PipelineError, match="Approval archive SHA-256 mismatch"):
        generate_inventory(
            teammate="Faheem",
            assignments_path=_assignments(tmp_path / "assignments.yaml"),
            archive_directory=archive_directory,
            evidence_directory=tmp_path / "evidence",
            base_config_path=BASE_CONFIG,
            approvals_path=_approval(
                tmp_path / "approvals.yaml", archive, archive_sha256="0" * 64
            ),
        )


def test_unapproved_missing_date_remains_review_evidence(tmp_path: Path) -> None:
    archive_directory = tmp_path / "archives"
    archive_directory.mkdir()
    archive = _archive(archive_directory, omit={"2016-02-29"})
    evidence = tmp_path / "evidence"
    plan = generate_inventory(
        teammate="Faheem",
        assignments_path=_assignments(tmp_path / "assignments.yaml"),
        archive_directory=archive_directory,
        evidence_directory=evidence,
        base_config_path=BASE_CONFIG,
        approvals_path=_approval(tmp_path / "approvals.yaml", archive),
    )

    assert plan["quarters"][0]["unapproved_missing_dates"] == ["2016-02-29"]
    assert plan["quarters"][0]["expected_validation_status"] == "requires_review"
