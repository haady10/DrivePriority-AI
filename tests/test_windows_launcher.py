from __future__ import annotations

import csv
from datetime import date, timedelta
import hashlib
import io
import json
from pathlib import Path
import shutil
import subprocess
import sys
import zipfile

import pytest
import yaml


ROOT = Path(__file__).resolve().parents[1]
POWERSHELL = shutil.which("powershell.exe")


def _fixture(tmp_path: Path) -> dict[str, Path | str]:
    archive_directory = tmp_path / "raw"
    archive_directory.mkdir()
    archive = archive_directory / "data_Q1_2016.zip"
    header = [
        "date",
        "serial_number",
        "model",
        "manufacturer",
        "capacity_bytes",
        "failure",
        "smart_5_raw",
    ]
    start = date(2016, 1, 1)
    end = date(2016, 3, 31)
    with zipfile.ZipFile(archive, "w", compression=zipfile.ZIP_DEFLATED) as target:
        for offset in range((end - start).days + 1):
            day = (start + timedelta(days=offset)).isoformat()
            stream = io.StringIO(newline="")
            writer = csv.writer(stream, lineterminator="\n")
            writer.writerow(header)
            writer.writerow([day, f"SN-{day}", "Model", "Vendor", "1000", "0", "-1"])
            target.writestr(f"{day}.csv", stream.getvalue())
    archive_sha = hashlib.sha256(archive.read_bytes()).hexdigest()
    assignments = tmp_path / "assignments.yaml"
    assignments.write_text(
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
    approvals = tmp_path / "approvals.yaml"
    approvals.write_text(
        yaml.safe_dump(
            {
                "version": "1.0.0",
                "approvals": {
                    "2016Q1": {
                        "decision": "authorize_cleaning",
                        "archive_sha256": archive_sha,
                        "reviewed_by": "synthetic-launcher-test",
                        "reviewed_at": "2026-11-03T00:00:00Z",
                        "evidence_reference": "synthetic-launcher-test",
                        "authenticity_status": "unverified",
                        "archive_integrity_reviewed": True,
                        "date_coverage_reviewed": True,
                        "official_url_reviewed": True,
                        "approved_missing_dates": [],
                        "reason": "synthetic execution fixture",
                    }
                },
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    package = tmp_path / "master.zip"
    package.write_bytes(b"synthetic package identity")
    return {
        "archive_directory": archive_directory,
        "assignments": assignments,
        "approvals": approvals,
        "package": package,
        "package_sha": hashlib.sha256(package.read_bytes()).hexdigest(),
    }


def _run_launcher(tmp_path: Path, mode: str) -> tuple[subprocess.CompletedProcess[str], Path, Path]:
    fixture = _fixture(tmp_path)
    evidence = tmp_path / "evidence"
    output = tmp_path / "output"
    command = [
        str(POWERSHELL),
        "-NoProfile",
        "-ExecutionPolicy",
        "Bypass",
        "-File",
        str(ROOT / "RUN_C1_2.ps1"),
        "-PackagePath",
        str(fixture["package"]),
        "-ExpectedPackageSha256",
        str(fixture["package_sha"]),
        "-Teammate",
        "Faheem",
        "-ArchiveDirectory",
        str(fixture["archive_directory"]),
        "-EvidenceRoot",
        str(evidence),
        "-OutputRoot",
        str(output),
        "-PythonPath",
        sys.executable,
        "-AssignmentsPath",
        str(fixture["assignments"]),
        "-ApprovalsPath",
        str(fixture["approvals"]),
        mode,
    ]
    completed = subprocess.run(
        command,
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
        timeout=120,
    )
    return completed, evidence, output


@pytest.mark.skipif(POWERSHELL is None, reason="Windows PowerShell is unavailable")
def test_windows_launcher_dry_run_creates_evidence_only(tmp_path: Path) -> None:
    completed, evidence, output = _run_launcher(tmp_path, "-DryRunOnly")

    assert completed.returncode == 0, completed.stderr + completed.stdout
    run_directory = next(evidence.glob("Faheem-*"))
    summary = json.loads((run_directory / "teammate_summary.json").read_text())
    assert summary["quarters"][0]["outcome"] == "DRY_RUN_PASS"
    assert not output.exists()


@pytest.mark.skipif(POWERSHELL is None, reason="Windows PowerShell is unavailable")
def test_windows_launcher_separates_requires_review_output(tmp_path: Path) -> None:
    completed, evidence, output = _run_launcher(tmp_path, "-ExecuteCleaning")

    assert completed.returncode == 0, completed.stderr + completed.stdout
    run_directory = next(evidence.glob("Faheem-*"))
    summary = json.loads((run_directory / "teammate_summary.json").read_text())
    quarter = summary["quarters"][0]
    assert quarter["outcome"] == "REQUIRES_REVIEW"
    assert quarter["validation_status"] == "requires_review"
    cleaned = output / "requires_review" / "2016Q1"
    assert (cleaned / "source_manifest.json").is_file()
    assert list((cleaned / "data" / "2016Q1").glob("*.parquet"))
    assert not (output / "approved" / "2016Q1").exists()
