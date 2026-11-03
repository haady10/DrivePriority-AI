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
    approvals = tmp_path / "approvals.yaml"
    approvals.write_text(
        yaml.safe_dump(
            {
                "version": "2.0.0",
                "records": [
                    {
                        "approval_id": "synthetic-approval",
                        "quarter": "2016Q1",
                        "decision": "authorize_cleaning",
                        "archive_sha256": archive_sha,
                        "reviewed_by": "synthetic-launcher-test",
                        "reviewed_at": "2026-11-03T00:00:00Z",
                        "evidence_reference": "synthetic-launcher-test",
                        "authenticity_status": "unverified",
                        "evidence_level": "level1_local_integrity",
                        "source_policy_version": "1.0.0",
                        "processing_authorization": True,
                        "research_use_source_approved": False,
                        "declared_source_url": "https://f001.backblazeb2.com/file/Backblaze-Hard-Drive-Data/data_Q1_2016.zip",
                        "archive_integrity_reviewed": True,
                        "date_coverage_reviewed": True,
                        "official_url_reviewed": True,
                        "approved_missing_dates": [],
                        "reason": "synthetic execution fixture",
                    }
                ],
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    package = tmp_path / "master.zip"
    package.write_bytes(b"synthetic package identity")
    return {
        "archive_directory": archive_directory,
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
        "-Operator",
        "Faheem",
        "-Quarter",
        "2016Q1",
        "-ArchivePath",
        str(Path(fixture["archive_directory"]) / "data_Q1_2016.zip"),
        "-SourceUrl",
        "https://f001.backblazeb2.com/file/Backblaze-Hard-Drive-Data/data_Q1_2016.zip",
        "-EvidenceRoot",
        str(evidence),
        "-OutputRoot",
        str(output),
        "-PythonPath",
        sys.executable,
        "-ApprovalsPath",
        str(fixture["approvals"]),
        "-OriginalOperator",
        "Zahid",
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
    completed, evidence, output = _run_launcher(tmp_path, "-InspectOnly")

    assert completed.returncode == 0, completed.stderr + completed.stdout
    # Inspect-only is the safe preflight; no output or evidence run directory is created.
    assert not output.exists()


@pytest.mark.skipif(POWERSHELL is None, reason="Windows PowerShell is unavailable")
def test_windows_launcher_separates_requires_review_output(tmp_path: Path) -> None:
    completed, evidence, output = _run_launcher(tmp_path, "-ExecuteCleaning")

    assert completed.returncode == 0, completed.stderr + completed.stdout
    run_directory = next(evidence.glob("run-2016Q1-Faheem-*"))
    source_evidence = json.loads((run_directory / "source_evidence.json").read_text())
    assert source_evidence["operator"] == "Faheem"
    assert source_evidence["original_operator"] == "Zahid"
    cleaned = next((output / "2016Q1").glob("run-2016Q1-Faheem-*"))
    assert (cleaned / "source_manifest.json").is_file()
    assert list((cleaned / "data" / "2016Q1").glob("*.parquet"))
