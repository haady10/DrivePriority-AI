from __future__ import annotations

import json
import os
from pathlib import Path
import shutil
import subprocess
import sys


ROOT = Path(__file__).resolve().parents[1]


def _package(tmp_path: Path) -> Path:
    package = tmp_path / "package with spaces"
    package.mkdir()
    shutil.copy2(ROOT / "RUN_ME.cmd", package / "RUN_ME.cmd")
    shutil.copytree(ROOT / "src", package / "src")
    return package


def test_windows_launcher_is_gitless_and_supports_paths_with_spaces(tmp_path: Path) -> None:
    package = _package(tmp_path)
    assert not (package / ".git").exists()
    reports = tmp_path / "quarter reports with spaces"
    reports.mkdir()
    (reports / "validation_results.json").write_text(
        json.dumps(
            {
                "status": "requires_review",
                "review_reasons": ["independent_archive_checksum_unavailable"],
            }
        ),
        encoding="utf-8",
    )
    environment = os.environ.copy()
    environment["DRIVEPRIORITY_PYTHON"] = sys.executable
    completed = subprocess.run(
        [
            "cmd.exe",
            "/d",
            "/c",
            "call",
            str(package / "RUN_ME.cmd"),
            "status",
            "--root",
            str(reports),
        ],
        text=True,
        capture_output=True,
        env=environment,
        check=False,
    )

    assert completed.returncode == 0, completed.stderr + completed.stdout
    assert "requires_review" in completed.stdout
    assert "independent_archive_checksum_unavailable" in completed.stdout


def test_windows_launcher_help_succeeds(tmp_path: Path) -> None:
    package = _package(tmp_path)
    environment = os.environ.copy()
    environment["DRIVEPRIORITY_PYTHON"] = sys.executable

    completed = subprocess.run(
        ["cmd.exe", "/d", "/c", "call", str(package / "RUN_ME.cmd")],
        text=True,
        capture_output=True,
        env=environment,
        check=False,
    )

    assert completed.returncode == 0, completed.stderr + completed.stdout
    assert "DrivePriority AI master workflow" in completed.stdout

    for argument in ("help", "--help"):
        completed = subprocess.run(
            ["cmd.exe", "/d", "/c", "call", str(package / "RUN_ME.cmd"), argument],
            text=True,
            capture_output=True,
            env=environment,
            check=False,
        )
        assert completed.returncode == 0, completed.stderr + completed.stdout
        assert "DrivePriority AI master workflow" in completed.stdout
