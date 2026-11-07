from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path, PureWindowsPath
import shutil
import subprocess
import zipfile

import pytest

from drivepriority.integration import operator


HEX = "a" * 64
COMMIT = "b" * 40


def _hash(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _inventory(path: Path) -> dict[str, object]:
    return {"path": path.name, "bytes": path.stat().st_size, "sha256": _hash(path)}


def make_evidence_zip(root: Path, quarter: str = "2016Q1", *, status: str = "passed", with_approval: bool = True,
                      gitless_runtime: str | None = None) -> Path:
    package = root / "payload"
    clean = package / "arbitrary" / "cleaned"
    analysis = package / "different" / "analysis"
    revalidation = package / "final" / "review"
    clean.mkdir(parents=True)
    analysis.mkdir(parents=True)
    revalidation.mkdir(parents=True)
    source_manifest = {
        "assigned_quarter": quarter,
        "source": {"sha256": HEX},
        "software": (
            {"pipeline_version": "0.3.0", "git": {"commit": COMMIT}}
            if gitless_runtime is None
            else {"pipeline_version": "1.0.0-rc2", "recovery_identity": {"runtime_source_sha256": gitless_runtime}}
        ),
    }
    manifest_path = clean / "source_manifest.json"
    manifest_path.write_text(json.dumps(source_manifest), encoding="utf-8")
    source_sha = _hash(manifest_path)
    anomaly = analysis / "anomaly_report.json"
    anomaly.write_text(json.dumps({"quarter": quarter}), encoding="utf-8")
    analysis_manifest = analysis / "analysis_manifest.json"
    analysis_manifest.write_text(
        json.dumps({"quarter": quarter, "source_manifest_sha256": source_sha, "outputs": [_inventory(anomaly)]}),
        encoding="utf-8",
    )
    revalidation_report = revalidation / "revalidation_report.json"
    revalidation_report.write_text(
        json.dumps(
            {
                "quarter": quarter,
                "status": status,
                "source_manifest_sha256": source_sha,
                "analysis_manifest_sha256": _hash(analysis_manifest),
                "source": (
                    {
                        "approval_evidence": {
                            "archive_sha256": HEX,
                            "research_use_source_approved": True,
                            "evidence_level": "level2_official_download_supported",
                        }
                    }
                    if with_approval
                    else {}
                ),
            }
        ),
        encoding="utf-8",
    )
    revalidation_manifest = revalidation / "revalidation_manifest.json"
    revalidation_manifest.write_text(json.dumps({"outputs": [_inventory(revalidation_report)]}), encoding="utf-8")
    destination = root / f"{quarter}.zip"
    with zipfile.ZipFile(destination, "w", zipfile.ZIP_DEFLATED) as archive:
        for path in package.rglob("*"):
            if path.is_file():
                archive.write(path, path.relative_to(package).as_posix())
    return destination


def test_package_inspection_supports_nested_layout_and_binds_evidence(tmp_path: Path) -> None:
    package = make_evidence_zip(tmp_path)
    result = operator.inspect_package(package, tmp_path / "work")
    assert result["quarter"] == "2016Q1"
    assert result["validation_status"] == "passed"
    assert result["trust"]["pipeline_commit"] == COMMIT
    assert result["zip_members"] == 5


def _ledger(quarter: str, sha: str) -> dict[str, object]:
    return {"central_research_use_approvals": {quarter: {
        "archive_sha256": sha, "evidence_level": "level2_official_download_supported",
        "research_use_source_approved": True, "approved_by": "Coordinator", "approved_on": "2026-11-05"}}}


def test_package_without_in_package_approval_is_rejected_by_default(tmp_path: Path) -> None:
    package = make_evidence_zip(tmp_path, with_approval=False)
    with pytest.raises(operator.OperatorError, match="research-use source approval"):
        operator.inspect_package(package, tmp_path / "work")


def test_central_ledger_approval_accepts_package_for_exact_quarter_and_hash(tmp_path: Path, monkeypatch) -> None:
    package = make_evidence_zip(tmp_path, with_approval=False)
    monkeypatch.setattr(operator, "_load_policy", lambda _path: _ledger("2016Q1", HEX))
    result = operator.inspect_package(package, tmp_path / "work")
    assert result["quarter"] == "2016Q1"


@pytest.mark.parametrize("quarter,sha", [("2016Q2", HEX), ("2016Q1", "c" * 64)])
def test_central_ledger_does_not_accept_other_quarter_or_hash(tmp_path: Path, monkeypatch, quarter: str, sha: str) -> None:
    package = make_evidence_zip(tmp_path, with_approval=False)
    monkeypatch.setattr(operator, "_load_policy", lambda _path: _ledger(quarter, sha))
    with pytest.raises(operator.OperatorError, match="research-use source approval"):
        operator.inspect_package(package, tmp_path / "work")


def test_windows_70_character_root_avoids_legacy_max_path_and_extracts(tmp_path: Path) -> None:
    root = PureWindowsPath("C:\\" + "r" * 67)
    assert len(str(root)) == 70
    package_hash = "a" * 64
    nested = PureWindowsPath(
        "2016Q2_Submission/Analysis/run-001/analysis_manifest.json"
    )
    legacy_staging = (
        root
        / ".Integration_Work"
        / "Packages"
        / ("." + package_hash + ".staging-" + "0" * 32)
        / nested
    )
    target, staging = operator._cache_paths(
        root / ".Integration_Work", package_hash, "0" * 32
    )
    assert len(str(legacy_staging)) == 262
    assert len(str(target / nested)) < 260
    assert len(str(staging / nested)) < 260

    package = make_evidence_zip(tmp_path)
    work = tmp_path / ".Integration_Work"
    result = operator.inspect_package(package, work)
    actual_target, _ = operator._cache_paths(
        work, result["package_sha256"], "unused-token"
    )
    assert (
        actual_target
        / "arbitrary"
        / "cleaned"
        / "source_manifest.json"
    ).is_file()


def test_short_cache_prefix_collision_fails_closed(tmp_path: Path) -> None:
    work = tmp_path / ".Integration_Work"
    first_hash = "a" * 64
    colliding_hash = "a" * 20 + "b" * 44
    target, _ = operator._cache_paths(work, first_hash, "first")
    target.mkdir(parents=True)
    (target / ".drivepriority-package.json").write_text(
        json.dumps({"package_sha256": first_hash}), encoding="utf-8"
    )
    with pytest.raises(operator.OperatorError, match="Cached package identity mismatch"):
        operator._extract_verified(
            tmp_path / "not-opened.zip", colliding_hash, work
        )


@pytest.mark.parametrize("damage", ["missing", "corrupt"])
def test_package_inspection_rejects_missing_or_corrupted_evidence(tmp_path: Path, damage: str) -> None:
    package = make_evidence_zip(tmp_path)
    unpacked = tmp_path / "unpacked"
    with zipfile.ZipFile(package) as archive:
        archive.extractall(unpacked)
    target = next(unpacked.rglob("anomaly_report.json"))
    if damage == "missing":
        target.unlink()
    else:
        target.write_text("changed", encoding="utf-8")
    rebuilt = tmp_path / "damaged.zip"
    with zipfile.ZipFile(rebuilt, "w", zipfile.ZIP_DEFLATED) as archive:
        for path in unpacked.rglob("*"):
            if path.is_file():
                archive.write(path, path.relative_to(unpacked).as_posix())
    with pytest.raises(operator.OperatorError):
        operator.inspect_package(rebuilt, tmp_path / "work")


def test_package_inspection_rejects_invalid_zip_and_traversal(tmp_path: Path) -> None:
    bad = tmp_path / "bad.zip"
    bad.write_bytes(b"not a zip")
    with pytest.raises(operator.OperatorError, match="corrupted ZIP"):
        operator.inspect_package(bad, tmp_path / "work")
    traversal = tmp_path / "traversal.zip"
    with zipfile.ZipFile(traversal, "w") as archive:
        archive.writestr("../outside.txt", "unsafe")
    with pytest.raises(operator.OperatorError, match="Unsafe ZIP"):
        operator.inspect_package(traversal, tmp_path / "work2")


def _candidate(path: Path, quarter: str, package_hash: str | None = None, status: str = "passed") -> dict[str, object]:
    return {
        "quarter": quarter,
        "zip_filename": path.name,
        "package_sha256": package_hash or hashlib.sha256(path.read_bytes()).hexdigest(),
        "source_manifest_sha256": "c" * 64,
        "revalidation_report_sha256": "d" * 64,
        "validation_status": status,
        "trust": {"root": str(path.parent), "archive_sha256": HEX, "pipeline_commit": COMMIT, "revalidation_root": str(path.parent)},
    }


@pytest.fixture
def workflow_fakes(monkeypatch):
    def inspect(path, work, **kwargs):
        quarter = path.stem.split("-")[0]
        return _candidate(path, quarter)

    def write_snapshot(sources, output, **kwargs):
        output.mkdir(parents=True)
        quarters = {q: {"archive_sha256": item["archive_sha256"]} for q, item in sources.items()}
        (output / "central_manifest.json").write_text(json.dumps({"input_quarters": quarters}), encoding="utf-8")
        return {"status": "PASS", "rows": len(quarters), "quarters": sorted(quarters)}

    def update(existing, sources, output, **kwargs):
        old = json.loads((existing / "central_manifest.json").read_text(encoding="utf-8"))["input_quarters"]
        merged = {**old, **{q: {"archive_sha256": item["archive_sha256"]} for q, item in sources.items()}}
        output.mkdir(parents=True)
        (output / "central_manifest.json").write_text(json.dumps({"input_quarters": merged}), encoding="utf-8")
        return {"status": "PASS", "rows": len(merged), "quarters": sorted(merged)}

    def validate(path):
        if not path or not (path / "central_manifest.json").is_file():
            return {"status": "FAIL", "errors": ["missing"]}
        quarters = json.loads((path / "central_manifest.json").read_text(encoding="utf-8"))["input_quarters"]
        return {"status": "PASS", "total_rows": len(quarters)}

    monkeypatch.setattr(operator, "inspect_package", inspect)
    monkeypatch.setattr(operator, "check_quarter", lambda *a, **k: {"decision": "ADMIT", "reasons": []})
    monkeypatch.setattr(operator, "integrate_quarters", write_snapshot)
    monkeypatch.setattr(operator, "incremental_update", update)
    monkeypatch.setattr(operator, "validate_snapshot", validate)


def _input(root: Path, name: str, content: bytes = b"zip") -> Path:
    folder = root / "Input_Quarters"
    folder.mkdir(parents=True, exist_ok=True)
    path = folder / name
    path.write_bytes(content)
    return path


def test_one_multiple_incremental_and_no_change_runs(tmp_path: Path, workflow_fakes) -> None:
    _input(tmp_path, "2016Q1-a.zip")
    first = operator.run_integration(tmp_path, minimum_free_bytes=0)
    assert first["newly_admitted"] == ["2016Q1"]
    _input(tmp_path, "2016Q2-a.zip")
    _input(tmp_path, "2016Q3-a.zip")
    second = operator.run_integration(tmp_path, minimum_free_bytes=0)
    assert second["newly_admitted"] == ["2016Q2", "2016Q3"]
    assert second["total_integrated_quarters"] == 3
    snapshot = second["dataset_output"]
    third = operator.run_integration(tmp_path, minimum_free_bytes=0)
    assert third["newly_admitted"] == []
    assert third["dataset_output"] == snapshot
    assert len(list((tmp_path / "Integrated_Data").glob("Snapshot-*"))) == 2


def test_duplicate_same_zip_skips_and_conflicting_quarter_blocks(tmp_path: Path, workflow_fakes, monkeypatch) -> None:
    first = _input(tmp_path, "2016Q1-a.zip", b"same")
    operator.run_integration(tmp_path, minimum_free_bytes=0)
    shutil.copyfile(first, first.with_name("2016Q1-copy.zip"))
    unchanged = operator.run_integration(tmp_path, minimum_free_bytes=0)
    assert unchanged["blocked"] == []
    _input(tmp_path, "2016Q1-conflict.zip", b"different")
    conflict = operator.run_integration(tmp_path, minimum_free_bytes=0)
    assert "Conflicting package" in conflict["blocked"][0]["reason"]
    assert conflict["total_integrated_quarters"] == 1


@pytest.mark.parametrize("decision", ["REVIEW", "REJECT"])
def test_review_and_failed_admission_are_blocked(tmp_path: Path, workflow_fakes, monkeypatch, decision: str) -> None:
    _input(tmp_path, "2016Q1-a.zip")
    monkeypatch.setattr(operator, "check_quarter", lambda *a, **k: {"decision": decision, "reasons": ["not eligible"]})
    report = operator.run_integration(tmp_path, minimum_free_bytes=0)
    assert report["newly_admitted"] == []
    assert report["blocked"] and report["total_integrated_quarters"] == 0


def test_eligible_passed_with_warnings_is_admitted(tmp_path: Path, workflow_fakes, monkeypatch) -> None:
    package = _input(tmp_path, "2016Q1-a.zip")
    monkeypatch.setattr(
        operator,
        "inspect_package",
        lambda path, work, **kwargs: _candidate(package, "2016Q1", status="passed_with_warnings"),
    )
    report = operator.run_integration(tmp_path, minimum_free_bytes=0)
    assert report["newly_admitted"] == ["2016Q1"]
    inventory = json.loads((tmp_path / "Reports" / "integration_inventory.json").read_text(encoding="utf-8"))
    assert inventory["quarters"]["2016Q1"]["validation_status"] == "passed_with_warnings"


def test_interrupted_publication_recovers_without_rewriting_snapshot(tmp_path: Path, workflow_fakes) -> None:
    _input(tmp_path, "2016Q1-a.zip")
    with pytest.raises(operator.OperatorError, match="Controlled interruption"):
        operator.run_integration(tmp_path, minimum_free_bytes=0, _interrupt_after_snapshot=True)
    snapshot = tmp_path / "Integrated_Data" / "Snapshot-0001"
    before = _hash(snapshot / "central_manifest.json")
    report = operator.run_integration(tmp_path, minimum_free_bytes=0)
    assert report["recovered_pending_publication"] is True
    assert report["total_integrated_quarters"] == 1
    assert _hash(snapshot / "central_manifest.json") == before


def test_interrupted_incremental_checkpoint_resumes_automatically(tmp_path: Path, workflow_fakes, monkeypatch) -> None:
    _input(tmp_path, "2016Q1-a.zip")
    operator.run_integration(tmp_path, minimum_free_bytes=0)
    _input(tmp_path, "2016Q2-a.zip")
    calls: list[bool] = []

    def interrupted_then_resume(existing, sources, output, **kwargs):
        calls.append(bool(kwargs.get("resume")))
        checkpoint = output.parent / ("." + output.name + ".incremental-checkpoint")
        if not kwargs.get("resume"):
            checkpoint.mkdir()
            raise operator.IncrementalError("controlled checkpoint interruption")
        checkpoint.rmdir()
        prior = json.loads((existing / "central_manifest.json").read_text(encoding="utf-8"))["input_quarters"]
        merged = {**prior, **{q: {"archive_sha256": item["archive_sha256"]} for q, item in sources.items()}}
        output.mkdir()
        (output / "central_manifest.json").write_text(json.dumps({"input_quarters": merged}), encoding="utf-8")
        return {"status": "PASS", "rows": len(merged)}

    monkeypatch.setattr(operator, "incremental_update", interrupted_then_resume)
    with pytest.raises(operator.IncrementalError, match="checkpoint interruption"):
        operator.run_integration(tmp_path, minimum_free_bytes=0)
    report = operator.run_integration(tmp_path, minimum_free_bytes=0)
    assert calls == [False, True]
    assert report["total_integrated_quarters"] == 2


def test_launcher_works_from_path_with_spaces(tmp_path: Path) -> None:
    root = tmp_path / "Integration Package With Spaces"
    root.mkdir()
    for name in ("RUN_INTEGRATION.cmd", "requirements.txt"):
        shutil.copy2(Path(__file__).parents[1] / name, root / name)
    shutil.copytree(Path(__file__).parents[1] / "src", root / "src")
    shutil.copytree(Path(__file__).parents[1] / "configs", root / "configs")
    env = os.environ.copy()
    env["DRIVEPRIORITY_PYTHON"] = os.fspath(Path(os.sys.executable))
    env["DRIVEPRIORITY_NO_PAUSE"] = "1"
    completed = subprocess.run(["cmd", "/c", str(root / "RUN_INTEGRATION.cmd")], cwd=root, env=env, text=True, capture_output=True, timeout=60)
    assert completed.returncode == 0, completed.stdout + completed.stderr
    assert (root / "Reports" / "integration_run_0001.txt").is_file()
    assert "Integration finished successfully" in completed.stdout


def _registry(tmp_path: Path, **extra: object) -> Path:
    path = tmp_path / "registry.json"
    path.write_text(json.dumps({"releases": {"1.0.0-rc2": {"package_sha256": "c" * 64, **extra}}}), encoding="utf-8")
    return path


def test_gitless_release_without_runtime_pin_keeps_version_only_check(tmp_path: Path) -> None:
    package = make_evidence_zip(tmp_path, gitless_runtime="a" * 64)
    result = operator.inspect_package(package, tmp_path / "work", release_registry=_registry(tmp_path))
    assert result["trust"]["runtime_source_sha256"] == "a" * 64


def test_gitless_release_with_matching_runtime_pin_is_accepted(tmp_path: Path) -> None:
    package = make_evidence_zip(tmp_path, gitless_runtime="A" * 64)
    registry = _registry(tmp_path, runtime_source_sha256=["b" * 64, "a" * 64])
    assert operator.inspect_package(package, tmp_path / "work", release_registry=registry)["quarter"] == "2016Q1"


def test_gitless_release_with_different_runtime_pin_is_blocked(tmp_path: Path) -> None:
    package = make_evidence_zip(tmp_path, gitless_runtime="a" * 64)
    registry = _registry(tmp_path, runtime_source_sha256="b" * 64)
    with pytest.raises(operator.OperatorError, match="not the one pinned"):
        operator.inspect_package(package, tmp_path / "work", release_registry=registry)
