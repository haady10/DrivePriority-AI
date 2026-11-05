import json
import os
import sys
import time
import zipfile
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools"))
import auto_integrate as ai  # noqa: E402

HEX = "a" * 64


def make_zip(path: Path, quarter: str, *, approval: bool = True, version: str = "1.0.0-rc2", sha: str = HEX) -> Path:
    manifest = {"assigned_quarter": quarter, "source": {"sha256": sha},
                "software": {"pipeline_version": version, "recovery_identity": {"runtime_source_sha256": "b" * 64}},
                "processing": {"exception_rows": 0}, "outputs": []}
    report = {"status": "passed_with_warnings", "source": {"approval_evidence": (
        {"evidence_level": "level2_official_download_supported", "research_use_source_approved": True,
         "archive_sha256": sha} if approval else {})}}
    with zipfile.ZipFile(path, "w") as z:
        z.writestr("source_manifest.json", json.dumps(manifest))
        z.writestr("revalidation_report.json", json.dumps(report))
        for name in ("analysis_manifest.json", "anomaly_report.json", "revalidation_manifest.json"):
            z.writestr(name, "{}")
    old = time.time() - 3600
    os.utime(path, (old, old))
    return path


@pytest.fixture
def env(tmp_path):
    root = tmp_path / "repo"
    (root / "configs").mkdir(parents=True)
    (root / "configs" / "central_policy.json").write_text(json.dumps({"version": "1.0.0"}), encoding="utf-8")
    onedrive = tmp_path / "onedrive"
    onedrive.mkdir()
    registry = tmp_path / "registry.json"
    registry.write_text(json.dumps({"releases": {"1.0.0-rc2": {"package_sha256": "c" * 64}}}), encoding="utf-8")
    return root, onedrive, registry


def watcher(env, **kw):
    root, onedrive, registry = env
    return ai.Watcher(root, onedrive, registry, settle_minutes=1, **kw)


def test_ready_zip_is_copied_and_old_zip_of_same_quarter_is_moved_not_deleted(env):
    w = watcher(env)
    old = make_zip(w.inputs / "2024Q4_old.zip", "2024Q4", sha="d" * 64)
    new = make_zip(env[1] / "2024Q4_new.zip", "2024Q4")
    assert w.handle(new) == "ready_copied_to_input"
    assert (w.inputs / "2024Q4_new.zip").is_file() and not old.exists()
    assert list(w.old.glob("2024Q4_old.replaced-*.zip"))


def test_wrong_pipeline_version_is_blocked_not_copied(env):
    w = watcher(env)
    zip_ = make_zip(env[1] / "2023Q4.zip", "2023Q4", version="1.0.0-rc1")
    assert w.handle(zip_) == "blocked_needs_person"
    assert not list(w.inputs.glob("*.zip"))


def test_ledger_added_only_for_the_single_missing_approval_problem_in_scope(env):
    w = watcher(env, auto_ledger=True)
    ok = make_zip(env[1] / "2024Q3.zip", "2024Q3", approval=False, sha="e" * 64)
    assert w.handle(ok) == "ready_copied_to_input"
    policy = json.loads((env[0] / "configs" / "central_policy.json").read_text())
    assert policy["central_research_use_approvals"]["2024Q3"]["archive_sha256"] == "e" * 64
    assert list((env[0] / "configs").glob("central_policy.json.bak-*"))
    out_of_scope = make_zip(env[1] / "2019Q2.zip", "2019Q2", approval=False, sha="f" * 64)
    assert w.handle(out_of_scope) == "blocked_needs_person"
    wrong_version = make_zip(env[1] / "2025Q1.zip", "2025Q1", approval=False, version="1.0.0-rc1", sha="1" * 64)
    assert w.handle(wrong_version) == "blocked_needs_person"
    assert "2025Q1" not in json.loads((env[0] / "configs" / "central_policy.json").read_text())["central_research_use_approvals"]


def test_dry_run_changes_nothing_and_already_handled_hash_is_not_repeated(env):
    w = watcher(env, dry_run=True, auto_ledger=True)
    zip_ = make_zip(env[1] / "2025Q2.zip", "2025Q2", approval=False)
    assert w.handle(zip_) == "blocked_needs_person"
    assert not list(w.inputs.glob("*.zip"))
    assert "central_research_use_approvals" not in json.loads((env[0] / "configs" / "central_policy.json").read_text())
    assert w.handle(zip_) is None


def test_quarter_already_in_snapshot_is_left_for_a_person(env):
    w = watcher(env)
    snap = env[0] / "Integrated_Data" / "Snapshot-0003"
    snap.mkdir(parents=True)
    (snap / "central_manifest.json").write_text(json.dumps({"input_quarters": {"2022Q2": {}}}), encoding="utf-8")
    zip_ = make_zip(env[1] / "2022Q2.zip", "2022Q2")
    assert w.handle(zip_) == "quarter_already_integrated_needs_person"
    assert not list(w.inputs.glob("*.zip"))


def test_files_still_syncing_are_skipped(env):
    w = watcher(env)
    fresh = make_zip(env[1] / "2022Q1.zip", "2022Q1")
    now = time.time()
    os.utime(fresh, (now, now))
    assert w.candidates() == []


def test_dry_run_does_not_persist_state_so_a_real_run_still_handles_the_file(env):
    dry = watcher(env, dry_run=True)
    zip_ = make_zip(env[1] / "2024Q4.zip", "2024Q4")
    assert dry.handle(zip_) == "ready_dry_run"
    dry.save()
    assert not dry.state_path.exists()
    real = watcher(env)
    assert real.handle(zip_) == "ready_copied_to_input"


def test_does_not_start_a_second_operator_while_one_is_running(env, monkeypatch):
    w = watcher(env)
    make_zip(env[1] / "2024Q4.zip", "2024Q4")
    calls = []
    monkeypatch.setattr(ai.Watcher, "operator_running", staticmethod(lambda: True))
    monkeypatch.setattr(ai.Watcher, "integrate", lambda self: calls.append(1))
    w.cycle()
    assert calls == []
    monkeypatch.setattr(ai.Watcher, "operator_running", staticmethod(lambda: False))
    w.cycle()
    assert calls == [1]


def test_stops_when_target_quarter_count_is_reached(env):
    snap = env[0] / "Integrated_Data" / "Snapshot-0009"
    snap.mkdir(parents=True)
    (snap / "central_manifest.json").write_text(json.dumps({"input_quarters": {"2016Q1": {}, "2016Q2": {}}}), encoding="utf-8")
    rc = ai.main(["--root", str(env[0]), "--onedrive", str(env[1]), "--registry", str(env[2]),
                  "--stop-at-quarters", "2", "--poll-minutes", "60"])
    assert rc == 0
    assert "DONE: the latest snapshot holds 2 quarters" in (env[0] / "Reports" / "auto_integrate.log").read_text()


def test_waits_instead_of_integrating_when_disk_is_too_small(env, monkeypatch):
    w = watcher(env)
    snap = env[0] / "Integrated_Data" / "Snapshot-0001"
    snap.mkdir(parents=True)
    (snap / "part.bin").write_bytes(b"x" * 1000)
    monkeypatch.setattr(ai.shutil, "disk_usage", lambda p: type("U", (), {"free": 5 * 1024**3})())
    assert w.enough_disk() is False
    monkeypatch.setattr(ai.shutil, "disk_usage", lambda p: type("U", (), {"free": 50 * 1024**3})())
    assert w.enough_disk() is True
