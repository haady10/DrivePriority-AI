from __future__ import annotations

import json

import pytest

from drivepriority.integration import central_writer as cw
from drivepriority.integration.incremental import IncrementalError, incremental_update
from test_central_writer import fixtures


def _admit(*_args, **_kwargs):
    return {"decision": "ADMIT", "checks": {"parquet_rows": 1}}


def _existing_and_new(tmp_path, monkeypatch):
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    sources = fixtures(tmp_path, pa, pq)
    monkeypatch.setattr(cw, "check_quarter", _admit)
    existing = tmp_path / "existing"
    cw.integrate_quarters({"2016Q1": sources["2016Q1"]}, existing, batch_size=1)
    return existing, {"2016Q2": sources["2016Q2"]}


def test_incremental_addition_preserves_prior_quarter_and_expands_schema(
    tmp_path, monkeypatch
):
    existing, new_sources = _existing_and_new(tmp_path, monkeypatch)
    output = tmp_path / "updated"

    result = incremental_update(existing, new_sources, output, batch_size=1)

    assert result["previous_quarters"] == ["2016Q1"]
    assert result["added_quarters"] == ["2016Q2"]
    assert result["independent_validation"] == "PASS"
    manifest = json.loads((output / "central_manifest.json").read_text(encoding="utf-8"))
    assert set(manifest["input_quarters"]) == {"2016Q1", "2016Q2"}
    assert manifest["total_rows"] == 2


def test_duplicate_quarter_addition_is_rejected(tmp_path, monkeypatch):
    existing, new_sources = _existing_and_new(tmp_path, monkeypatch)
    duplicate = {"2016Q1": dict(next(iter(new_sources.values())))}

    with pytest.raises(IncrementalError, match="Duplicate quarter"):
        incremental_update(existing, duplicate, tmp_path / "updated")


def test_interrupted_incremental_plan_resumes_safely(tmp_path, monkeypatch):
    existing, new_sources = _existing_and_new(tmp_path, monkeypatch)
    output = tmp_path / "updated"

    with pytest.raises(IncrementalError, match="Controlled interruption"):
        incremental_update(
            existing,
            new_sources,
            output,
            batch_size=1,
            _interrupt_after_checkpoint=True,
        )
    assert not output.exists()

    result = incremental_update(
        existing,
        new_sources,
        output,
        batch_size=1,
        resume=True,
    )

    assert result["independent_validation"] == "PASS"
    assert not (tmp_path / ".updated.incremental-checkpoint").exists()


def test_corrupt_incremental_checkpoint_is_rejected(tmp_path, monkeypatch):
    existing, new_sources = _existing_and_new(tmp_path, monkeypatch)
    output = tmp_path / "updated"
    with pytest.raises(IncrementalError, match="Controlled interruption"):
        incremental_update(
            existing,
            new_sources,
            output,
            _interrupt_after_checkpoint=True,
        )
    state_path = tmp_path / ".updated.incremental-checkpoint" / "state.json"
    state = json.loads(state_path.read_text(encoding="utf-8"))
    state["identity"] = "0" * 64
    state_path.write_text(json.dumps(state), encoding="utf-8")

    with pytest.raises(IncrementalError, match="inconsistent or corrupt"):
        incremental_update(existing, new_sources, output, resume=True)


def test_rebuild_keeps_revalidation_evidence_of_previously_admitted_quarters(tmp_path, monkeypatch):
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    sources = fixtures(tmp_path, pa, pq)
    calls = []

    def _recording_admit(root, **kwargs):
        calls.append((kwargs.get("expected_quarter"), kwargs.get("revalidation_root")))
        return {"decision": "ADMIT", "checks": {"parquet_rows": 1}}

    monkeypatch.setattr(cw, "check_quarter", _recording_admit)
    existing = tmp_path / "existing"
    first = dict(sources["2016Q1"])
    first["revalidation_root"] = str(tmp_path / "revalidation-2016Q1")
    cw.integrate_quarters({"2016Q1": first}, existing, batch_size=1)
    calls.clear()

    incremental_update(existing, {"2016Q2": sources["2016Q2"]}, tmp_path / "updated", batch_size=1)

    assert ("2016Q1", str(tmp_path / "revalidation-2016Q1")) in calls
    assert ("2016Q2", None) in calls
