import hashlib
import json
from datetime import date
from pathlib import Path

import pytest

pa = pytest.importorskip("pyarrow")
pq = pytest.importorskip("pyarrow.parquet")

from drivepriority.features.compact import CompactError, compact_snapshot  # noqa: E402

SCHEMA = pa.schema([pa.field("date", pa.date32(), nullable=False), pa.field("serial_number", pa.string(), nullable=False),
                    pa.field("failure", pa.uint8(), nullable=False), pa.field("smart_5_raw", pa.decimal128(38, 0))])


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def make_snapshot(tmp_path: Path, *, failure_rows_wrong: bool = False) -> Path:
    root = tmp_path / "Snapshot-0001"
    files, quarters, total = [], {}, 0
    for quarter, day0 in (("2021Q1", 1), ("2021Q2", 1)):
        folder = root / quarter[:4] / quarter[4:]
        folder.mkdir(parents=True)
        rows = failures = 0
        for part in range(3):
            n = 5
            table = pa.table({"date": pa.array([date(2021, 1 if quarter.endswith("1") else 4, day0 + part)] * n, pa.date32()),
                              "serial_number": pa.array([f"S{part}{i}" for i in range(n)]),
                              "failure": pa.array([1 if (part == 1 and i == 0) else 0 for i in range(n)], pa.uint8()),
                              "smart_5_raw": pa.array([None if i == 2 else i for i in range(n)], pa.decimal128(38, 0))},
                             schema=SCHEMA)
            path = folder / f"part-{part:08d}.parquet"
            pq.write_table(table, path, compression="zstd")
            files.append({"path": f"{quarter[:4]}/{quarter[4:]}/{path.name}", "sha256": _sha(path),
                          "bytes": path.stat().st_size, "rows": n})
            rows += n
            failures += 1 if part == 1 else 0
        quarters[quarter] = {"integrated_rows": rows, "failure_rows": failures + (1 if failure_rows_wrong else 0)}
        total += rows
    manifest = {"version": "brick2-step4-v1", "input_quarters": quarters, "files": files, "total_rows": total,
                "schema_arrow_sha256": hashlib.sha256(SCHEMA.serialize().to_pybytes()).hexdigest()}
    (root / "central_manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    return root


def test_compacts_to_one_file_per_quarter_with_identical_rows(tmp_path):
    root = make_snapshot(tmp_path)
    out = tmp_path / "compact"
    result = compact_snapshot(root, out, rows_per_group=1000)
    assert result["total_rows"] == 30 and result["total_failure_rows"] == 2
    assert sorted(p.relative_to(out).as_posix() for p in out.rglob("*.parquet")) == [
        "2021/Q1/part-00000000.parquet", "2021/Q2/part-00000000.parquet"]
    original = pa.concat_tables([pq.read_table(p) for p in sorted((root / "2021" / "Q1").glob("*.parquet"))])
    compact = pq.read_table(out / "2021" / "Q1" / "part-00000000.parquet")
    assert compact.equals(original) and compact.schema.equals(SCHEMA)
    assert json.loads((out / "compact_manifest.json").read_text())["kind"] == "derived_analysis_copy_not_a_snapshot"


def test_tampered_source_file_is_rejected_and_nothing_is_published(tmp_path):
    root = make_snapshot(tmp_path)
    victim = next((root / "2021" / "Q1").glob("*.parquet"))
    victim.write_bytes(victim.read_bytes() + b"x")
    with pytest.raises(CompactError, match="differs from manifest"):
        compact_snapshot(root, tmp_path / "compact", rows_per_group=1000)
    assert not (tmp_path / "compact").exists()
    assert not list(tmp_path.glob(".compact.staging-*"))


def test_count_mismatch_with_manifest_is_rejected(tmp_path):
    root = make_snapshot(tmp_path, failure_rows_wrong=True)
    with pytest.raises(CompactError, match="counts differ"):
        compact_snapshot(root, tmp_path / "compact", rows_per_group=1000)
    assert not (tmp_path / "compact").exists()


def test_refuses_existing_output_and_output_inside_snapshot(tmp_path):
    root = make_snapshot(tmp_path)
    (tmp_path / "exists").mkdir()
    with pytest.raises(CompactError, match="already exists"):
        compact_snapshot(root, tmp_path / "exists")
    with pytest.raises(CompactError, match="inside the snapshot"):
        compact_snapshot(root, root / "sub")
