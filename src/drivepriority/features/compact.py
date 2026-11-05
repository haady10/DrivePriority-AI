"""Compact an integrated snapshot into one Parquet file per quarter (read-only on the snapshot).

Why: the central writer emits one Parquet file per 8,192-row batch (about 49,000 files for 24
quarters). DuckDB keeps footer metadata for every file in memory outside its memory limit, so
profiling and Phase 3 ran out of memory no matter what limit was set. A compacted copy has a few
dozen large files and reads quickly with a small memory footprint.

The snapshot is never changed. Every source file is hashed against central_manifest.json, rows are
copied unchanged in the same order, and per-quarter row and failure counts must equal the manifest
before the output folder is published. The output is a derived analysis copy, not a new snapshot:
it contains no evidence files and must not be fed back to the integration.

Usage (Windows, from the repo root):
  .venv\\Scripts\\python -m drivepriority.features.compact --snapshot "<Integrated_Data\\Snapshot-0006>" --out "<Integrated_Data\\Snapshot-0006_compact>"
"""
from __future__ import annotations

import argparse
import hashlib
import io
import json
import shutil
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path


class CompactError(RuntimeError):
    pass


def compact_snapshot(snapshot: str | Path, out: str | Path, *, rows_per_group: int = 200_000) -> dict:
    import pyarrow as pa
    import pyarrow.compute as pc
    import pyarrow.parquet as pq

    root = Path(snapshot).expanduser().resolve(strict=True)
    out = Path(out).expanduser().resolve()
    if out.exists():
        raise CompactError(f"Output already exists, refusing to overwrite: {out}")
    if root in out.parents or out in root.parents or out == root:
        raise CompactError("Output must not be inside the snapshot")
    if not 1_000 <= rows_per_group <= 1_000_000:
        raise CompactError("rows_per_group must be between 1,000 and 1,000,000")
    manifest_path = root / "central_manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if manifest.get("version") != "brick2-step4-v1":
        raise CompactError("Unsupported central manifest version")
    quarters_meta = manifest.get("input_quarters")
    files = manifest.get("files")
    if not isinstance(quarters_meta, dict) or not isinstance(files, list) or not files:
        raise CompactError("Manifest has no quarters or files")

    by_quarter: dict[str, list[dict]] = {}
    for record in files:
        parts = Path(record["path"]).parts
        if len(parts) != 3 or not parts[2].endswith(".parquet"):
            raise CompactError(f"Unexpected file path in manifest: {record['path']}")
        by_quarter.setdefault(parts[0] + parts[1], []).append(record)
    if set(by_quarter) != set(quarters_meta):
        raise CompactError("Manifest files and input_quarters list different quarters")

    staging = out.parent / ("." + out.name + ".staging-" + uuid.uuid4().hex)
    staging.mkdir(parents=True)
    started = time.time()
    summary: dict[str, dict] = {}
    expected_schema = None
    try:
        for quarter in sorted(by_quarter):
            records = sorted(by_quarter[quarter], key=lambda r: r["path"])
            meta = quarters_meta[quarter]
            target_dir = staging / quarter[:4] / quarter[4:]
            target_dir.mkdir(parents=True)
            target = target_dir / "part-00000000.parquet"
            writer = None
            pending: list = []
            pending_rows = 0
            rows = 0
            failures = 0

            def flush() -> None:
                nonlocal pending, pending_rows
                if pending:
                    writer.write_table(pa.Table.from_batches(pending, schema=expected_schema))
                    pending, pending_rows = [], 0

            for record in records:
                path = root / record["path"]
                data = path.read_bytes()
                if len(data) != record["bytes"] or hashlib.sha256(data).hexdigest() != record["sha256"].lower():
                    raise CompactError(f"Source file differs from manifest: {record['path']}")
                with pq.ParquetFile(io.BytesIO(data)) as source:
                    schema = source.schema_arrow
                    if expected_schema is None:
                        expected_schema = schema
                        if hashlib.sha256(schema.serialize().to_pybytes()).hexdigest() != str(
                                manifest.get("schema_arrow_sha256", "")).lower():
                            raise CompactError("Schema fingerprint differs from manifest")
                    elif not schema.equals(expected_schema, check_metadata=True):
                        raise CompactError(f"Schema differs in {record['path']}")
                    if writer is None:
                        writer = pq.ParquetWriter(target, expected_schema, compression="zstd", version="2.6")
                    for batch in source.iter_batches(batch_size=8192):
                        rows += batch.num_rows
                        failures += int(pc.sum(pc.cast(pc.equal(batch.column("failure"), 1), pa.int64())).as_py() or 0)
                        pending.append(batch)
                        pending_rows += batch.num_rows
                        if pending_rows >= rows_per_group:
                            flush()
            flush()
            if writer is None:
                raise CompactError(f"No data for {quarter}")
            writer.close()
            if rows != meta["integrated_rows"] or failures != meta["failure_rows"]:
                raise CompactError(
                    f"{quarter}: counts differ from manifest (rows {rows} vs {meta['integrated_rows']}, "
                    f"failures {failures} vs {meta['failure_rows']})")
            with pq.ParquetFile(target) as check:
                if check.metadata.num_rows != rows or not check.schema_arrow.equals(expected_schema, check_metadata=True):
                    raise CompactError(f"{quarter}: written file does not read back correctly")
            digest = hashlib.sha256(target.read_bytes()).hexdigest()
            summary[quarter] = {"rows": rows, "failure_rows": failures, "source_files": len(records),
                                "output": target.relative_to(staging).as_posix(),
                                "output_bytes": target.stat().st_size, "output_sha256": digest}
            print(f"{quarter}: {rows:,} rows, {failures:,} failures, {len(records)} files -> 1", flush=True)
        result = {
            "kind": "derived_analysis_copy_not_a_snapshot",
            "created_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "source_snapshot": root.name,
            "source_central_manifest_sha256": hashlib.sha256(manifest_path.read_bytes()).hexdigest(),
            "schema_arrow_sha256": manifest["schema_arrow_sha256"],
            "rows_per_row_group": rows_per_group,
            "total_rows": sum(q["rows"] for q in summary.values()),
            "total_failure_rows": sum(q["failure_rows"] for q in summary.values()),
            "quarters": summary,
            "seconds": round(time.time() - started, 1),
        }
        if result["total_rows"] != manifest.get("total_rows"):
            raise CompactError("Total rows differ from manifest")
        (staging / "compact_manifest.json").write_text(json.dumps(result, indent=2, sort_keys=True), encoding="utf-8")
        if out.exists():
            raise CompactError("Output appeared during the run: refusing overwrite")
        staging.rename(out)
        return result
    except BaseException:
        shutil.rmtree(staging, ignore_errors=True)
        raise


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--snapshot", required=True)
    parser.add_argument("--out", required=True)
    parser.add_argument("--rows-per-group", type=int, default=200_000)
    args = parser.parse_args(argv)
    try:
        result = compact_snapshot(args.snapshot, args.out, rows_per_group=args.rows_per_group)
    except CompactError as exc:
        print(f"FAILED: {exc}", file=sys.stderr)
        return 1
    print(f"OK: {result['total_rows']:,} rows, {result['total_failure_rows']:,} failure flags, "
          f"{len(result['quarters'])} quarters, {result['seconds']} s")
    return 0


if __name__ == "__main__":
    sys.exit(main())
