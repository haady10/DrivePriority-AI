"""Derive the HDD-only modelling cohort from a finished Phase 3 folder (research rule R13).

Reads model_train/valid/test.parquet, keeps only drives whose normalised model is classified HDD in the
versioned registry (configs/drive_type_registry_v1.csv), and writes a NEW folder. The integrated snapshot and
the original Phase 3 folder are never changed. Why this is valid without rerunning Phase 3: whole drives were
sampled by a hash of drive_id, independent of drive type, so removing whole drives keeps the weights correct.

* A model that is missing from the registry stops the run (types are never guessed).
* SSD and UNKNOWN models are excluded from the primary cohort; counts per split and model are written to
  hdd_exclusions.csv and to the manifest (`cohort` section) with the registry hash.
* Rows, labels, features and weights of kept drives are copied unchanged, in the original order.

Usage (Windows, from the repo root):
  $env:PYTHONPATH = "src"
  .venv\\Scripts\\python -m drivepriority.features.hdd_filter --data data\\processed\\phase3_final --out data\\processed\\phase3_final_hdd
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import shutil
import time
from pathlib import Path

from drivepriority.features.common import connect, write_json

SPLITS = ("train", "valid", "test")
NORM = "regexp_replace(trim(coalesce(model, '')), '\\s+', ' ', 'g')"
SIDE_FILES = ("feature_dictionary.csv", "duplicate_audit.csv", "duplicate_conflicts.parquet", "FINAL_INFO.txt")


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for part in iter(lambda: fh.read(1 << 22), b""):
            h.update(part)
    return h.hexdigest()


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--data", required=True, help="Phase 3 folder (model_*.parquet + phase3_manifest.json)")
    p.add_argument("--out", required=True, help="New folder for the HDD-only files")
    p.add_argument("--registry", default="configs/drive_type_registry_v1.csv")
    p.add_argument("--memory-limit", default="8GB")
    p.add_argument("--temp-dir", default="results/_duckdb_tmp")
    p.add_argument("--threads", type=int, default=None)
    a = p.parse_args(argv)

    t0 = time.time()
    data, out, reg = Path(a.data).resolve(strict=True), Path(a.out).resolve(), Path(a.registry).resolve(strict=True)
    if out == data:
        raise SystemExit("--out must be a different folder from --data")
    if out.exists() and any(out.iterdir()):
        raise SystemExit(f"{out} is not empty; refusing to overwrite")
    manifest = json.loads((data / "phase3_manifest.json").read_text(encoding="utf-8"))
    ends = {"train": manifest["splits"]["train_end"], "valid": manifest["splits"]["valid_end"],
            "test": manifest["splits"]["test_end"]}
    for s in SPLITS:
        if not (data / f"model_{s}.parquet").is_file():
            raise SystemExit(f"Missing {data / f'model_{s}.parquet'}")

    with reg.open(encoding="utf-8", newline="") as fh:
        rows = list(csv.DictReader(fh))
    types = {r["model_norm"]: r["device_type"] for r in rows}
    out.mkdir(parents=True, exist_ok=True)

    con = connect(a.memory_limit, a.temp_dir, a.threads)
    con.execute("CREATE TEMP TABLE registry(model_norm VARCHAR, device_type VARCHAR)")
    con.executemany("INSERT INTO registry VALUES (?, ?)", list(types.items()))

    seen = set()
    for s in SPLITS:
        f = (data / f"model_{s}.parquet").as_posix()
        seen |= {r[0] for r in con.execute(f"SELECT DISTINCT {NORM} FROM read_parquet('{f}')").fetchall()}
    missing = sorted(seen - set(types))
    if missing:
        raise SystemExit(f"Models not in the registry (types are never guessed): {missing[:10]} ... "
                         f"({len(missing)} in total). Update the registry first.")

    stats, excl = {}, []
    for s in SPLITS:
        src = (data / f"model_{s}.parquet").as_posix()
        dst = (out / f"model_{s}.parquet").as_posix()
        con.execute(f"""
            COPY (SELECT t.* FROM read_parquet('{src}') t
                  JOIN registry r ON r.model_norm = {NORM.replace('model', 't.model')}
                  WHERE r.device_type = 'HDD')
            TO '{dst}' (FORMAT parquet, COMPRESSION zstd)""")
        before = con.execute(f"""SELECT count(*), count(DISTINCT drive_id),
              count(DISTINCT drive_id) FILTER (WHERE eval_fail_date IS NOT NULL AND eval_fail_date <= DATE '{ends[s]}')
              FROM read_parquet('{src}')""").fetchone()
        after = con.execute(f"""SELECT count(*), count(DISTINCT drive_id), min(date), max(date),
              count(DISTINCT drive_id) FILTER (WHERE eval_fail_date IS NOT NULL AND eval_fail_date <= DATE '{ends[s]}')
              FROM read_parquet('{dst}')""").fetchone()
        labels = {}
        for h in (7, 14, 30):
            l = con.execute(f"""SELECT count(*) FILTER (WHERE label_{h}d = 1), count(*) FILTER (WHERE label_{h}d = 0),
                                count(*) FILTER (WHERE label_{h}d IS NULL) FROM read_parquet('{dst}')""").fetchone()
            labels[f"{h}d"] = {"positive": l[0], "negative": l[1], "unknown": l[2],
                               "positive_pct_of_known": round(100.0 * l[0] / max(l[0] + l[1], 1), 4)}
        stats[s] = {"rows": after[0], "drives": after[1], "first_date": after[2], "last_date": after[3],
                    "drives_with_failure_event": after[4], "labels": labels,
                    "before": {"rows": before[0], "drives": before[1], "drives_with_failure_event": before[2]}}
        for m, dt, n_rows, n_drives, n_fail in con.execute(f"""
            SELECT {NORM.replace('model', 't.model')} AS m, r.device_type, count(*), count(DISTINCT t.drive_id),
                   count(DISTINCT t.drive_id) FILTER (WHERE t.eval_fail_date IS NOT NULL
                                                      AND t.eval_fail_date <= DATE '{ends[s]}')
            FROM read_parquet('{src}') t JOIN registry r ON r.model_norm = {NORM.replace('model', 't.model')}
            WHERE r.device_type <> 'HDD' GROUP BY 1, 2 ORDER BY 3 DESC""").fetchall():
            excl.append([s, m, dt, n_rows, n_drives, n_fail])

    with open(out / "hdd_exclusions.csv", "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(["split", "model", "device_type", "rows_removed", "drives_removed", "failed_drives_removed"])
        w.writerows(excl)
    for name in SIDE_FILES:
        if (data / name).is_file():
            shutil.copy2(data / name, out / name)

    manifest["phase"] = "3 - labels, features, splits (HDD-only cohort, rule R13)"
    manifest["split_stats"] = {s: {k: v for k, v in stats[s].items() if k != "before"} for s in SPLITS}
    manifest["files"] = {s: f"model_{s}.parquet" for s in SPLITS}
    manifest["cohort"] = {
        "name": "HDD-only", "registry": reg.name, "registry_sha256": _sha256(reg),
        "rule": "keep drives whose normalised model is classified HDD; SSD and UNKNOWN excluded; "
                "names are hints, not manufacturer-verified; boot vs data HDD cannot be told apart",
        "source_phase3_dir": data.name,
        "source_files_sha256": {s: _sha256(data / f"model_{s}.parquet") for s in SPLITS},
        "output_files_sha256": {s: _sha256(out / f"model_{s}.parquet") for s in SPLITS},
        "before_after": {s: {"before": stats[s]["before"],
                             "after": {"rows": stats[s]["rows"], "drives": stats[s]["drives"],
                                       "drives_with_failure_event": stats[s]["drives_with_failure_event"]}}
                         for s in SPLITS},
        "weights_unchanged": True, "runtime_seconds": round(time.time() - t0, 1)}
    write_json(out / "phase3_manifest.json", manifest)
    print(f"Done in {manifest['cohort']['runtime_seconds']} s. Output: {out}")
    for s in SPLITS:
        b, af = stats[s]["before"], stats[s]
        print(f"  {s:5s}: rows {b['rows']:>12,} -> {af['rows']:>12,}   drives {b['drives']:>8,} -> {af['drives']:>8,}"
              f"   failed {b['drives_with_failure_event']:>6,} -> {af['drives_with_failure_event']:>6,}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
