"""Full-fleet replay input (research rule R18): features and frozen-model scores for ALL eligible HDDs.

Phase 4 scored a 10% sample of drives. The primary Phase 5 result must replay every HDD, so this job
rebuilds the Phase 3 features for all drives of the validation (or test) period and scores them with
the SAVED Phase 4 models. Nothing is trained and nothing is tuned here.

Two steps, run one after the other on the PC that holds the snapshot:

  features   DuckDB only. For each quarter of the split it reads that quarter plus 31 earlier days,
             applies the Phase 3 rules unchanged (same duplicate rule, same feature SQL, same labels,
             failure day and later rows removed, HDD-only by the registry) and writes
             features_<split>_<quarter>.parquet.
             CHECK: every row of the Phase 3 sample file must be found again with identical features
             and labels. A single difference stops the run.

  score      H2O only. Loads the saved DRF (7, 14, 30 day) and GBM (30 day) models, scores month by
             month and writes replay_<split>/part_<month>.parquet with the columns the replay needs.
             CHECK: for the sample drives the new scores must equal the Phase 4 scores.

Then:  python -m drivepriority.simulation.run_phase5 --input <out>/replay_<split> --split <split>
           --population full_fleet --out <results folder>

The snapshot is read-only. A validation build reads no row later than valid_end + 31 days (the outcome
window of the last validation day). The test split needs --final-test-run and the committed authorization
file, like the replay itself.
"""
from __future__ import annotations

import argparse
import csv
import json
import time
from datetime import date, timedelta
from pathlib import Path

from drivepriority.features.build_dataset import HORIZONS, feature_definitions
from drivepriority.features.common import (DRIVE_ID_SQL, MODEL_ATTRS, SMART_RULE_ATTRS, connect,
                                           create_source_view, run_metadata, write_json)
from drivepriority.simulation.config import check_test_authorization, load_protocol
from drivepriority.simulation.replay_input import OUTCOME_DAYS, PRIMARY_MODEL, RUNNER_UP_30, sha256_file

LOOKBACK_DAYS = 31          # the longest feature window is 30 days back
NORM = "regexp_replace(trim(coalesce(model, '')), '\\s+', ' ', 'g')"
SCORE_TOLERANCE = 1e-6


def quarters(first: date, last: date) -> list[tuple[str, date, date]]:
    """Calendar quarters that cover first..last, clipped to that range."""
    out, d = [], date(first.year, 3 * ((first.month - 1) // 3) + 1, 1)
    while d <= last:
        nxt = date(d.year + (d.month + 2) // 12, (d.month + 2) % 12 + 1, 1)
        out.append((f"{d.year}Q{(d.month - 1) // 3 + 1}", max(d, first), min(nxt - timedelta(days=1), last)))
        d = nxt
    return out


def months(first: date, last: date) -> list[tuple[str, date, date]]:
    out, d = [], date(first.year, first.month, 1)
    while d <= last:
        nxt = date(d.year + d.month // 12, d.month % 12 + 1, 1)
        out.append((f"{d.year}-{d.month:02d}", max(d, first), min(nxt - timedelta(days=1), last)))
        d = nxt
    return out


def split_bounds(manifest: dict, split: str) -> tuple[date, date]:
    sp = manifest["splits"]
    ends = {k: date.fromisoformat(sp[f"{k}_end"]) for k in ("train", "valid", "test")}
    if split == "valid":
        return ends["train"] + timedelta(days=1), ends["valid"]
    return ends["valid"] + timedelta(days=1), ends["test"]


def _lock(split: str, final: bool) -> dict | None:
    """Validation needs nothing. The test split needs --final-test-run AND the committed authorization file."""
    if split not in ("valid", "test"):
        raise SystemExit(f"Unknown split: {split}")
    if split != "test":
        return None
    if not final:
        raise SystemExit("The test split is locked. Use --final-test-run only for the final run after the "
                         "independent review.")
    return check_test_authorization(load_protocol(), "full_fleet")


# ====================================================================== step 1: features

def build_features(a) -> int:
    auth = _lock(a.split, a.final_test_run)
    t0 = time.time()
    out = Path(a.out).expanduser().resolve()
    out.mkdir(parents=True, exist_ok=True)
    p3 = Path(a.phase3).expanduser().resolve(strict=True)
    manifest = json.loads((p3 / "phase3_manifest.json").read_text(encoding="utf-8"))
    if (manifest.get("cohort") or {}).get("name") != "HDD-only":
        raise SystemExit("--phase3 must be the HDD-only Phase 3 folder (03_Phase3_FINAL_HDD)")
    first, last = split_bounds(manifest, a.split)
    sample_file = p3 / f"model_{a.split}.parquet"
    if not sample_file.is_file():
        raise SystemExit(f"Missing {sample_file}")
    reg = Path(a.registry).expanduser().resolve(strict=True)
    reg_sha = sha256_file(reg)
    if reg_sha != manifest["cohort"].get("registry_sha256"):
        raise SystemExit("The drive-type registry is not the one used for the Phase 3 HDD cohort (hash differs)")

    con = connect(a.memory_limit, a.temp_dir, a.threads)
    create_source_view(con, a.snapshot)
    with reg.open(encoding="utf-8", newline="") as fh:
        types = {r["model_norm"]: r["device_type"] for r in csv.DictReader(fh)}
    con.execute("CREATE TEMP TABLE registry(model_norm VARCHAR, device_type VARCHAR)")
    con.executemany("INSERT INTO registry VALUES (?, ?)", list(types.items()))

    # Reading boundary. A row of day t is labelled by what happens up to t + 30 (and the replay judges a
    # decision by failures up to t + 31), so the outcome window of the last day ends OUTCOME_DAYS later.
    # Nothing after that day is read: a validation build never touches later test-period rows.
    test_end = date.fromisoformat(manifest["splits"]["test_end"])
    read_until = min(last + timedelta(days=OUTCOME_DAYS), test_end)
    print(f"[1/3] Drive table from rows up to {read_until} (first seen, last seen, first failure) ...", flush=True)
    con.execute(f"""
        CREATE TEMP TABLE drives AS
        SELECT drive_id, any_value(serial_number) AS serial_number, any_value(model) AS model,
               min(date) AS first_seen, max(date) AS last_seen,
               min(date) FILTER (WHERE failure = 1) AS fail_date
        FROM src WHERE date <= DATE '{read_until}' GROUP BY drive_id""")
    ext_first = first - timedelta(days=LOOKBACK_DAYS)
    unknown = [r[0] for r in con.execute(f"""
        SELECT DISTINCT {NORM} FROM drives
        WHERE last_seen >= DATE '{ext_first}' AND first_seen <= DATE '{last}'
          AND {NORM} NOT IN (SELECT model_norm FROM registry)""").fetchall()]
    if unknown:
        raise SystemExit(f"Models not in the registry (types are never guessed): {unknown[:10]} "
                         f"({len(unknown)} in total). Stop and tell the coordinator.")
    con.execute(f"""
        CREATE TEMP TABLE hdd AS
        SELECT d.* FROM drives d JOIN registry r ON r.model_norm = {NORM.replace('model', 'd.model')}
        WHERE r.device_type = 'HDD' AND d.last_seen >= DATE '{ext_first}' AND d.first_seen <= DATE '{last}'""")
    n_hdd = con.execute("SELECT count(*) FROM hdd").fetchone()[0]
    print(f"  HDD drives observed in or just before {first}..{last}: {n_hdd:,}", flush=True)

    feats = feature_definitions()
    feature_names = [n for n, _, _ in feats]
    if feature_names != list(manifest["feature_columns"]):
        raise SystemExit("The feature list of this code differs from the Phase 3 manifest; stopping")
    feat_sql = ",\n               ".join(f"{expr} AS {name}" for name, expr, _ in feats)
    rule = " OR ".join(f"smart_{x}_raw > 0" for x in SMART_RULE_ATTRS)
    rule_score = " + ".join(f"coalesce(smart_{x}_raw, 0)" for x in SMART_RULE_ATTRS)
    sig_cols = ["s.capacity_bytes", "s.failure"] + [f"s.smart_{x}_raw" for x in MODEL_ATTRS]
    nn_expr = " + ".join(f"CAST({c} IS NOT NULL AS INTEGER)" for c in sig_cols[2:])
    labels = [f"""CASE WHEN date + INTERVAL {h} DAY > DATE '{last}' THEN NULL
                     WHEN fail_date IS NOT NULL AND fail_date > date AND fail_date <= date + INTERVAL {h} DAY THEN 1
                     WHEN last_seen >= date + INTERVAL {h} DAY THEN 0
                     ELSE NULL END AS label_{h}d""" for h in HORIZONS]
    compare = feature_names + ["smart_rule_flag", "smart_rule_score"] + [f"label_{h}d" for h in HORIZONS]

    print("[2/3] Features per quarter (each with 31 earlier days) ...", flush=True)
    done, total_rows, checked, missing, different, unknown_labels = [], 0, 0, 0, 0, 0
    for name, qa, qb in quarters(first, last):
        if a.only and name not in a.only:
            continue
        t1 = time.time()
        ext = qa - timedelta(days=LOOKBACK_DAYS)
        f_out = out / f"features_{a.split}_{name}.parquet"
        con.execute(f"""
            CREATE OR REPLACE TEMP TABLE obs AS
            SELECT * EXCLUDE (rn) FROM (
              SELECT s.*, d.first_seen, d.last_seen, d.fail_date,
                     row_number() OVER (PARTITION BY s.drive_id, s.date
                                        ORDER BY s.failure DESC, ({nn_expr}) DESC,
                                                 hash({', '.join(sig_cols)}) ASC) AS rn
              FROM src s JOIN hdd d USING (drive_id)
              WHERE s.date BETWEEN DATE '{ext}' AND DATE '{qb}')
            WHERE rn = 1""")
        con.execute(f"""
            CREATE OR REPLACE TEMP TABLE feat AS
            SELECT drive_id, date, model, manufacturer_group, first_seen, last_seen, fail_date,
                   {feat_sql},
                   CAST(({rule}) AS INTEGER) AS smart_rule_flag,
                   {rule_score} AS smart_rule_score
            FROM obs
            WINDOW wd  AS (PARTITION BY drive_id ORDER BY date),
                   w7  AS (PARTITION BY drive_id ORDER BY date RANGE BETWEEN INTERVAL 7 DAYS PRECEDING AND CURRENT ROW),
                   w30 AS (PARTITION BY drive_id ORDER BY date RANGE BETWEEN INTERVAL 30 DAYS PRECEDING AND CURRENT ROW)""")
        # days_since_prev_obs looks at the previous observation, which can be older than the 31 days read
        # here. For the few drives that return after a longer absence, fetch their last earlier date.
        con.execute(f"""
            CREATE OR REPLACE TEMP TABLE need AS
            SELECT f.drive_id, d.serial_number FROM feat f JOIN hdd d USING (drive_id)
            WHERE f.date BETWEEN DATE '{qa}' AND DATE '{qb}' AND f.days_since_prev_obs IS NULL
              AND f.date > f.first_seen""")
        n_need = con.execute("SELECT count(*) FROM need").fetchone()[0]
        if n_need:
            con.execute(f"""
                CREATE OR REPLACE TEMP TABLE prev AS
                SELECT drive_id, max(date) AS prev_date FROM (
                  SELECT {DRIVE_ID_SQL} AS drive_id, CAST(date AS DATE) AS date FROM src_raw
                  WHERE serial_number IN (SELECT serial_number FROM need) AND CAST(date AS DATE) < DATE '{ext}')
                WHERE drive_id IN (SELECT drive_id FROM need) GROUP BY drive_id""")
            con.execute("""
                UPDATE feat SET days_since_prev_obs = CAST(feat.date - p.prev_date AS INTEGER)
                FROM prev p WHERE feat.drive_id = p.drive_id AND feat.days_since_prev_obs IS NULL
                  AND feat.date > feat.first_seen""")
        con.execute(f"""
            COPY (SELECT drive_id, date, model, manufacturer_group, fail_date AS eval_fail_date,
                         {', '.join(feature_names)}, smart_rule_flag, smart_rule_score, {', '.join(labels)}
                  FROM feat
                  WHERE date BETWEEN DATE '{qa}' AND DATE '{qb}' AND (fail_date IS NULL OR date < fail_date)
                  ORDER BY date, drive_id)
            TO '{f_out.as_posix()}' (FORMAT parquet, COMPRESSION zstd)""")
        rows, drives_q = con.execute(f"SELECT count(*), count(DISTINCT drive_id) FROM read_parquet('{f_out.as_posix()}')").fetchone()
        # ---- reconciliation with the Phase 3 sample (same drive-days must carry identical values)
        # One difference is expected and allowed, and it is counted openly: a drive that vanishes from the data
        # and returns after the reading boundary. Phase 3 read the later rows and labelled the day 0; this build
        # stops at the boundary (B1) and leaves the label unknown (empty). Only "Phase 3 = 0, here = unknown,
        # no failure" is allowed; any other label difference, and any feature difference, still stops the run.
        feat_cols = [c for c in compare if not c.startswith("label_")]
        lab_cols = [c for c in compare if c.startswith("label_")]
        allowed = {c: f"(p.{c} = 0 AND f.{c} IS NULL AND f.eval_fail_date IS NULL)" for c in lab_cols}
        diff = " OR ".join([f"p.{c} IS DISTINCT FROM f.{c}" for c in feat_cols]
                           + [f"(p.{c} IS DISTINCT FROM f.{c} AND NOT {allowed[c]})" for c in lab_cols])
        unk = " OR ".join(f"(p.{c} IS DISTINCT FROM f.{c} AND {allowed[c]})" for c in lab_cols)
        n_s, n_found, n_diff, n_unk = con.execute(f"""
            SELECT count(*), count(f.drive_id), count(*) FILTER (WHERE f.drive_id IS NOT NULL AND ({diff})),
                   count(*) FILTER (WHERE f.drive_id IS NOT NULL AND NOT ({diff}) AND ({unk}))
            FROM (SELECT * FROM read_parquet('{sample_file.as_posix()}')
                  WHERE date BETWEEN DATE '{qa}' AND DATE '{qb}') p
            LEFT JOIN read_parquet('{f_out.as_posix()}') f USING (drive_id, date)""").fetchone()
        checked, missing, different = checked + n_s, missing + (n_s - n_found), different + n_diff
        unknown_labels += n_unk
        total_rows += rows
        done.append({"quarter": name, "first": str(qa), "last": str(qb), "rows": rows, "drives": drives_q,
                     "returning_drives_fixed": n_need, "sample_rows_checked": n_s,
                     "sample_rows_missing": n_s - n_found, "sample_rows_different": n_diff,
                     "sample_rows_label_unknown_here_zero_in_phase3": n_unk,
                     "file": f_out.name, "sha256": sha256_file(f_out), "seconds": round(time.time() - t1, 1)})
        print(f"  {name}: {rows:,} rows, {drives_q:,} drives; sample check {n_s:,} rows, "
              f"{n_s - n_found:,} missing, {n_diff:,} different, {n_unk:,} label-unknown-here (allowed, see note); "
              f"{done[-1]['seconds']} s", flush=True)
        if (n_s - n_found or n_diff) and not a.allow_mismatch:
            write_json(out / f"features_{a.split}_FAILED.json", {"quarters": done})
            raise SystemExit(f"{name}: the full-fleet features do NOT reproduce the Phase 3 sample. "
                             f"Stop and tell the coordinator. Nothing after this quarter was written.")

    print("[3/3] Manifest ...", flush=True)
    write_json(out / f"features_{a.split}_manifest.json", {
        "what": "Full-fleet Phase 3 features for the Phase 5 replay (rule R18); HDD-only",
        "split": a.split, "first_day": str(first), "last_day": str(last), "rows": total_rows,
        "snapshot_rows_read_until": str(read_until), "test_authorization": auth,
        "hdd_drives": n_hdd, "feature_columns": feature_names, "registry_sha256": reg_sha,
        "phase3_manifest_sha256": sha256_file(p3 / "phase3_manifest.json"),
        "sample_reconciliation": {"file": sample_file.name, "sha256": sha256_file(sample_file),
                                  "rows_checked": checked, "rows_missing": missing, "rows_different": different,
                                  "rows_label_unknown_here_zero_in_phase3": unknown_labels,
                                  "note_label_unknown": "drive absent, returned after the reading boundary; "
                                                        "Phase 3 saw the return, this build does not read past the boundary",
                                  "passed": missing == 0 and different == 0},
        "quarters": done, "code_sha256": sha256_file(Path(__file__)), "runtime_seconds": round(time.time() - t0, 1),
        "run": run_metadata({k: str(v) for k, v in vars(a).items() if k != "func"})})
    print(f"Done in {round(time.time() - t0, 1)} s. {total_rows:,} rows. Sample check: {checked:,} rows, "
          f"{missing:,} missing, {different:,} different. Output: {out}")
    return 0 if (missing == 0 and different == 0) else 2


# ====================================================================== step 2: scores

def score_features(a) -> int:
    import duckdb
    import h2o
    import pandas as pd

    from drivepriority.models.train_h2o import EXCLUDED_FEATURES

    _lock(a.split, a.final_test_run)
    t0 = time.time()
    feat_dir = Path(a.features).expanduser().resolve(strict=True)
    p4 = Path(a.phase4).expanduser().resolve(strict=True)
    out = Path(a.out).expanduser().resolve()
    part_dir = out / f"replay_{a.split}"
    part_dir.mkdir(parents=True, exist_ok=True)
    fman = json.loads((feat_dir / f"features_{a.split}_manifest.json").read_text(encoding="utf-8"))
    if not fman["sample_reconciliation"]["passed"]:
        raise SystemExit("The feature step did not pass its sample check; scoring refused")
    first, last = date.fromisoformat(fman["first_day"]), date.fromisoformat(fman["last_day"])

    # which saved model belongs to which replay column
    wanted = {"ml_h30": (30, PRIMARY_MODEL[30]), "gbm_h30": (30, RUNNER_UP_30),
              "ml_h14": (14, PRIMARY_MODEL[14]), "ml_h7": (7, PRIMARY_MODEL[7])}
    infos, model_files, features = {}, {}, None
    for col, (h, name) in wanted.items():
        info = infos.setdefault(h, json.loads((p4 / f"h{h}" / "run_info.json").read_text(encoding="utf-8")))
        if not info.get("protocol_run") or info.get("horizon_days") != h:
            raise SystemExit(f"{p4 / f'h{h}'} is not a protocol run for horizon {h}")
        if info["h2o"] != h2o.__version__:
            raise SystemExit(f"H2O {h2o.__version__} is installed but the models were trained with {info['h2o']}. "
                             f"Install exactly that version; saved H2O models cannot be read by another one.")
        f = p4 / f"h{h}" / "models" / Path(info["saved_models"][name].replace("\\", "/")).name
        if not f.is_file():
            raise SystemExit(f"Saved model not found: {f}")
        model_files[col] = f
        fl = [c for c in info["features_used"] if c not in EXCLUDED_FEATURES]
        if features is not None and fl != features:
            raise SystemExit("The horizons were trained on different feature lists; stopping")
        features = fl
    if [c for c in fman["feature_columns"] if c not in EXCLUDED_FEATURES] != features:
        raise SystemExit("The feature files do not hold the model inputs in the trained order; stopping")
    feat_sql = ", ".join(f'CAST("{c}" AS DOUBLE) AS "{c}"' for c in features)
    rule_count = " + ".join(f"CAST(coalesce(smart_{x}_raw > 0, FALSE) AS INTEGER)" for x in SMART_RULE_ATTRS)
    glob = (feat_dir / f"features_{a.split}_*.parquet").as_posix()
    sample = {col: p4 / f"h{h}" / f"scores_{a.split}.parquet" for col, (h, _) in wanted.items()}
    sample = {c: f for c, f in sample.items() if f.is_file()}
    if not sample and not a.no_reconcile:
        raise SystemExit(f"No Phase 4 scores_{a.split}.parquet found to check against (use --no-reconcile only "
                         f"if the coordinator agrees)")

    con = duckdb.connect()
    con.execute(f"SET memory_limit='{a.duckdb_memory}'")
    con.execute(f"SET temp_directory='{(out / '_duckdb_tmp').as_posix()}'")
    report, total = [], 0
    print("[1/2] Starting H2O and loading the saved models ...", flush=True)
    try:
        h2o.init(port=a.port, max_mem_size=a.max_mem_size, nthreads=a.nthreads)
        h2o.no_progress()
        models = {col: h2o.load_model(f.as_posix()) for col, f in model_files.items()}
        print("[2/2] Scoring month by month ...", flush=True)
        for name, ma, mb in months(first, last):
            if a.only and name not in a.only:
                continue
            t1 = time.time()
            part = part_dir / f"part_{name}.parquet"
            tmp = out / f"_h2o_{name}.parquet"
            con.execute(f"""
                CREATE OR REPLACE TEMP TABLE m AS
                SELECT row_number() OVER (ORDER BY date, drive_id) AS row_id, *
                FROM read_parquet('{glob}') WHERE date BETWEEN DATE '{ma}' AND DATE '{mb}'""")
            n = con.execute("SELECT count(*) FROM m").fetchone()[0]
            if n == 0:
                continue
            con.execute(f"""COPY (SELECT row_id, {feat_sql} FROM m ORDER BY row_id)
                            TO '{tmp.as_posix()}' (FORMAT parquet, COMPRESSION zstd)""")
            frame = h2o.import_file(tmp.as_posix())
            scores = None
            for col, model in models.items():
                pred = model.predict(frame)
                df = frame[["row_id"]].cbind(pred["p1"]).as_data_frame(use_multi_thread=True)
                df.columns = ["row_id", col]
                scores = df if scores is None else scores.merge(df, on="row_id", how="inner")
                h2o.remove(pred)
            h2o.remove(frame)
            tmp.unlink(missing_ok=True)
            if len(scores) != n or scores["row_id"].nunique() != n:
                raise SystemExit(f"{name}: scores do not line up with the rows; stopping")
            con.register("scores_df", scores)
            con.execute(f"""
                COPY (SELECT m.drive_id, m.date, m.eval_fail_date AS fail_date,
                             CAST(m.label_7d AS TINYINT) AS label_7d, CAST(m.label_14d AS TINYINT) AS label_14d,
                             CAST(m.label_30d AS TINYINT) AS label_30d,
                             CAST({rule_count} AS TINYINT) AS smart_count,
                             CAST(m.smart_rule_score AS DOUBLE) AS smart_sum,
                             {', '.join(f's.{c}' for c in wanted)}
                      FROM m JOIN scores_df s USING (row_id) ORDER BY m.date, m.drive_id)
                TO '{part.as_posix()}' (FORMAT parquet, COMPRESSION zstd)""")
            con.unregister("scores_df")
            rec = {"month": name, "rows": n, "file": part.name, "sha256": sha256_file(part)}
            for col, f in sample.items():          # sample drives: new score against the Phase 4 score
                h, mname = wanted[col]
                n_s, n_found, worst = con.execute(f"""
                    SELECT count(*), count(r.drive_id), max(abs(p.score - r.{col}))
                    FROM (SELECT drive_id, date, score FROM read_parquet('{f.as_posix()}')
                          WHERE model = '{mname}' AND date BETWEEN DATE '{ma}' AND DATE '{mb}') p
                    LEFT JOIN read_parquet('{part.as_posix()}') r USING (drive_id, date)""").fetchone()
                rec[f"{col}_sample_rows"], rec[f"{col}_sample_missing"] = n_s, n_s - n_found
                rec[f"{col}_max_abs_difference"] = worst
                if (n_s - n_found) or (worst is not None and worst > SCORE_TOLERANCE):
                    rec["passed"] = False
            rec.setdefault("passed", True)
            rec["seconds"] = round(time.time() - t1, 1)
            report.append(rec)
            total += n
            worst_all = max([rec.get(f"{c}_max_abs_difference") or 0 for c in sample] + [0])
            print(f"  {name}: {n:,} rows; largest difference to Phase 4 on sample drives {worst_all:.2e}; "
                  f"{rec['seconds']} s", flush=True)
            if not rec["passed"] and not a.no_reconcile:
                write_json(out / f"replay_{a.split}_FAILED.json", {"months": report})
                raise SystemExit(f"{name}: the scores do NOT reproduce Phase 4 on the sample drives. "
                                 f"Stop and tell the coordinator.")
    finally:
        try:
            h2o.cluster().shutdown(prompt=False)
        except Exception:
            pass
    con.close()
    write_json(out / f"replay_{a.split}_manifest.json", {
        "what": "Full-fleet replay input for Phase 5 (rule R18): frozen Phase 4 model scores for all HDDs",
        "split": a.split, "first_day": str(first), "last_day": str(last), "rows": total,
        "models": {c: {"horizon": h, "model": m, "file": model_files[c].name, "sha256": sha256_file(model_files[c])}
                   for c, (h, m) in wanted.items()},
        "h2o": h2o.__version__, "features_manifest_sha256": sha256_file(feat_dir / f"features_{a.split}_manifest.json"),
        "sample_reconciliation": {"tolerance": SCORE_TOLERANCE, "checked_against": {c: f.name for c, f in sample.items()},
                                  "passed": all(r["passed"] for r in report) and bool(sample)},
        "months": report, "code_sha256": sha256_file(Path(__file__)), "runtime_seconds": round(time.time() - t0, 1),
        "run": run_metadata({k: str(v) for k, v in vars(a).items() if k != "func"})})
    print(f"Done in {round(time.time() - t0, 1)} s. {total:,} rows scored. Output: {part_dir}")
    return 0


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="step", required=True)
    f = sub.add_parser("features", help="step 1: full-fleet Phase 3 features (DuckDB)")
    f.add_argument("--snapshot", required=True, help="integrated snapshot folder (read-only)")
    f.add_argument("--phase3", required=True, help="03_Phase3_FINAL_HDD folder (manifest + sample files)")
    f.add_argument("--registry", default="configs/drive_type_registry_v1.csv")
    f.add_argument("--split", required=True, choices=("valid", "test"))
    f.add_argument("--out", required=True)
    f.add_argument("--memory-limit", default="20GB")
    f.add_argument("--temp-dir", default="results/_duckdb_tmp")
    f.add_argument("--threads", type=int, default=None)
    f.add_argument("--only", nargs="*", help="only these quarters, e.g. 2024Q1 (testing)")
    f.add_argument("--allow-mismatch", action="store_true", help="debugging only: do not stop on a failed sample check")
    f.add_argument("--final-test-run", action="store_true")
    f.set_defaults(func=build_features)
    s = sub.add_parser("score", help="step 2: score with the saved Phase 4 models (H2O)")
    s.add_argument("--features", required=True, help="output folder of the features step")
    s.add_argument("--phase4", required=True, help="04_Phase4_FINAL folder with h30, h14, h7 (models + run_info.json)")
    s.add_argument("--split", required=True, choices=("valid", "test"))
    s.add_argument("--out", required=True)
    s.add_argument("--max-mem-size", default="12G")
    s.add_argument("--port", type=int, default=54321)
    s.add_argument("--nthreads", type=int, default=-1)
    s.add_argument("--duckdb-memory", default="6GB")
    s.add_argument("--only", nargs="*", help="only these months, e.g. 2024-01 (testing)")
    s.add_argument("--no-reconcile", action="store_true", help="skip the check against Phase 4 sample scores")
    s.add_argument("--final-test-run", action="store_true")
    s.set_defaults(func=score_features)
    a = ap.parse_args(argv)
    return a.func(a)


if __name__ == "__main__":
    raise SystemExit(main())
