"""Phase 4: train and evaluate failure-warning models for ONE horizon (7, 14 or 30 days).

Input : the Phase 3 folder (model_train/valid/test.parquet + phase3_manifest.json).
Models: lr   plain logistic regression (H2O GLM, no penalty)
        glm  elastic-net logistic regression with lambda search
        gbm  gradient boosting (early stopping on validation AUCPR)
        drf  distributed random forest (early stopping on validation AUCPR)
        plus the traditional SMART rule as a baseline (not trained).
Output: metrics.csv, rule_baseline.csv, calibration.csv, scores_valid.parquet, scores_test.parquet,
        importance_<model>.csv, checks.json, run_info.json (in --out).

Rules that keep the result honest:
* Rows with an unknown (NULL) label are never treated as healthy. They are dropped from
  training and from the metrics, but in validation/test they ARE scored and written to the score
  files with an empty label, because the inspection simulation needs every observable drive.
* Only the features listed in phase3_manifest.json are model inputs. drive_id, date, split,
  weight, eval_fail_date and model are never inputs (checked, the run stops otherwise).
* Training uses the sampling weight, so probabilities refer to the real fleet. Healthy training
  rows can be thinned further with --neg-keep; their weight is divided by the keep fraction.
* Validation is used only for early stopping / lambda choice. Test is touched once, for scoring.
* Protocol (v4, after the independent code review of 10 Nov 2026):
  - history_days is NOT a model input (it counts from the first day in the data, 2016-01-01, so it
    is wrong for drives already running then and drifts with calendar time; age_days is the real age).
  - one --neg-keep value (PROTOCOL_NEG_KEEP) for all horizons so they stay comparable; a different
    value stops the run unless --not-protocol is given (then run_info says protocol_run = false).
  - GBM leaf values are capped (max_abs_leafnode_pred) so scores do not pile up at exactly 0 or 1.
  - smart_rule_flag NULL (no counter above 0, some not reported) is read as 0, never left empty.
  - top-K metrics share ties fairly, and checks.json reports score health and ties.

Usage (Windows, from the repo root; Java and H2O must work, see tests/test_h2o_setup.py):
  $env:PYTHONPATH = "src"
  .venv\\Scripts\\python -m drivepriority.models.train_h2o --data data\\processed\\phase3_s9 --horizon 7 --out results\\phase4\\h7
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

import duckdb

from drivepriority.models.metrics import calibration_table, evaluate, rule_metrics, score_health

HORIZONS = (7, 14, 30)
ALL_MODELS = ("lr", "glm", "gbm", "drf")
FORBIDDEN_FEATURES = {"drive_id", "date", "split", "weight", "eval_fail_date", "model",
                      "manufacturer_group", "smart_rule_flag", "smart_rule_score", "dup_conflict",
                      "first_seen", "last_seen", "fail_date", "in_train", "in_eval",
                      "train_weight", "eval_weight",
                      "label_7d", "label_14d", "label_30d", "y", "w", "row_id"}
# Columns that Phase 3 provides as features but the frozen protocol does not use (reason recorded).
EXCLUDED_FEATURES = {"history_days": "left-censored at the data start (2016-01-01) and drifts with "
                                     "calendar time; age_days (SMART 9) gives the true age"}
PROTOCOL_NEG_KEEP = 0.25
GBM_MAX_ABS_LEAF = 2.0
SATURATION_WARN_SHARE = 0.001   # warn if more than 0.1% of rows sit at exactly 0 or 1
TIE_AT_CUT_WARN = 0.10          # warn if the rows tied at the top-1% cut exceed 10% of K


def _java_version() -> str:
    import subprocess
    try:
        r = subprocess.run(["java", "-version"], capture_output=True, text=True, timeout=30)
        lines = [ln for ln in (r.stderr or r.stdout).splitlines() if "version" in ln]
        return lines[0].strip() if lines else "unknown"
    except Exception:
        return "unknown"


def _git_commit() -> str:
    import subprocess
    try:
        r = subprocess.run(["git", "rev-parse", "HEAD"], capture_output=True, text=True, timeout=30,
                           cwd=Path(__file__).resolve().parent)
        return r.stdout.strip() or "not a git checkout"
    except Exception:
        return "not a git checkout"


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for part in iter(lambda: fh.read(1 << 22), b""):
            h.update(part)
    return h.hexdigest()


def prepare_files(data: Path, out: Path, horizon: int, features: list[str], neg_keep: float,
                  duckdb_memory: str, threads: int | None) -> dict:
    """Write the compact files H2O reads (features cast to DOUBLE) plus the metadata of valid/test."""
    import duckdb

    label = f"label_{horizon}d"
    con = duckdb.connect()
    con.execute(f"SET memory_limit='{duckdb_memory}'")
    con.execute(f"SET temp_directory='{(out / '_duckdb_tmp').as_posix()}'")
    if threads:
        con.execute(f"SET threads={int(threads)}")
    feat_sql = ", ".join(f'CAST("{c}" AS DOUBLE) AS "{c}"' for c in features)
    counts: dict = {}

    train_src = (data / "model_train.parquet").as_posix()
    keep = ""
    if neg_keep < 1.0:
        bucket = int(round(neg_keep * 10000))
        # md5 is fixed by its standard, unlike DuckDB's hash(), so the same rows are kept on any version.
        keep = (f" AND ({label} = 1 OR (md5_number(drive_id || '|' || CAST(date AS VARCHAR)) % 10000)"
                f" < {bucket})")
    w_sql = f"CASE WHEN {label} = 0 THEN CAST(weight AS DOUBLE) / {neg_keep} ELSE CAST(weight AS DOUBLE) END"
    con.execute(f"""
        COPY (SELECT {feat_sql}, CAST({label} AS INTEGER) AS y, {w_sql} AS w
              FROM read_parquet('{train_src}') WHERE {label} IS NOT NULL{keep})
        TO '{(out / 'h2o_train.parquet').as_posix()}' (FORMAT parquet, COMPRESSION zstd)""")
    r = con.execute(f"""SELECT count(*), count(*) FILTER (WHERE {label} = 1), count(*) FILTER (WHERE {label} IS NULL),
                               min(date), max(date) FROM read_parquet('{train_src}')""").fetchone()
    kept = con.execute(f"SELECT count(*), sum(y) FROM read_parquet('{(out / 'h2o_train.parquet').as_posix()}')").fetchone()
    counts["train"] = {"rows_in_file": r[0], "positives_in_file": r[1], "unknown_label_rows_dropped": r[2],
                       "first_date": str(r[3]), "last_date": str(r[4]), "rows_used": kept[0],
                       "positives_used": int(kept[1] or 0), "neg_keep": neg_keep}

    for split in ("valid", "test"):
        src = (data / f"model_{split}.parquet").as_posix()
        # ALL rows get a row_id: unknown-label rows stay (they are scored, not evaluated).
        con.execute(f"""
            CREATE TEMP TABLE t_{split} AS
            SELECT row_number() OVER (ORDER BY date, drive_id) AS row_id, *
            FROM read_parquet('{src}')""")
        # Labelled rows only: H2O validation frame (early stopping / lambda choice).
        con.execute(f"""
            COPY (SELECT row_id, {feat_sql}, CAST({label} AS INTEGER) AS y, CAST(weight AS DOUBLE) AS w
                  FROM t_{split} WHERE {label} IS NOT NULL ORDER BY row_id)
            TO '{(out / f'h2o_{split}.parquet').as_posix()}' (FORMAT parquet, COMPRESSION zstd)""")
        # Every row, labelled or not: scored for the daily risk-score export.
        con.execute(f"""
            COPY (SELECT row_id, {feat_sql} FROM t_{split} ORDER BY row_id)
            TO '{(out / f'h2o_{split}_all.parquet').as_posix()}' (FORMAT parquet, COMPRESSION zstd)""")
        con.execute(f"""
            COPY (SELECT row_id, drive_id, date, eval_fail_date, CAST({label} AS INTEGER) AS y,
                         CAST(weight AS DOUBLE) AS w,
                         -- NULL only when no counter is above 0 and some are not reported: that is "no alert".
                         CAST(coalesce(smart_rule_flag, 0) AS INTEGER) AS smart_rule_flag,
                         CAST(smart_rule_score AS DOUBLE) AS smart_rule_score
                  FROM t_{split} ORDER BY row_id)
            TO '{(out / f'meta_{split}.parquet').as_posix()}' (FORMAT parquet, COMPRESSION zstd)""")
        r = con.execute(f"""SELECT count(*), count(y), count(*) FILTER (WHERE y = 1), min(date), max(date),
                                   count(*) FILTER (WHERE eval_fail_date IS NOT NULL AND eval_fail_date <= date),
                                   count(*) FILTER (WHERE (smart_rule_flag > 0) <> (smart_rule_score > 0))
                            FROM read_parquet('{(out / f'meta_{split}.parquet').as_posix()}')""").fetchone()
        null_flags = con.execute(f"SELECT count(*) FILTER (WHERE smart_rule_flag IS NULL) FROM t_{split}").fetchone()[0]
        counts[split] = {"rows_scored": r[0], "rows_used": r[1], "positives_used": r[2],
                         "first_date": str(r[3]), "last_date": str(r[4]),
                         "rows_at_or_after_failure": r[5], "unknown_label_rows_dropped": 0,
                         "unknown_label_rows_scored_not_evaluated": r[0] - r[1],
                         "smart_rule_flag_null_read_as_0": null_flags,
                         "smart_rule_flag_vs_score_mismatch": r[6]}
    con.close()
    return counts


def single_feature_auc(valid_path: Path, features: list[str], label: str,
                       max_rows: int = 1_000_000) -> dict:
    """ROC AUC of each feature on its own (validation, labelled rows; NULL = lowest). A value near 1
    means one column almost gives the answer away, which points to leakage."""
    from sklearn.metrics import roc_auc_score
    cols = ", ".join(f'CAST("{c}" AS DOUBLE) AS "{c}"' for c in features)
    con = duckdb.connect()
    # The sample is taken AFTER the label filter (DuckDB samples before WHERE, hence the subquery).
    df = con.execute(f"""SELECT * FROM (SELECT {cols}, CAST({label} AS INTEGER) AS y
                                        FROM read_parquet('{valid_path.as_posix()}') WHERE {label} IS NOT NULL)
                         USING SAMPLE reservoir({int(max_rows)} ROWS) REPEATABLE (7)""").df()
    con.close()
    out = {}
    if df.empty or df["y"].nunique() < 2:
        return out
    for c in features:
        v = df[c].to_numpy(dtype=float)
        if np.all(np.isnan(v)):
            continue
        v = np.where(np.isnan(v), np.nanmin(v) - 1, v)
        auc = roc_auc_score(df["y"], v)
        out[c] = float(max(auc, 1 - auc))
    return out


def build_models(names: list[str], args) -> dict:
    from h2o.estimators import (H2OGeneralizedLinearEstimator, H2OGradientBoostingEstimator,
                                H2ORandomForestEstimator)
    common_stop = dict(stopping_rounds=5, stopping_metric="aucpr", stopping_tolerance=1e-3)
    models = {}
    for name in names:
        if name == "lr":
            models[name] = H2OGeneralizedLinearEstimator(
                family="binomial", lambda_=0, standardize=True, remove_collinear_columns=True,
                missing_values_handling="mean_imputation", seed=args.seed)
        elif name == "glm":
            models[name] = H2OGeneralizedLinearEstimator(
                family="binomial", alpha=0.5, lambda_search=True, nlambdas=20, standardize=True,
                missing_values_handling="mean_imputation", seed=args.seed)
        elif name == "gbm":
            models[name] = H2OGradientBoostingEstimator(
                ntrees=args.gbm_trees, max_depth=6, learn_rate=0.1, sample_rate=0.8, col_sample_rate=0.8,
                min_rows=50, score_tree_interval=5, seed=args.seed,
                # Without a cap, leaves holding only positive (or only negative) rows get near-infinite
                # values and many scores become exactly 0 or 1, which destroys the ranking (review B1).
                max_abs_leafnode_pred=GBM_MAX_ABS_LEAF, **common_stop)
        elif name == "drf":
            models[name] = H2ORandomForestEstimator(
                ntrees=args.drf_trees, max_depth=args.drf_depth, sample_rate=0.632, min_rows=50,
                score_tree_interval=5, seed=args.seed, stopping_rounds=3, stopping_metric="aucpr",
                stopping_tolerance=1e-3)
        else:
            raise SystemExit(f"Unknown model: {name}")
    return models


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--data", required=True, help="Phase 3 output folder")
    p.add_argument("--horizon", type=int, required=True, choices=HORIZONS)
    p.add_argument("--out", required=True)
    p.add_argument("--models", default=",".join(ALL_MODELS), help="comma list of lr,glm,gbm,drf")
    p.add_argument("--neg-keep", type=float, default=PROTOCOL_NEG_KEEP,
                   help=f"fraction of healthy TRAINING rows kept (weights are scaled up). The protocol "
                        f"value is {PROTOCOL_NEG_KEEP} for all horizons; others need --not-protocol")
    p.add_argument("--not-protocol", action="store_true",
                   help="allow settings that differ from the frozen protocol (practice/sensitivity runs only)")
    p.add_argument("--max-mem-size", default="8G", help="H2O memory, e.g. 8G")
    p.add_argument("--port", type=int, default=54321,
                   help="H2O port. Use different ports (e.g. 54321 and 54421) to run two horizons on one PC at once")
    p.add_argument("--nthreads", type=int, default=-1)
    p.add_argument("--duckdb-memory", default="4GB")
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--gbm-trees", type=int, default=200)
    p.add_argument("--drf-trees", type=int, default=100)
    p.add_argument("--drf-depth", type=int, default=12)
    p.add_argument("--save-models", action="store_true", help="also save the binary H2O models")
    a = p.parse_args(argv)
    import socket
    for q in (a.port, a.port + 1):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
            probe.settimeout(1)
            if probe.connect_ex(("127.0.0.1", q)) == 0:
                raise SystemExit(f"Port {q} is already in use (another H2O run?). Stopping so two runs do not "
                                 f"share one H2O cluster. Choose another one, e.g. --port {a.port + 100}.")
    if not 0 < a.neg_keep <= 1:
        raise SystemExit("--neg-keep must be in (0, 1]")
    if a.neg_keep != PROTOCOL_NEG_KEEP and not a.not_protocol:
        raise SystemExit(f"--neg-keep {a.neg_keep} differs from the protocol value {PROTOCOL_NEG_KEEP} "
                         f"(all horizons must use the same value). Use --not-protocol only for practice runs.")
    names = [m.strip() for m in a.models.split(",") if m.strip()]
    for m in names:
        if m not in ALL_MODELS:
            raise SystemExit(f"Unknown model: {m}")

    t0 = time.time()
    data = Path(a.data).expanduser().resolve(strict=True)
    out = Path(a.out).expanduser().resolve()
    out.mkdir(parents=True, exist_ok=True)
    manifest = json.loads((data / "phase3_manifest.json").read_text(encoding="utf-8"))
    phase3_features = list(manifest["feature_columns"])
    features = [c for c in phase3_features if c not in EXCLUDED_FEATURES]
    excluded = {c: EXCLUDED_FEATURES[c] for c in phase3_features if c in EXCLUDED_FEATURES}
    forbidden = FORBIDDEN_FEATURES | set(manifest.get("never_use_as_features", []))
    bad = forbidden & set(features)
    if bad:
        raise SystemExit(f"Forbidden columns in the feature list, stopping: {sorted(bad)}")
    if not features:
        raise SystemExit("No features left to train on")
    for split in ("train", "valid", "test"):
        if not (data / f"model_{split}.parquet").is_file():
            raise SystemExit(f"Missing {data / f'model_{split}.parquet'}")

    print("[1/6] Preparing files (features as DOUBLE; unknown labels: not trained on, scored later) ...", flush=True)
    counts = prepare_files(data, out, a.horizon, features, a.neg_keep, a.duckdb_memory,
                           None if a.nthreads < 1 else a.nthreads)
    for split, c in counts.items():
        unk = (f"{c['unknown_label_rows_dropped']:,} unknown-label rows dropped" if split == "train"
               else f"{c['unknown_label_rows_scored_not_evaluated']:,} unknown-label rows scored, not evaluated")
        print(f"  {split}: {c['rows_used']:,} rows used, {c['positives_used']:,} positives, {unk}", flush=True)
    sp = manifest.get("splits", {})
    try:
        bounds = {"train": (None, sp["train_end"]), "valid": (sp["train_end"], sp["valid_end"]),
                  "test": (sp["valid_end"], sp["test_end"])}
    except KeyError as exc:
        raise SystemExit(f"phase3_manifest.json has no split boundary {exc}; stopping")
    for split, (lo, hi) in bounds.items():
        first, last = counts[split]["first_date"], counts[split]["last_date"]
        if last > hi or (lo is not None and first <= lo):
            raise SystemExit(f"{split} has dates {first}..{last} outside its range "
                             f"({'start' if lo is None else '> ' + lo}..{hi}); stopping")
    for split in ("valid", "test"):
        if counts[split]["rows_at_or_after_failure"]:
            raise SystemExit(f"{split} has rows on or after the failure day; stopping")
    if counts["train"]["positives_used"] == 0 or counts["valid"]["positives_used"] == 0:
        raise SystemExit("Train or validation has no positive rows for this horizon; cannot train.")

    import h2o
    print("[2/6] Starting H2O ...", flush=True)
    h2o.init(port=a.port, max_mem_size=a.max_mem_size, nthreads=a.nthreads)
    h2o.no_progress()
    frames = {}
    for split in ("train", "valid"):
        fr = h2o.import_file((out / f"h2o_{split}.parquet").as_posix())
        fr["y"] = fr["y"].asfactor()
        frames[split] = fr
    if sorted(frames["train"]["y"].levels()[0]) != ["0", "1"]:
        raise SystemExit("Training label does not have both classes")

    print("[3/6] Training models ...", flush=True)
    models = build_models(names, a)
    trained, timings = {}, {}
    for name, model in models.items():
        t1 = time.time()
        model.train(x=features, y="y", training_frame=frames["train"], validation_frame=frames["valid"],
                    weights_column="w")
        timings[name] = round(time.time() - t1, 1)
        trained[name] = model
        print(f"  {name}: {timings[name]} s", flush=True)
        if a.save_models:
            h2o.save_model(model, path=(out / "models").as_posix(), force=True)

    print("[4/6] Scoring validation and test ...", flush=True)
    metas, score_rows = {}, {}
    for split in ("valid", "test"):
        metas[split] = pd.read_parquet(out / f"meta_{split}.parquet")
        scoring = h2o.import_file((out / f"h2o_{split}_all.parquet").as_posix())  # every row, labelled or not
        frame_parts = []
        for name, model in trained.items():
            pred = model.predict(scoring)
            df = scoring[["row_id"]].cbind(pred["p1"]).as_data_frame(use_multi_thread=True)
            df.columns = ["row_id", "score"]
            df["model"] = name
            frame_parts.append(df)
        score_rows[split] = pd.concat(frame_parts, ignore_index=True)
        if len(trained):
            for name in trained:
                got = score_rows[split][score_rows[split]["model"] == name]
                if len(got) != len(metas[split]) or not np.array_equal(
                        np.sort(got["row_id"].to_numpy()), metas[split]["row_id"].to_numpy()):
                    raise SystemExit(f"Scores of {name} on {split} do not line up with the rows; stopping")

    print("[5/6] Metrics ...", flush=True)
    metric_rows, rule_rows, calib = [], [], []
    for split in ("valid", "test"):
        # Metrics use only rows whose outcome is known; unknown-label rows are scored but not evaluated.
        meta = metas[split]
        meta = meta[meta["y"].notna()].copy()
        meta["y"] = meta["y"].astype(int)
        flag_count = int((meta["smart_rule_flag"] > 0).sum())
        rm = rule_metrics(meta["y"], meta["smart_rule_flag"])
        rule_rows.append({"horizon": a.horizon, "split": split, **rm})
        base = evaluate(meta["y"], meta["smart_rule_score"], is_probability=False, rule_flag_count=flag_count)
        metric_rows.append({"horizon": a.horizon, "split": split, "model": "smart_rule_score", **base})
        for name in trained:
            sc = score_rows[split][score_rows[split]["model"] == name].sort_values("row_id")
            sc = sc.set_index("row_id").loc[meta["row_id"]]
            metric_rows.append({"horizon": a.horizon, "split": split, "model": name,
                                **evaluate(meta["y"], sc["score"].to_numpy(), is_probability=True,
                                           rule_flag_count=flag_count)})
            ct = calibration_table(meta["y"], sc["score"].to_numpy())
            ct.insert(0, "model", name)
            ct.insert(0, "split", split)
            ct.insert(0, "horizon", a.horizon)
            calib.append(ct)
    metrics = pd.DataFrame(metric_rows)
    metrics.to_csv(out / "metrics.csv", index=False)
    pd.DataFrame(rule_rows).to_csv(out / "rule_baseline.csv", index=False)
    if calib:
        pd.concat(calib, ignore_index=True).to_csv(out / "calibration.csv", index=False)

    print("[6/6] Saving scores, importance and checks ...", flush=True)
    for split in ("valid", "test"):
        merged = score_rows[split].merge(metas[split], on="row_id", how="left")
        merged["horizon"] = a.horizon
        merged["y"] = merged["y"].astype("Int64")  # empty = outcome unknown (kept as a competitor in the replay)
        merged["label_known"] = merged["y"].notna()
        merged = merged[["horizon", "model", "row_id", "drive_id", "date", "eval_fail_date", "y", "label_known",
                         "w", "score", "smart_rule_flag", "smart_rule_score"]].rename(
            columns={"y": "label", "w": "weight"})
        merged.sort_values(["model", "date", "drive_id"]).to_parquet(out / f"scores_{split}.parquet", index=False)
    for name, model in trained.items():
        try:
            if name in ("gbm", "drf"):
                model.varimp(use_pandas=True).to_csv(out / f"importance_{name}.csv", index=False)
            else:
                coef = model.coef()
                pd.DataFrame({"feature": list(coef), "coefficient": list(coef.values())}).to_csv(
                    out / f"importance_{name}.csv", index=False)
        except Exception as exc:  # importance is a convenience, never fatal
            print(f"  note: no importance for {name}: {exc}", flush=True)

    warnings = []
    for _, row in metrics[metrics["model"] != "smart_rule_score"].iterrows():
        if row["roc_auc"] == row["roc_auc"] and row["roc_auc"] > 0.999:
            warnings.append(f"{row['model']}: {row['split']} AUC {row['roc_auc']:.4f} is suspiciously high, "
                            f"check for leakage")
        if row["average_precision"] == row["average_precision"] and row["average_precision"] > 0.95:
            warnings.append(f"{row['model']}: {row['split']} AP {row['average_precision']:.4f} is suspiciously "
                            f"high, check for leakage")
    # One feature alone should never separate the classes almost perfectly (validation, labelled rows).
    single = single_feature_auc(data / "model_valid.parquet", features,
                                f"label_{a.horizon}d")
    for f, v in single.items():
        if v > 0.99:
            warnings.append(f"feature {f} alone has validation AUC {v:.4f}; check it for leakage")
    health = {}
    for split in ("valid", "test"):
        for name in trained:
            sc = score_rows[split][score_rows[split]["model"] == name]["score"].to_numpy()
            h = score_health(sc)
            health[f"{split}/{name}"] = h
            if h["rows"] and (h["rows_at_or_above_0.999999"] + h["rows_at_or_below_1e-12"]) > SATURATION_WARN_SHARE * h["rows"]:
                warnings.append(f"{name}: {split} has many scores saturated at 0 or 1 (ranking is lost there)")
    for _, row in metrics[metrics["model"] != "smart_rule_score"].iterrows():
        k = max(1, int(np.ceil(0.01 * row["rows"])))
        if row["tied_at_cut_top_1pct"] > TIE_AT_CUT_WARN * k:
            warnings.append(f"{row['model']}: {row['split']} has {int(row['tied_at_cut_top_1pct']):,} rows tied at the "
                            f"top-1% cut (K={k:,}); ranking there is a shared tie")
    checks = {
        "features_used": len(features),
        "forbidden_features_present": [],
        "rows_at_or_after_failure": {s: counts[s].get("rows_at_or_after_failure", 0) for s in ("valid", "test")},
        "unknown_labels_dropped": {s: counts[s]["unknown_label_rows_dropped"] for s in counts},
        "unknown_labels_scored_not_evaluated": {s: counts[s]["unknown_label_rows_scored_not_evaluated"]
                                                for s in ("valid", "test")},
        "rows_scored": {s: counts[s]["rows_scored"] for s in ("valid", "test")},
        "date_ranges": {s: [counts[s]["first_date"], counts[s]["last_date"]] for s in counts},
        "date_ranges_inside_manifest_splits": True,
        "excluded_features": excluded,
        "smart_rule_flag_null_read_as_0": {s: counts[s]["smart_rule_flag_null_read_as_0"] for s in ("valid", "test")},
        "smart_rule_flag_vs_score_mismatch": {s: counts[s]["smart_rule_flag_vs_score_mismatch"] for s in ("valid", "test")},
        "best_single_feature_valid_auc": dict(sorted(single.items(), key=lambda kv: -kv[1])[:5]),
        "score_health": health,
        "warnings": warnings,
    }
    (out / "checks.json").write_text(json.dumps(checks, indent=2), encoding="utf-8")
    info = {
        "created_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "horizon_days": a.horizon, "models": names, "seed": a.seed, "neg_keep": a.neg_keep,
        "protocol_run": (a.neg_keep == PROTOCOL_NEG_KEEP) and not a.not_protocol,
        "kit_version": "v4",
        "features_used": features, "excluded_features": excluded,
        "params": {"gbm_trees": a.gbm_trees, "gbm_max_depth": 6, "gbm_learn_rate": 0.1,
                   "gbm_max_abs_leafnode_pred": GBM_MAX_ABS_LEAF, "drf_trees": a.drf_trees,
                   "drf_depth": a.drf_depth, "min_rows": 50},
        "resources": {"max_mem_size": a.max_mem_size, "nthreads": a.nthreads, "port": a.port,
                      "duckdb_memory": a.duckdb_memory, "cpu_count": os.cpu_count()},
        "train_seconds": timings, "total_seconds": round(time.time() - t0, 1),
        "data_dir": str(data),
        "data_sha256": {f"model_{s}.parquet": _sha256(data / f"model_{s}.parquet") for s in ("train", "valid", "test")},
        "phase3_manifest_sha256": _sha256(data / "phase3_manifest.json"),
        "phase3_splits": manifest.get("splits"), "counts": counts,
        "python": sys.version.split()[0], "h2o": h2o.__version__, "pandas": pd.__version__,
        "duckdb": duckdb.__version__, "numpy": np.__version__, "java": _java_version(),
        "code_sha256": {f: _sha256(Path(__file__).resolve().parent / f) for f in ("train_h2o.py", "metrics.py")},
        "git_commit": _git_commit(),
        "machine": platform.node(), "os": platform.platform(),
    }
    (out / "run_info.json").write_text(json.dumps(info, indent=2, default=str), encoding="utf-8")
    h2o.cluster().shutdown(prompt=False)
    for tmp in ("h2o_train.parquet", "h2o_valid.parquet", "h2o_test.parquet",
                "h2o_valid_all.parquet", "h2o_test_all.parquet"):
        (out / tmp).unlink(missing_ok=True)

    show = metrics[["split", "model", "average_precision", "roc_auc", "recall_at_smart_volume"]]
    print(show.to_string(index=False, float_format=lambda v: f"{v:.4f}"))
    for w in warnings:
        print("WARNING:", w)
    print(f"Done in {info['total_seconds']} s. Output: {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
