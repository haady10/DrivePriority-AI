"""Build and load the replay input: one row per drive per observed day.

Two steps, kept apart on purpose:

1. prepare_from_phase4(): reads the Phase 4 `scores_<split>.parquet` files (30-day required, 14-day and
   7-day when present), puts the frozen model scores side by side and writes `replay_<split>.parquet`.
   No value is changed, no row is dropped, and every scored row stays a candidate (also rows whose
   outcome is unknown). A full-fleet scoring job can write the same columns later; the replay does
   not care where the table came from.
2. load_replay_input(): reads that table into plain numpy arrays for the replay.

Columns of the replay table:
    drive_id, date                     the key (unique)
    fail_date                          first recorded failure of the drive (empty if none).
                                       OUTCOME: never given to the decision code.
    label_7d / label_14d / label_30d   1 / 0 / empty. OUTCOME: used only to describe results.
    smart_count                        number of the five SMART counters above 0 (0-5)
    smart_sum                          raw sum of the five counters (legacy baseline; may be empty)
    ml_h30, gbm_h30, ml_h14, ml_h7     frozen model scores (ml_* = the primary model of that horizon)
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path

import numpy as np

from .hashing import drive_keys, splitmix64, unit_float

# Primary model per horizon and the 30-day runner-up, as frozen in docs/protocol_frozen_before_test.md section 11.
PRIMARY_MODEL = {7: "drf", 14: "drf", 30: "drf"}
RUNNER_UP_30 = "gbm"
EPOCH = date(1970, 1, 1)
OUTCOME_COLUMNS = ("fail_date", "label_7d", "label_14d", "label_30d")
NO_FAIL = -1
COUNT_STEP = float(2 ** 49)   # see add_derived_scores
OUTCOME_DAYS = 31              # a decision on day t is judged by failures up to t + 31 (inspect t+1, 30-day lead)


def sq(path) -> str:
    """A file path made safe inside a single-quoted SQL string (a quote in a folder name is doubled)."""
    return Path(path).as_posix().replace("'", "''")


def day_number(d: date) -> int:
    """Days since 1970-01-01 (the integer form of a date used everywhere in the replay)."""
    return (d - EPOCH).days


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for block in iter(lambda: fh.read(1 << 20), b""):
            h.update(block)
    return h.hexdigest()


def prepare_from_phase4(phase4_dir: Path, split: str, out_path: Path, first_day: date, last_day: date,
                        *, allow_test: bool = False, duckdb_memory: str = "4GB") -> dict:
    """Turn Phase 4 score files into the replay table. Returns a small manifest (also written as JSON).

    The test split is refused unless allow_test is set (the final run after the independent review).
    The run stops if anything looks wrong: duplicate drive-days, rows on or after the failure day,
    dates outside the split, horizons that do not cover the same rows, scores outside [0, 1].
    """
    import duckdb

    if split not in ("valid", "test"):
        raise SystemExit(f"Unknown split: {split}")
    if split == "test" and not allow_test:
        raise SystemExit("The test split is locked. It is replayed once, after the independent review, "
                         "with --final-test-run.")
    phase4_dir = Path(phase4_dir)
    files = {h: phase4_dir / f"h{h}" / f"scores_{split}.parquet" for h in (30, 14, 7)}
    if not files[30].is_file():
        raise SystemExit(f"Missing {files[30]} (the 30-day scores are required)")
    have = [h for h in (30, 14, 7) if files[h].is_file()]
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect()
    con.execute(f"SET memory_limit='{duckdb_memory}'")
    con.execute(f"SET temp_directory='{sq(out_path.parent / '_duckdb_tmp')}'")
    for h in have:
        con.execute(f"CREATE TEMP VIEW s{h} AS SELECT * FROM read_parquet('{sq(files[h])}')")
    models30 = {r[0] for r in con.execute("SELECT DISTINCT model FROM s30").fetchall()}
    if PRIMARY_MODEL[30] not in models30:
        raise SystemExit(f"The 30-day file has no '{PRIMARY_MODEL[30]}' scores; found {sorted(models30)}")
    has_gbm = RUNNER_UP_30 in models30

    select = ["b.drive_id", "CAST(b.date AS DATE) AS date", "CAST(b.eval_fail_date AS DATE) AS fail_date",
              "CAST(b.label AS TINYINT) AS label_30d", "CAST(b.smart_rule_count AS TINYINT) AS smart_count",
              "CAST(b.smart_rule_score AS DOUBLE) AS smart_sum", "CAST(b.score AS DOUBLE) AS ml_h30"]
    joins = []
    if has_gbm:
        select.append("CAST(g.score AS DOUBLE) AS gbm_h30")
        joins.append(f"LEFT JOIN (SELECT drive_id, date, score FROM s30 WHERE model = '{RUNNER_UP_30}') g "
                     f"ON g.drive_id = b.drive_id AND g.date = b.date")
    for h in (14, 7):
        if h in have:
            select += [f"CAST(x{h}.label AS TINYINT) AS label_{h}d", f"CAST(x{h}.score AS DOUBLE) AS ml_h{h}"]
            joins.append(f"LEFT JOIN (SELECT drive_id, date, label, score FROM s{h} "
                         f"WHERE model = '{PRIMARY_MODEL[h]}') x{h} "
                         f"ON x{h}.drive_id = b.drive_id AND x{h}.date = b.date")
    con.execute(f"""
        CREATE TEMP TABLE replay AS
        SELECT {', '.join(select)}
        FROM (SELECT * FROM s30 WHERE model = '{PRIMARY_MODEL[30]}') b
        {' '.join(joins)}""")

    score_cols = ["ml_h30"] + (["gbm_h30"] if has_gbm else []) + [f"ml_h{h}" for h in (14, 7) if h in have]
    n, d0, d1, bad_after, n_drives, n_fail = con.execute("""
        SELECT count(*), min(date), max(date),
               count(*) FILTER (WHERE fail_date IS NOT NULL AND fail_date <= date),
               count(DISTINCT drive_id), count(DISTINCT drive_id) FILTER (WHERE fail_date IS NOT NULL)
        FROM replay""").fetchone()
    dup = con.execute("""SELECT coalesce(sum(c - 1), 0) FROM (SELECT count(*) AS c FROM replay
                          GROUP BY drive_id, date HAVING count(*) > 1)""").fetchone()[0]
    problems = []
    if dup:
        problems.append(f"{int(dup):,} duplicate drive-day rows")
    if bad_after:
        problems.append(f"{bad_after:,} rows on or after the drive's failure day")
    if d0 < first_day or d1 > last_day:
        problems.append(f"dates {d0}..{d1} outside the split {first_day}..{last_day}")
    for h in have:
        rows_h = con.execute(f"SELECT count(*) FROM s{h} WHERE model = '{PRIMARY_MODEL[h]}'").fetchone()[0]
        if rows_h != n:
            problems.append(f"the {h}-day file has {rows_h:,} rows for {PRIMARY_MODEL[h]}, the 30-day file {n:,}")
    for c in score_cols:
        miss, lo, hi = con.execute(f"SELECT count(*) FILTER (WHERE {c} IS NULL), min({c}), max({c}) FROM replay").fetchone()
        if miss:
            problems.append(f"{miss:,} rows have no {c} score (the horizons do not cover the same rows)")
        if lo is not None and (lo < 0 or hi > 1):
            problems.append(f"{c} is outside [0, 1]: {lo}..{hi}")
    inconsistent = con.execute("""SELECT count(*) FROM (SELECT drive_id FROM replay GROUP BY drive_id
                                  HAVING count(DISTINCT fail_date) > 1)""").fetchone()[0]
    if inconsistent:
        problems.append(f"{inconsistent:,} drives have more than one failure date")
    if problems:
        raise SystemExit("Replay input NOT written. Problems found in the Phase 4 score files: " + "; ".join(problems))

    con.execute(f"""COPY (SELECT * FROM replay ORDER BY date, drive_id)
                    TO '{sq(out_path)}' (FORMAT parquet, COMPRESSION zstd)""")
    con.close()
    manifest = {
        "split": split, "rows": n, "drives": n_drives, "drives_with_failure_date": n_fail,
        "first_date": str(d0), "last_date": str(d1), "horizons_present": have, "score_columns": score_cols,
        "primary_model": PRIMARY_MODEL, "runner_up_30": RUNNER_UP_30 if has_gbm else None,
        "inputs_sha256": {str(files[h].relative_to(phase4_dir)): sha256_file(files[h]) for h in have},
        "output": out_path.name, "output_sha256": sha256_file(out_path),
    }
    out_path.with_suffix(".manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return manifest


@dataclass
class ReplayData:
    """The replay table as arrays. Rows are sorted by day; `day_start[i]:day_start[i+1]` are the rows of
    calendar day `first_day + i` (an empty slice means the source has no data for that day)."""
    first_day: int
    last_day: int
    day: np.ndarray            # int32 per row
    drive: np.ndarray          # int32 per row: index into the per-drive arrays
    day_start: np.ndarray      # int64, length n_days + 1
    drive_ids: np.ndarray      # object, per drive
    drive_key: np.ndarray      # uint64, per drive (machine-independent)
    scores: dict = field(default_factory=dict)       # name -> float64 per row (NaN = missing)
    # ---- outcomes: used only AFTER decisions (events.py, prediction.py); the engine never sees them
    fail_day: np.ndarray = None                       # int32 per drive, NO_FAIL when none
    labels: dict = field(default_factory=dict)        # horizon -> int8 per row (-1 = unknown)

    @property
    def n_days(self) -> int:
        return self.last_day - self.first_day + 1

    @property
    def n_drives(self) -> int:
        return len(self.drive_ids)

    def rows_of_day(self, i: int) -> slice:
        return slice(int(self.day_start[i]), int(self.day_start[i + 1]))

    def subset_drives(self, keep: np.ndarray) -> "ReplayData":
        """A copy with only the drives where `keep` is True (whole drives; used for the fleet-competition
        sensitivity). Drive numbering is kept, so results can be joined back."""
        rows = keep[self.drive]
        day = self.day[rows]
        return ReplayData(
            first_day=self.first_day, last_day=self.last_day, day=day, drive=self.drive[rows],
            day_start=_day_starts(day, self.first_day, self.last_day), drive_ids=self.drive_ids,
            drive_key=self.drive_key, scores={k: v[rows] for k, v in self.scores.items()},
            fail_day=np.where(keep, self.fail_day, NO_FAIL).astype(np.int32),
            labels={k: v[rows] for k, v in self.labels.items()})


def _day_starts(day: np.ndarray, first_day: int, last_day: int) -> np.ndarray:
    return np.searchsorted(day, np.arange(first_day, last_day + 2), side="left").astype(np.int64)


def load_replay_input(path: Path, first_day: date, last_day: date) -> ReplayData:
    """Read replay_<split>.parquet (a file or a folder of parquet files with the same columns)."""
    import pyarrow as pa
    import pyarrow.compute as pc
    import pyarrow.dataset as ds

    dataset = ds.dataset(str(path), format="parquet")
    names = set(dataset.schema.names)
    for required in ("drive_id", "date", "fail_date", "smart_count", "ml_h30"):
        if required not in names:
            raise SystemExit(f"Replay input {path} has no column '{required}'")
    tbl = dataset.to_table()
    ids = pc.dictionary_encode(tbl["drive_id"]).unify_dictionaries().combine_chunks()
    drive = ids.indices.to_numpy(zero_copy_only=False).astype(np.int32)
    drive_ids = np.array(ids.dictionary.to_pylist(), dtype=object)
    day = tbl["date"].cast(pa.date32()).cast(pa.int32()).to_numpy().astype(np.int32)

    def col(name, fill):
        return tbl[name].combine_chunks().fill_null(fill).to_numpy(zero_copy_only=False)

    fail_row = tbl["fail_date"].cast(pa.date32()).cast(pa.int32()).combine_chunks().fill_null(NO_FAIL).to_numpy()
    scores = {"smart_count": col("smart_count", 0).astype(np.float64)}
    if "smart_sum" in names:
        scores["smart_sum"] = tbl["smart_sum"].combine_chunks().cast(pa.float64()).fill_null(float("nan")).to_numpy()
    for name in sorted(names):
        if name.startswith(("ml_h", "gbm_h")):
            scores[name] = tbl[name].combine_chunks().cast(pa.float64()).fill_null(float("nan")).to_numpy()
    labels = {int(n[6:-1]): col(n, -1).astype(np.int8) for n in names if n.startswith("label_") and n.endswith("d")}
    del tbl

    if len(day) and not np.all(day[1:] >= day[:-1]):          # the replay needs rows in day order
        order = np.argsort(day, kind="stable")
        day, drive, fail_row = day[order], drive[order], fail_row[order]
        scores = {k: v[order] for k, v in scores.items()}
        labels = {k: v[order] for k, v in labels.items()}
    d0, d1 = day_number(first_day), day_number(last_day)
    if len(day) and (day[0] < d0 or day[-1] > d1):
        raise SystemExit(f"Replay input has dates outside {first_day}..{last_day}; stopping")

    # Failure dates later than the outcome window of the last decision day are not needed for any result of
    # this split. They are dropped here, so a validation replay never holds a later (test-period) failure date.
    fail_row = np.where(fail_row > d1 + OUTCOME_DAYS, NO_FAIL, fail_row).astype(fail_row.dtype)
    n_drives = len(drive_ids)
    fail_day = np.full(n_drives, NO_FAIL, dtype=np.int32)
    fail_day[drive] = fail_row
    if np.any(fail_day[drive] != fail_row):
        raise SystemExit("A drive has two different failure dates in the replay input; stopping")
    f_row = fail_day[drive]
    if np.any((f_row != NO_FAIL) & (day >= f_row)):
        raise SystemExit("Replay input has rows on or after a drive's failure day; stopping")
    key = day.astype(np.int64) * n_drives + drive
    if len(np.unique(key)) != len(key):
        raise SystemExit("Replay input has duplicate drive-day rows; stopping")
    return ReplayData(first_day=d0, last_day=d1, day=day, drive=drive, day_start=_day_starts(day, d0, d1),
                      drive_ids=drive_ids, drive_key=drive_keys(drive_ids), scores=scores,
                      fail_day=fail_day, labels=labels)


def add_derived_scores(data: ReplayData) -> None:
    """Add the score sources that are simple functions of the columns (protocol section 5).

    smart_count_sum: SMART count first, the raw counter sum only decides inside equal counts (row-wise formula).
    random:          a fixed pseudo-random number per drive and day (the no-skill reference).
    """
    if "smart_sum" in data.scores and "smart_count_sum" not in data.scores:
        # A fixed formula per row (no ranking over the period, so no later row can influence an earlier one):
        # count * 2^49 + (sum + 1). Sums are whole numbers below 2^49, so the value is exact in float64 and
        # orders rows by count first and by sum inside equal counts. A missing sum counts as below zero.
        s = np.nan_to_num(data.scores["smart_sum"], nan=-1.0)
        data.scores["smart_count_sum"] = data.scores["smart_count"] * COUNT_STEP + np.clip(s + 1.0, 0.0, COUNT_STEP - 1.0)
    if "random" not in data.scores:
        mixed = splitmix64(data.drive_key[data.drive] ^ splitmix64(data.day.astype(np.uint64) + np.uint64(777)))
        data.scores["random"] = unit_float(mixed)
