# Amendment A1: results (20 Nov 2026)

**Label.** Post hoc (amendment A1). Not blind: the frozen 2024 and test results, the by-make table and the
rotating-tie results had been seen. The amendment (`phase5_amendment_A1_fair_baselines_2026-11-20.md`, commit
8e3771d) was written and committed before anything below was run, and it is not changed here.

## Runs

| | 2024 | Test (2025-01 to 2026-06) |
|---|---|---|
| Build: rows checked (five-indicator count equal on every row) | 104,120,817, 0 problems | 178,021,959, 0 problems |
| Replay: cells | 168 | 168 |
| Reproduction gate (S1 and M1 against the frozen results) | passed, 344 values | passed, 344 values |
| Engine rule violations | 0 | 0 |
| Run time on the lab PC (32 cores) | 1,424 s | 1,644 s |
| Order | first | second, once, with `--post-hoc-test` after the clean full 2024 run |

Code: commit 8e3771d (clean checkout, recorded in `run_info.json`). Archives: `published_results/phase5_A1/`.

**Clock note.** The lab PC's clock was about four hours behind Gulf Standard Time on 20 Nov: its console shows
13:12–14:20 for runs that took place between about 17:12 and 18:20 GST (the operator's messages with the outputs
were sent at 17:44 and 18:37 GST), and `run_info.json` records `finished_utc` 10:20 UTC for the test run. The order
of events is fixed by content, not by that clock: both run records name commit 8e3771d, which was created at
14:34 GST, and the test run is refused by the code unless the 2024 run of the same commit is in the run register.

## Reporting rule A1.6 (budget 20)

Applied with `fair_baselines.py summarise` (`published_results/phase5_A1/wording_rule.csv`):
**no policy allows either claim.**

| Policy | "ML better" fails against | "SMART better" fails against |
|---|---|---|
| Next day, fresh | S2, S4, S5 (rotating ties) | S1–S3 (both), S4, S5 (frozen) |
| Next day, queue | S2, S4, S5 (rotating ties) | S1–S3 (both), S4, S5 (frozen) |
| Cooldown, fresh | S1–S3 (both), S4, S5 (rotating) | S1–S5 (frozen); S3, S4, S5 (rotating) |
| Cooldown, queue | S1–S3 (both), S4, S5 (rotating) | S1–S5 (frozen); S4, S5 (rotating) |

## Timely capture, test period, budget 20 (%)

| Ties | Policy | S1 | S2 | S3 | S4 | S5 | M1 | M2 |
|---|---|---|---|---|---|---|---|---|
| Frozen | Next day, fresh | 11.8 | 9.2 | 15.6 | 15.0 | 10.6 | 26.5 | 29.1 |
| | Next day, queue | 24.0 | 25.2 | 27.6 | 26.7 | 24.1 | 35.2 | 41.0 |
| | Cooldown, fresh | 43.5 | 45.4 | 43.8 | 42.0 | 39.5 | 44.1 | 53.6 |
| | Cooldown, queue | 47.9 | 47.3 | 48.3 | 44.8 | 41.3 | 47.6 | 54.3 |
| Rotating | Next day, fresh | 14.6 | 28.4 | 19.7 | 34.1 | 34.9 | 26.5 | 29.1 |
| | Next day, queue | 32.8 | 28.5 | 29.4 | 39.1 | 38.1 | 35.2 | 41.0 |
| | Cooldown, fresh | 50.4 | 47.9 | 45.3 | 48.6 | 47.1 | 44.1 | 53.6 |
| | Cooldown, queue | 52.8 | 49.7 | 49.8 | 50.4 | 47.8 | 47.6 | 54.3 |

## What the results say

- The confirmatory contrast (M1 − S1, next day, fresh, frozen ties: +14.7) stands as computed, but it does not
  survive fairer baselines as an ML advantage: with rotating ties, M1 − S4 = −7.6 [−8.9, −6.2] (2024: −3.8).
- Under the cooldown, M1 is ahead of none of S1–S5 with rotating ties, and of none of S1–S3 with frozen ties.
- Quotas help both kinds of score. M2 − M1 is +2.6 to +9.5 (test) and +1.9 to +5.4 (2024). M2 is ahead of
  both S4 and S5 in 7 of 8 policy × tie combinations in each period; the exception is next day, fresh, rotating ties.
  This is descriptive (not covered by the A1.6 rule).
- With frozen ties the coarse quota counts repeat the same drives (S4, next day, fresh: 2,835 distinct drives and
  15.0% capture, against 8,407 and 34.1% with rotating ties).
- Across the 56 cells at budget 20, capture rises with distinct drives inspected (Spearman 0.95 test, 0.91 in 2024);
  descriptive, not a matched-effort comparison.

Independent check (Linux, 20 Nov): M1 − S4 (next day, fresh, rotating ties) recomputed from
`event_capture.parquet` gives −7.58 with a 2,000-resample interval of [−8.9, −6.2], as in `contrasts.csv`; the
wording rule recomputed from the two zips is identical to the lab PC output.

## Not done (as listed in A1)

- The 112 unplaced failures (A1.7): not run yet (optional; needs the snapshot).
- Drive-model and calendar-week intervals for the A1 differences (A1.6 item 6, optional): not computed.
- A matched-effort comparison: not part of A1.

## Clarifications after the review of manuscript V4 (20 Nov 2026, evening)

These do not change any computed value in the archives.

1. **Quota allocation is approximately proportional, not "within one place".** A1.2 and the `QuotaRanker`
   docstring said that any first K places hold each make in proportion "within one slot". That is not guaranteed.
   The prefix of the (r + 0.5)/n order is the Webster (Sainte-Laguë) allocation of K places, and Webster can miss
   the exact share by more than one place: 73/9/9/9 drives and K = 7 give 4/1/1/1, against an exact share of
   5.11 for the largest make (reproduced with the repository code; test
   `test_quota_can_miss_the_exact_share_by_more_than_one_place`). The provable bound is
   |a − K·s| ≤ 1/2 + s·n/2 for a make with share s among n makes with drives. The code is unchanged; the docstring
   and the old test assertion were corrected. The empirical deviation on the observed days was not computed (it
   needs the daily drive counts by make from the replay input).
2. **A1 re-evaluated the overlapping frozen policies.** The S1 and M1 cells with frozen ties are the frozen
   policies E1–E8. A1 re-evaluated them in both periods only to verify that the post hoc engine reproduces the
   frozen results (the reproduction gate). The original runs and their archives were not changed. Statements such
   as "the test period was not rerun for the frozen policies" are therefore wrong; the correct statement is "the
   pre-specified batch was run once; the A1 batch re-evaluated the S1 and M1 policies for verification".
3. **Model-level intervals** (A1.6 item 6) were added on 20 Nov from the saved per-event table, with the method of
   Table IX (`tools/phase5_post_test/a1_model_cluster_bootstrap.py`, output `A1_model_cluster_bootstrap.csv`; the
   script first reproduces the frozen Table IX intervals exactly, `A1_model_cluster_check.json`). Under drive-model
   resampling most A1 differences include zero, e.g. M1 − S4 (next day, fresh, rotating ties) −7.6 with a model
   interval of −22.3 to +8.7 (test) and −3.8 with −18.1 to +12.8 (2024).
4. **Side-file integrity.** The replay loader now also compares the drive_id of every side-file row with the
   replay input (code change after the runs; test `test_side_files_with_swapped_drives_are_refused`). For the A1
   runs that were made, the side files used by the replay are byte-identical (SHA-256 in `run_info.json`) to the
   files the build step wrote after its (drive_id, date) join (`build_*_manifest.json`), in both periods.
5. **Equal effort (A1.6 item 5)** asks for capture against distinct drives and against completed inspections at the
   three budgets. Spearman correlations over the 56 cells of each budget (distinct / completed): test B10
   0.96 / 0.06, B20 0.95 / 0.26, B50 0.91 / 0.60; 2024 B10 0.96 / 0.43, B20 0.91 / 0.71, B50 0.82 / 0.83.
   Descriptive only; distinct drives are an outcome of the policy, so this does not separate breadth from ranking depth.
6. **Wording.** "Level with S3" (team notes) is wrong for the cooldown with rotating ties: M1 − S3 is −1.1
   [−2.5, +0.2] with fresh Top-K (not established either way) and −2.1 [−3.3, −1.0] with the queue (M1 behind).
   Quotas raise the capture of the SMART count only under next-day re-inspection; under the cooldown S4 − S1 is
   −1.5 and −3.1 (frozen ties) and −1.9 and −2.4 (rotating ties) points on the test period.
