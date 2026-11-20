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
