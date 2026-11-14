# How the Phase 5 test results will be read (written before any test number exists)

Written 13 Nov 2026 about 16:30 Dubai time, while the test-period features build was still running and before
any test score, replay or result existed. It changes no setting: `configs/phase5_protocol_v1.json`, the
simulation code and the authorization are unchanged. It is a stricter reading of `docs/phase5_protocol.md`
section 16, written after the 2024 validation results were seen and after two AI reviews of them (the
committed review `docs/phase5_review_validation_fullfleet_2026-11-13.md` and a second, independent AI review
of the same package, "accept with changes"). A human independent review is still open.

The study is a decision-support simulation on old data. It does not show that any failure was prevented.

## 1. Confirmatory contrast (one)

`delta_ml_minus_smart|next|fresh` (ML 30-day DRF minus SMART count, next-day reinspection, fresh Top-K,
budget 20 per 10,000). Validation 2024: +14.1 points [+13.0, +15.2].

## 2. "Replicated" (stricter than section 16)

A contrast is called replicated only if
- the validation interval excludes zero, and
- the test estimate has the same sign, and
- the test interval excludes zero.

Section 16 does not ask the validation interval to exclude zero; this reading does, so that a pattern that was
never shown on validation cannot be "replicated" by the test.

## 3. Contrasts that enter the test as "not established on validation"

- ML minus SMART under the 7-day cooldown (fresh and queue). On validation the sign depends on the tie rule
  (+1.8 frozen drive ties, -3.7 day-rotating ties for fresh Top-K; -0.6 and -1.2 with the queue), on the
  SMART variant (+0.4 / -0.3 against `smart_count_sum`) and on the ML model (GBM: -1.6 / -2.1), and it drifts
  down during 2024 (fresh Top-K: +4.5 for failures February-June, -0.3 for July-December).
  These are reported for the test under BOTH tie modes and against `smart_count_sum`, and described as
  "no reliable difference" unless all of them agree in sign and exclude zero.
- ML minus SMART next-day with the queue (+4.6 on validation) is reported, with the per-make table, as
  secondary.

## 4. What is reported with every headline difference

- Per-make table (Seagate, HGST, Toshiba, WDC) of timely capture for every cell shown. On validation the
  next-day SMART cell captured only Seagate failures (0% of 2,410 non-Seagate events).
- The stronger-baseline check (protocol section 5) is triggered: `smart_count_sum` beat `smart_count` on
  validation in three of four cells (13.3 vs 12.3, 29.3 vs 28.6, 47.2 vs 45.8; 51.1 vs 51.4). The test
  report therefore also shows ML against `smart_count_sum`.
- `queue_removed_cap_fell` for every queue cell (rule C-2; 0 in 2024).
- The note that the 2024 ML scores are not fully out of sample (2024 was used for early stopping and to pick
  DRF as primary in Phase 4); the test period is the clean estimate.
- Intervals hold for this fleet mix and these events only (drive-cluster bootstrap; not retraining, not a new
  fleet mix). Resampling whole drive models gives much wider intervals on validation.

## 5. Reading of the design

Queue and cooldown act as one factor in practice: the measured typical gap between two inspections of the
same drive is about 1, 3, 7 and 9 days in cells E1/E2, E5/E6, E3/E4 and E7/E8 (the queue is always full, so
almost every queued inspection waits 2 days). The report states RQ1 as "how the ML advantage changes with the
reinspection interval" and does not describe separate queue and cooldown effects. The 1K/2K/2-day queue
setting equals fresh Top-K exactly and is not counted as a queue sensitivity.

## 6. Not changed after the test

The primary score stays `ml_h30` even though `ml_h7` did better on validation (RQ3 result). No setting is
chosen from test numbers.

## 7. Provenance to be committed after the run

SHA-256 of every `replay_test/part_*.parquet` and of the features and replay manifests, next to
`run_info.json` and the lock register entry, because `run_info.json` leaves `input_sha256` empty for a
folder input.

## 8. Input completeness check (added 13 Nov about 16:25, still before any test number exists)

A second AI review showed that `checks.json` does not validate the replay input: a synthetic input missing
August to December still gave `all_passed: true`. The test results are accepted only if, in addition:
- the features step reports 0 missing and 0 different rows against the Phase 3 test sample for every quarter
  2025Q1..2026Q2;
- the scoring step reports its sample reconciliation within 1e-6 for every month 2025-01..2026-06 and writes
  18 monthly part files;
- `daily_capacity.csv` has observed drives > 0 and K > 0 on every decision day from 2025-01-01 to 2026-06-29,
  with no gap and no sudden drop that the fleet does not explain;
- the SHA-256 of every part file and both manifests is committed with `run_info.json`.
If any of these fails, the replay is not reported; a repeat is a logged deviation (protocol section 19).

## 9. Corrections to how some columns are described (from the same review; no change to primary results)

- `late_only` includes drives that were also inspected more than 30 days before failure; it is described as
  "no timely inspection, at least one inspection in the last 7 days", or recomputed strictly from the logs.
- The RQ3 "actionable" AP is affected by the label gap in the last 30 days of each split (failing rows kept,
  healthy rows dropped); it is reported with that caveat for all scores alike.
- RQ2 `confirm_c` rules rank by the minimum of the last c observed scores; they are not literal repeat alerts.
- The no-future check can false-alarm when K = 0 (tiny fleets only; not the case for the full fleet).
