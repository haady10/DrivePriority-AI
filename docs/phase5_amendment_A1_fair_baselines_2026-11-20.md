# Phase 5 amendment A1: post hoc fair SMART baselines and the unplaced failures (20 Nov 2026)

**Status.** Written and committed on 20 Nov 2026, before any analysis below was run. The frozen protocol file
(`configs/phase5_protocol_v1.json`, SHA-256 `22110661…60af6`) is not changed. The frozen 2024 and test results in
`published_results/phase5/` are not changed and are not replaced. Everything this amendment produces is labelled
**post hoc (amendment A1)** wherever it is reported.

**Why.** The strict publication review of 19 Nov 2026 (finding F1) and our own check of 14 Nov
(`phase5_finding_smart_attribute_availability`) show that the frozen SMART baseline (count of SMART 5, 187, 188,
197 and 198 above zero, a missing value read as zero) can only reach a count of 4 or 5 on Seagate drives, because
the other makes do not report SMART 187 and 188. At 20 inspections per 10,000 drives, the Seagate drives at count
4 or 5 outnumber the daily slots on every day of both periods, so the next-day fresh Top-K SMART policy (E1)
inspects only Seagate drives. The ML-versus-SMART difference therefore mixes ranking skill with manufacturer
reach. A fair comparison needs SMART baselines that every make can reach.

**What has already been seen (stated openly).** The frozen 2024 and test results, the by-make table, the
rotating-tie results at budget 20 and the SMART signal shares by make. These analyses are therefore **not blind**.
The test period is no longer untouched for them. They are sensitivity analyses that test whether a reported
difference survives fairer baselines; they cannot be confirmatory. A confirmatory test needs a new, untouched
period.

## A1.1 Inputs (no new model, no new feature)

- The frozen full-fleet replay inputs of 2024 and of the test period (`replay_valid`, `replay_test`: monthly
  `part_YYYY-MM.parquet`), exactly as used for the frozen results. Their SHA-256 are recorded in the run record.
- The full-fleet feature files of the same periods (`features_valid_*.parquet`, `features_test_*.parquet`), only
  to read SMART 5, 197 and 198 for each drive-day. The build step checks, on every row, that it finds the same
  drive-day and that the five-indicator count recomputed from these files equals the `smart_count` of the replay
  input. A single difference stops the run.
- Nothing is retrained, rescored or tuned. The ML score is the frozen 30-day random forest score `ml_h30`.

## A1.2 Scores (seven, fixed now)

| Name | Definition |
|---|---|
| S1 `smart_count` | The frozen baseline (reproduction). |
| S2 `smart_count3` | Number of SMART 5, 197 and 198 above zero (a missing value is not above zero, as in S1). These three are reported in at least 94% of rows of every make (finding note of 14 Nov, 2025-06-02 fleet). |
| S3 `smart_count_make_pct` | Within-make percentile: for each day and make, the share of that make's observed drives with a strictly lower S1. Uses only the rows of that day. |
| S4 `smart_count+quota` | S1 ranked with make quotas (below). |
| S5 `smart_count3+quota` | S2 ranked with make quotas. |
| M1 `ml_h30` | The frozen ML score (reproduction). |
| M2 `ml_h30+quota` | M1 ranked with make quotas. |

**Make** comes from the model string, the same rule as Table XI: `ST` → Seagate, `HGST` → HGST, `TOSHIBA` →
Toshiba, `WD` or `WUH` → WDC, anything else → Other.

**Make quotas (Webster / Sainte-Laguë order).** On each day, inside each make the drives are ordered by score and
then by the tie key; a drive at place *r* (0, 1, 2, …) of a make with *n* observed drives that day gets the key
(*r* + 0.5) / *n*. All drives are then ordered by this key, then by the tie key. Any first *K* places of this order
hold each make in proportion to its share of the day's observed drives (the Webster apportionment, within one
slot). The cooldown and queue rules are applied to this order exactly as to a plain score order, so under those
rules the shares are close to, but not exactly, proportional. Nothing in the quota is tuned.

## A1.3 Policies and settings (all from the frozen protocol)

- The four frozen policies: next-day or 7-day cooldown re-inspection × fresh Top-K or the primary queue
  (q2-3-3).
- Budgets 10, 20 and 50 per 10,000 observed drives (the protocol's primary and sensitivity budgets).
- Both tie orders of the protocol: frozen per drive (the primary mode) and rotating per day (the sensitivity mode).
- Ten tie seeds (0–9). Engine, events, the timely window (7–30 days before failure), the capacity rule and the
  event bootstrap (2,000 resamples, seed 20261111) are the frozen code and settings.

That is 7 scores × 4 policies × 3 budgets × 2 tie orders = 168 cells per period.

## A1.4 Reproduction gate

S1 and M1 under frozen ties at budgets 10, 20 and 50, and under rotating ties at budget 20, must reproduce the
frozen cell results (coverage, completed inspections, distinct drives inspected, repeat share) to 1e-12, and the
per-event capture shares of E1–E8 must equal `event_coverage.parquet` of the frozen archive. If anything differs,
the run is marked failed and nothing from it is reported.

## A1.5 Order of work

2024 first, then the test period, with the same commit and the same settings. The test run is refused unless a
2024 run of the same commit finished on the same machine with the reproduction gate passed. If the 2024 run shows
a bug, the fix is written up as amendment A1-b before anything is run again. The test period is run once; a crash
before the end may be restarted, and every start is recorded.

## A1.6 How the results will be reported (fixed now)

1. **All 336 cells** (168 per period) go into a supplementary table. None is left out.
2. **Paired differences** with event-bootstrap 95% intervals, for every policy, budget and tie order:
   M1 − S*k* for *k* = 1…5; M2 − S4 and M2 − S5 (the make-balanced comparisons); M2 − M1 and S4 − S1 (what the
   quota does to each score).
3. **Wording rule.** In a policy cell at budget 20 we may write that the ML ranking captured more failures in time
   than SMART only if, in the test period, the 95% interval of M1 − S*k* excludes zero on the same side for
   **every** *k* = 1…5 under **both** tie orders, and the 2024 estimates have the same sign. (A claim must survive
   every fair baseline: an intersection-union rule, so no multiplicity correction is needed for this joint claim.)
   Otherwise we write that ML and SMART are not distinguishable against at least one fair baseline, and name it.
   The same rule with the sign reversed applies to a claim that SMART did better.
4. The **Seagate share** of completed inspections is reported for every cell.
5. **Equal effort** is descriptive only: coverage against distinct drives inspected and against completed
   inspections across the three budgets. No interval and no new replay.
6. Model-level and calendar-week intervals for the headline differences may be added from the saved per-event
   table, by the same post hoc tools as for the frozen results. No replay.

## A1.7 The 112 unplaced failures (read-only)

Itemise, from the snapshot, every HDD-cohort failure that is in none of the training selection, the 2024 replay
and the test replay, with its reason: (a) no record before the failure day at all; (b) earlier records only in an
earlier period (as for the six failures on 1 January 2025); (c) an earlier record in the same period (not
explained by the selection rule; each one is looked at). The query reconciles its totals with Table II
(29,222 / 28,987 / 17,442 / 4,607 / 6,826) and reports any difference. Nothing is replayed.

## A1.8 What will not be done

No retraining, no new feature, no new score beyond the seven above, no other budget, endpoint or queue setting,
no choice of the "best" SMART baseline after seeing results, and no change to the frozen results.
