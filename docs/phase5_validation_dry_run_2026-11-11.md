# Phase 5 dry run on the 2024 validation year (10% evaluation sample)

Date: 2026-11-11. Machine: coordinator laptop (Windows, Python 3.11.9). Run time: 3,684 s (61 minutes).
Protocol: `phase5-v1`, hash `065900cc…f2d2`. Code: branch `phase5/simulation`, commit `61ba45c`.

**Status of these numbers: exploratory.** They come from the 10% sample of drives and from the validation
year. Rule R18 says the primary result must come from all HDDs. The test split (2025 to June 2026) was not
opened. This is a simulation of inspection slots. Nothing here says a failure was prevented.

## Input and checks

- 10,467,899 drive-days, 32,885 drives, 365 decision days, about 28,500 drives observed per day.
- Budget 20 per 10,000 drives gives about 57 inspection slots per day (20,688 slots in the year).
- 495 first failures in the year: 464 eligible events, 30 excluded as burn-in, 1 with no opportunity day.
- All built-in checks passed (`checks.json`: 0 engine rule violations, same event set in every cell,
  no row on or after a failure day, no duplicate drive-day, cut replay equals full replay prefix).
- The last calendar day (2024-12-31) has no decision row by design: its inspection day is outside the split.

## RQ1: the eight cells (timely capture = inspected 7 to 30 days before the failure)

| Cell | Score | Reinspection | Scheduling | Timely capture | 95% interval | Distinct drives inspected |
|---|---|---|---|---|---|---|
| E1 | SMART count | next day | fresh Top-K | 12.2% | 9.4 to 15.0 | 205 |
| E2 | ML (DRF 30d) | next day | fresh Top-K | 26.1% | 22.2 to 30.0 | 374 |
| E5 | SMART count | next day | queue | 29.8% | 25.8 to 33.9 | 485 |
| E6 | ML (DRF 30d) | next day | queue | 33.0% | 28.9 to 37.3 | 638 |
| E3 | SMART count | 7-day cooldown | fresh Top-K | 46.1% | 42.0 to 50.2 | 864 |
| E4 | ML (DRF 30d) | 7-day cooldown | fresh Top-K | 46.6% | 42.0 to 51.3 | 1,066 |
| E7 | SMART count | 7-day cooldown | queue | 52.8% | 48.2 to 57.3 | 1,036 |
| E8 | ML (DRF 30d) | 7-day cooldown | queue | 50.4% | 45.9 to 55.2 | 1,243 |

Random selection with the same budget: 3.7% to 4.3%.

ML minus SMART (paired drive-cluster bootstrap, 2,000 resamples):

| Policy | Difference | 95% interval | Excludes zero |
|---|---|---|---|
| next day, fresh | +13.9 points | +10.6 to +17.4 | yes |
| next day, queue | +3.2 | -1.0 to +7.4 | no |
| cooldown, fresh | +0.5 | -2.8 to +3.9 | no |
| cooldown, queue | -2.3 | -5.8 to +1.0 | no |

Policy effects: 7-day cooldown adds +34 points for SMART and +20 for ML (fresh Top-K); the queue adds +18 for
SMART and +7 for ML (next day). All these intervals exclude zero. The three-way interaction is +7.9 points
(+2.7 to +13.2).

Without a cooldown, 98% to 99% of the slots go to a drive that was already inspected before.

Sensitivity (direction of ML minus SMART):

- All five random fleet halves: same pattern (positive and clear for next day/fresh; near zero with cooldown).
- Budgets 10 and 50: positive and clear for next day/fresh; with cooldown mostly not clear.
- Runner-up model (GBM) and SMART count+sum: same pattern.
- Day-level tie-breaking: with cooldown and fresh Top-K, SMART is 5.1 points **above** ML (interval excludes zero).
- SMART raw sum is a weak baseline (1.5% to 24%); ML is far above it in all policies.
- Queue 1K/2K/2d gives the same result as fresh Top-K; queue 2K/5K/7d gives 36% to 54%.

## RQ2: stability of the daily list

| Score | Rule | New entries per day (list of K) | Mean days on list | Timely capture, next day | with cooldown |
|---|---|---|---|---|---|
| SMART count | daily | 0.9 | 54 | 12.2% | 46.1% |
| ML | daily | 2.7 | 20 | 26.1% | 46.6% |
| ML | 2 days in a row | 2.5 | 21 | 24.1% | 45.3% |
| ML | 3 days in a row | 2.4 | 23 | 23.3% | 45.0% |
| ML | EWMA 0.5 | 2.1 | 25 | 24.6% | 46.1% |
| ML | EWMA 0.2 | 1.7 | 31 | 22.2% | 45.3% |
| ML | keep 6 more days | 2.0 | 27 | 25.4% | 46.8% |

The ML list changes about three times as fast as the SMART list. "Keep 6 more days" cuts ML list entries by
26% with no measurable loss of timely capture. EWMA 0.2 cuts entries by 37% and costs about 4 points without
cooldown and about 1 point with cooldown.

## RQ3: warning horizons

Budgeted timely capture (fresh Top-K):

| Score | Next day | 7-day cooldown |
|---|---|---|
| ML 7-day model | 29.1% | 54.3% |
| ML 14-day model | 28.2% | 48.5% |
| ML 30-day model | 26.1% | 46.6% |
| Urgency-first (7, 14, 30) | 28.0% | 52.2% |

Prediction quality on the same actionable label (failure 8 to 31 days after the decision day):
average precision 0.068 (7-day model), 0.065 (14-day), 0.059 (30-day), 0.051 (GBM 30-day), 0.028 (SMART count).

Each model is best on its own label (30-day model AP 0.0845 on the 30-day label), but for the question
"which drives to inspect so that there are at least 7 days of lead", the 7-day model ranks best in this sample.
The intervals of the three horizons overlap, so this is a direction, not a confirmed difference.

## What could be wrong

1. 10% sample: only 464 events. Intervals are about ±4 points. Differences below that are not confirmed.
2. Validation year only. No parameter was chosen from these numbers; the protocol was fixed before the run.
3. An inspection is counted as useful only by its timing. The simulation does not know whether a real
   inspection would have found the problem.
4. Inspected drives stay in the fleet. In reality a replaced drive would free later slots.
5. The capacity is a share of the sampled drives (about 57 slots per day), not of the real fleet.
6. Drives that fail without any SMART signal cannot be caught by any score: 41% of events were not caught
   in time by any of the eight cells.
7. Run time on the laptop was 61 minutes; the full fleet (10 times the rows) needs the lab PC.

## Files

Results are in `D:\Dev_Workspace\phase5_out\valid_sample10` on the coordinator laptop (not in git):
`cell_results.csv`, `rq1_contrasts.csv`, `rq2_stability.csv`, `rq3_horizons.csv`, `event_coverage.parquet`,
`service_log_E1..E8.parquet`, `queue_ledger_E5..E8.parquet`, `daily_capacity.csv`, `checks.json`, `run_info.json`.
