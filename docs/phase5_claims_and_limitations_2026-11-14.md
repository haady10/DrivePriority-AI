# Claim-to-evidence map and limitations (Phase 5, 14 Nov 2026)

Every sentence the report may make about Phase 5, with the file and column it comes from. Paths are inside the
test result folder (OneDrive `05_Phase5_RESULTS\02_test_2025_2026_fullfleet\phase5_test_full`, tables also in
`docs/phase5_test_run_record/`); the 2024 validation folder has the same layout. Coverage values are shares;
multiply by 100 for the percentages used in the text. Decision-support simulation on old data; no prevention
claim anywhere.

## A. Claim to evidence

| # | Claim | File | Where exactly |
|---|---|---|---|
| A1 | The eight primary policies capture 11.8 / 26.5 / 43.5 / 44.1 / 24.0 / 35.2 / 47.9 / 47.6 per cent | `cell_results.csv` | rows with `group = rq1_primary`, columns `coverage`, `coverage_ci_low`, `coverage_ci_high`, keyed by `e_cell` E1..E8 |
| A2 | Confirmatory contrast: ML minus SMART, next day, fresh Top-K, +14.7 [+13.8,+15.6]; 2024 +14.1 [+13.0,+15.2] | `rq1_contrasts.csv` | `set = primary_B20`, `contrast = delta_ml_minus_smart\|next\|fresh` |
| A3 | Replication rule (same sign and both intervals exclude zero) was fixed before the test | `docs/phase5_test_reading_plan_2026-11-13.md` | section 2, committed 0b83f55 / 2eb8f75, before any test score existed |
| A4 | At budget 20 the cooldown contrasts include zero (+0.6 and -0.3) | `rq1_contrasts.csv` | `set = primary_B20`, contrasts `...\|cool\|fresh` and `...\|cool\|queue` |
| A5 | At budgets 10 and 50 the cooldown contrasts favour ML and replicate (+9.8/+4.1 and +3.2/+4.4 on test) | `rq1_contrasts.csv` | `set = budget_B10`, `set = budget_B50` |
| A6 | With day-rotating ties SMART is ahead under the cooldown (-6.3 and -5.2 on test, -3.7 and -1.2 in 2024) | `rq1_contrasts.csv` | `set = tie_day`. The 2024 `cool\|queue` upper bound is just above zero, so it is not a strict replication |
| A7 | Against the stronger SMART baseline, ML is behind under the cooldown (-3.1 and -2.3) | `rq1_contrasts.csv` | `set = score_smart_count_sum` |
| A8 | With GBM instead of DRF, SMART is ahead under the cooldown (-3.4 and -9.1) | `rq1_contrasts.csv` | `set = score_gbm_h30` |
| A9 | Best policy at budget 20 on test is SMART count with rotating ties, cooldown, queue: 52.8 per cent | `cell_results.csv` | `group = sens_tie`, `score = smart_count`, `tie_mode = day`, cooldown + queue |
| A10 | The cooldown adds 31.7 points for SMART and 17.7 for ML (fresh Top-K) | `rq1_contrasts.csv` | `effect_cooldown\|smart\|fresh`, `effect_cooldown\|ml\|fresh` |
| A11 | The queue adds 12.2 and 8.8 points (next day) and 4.4 and 3.5 (cooldown) | `rq1_contrasts.csv` | `effect_queue\|...` |
| A12 | A random policy captures about 4.1 per cent | `cell_results.csv` | `group = sens_source`, `score = random` |
| A13 | 95.5 to 99.3 per cent of inspections are repeats; the policies reach 2,345 to 15,790 distinct drives | `cell_results.csv` | `repeat_share`, `distinct_drives_inspected`, `completed` |
| A14 | 118 to 476 inspections per timely capture | `cell_results.csv` | `inspections_per_timely_capture` |
| A15 | Retain rules raise ML list overlap 0.907 to 0.924 / 0.933 at +0.1 points of coverage | `rq2_stability.csv` | `score = ml_h30`, `rule` in {daily, retain_2, retain_6}, columns `list1K_mean_jaccard`, `coverage` |
| A16 | EWMA and confirm rules cost 0.7 to 2.5 points under the cooldown | `rq2_stability.csv` | same file, `rule` in {ewma_0.5, ewma_0.2, confirm_2, confirm_3} |
| A17 | SMART lists are already stable (0.977) and EWMA rules cost it 3 to 4 points | `rq2_stability.csv` | `score = smart_count` |
| A18 | Under the cooldown the 7-day model captures 50.6 against 44.1 for the 30-day model | `rq3_horizons.csv` | horizon rows, `coverage` and interval columns |
| A19 | Actionable AP (failure 8-31 days later): h7 0.048, h14 0.047, h30 0.042, GBM 0.029, SMART 0.018 | `rq3_horizons.csv` | prediction-quality rows |
| A20 | ML 30-day AP fell from 0.092 (2024) to 0.060 (test); SMART 0.032 to 0.023 | `rq3_horizons.csv` in both folders | own-label AP rows |
| A21 | Capture by make, and 36.2 per cent never captured by any primary policy | `event_coverage.parquet` | rows with `status = eligible`, columns `E1_timely_share`..`E8_timely_share`, make derived from `drive_id` |
| A22 | ML minus SMART by make under the cooldown: HGST -21.7, Toshiba -2.2, Seagate +9.7, WDC +12.1 | `event_coverage.parquet` | paired differences of the same columns; intervals in `T3_by_make.csv` |
| A23 | Resampling drive models instead of events widens the cooldown intervals to about -8 to +10 | `event_coverage.parquet` | model-cluster bootstrap over the 42 drive models |
| A24 | 6,271 eligible events, 368 burn-in, 187 without opportunity, 368,720 drives, 546 days | `checks.json`, `run_info.json` | `events`, `rows`, `drives`, `days` |
| A25 | All acceptance checks passed, 0 engine rule violations, no future information, 0 queue cap removals | `checks.json` | `all_passed`, `engine_rule_violations`, `no_future_information`, and `queue_removed_cap_fell` in `cell_results.csv` |
| A26 | The test replay ran once, from commit 68a8e3e, under a signed authorization | `run_info.json`, `TEST_REPLAY_LOCK.json`, `lock_register_entry.json`, `input_hashes.txt` | protocol sha256 22110661..., authorization sha256 521ac2cd..., one register entry |
| A27 | smart_187 and smart_188 are reported only by Seagate; the count ceiling is 5 for Seagate and 3 for the rest | `smart_by_make2.log`, `smart_count_dist.log` | in OneDrive `04_provenance`; write-up `docs/phase5_finding_smart_attribute_availability_2026-11-14.md` |
| A28 | 855 Seagate drives scored 4 or 5 on 2025-06-02, more than the daily budget of about 626 | `smart_count_dist.log`, `daily_capacity.csv` | count distribution and `capacity_K` |
| A29 | Pre-failure indicator availability: HGST 0.906, Seagate 0.830, WDC 0.765, Toshiba 0.642 | `smart_by_make2.log` | 30 days before failure, test period |
| A30 | 2025-05-12 is a one-day source gap at Backblaze's sac2 site, not a loss in our processing | `check_gap.log`, `check_gap2.log`, `Cleaned_quality_report.json` | `daily_source_rows` 316,450 / 245,997 / 316,370; write-up `docs/phase5_may12_gap_root_cause_2026-11-14.md` |
| A31 | Excluding the 281 events whose window touches the gap changes the contrasts by at most 0.23 points | recomputation from `event_coverage.parquet` | recorded in the gap note, section "Effect on the results" |

## B. Limitations

| # | Limitation | Why it matters | Where it is disclosed |
|---|---|---|---|
| B1 | An inspection has no consequence in the simulation: the drive stays in the fleet and can be inspected again | "Timely capture" means the drive was on the watch-list 7 to 30 days before failure, not that anything was repaired or prevented | review note section 3; A13 shows 95.5-99.3 per cent repeats |
| B2 | Next-day re-inspection is not realistic practice, and the confirmatory contrast lives in that cell | The headline +14.7 comes from the least realistic policy | corrected conclusions, item 1 |
| B3 | The SMART baseline is degenerate across makes (A27, A28) | Part of the next-day advantage is a weak baseline, not model skill | finding note of 14 Nov; per-make table required beside every headline |
| B4 | Primary ties are frozen per drive, which suppresses SMART's reach | The sign of the cooldown contrasts depends on it (A6) | both tie modes reported side by side |
| B5 | Intervals resample events only, not retraining and not the fleet mix | They are narrower than the real uncertainty; drive-model clustering widens them (A23) | limits section of every results note |
| B6 | Only tie-seed-0 service logs were saved | The ten-seed decisions cannot be re-derived from the saved files | run record |
| B7 | 2024 was used for early stopping and model choice in Phase 4; the Phase 4 models were scored on the 10 per cent test sample on 11 Nov | The test period is clean for the Phase 5 policy results only | corrected conclusions, item 7 |
| B8 | Queue and cooldown act as one factor, the time between re-inspections (about 1, 3, 7 and 9 days) | They should not be described as two separate mechanisms | reading plan section 5 |
| B9 | The 1K/2K/2-day queue setting is identical to fresh Top-K | It is not an independent queue sensitivity | reading plan section 5 |
| B10 | `late_only` counts drives that were also inspected more than 30 days early | The column name overstates what it measures | reading plan section 9 |
| B11 | The actionable AP is affected by the label gap in the last 30 days of the split | Affects all scores alike, so comparisons stand, levels do not | reading plan section 9 |
| B12 | `checks.json` does not verify that the replay input is complete | A missing month would not be caught there; the features and scoring steps did check it | reading plan section 8 |
| B13 | The lab PC clock is about 4 hours behind | Recorded UTC times in the run record are early | run record |
| B14 | 2025-05-12 is missing one data center (A30) | One day of reduced capacity and extra cancellations | gap note, with the bound at A31 |
| B15 | One dataset, one operator, one fleet | External validity is untested | report discussion |
| B16 | The 7-day ML model was not promoted to primary and has no paired comparison | Its apparent advantage (A18) is exploratory | reading plan section 6 |
| B17 | All reviews so far are by AI; no human independent review | Stated as open in every results note | run record and review notes |
| B18 | The archive's publisher checksum was never independently established (2025Q2 Level 2 approval) | Source authenticity rests on the recorded crc32 of the members actually read | gap note, confirmation status |
