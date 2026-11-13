# Phase 5 – record of the one-time full-fleet TEST run (13 Nov 2026)

Decision-support simulation on old data. It does NOT show that any failure was prevented.

Run: lab PC AACE-A-106-8, clean clone of branch `phase5/simulation` at commit `68a8e3e` (contains
`configs/phase5_test_authorization.json`: full_fleet 1, sample10 0). Frozen protocol sha256
`22110661c20a838c709c9712249a36dfba6feef84291fd9316cbe66b8fa60af6`; authorization sha256
`521ac2cd6be30d9beb6822c9b0654ffe557acfc450948a71ecf053588b8b8277`. Test period 2025-01-01..2026-06-30.
Features 18,303 s (178,021,959 rows; 0 missing / 0 different in all six quarters against the Phase 3 test
sample), scoring with the saved Phase 4 models, replay 2,401 s.

Files here: `run_info.json`, `checks.json`, `TEST_REPLAY_LOCK.json`, `cell_results.csv`, `rq1_contrasts.csv`,
`rq2_stability.csv`, `rq3_horizons.csv` (test), `validation_run_info.json` (2024). Logs, service logs and
parquet files are in OneDrive `05_Phase5_RESULTS`. Register entry: `lock_register_entry.json`; input hashes: `input_hashes.txt` (18 monthly part files, both manifests). Original register file
(`~/.drivepriority/phase5_test_replays_22110661c20a838c.json`) is also in OneDrive (run_info has
`input_sha256: null` for a folder input).

Checks: all_passed true; 0 engine rule violations; identical eligible event set in every cell (6,271 eligible,
368 burn-in, 187 no opportunity); no-future-information check passed; `queue_removed_cap_fell` = 0 in E5-E8.
One-day dip in observed drives on 2025-05-12 (243,354 against about 313,000 the next day).

## Primary timely capture (B = 20 per 10,000), test against 2024 validation
| Cell | Policy | Test [95% CI] | 2024 |
|---|---|---|---|
| E1 | SMART, next day, fresh Top-K | 11.8 [11.1,12.6] | 12.3 |
| E2 | ML, next day, fresh Top-K | 26.5 [25.4,27.6] | 26.4 |
| E3 | SMART, 7-day cooldown, fresh | 43.5 [42.4,44.7] | 45.8 |
| E4 | ML, 7-day cooldown, fresh | 44.1 [42.9,45.4] | 47.5 |
| E5 | SMART, next day, queue | 24.0 [23.1,25.0] | 28.6 |
| E6 | ML, next day, queue | 35.2 [34.1,36.4] | 33.2 |
| E7 | SMART, 7-day cooldown, queue | 47.9 [46.8,49.0] | 51.4 |
| E8 | ML, 7-day cooldown, queue | 47.6 [46.4,48.9] | 50.8 |

ML minus SMART (paired, points; reading rules in `docs/phase5_test_reading_plan_2026-11-13.md`):
- next day / fresh: +14.7 [+13.8,+15.6] (2024 +14.1). Confirmatory contrast: replicated.
- next day / queue: +11.2 [+10.2,+12.3] (2024 +4.6). Same sign, larger, because SMART with the queue fell from 28.6 to 24.0.
- cooldown / fresh: +0.6 [-0.4,+1.7] (2024 +1.8). Not replicated.
- cooldown / queue: -0.3 [-1.2,+0.7] (2024 -0.6). No difference.
- Under the cooldown, day-level ties (-6.3 / -5.2), `smart_count_sum` (-3.1 / -2.3) and GBM (-3.4 / -9.1) all put SMART ahead
  with intervals that exclude zero; budgets 10 and 50 favour ML (+9.8 / +4.1 and +3.3 / +4.4). No ML-versus-SMART claim under the cooldown.
- The schedule effect stays large: the cooldown adds +31.7 points for SMART and +17.6 for ML (fresh Top-K).

RQ2 (top-1K list overlap, ML): daily 0.907; confirm_2 0.920, confirm_3 0.928, ewma_0.5 0.927, ewma_0.2 0.944,
retain_2 0.924, retain_6 0.933. Coverage change under the cooldown (daily 44.1): retain_2 +0.1, retain_6 +0.1,
ewma_0.5 -0.7, confirm_2 -1.4, ewma_0.2 -2.0, confirm_3 -2.5.

RQ3 (ML): cooldown h7 50.6, h14 47.7, h30 44.1, urgency-first 48.5; next day h7 28.8, h14 28.8, h30 26.5,
urgency-first 28.5. Actionable AP (failure 8-31 days later): h7 0.048, h14 0.047, h30 0.042, GBM 0.029,
SMART count 0.018. The primary score stays `ml_h30`.

Missed failures: 36.2% of eligible events are never timely-captured in any of the 8 cells (2024: 38.1%). SMART
next day (E1) captures no non-Seagate failure. Under the cooldown HGST favours SMART (E3 .705 vs E4 .488), Seagate
and WDC favour ML. Cause for Toshiba and WDC not verified.

Limits: intervals cover resampling of events only; only tie-seed-0 service logs saved; 2024 was used for early
stopping in Phase 4 (test is the clean period); the fleet mix differs between years; the human independent
review is still open (AI reviews only).
