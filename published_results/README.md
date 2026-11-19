# Result archives (Phase 5) and post hoc outputs

Added 20 November 2026 in answer to the strict publication review of 19 November 2026 (finding 8). The two
archives are the complete, unchanged output folders of the two full-fleet replays, byte for byte as written by
`src/drivepriority/simulation/run_phase5.py` on 12–13 November 2026; nothing in them was edited afterwards.

## `phase5/`

| File | What it is | SHA-256 |
|---|---|---|
| `phase5_test_full_results.zip` | Test period 2025-01-01..2026-06-30, the single test replay of 13 Nov 2026 (8.4 MB) | `587a02e9b4087978dcf2fc27b15eea3f5ee1b0330941cdaf5155fad230eab462` |
| `phase5_valid_full_results.zip` | Development year 2024 (5.2 MB) | `51c01f5acb02c7bc44fceeaf3bb7f1ab4a606a814d2d01f6e498b08ee2368313` |

Each archive holds `cell_results.csv`, `rq1_contrasts.csv`, `rq2_stability.csv`, `rq3_horizons.csv`, `checks.json`,
`run_info.json`, `daily_capacity.csv`, `event_coverage.parquet` (one row per failure event: status, capture share
of each primary policy over ten tie seeds, earliest timely lead of seed 0), the service logs of the eight primary
policies for tie seed 0 (`service_log_E1..E8.parquet`) and the queue ledgers of the queue policies. The test
archive also holds `TEST_REPLAY_LOCK.json`. Per-event results were saved only for the eight primary policies, and
service logs only for seed 0.

Verify (needs pandas and pyarrow only, a few seconds):

```bash
python tools/verify_results.py
```

It checks the SHA-256 values in `phase5/SHA256SUMS.txt`, recomputes the eight primary capture rates and the
primary contrasts and interactions from `event_coverage.parquet` and compares them with `cell_results.csv` and
`rq1_contrasts.csv`, and rebuilds the seed-0 capture of every eligible event from the service logs.

## `phase5_post_test/`

Read-only analyses after the test replay. They read the archives above or the frozen daily replay inputs
(scores, labels and failure dates of every drive-day, about 1.7 GB of Parquet files, not in this repository).
No model was retrained and no policy was replayed. Scripts are in `tools/phase5_post_test/`.

| File | Script | Content |
|---|---|---|
| `actionable_ap_test.json`, `actionable_ap_2024.json` | `actionable_ap_reanalysis.py` | Actionable AP under three definitions: D0 = protocol (reproduces `rq3_horizons.csv` exactly), D2 = complete 31-day follow-up, D1 = D2 with drive-days 1–7 days before a failure counted as negatives |
| `signal_share_test.json`, `signal_share_2024.json` | `signal_share_replay_cohort.py` | Share of eligible failure events with any SMART indicator above zero in the 30 days before failure, by make and drive model; captured vs never-captured events |
| `time_block_bootstrap.json` | `time_block_bootstrap.py` | Primary contrasts with whole calendar weeks resampled (4,000 resamples) |
| `smart_count_day_facts_test.json`, `smart_count_day_facts_2024.json` | `smart_count_day_facts.py` | Replay-population drive counts and SMART-count facts of one day, and the every-day check that drives with a count of 4 or 5 outnumber the slots |
| `first_day_failures_2025-01-01.json` | `first_day_failures.py` | The six drives observed in 2024 that failed on 2025-01-01 (part of the 112 cohort failures outside every selection) |
| `by_make_test.csv`, `by_make_2024.csv` | `by_make_table.py` | Timely capture by manufacturer with paired event-bootstrap intervals (engine bootstrap, seed 20261111); manuscript Table XI and Fig. 5 |

The drive-model cluster bootstrap of 18 November is in `docs/phase5_test_run_record/model_cluster_bootstrap.json`.

The data behind every manuscript table and the six figures can be rebuilt from these files with
`python tools/manuscript_tables_figures.py <out_dir>` (pandas, pyarrow, matplotlib); the manuscript text is not in
this repository.
