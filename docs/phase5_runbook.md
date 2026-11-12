# Phase 5 runbook: how to run the inspection simulation

Read `docs/phase5_protocol.md` first. This file only says which commands to run.
Decision-support simulation only: simulated inspections, no claim that failures are prevented.

## What you need

- Python 3.11 with the packages in `requirements.txt` (no H2O and no Java are needed for the replay).
- The Phase 4 output folder with the sub-folders `h30`, `h14`, `h7`. Each needs `scores_valid.parquet`
  (and `scores_test.parquet` only for the final test run).
- About 6 GB of free memory for the 10% sample.

## 1. Check the code (2 minutes)

```
python -m pytest tests/test_phase5_simulation.py -q
```

All tests must pass. If one fails, stop and report it. Do not run the replay.

## 2. Validation replay (2024)

Windows (PowerShell), from the repository folder:

```
$env:PYTHONPATH = "src"
python -m drivepriority.simulation.run_phase5 --phase4 "D:\path\to\04_Phase4_FINAL" --split valid --population sample10 --out "D:\path\to\05_Phase5\valid_sample10"
```

Mac or Linux:

```
PYTHONPATH=src python -m drivepriority.simulation.run_phase5 --phase4 ~/DP/final_v5 --split valid --population sample10 --out ~/DP/phase5/valid_sample10
```

The run prints seven steps and ends with the table of the eight RQ1 cells and `Checks passed`.
If it prints `CHECKS FAILED`, the results must not be used: send `checks.json` to the coordinator.

`--quick` exists for a two-minute smoke test only. Its numbers must never be reported.

## 3. Files it writes

| File | What it is |
|---|---|
| `replay_valid.parquet` + `.manifest.json` | the replay input built from the Phase 4 scores, with input hashes |
| `cell_results.csv` | every cell: timely coverage with interval, secondary results, workload |
| `rq1_contrasts.csv` | ML minus SMART per policy, interactions, intervals (all sensitivity sets too) |
| `rq2_stability.csv` | persistence rules: watchlist turnover against timely coverage |
| `rq3_horizons.csv` | horizons: budgeted coverage and prediction quality |
| `event_coverage.parquet` | one row per failure event |
| `service_log_E1..E8.parquet`, `queue_ledger_E5..E8.parquet` | every simulated inspection of the eight primary cells (tie seed 0) |
| `daily_capacity.csv` | observed drives and K for every day |
| `checks.json`, `run_info.json` | acceptance checks; protocol hash, code hashes, versions |

## 4. Test replay (2025 to June 2026): once only

Only after the independent review accepts the work and the coordinator says so. The code refuses the test split
unless all of these are true:

1. The switch `--final-test-run` is given. `--quick` and `--config` are refused.
2. The protocol file is the frozen one (the code checks its hash).
3. The file `configs/phase5_test_authorization.json` exists and is committed. The coordinator writes it after
   the review:

```
{
  "protocol_sha256": "22110661c20a838c709c9712249a36dfba6feef84291fd9316cbe66b8fa60af6",
  "review_document": "<file name of the accepted review>",
  "authorized_by": "<name>",
  "authorized_on": "<date>",
  "test_replays_authorized": {"sample10": 1, "full_fleet": 1}
}
```

4. The test split was not replayed before on this machine for the same population. The register is in
   `~/.drivepriority/` (outside the output folder), so a new output folder does not give a second run.

```
python -m drivepriority.simulation.run_phase5 --phase4 "<Phase 4 folder>" --split test --population sample10 --out "<new empty folder>" --final-test-run
```

The register cannot see other machines. Run the test replay on one machine only and commit its `run_info.json`.
Any repeat is a deviation and must be logged in `docs/phase5_protocol.md` section 19.

## 5. Full fleet (primary results, rule R18)

The replay accepts any table with the columns listed in `src/drivepriority/simulation/replay_input.py`
through `--input <file or folder> --population full_fleet`. The full-fleet job writes that table in two steps
(`python -m drivepriority.simulation.full_fleet features ...` then `... score ...`; see the top of
`src/drivepriority/simulation/full_fleet.py`). A validation build reads no snapshot row after 31 January 2025
(validation end + 31 days, the outcome window of the last validation day).

## Rules

- Do not edit `configs/phase5_protocol_v1.json`. Its hash is written into every result.
- Do not delete or overwrite anything in the Phase 4 folders. Write Phase 5 results to a new folder.
- Never use test-year numbers to change a setting.
