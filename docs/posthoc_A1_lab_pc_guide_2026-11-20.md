# Amendment A1 on the lab PC: step by step (20 Nov 2026)

What this does: the fairer SMART baselines and the list of the 112 unplaced failures, as fixed in
`docs/phase5_amendment_A1_fair_baselines_2026-11-20.md`. It uses the frozen replay inputs and feature files that
are already on the lab PC. Nothing is retrained and no frozen result is changed. All output is labelled
"post hoc (amendment A1)".

Run it on the **lab PC**, because the inputs are there (`C:\DP-out\...`) and it ran the frozen Phase 5 replay.
Use PowerShell. Every command is one line.

## What you need

| Item | Amount |
|---|---|
| Free disk on `C:` | about 10 GB (temporary files; the results themselves are under 100 MB) |
| Memory | the same as the frozen Phase 5 replay (it loads the same table) |
| Time | about 1 to 3 hours for everything; step 3 prints the real estimate |
| Folders already on the lab PC | `C:\DP-out\scores_test\replay_test` (18 `part_*.parquet`), `C:\DP-out\features_test` (6 files), the 2024 replay input folder (12 `part_*.parquet`) and the 2024 feature folder (4 files), the snapshot, the HDD Phase 3 folder |

Keep the PC awake (Settings → Power → Sleep: Never) and do not close the PowerShell window while a step runs.

## Step 1. Get the new code

```powershell
cd D:\DP-test\DrivePriority-AI
git fetch origin
git checkout phase5/simulation
git pull
git status
git log -1 --oneline
```

`git status` must say "nothing to commit, working tree clean". Do not edit any file after this; the test-period
run checks it. (If the repository is in another folder on this PC, use that folder.)

## Step 2. Find the four input folders

```powershell
dir C:\DP-out
dir C:\DP-out\scores_test\replay_test
dir C:\DP-out\features_test
```

The 2024 folders were made the same way; look for a folder holding `replay_valid` with 12 files
`part_2024-01.parquet` … `part_2024-12.parquet`, and a features folder with `features_valid_2024Q1.parquet` …
`Q4`. Below they are written as `C:\DP-out\scores_valid2\replay_valid` and `C:\DP-out\features_valid2`; change the
paths if yours differ.

## Step 3. 2024: preflight, build, replay

```powershell
.\RUN_POST_HOC.cmd preflight --replay C:\DP-out\scores_valid2\replay_valid --features C:\DP-out\features_valid2 --split valid --out C:\DP-out\posthoc_A1
```

It checks Python, the packages, memory, disk and the folders, then builds and replays one month and prints
`PREFLIGHT OK` and an estimate in minutes. If it says `FAILED`, stop and send
`C:\DP-out\posthoc_A1\preflight\preflight.json`.

```powershell
.\RUN_POST_HOC.cmd build --replay C:\DP-out\scores_valid2\replay_valid --features C:\DP-out\features_valid2 --split valid --out C:\DP-out\posthoc_A1
.\RUN_POST_HOC.cmd replay --replay C:\DP-out\scores_valid2\replay_valid --split valid --out C:\DP-out\posthoc_A1
```

The build checks every row (same drive-day found, the SMART count reproduced). The replay ends with
`reproduction gate: PASSED` and a small table. If it says `FAILED`, stop and send the zip it names.

## Step 4. Test period (only after step 3 passed)

```powershell
.\RUN_POST_HOC.cmd preflight --replay C:\DP-out\scores_test\replay_test --features C:\DP-out\features_test --split test --out C:\DP-out\posthoc_A1
.\RUN_POST_HOC.cmd build --replay C:\DP-out\scores_test\replay_test --features C:\DP-out\features_test --split test --out C:\DP-out\posthoc_A1
.\RUN_POST_HOC.cmd replay --replay C:\DP-out\scores_test\replay_test --split test --out C:\DP-out\posthoc_A1 --post-hoc-test
```

The test replay is refused unless the 2024 replay of the same commit finished with the gate passed, and it runs
only once. If the PC stops in the middle, run the same command again: the finished parts are kept and reused.

## Step 5. The 112 unplaced failures

```powershell
.\RUN_POST_HOC.cmd unplaced --snapshot <snapshot folder> --phase3 <03_Phase3_FINAL_HDD folder> --replay-valid C:\DP-out\scores_valid2\replay_valid --replay-test C:\DP-out\scores_test\replay_test --out C:\DP-out\posthoc_A1\unplaced
```

The snapshot folder is `Snapshot-0010_compact` (for example
`C:\Users\1092467\Work\DrivePriority-AI\Integrated_Data\Snapshot-0010_compact`). The Phase 3 folder is the HDD-only
one with `model_train.parquet` and `phase3_manifest.json`. It reads the snapshot twice (about 10 to 30 minutes).

## Step 6. Send back

Upload these to a new OneDrive folder `05_Phase5_RESULTS\08_posthoc_A1`:

- `C:\DP-out\posthoc_A1\posthoc_A1_valid_results.zip`
- `C:\DP-out\posthoc_A1\posthoc_A1_test_results.zip`
- `C:\DP-out\posthoc_A1\unplaced\unplaced_failures.csv` and `unplaced_failures_summary.json`

Then tell Claude. The comparison tables, the wording rule (A1.6) and the manuscript update are done from these
files; no data has to be copied.

## If something goes wrong

Copy the last 20 lines of the PowerShell window and the file `C:\DP-out\posthoc_A1\results_<split>\progress.log`.
Do not change the code or the settings to make it pass.
