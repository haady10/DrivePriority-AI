# DrivePriority AI: Phase 4 protocol, frozen before the final test evaluation (rule R26)

Frozen: 10 November 2026, before any final Phase 4 run. Everything below is fixed. Any later change is a
**deviation**: it is added at the end of this file with the date, the reason, and whether test results had
been seen at that time. Nothing in this protocol may be changed because of test (2025-2026) results.

## 1. Data (fixed)

| Item | Value |
|---|---|
| Source snapshot | Snapshot-0010 (42 quarters, 2016 Q1 to 2026 Q2, 709,242,263 rows) |
| Modelling dataset | `03_Phase3_FINAL_HDD` (Phase 3 v3 + HDD-only filter, rule R13) |
| `model_train.parquet` SHA-256 | `5ef0ed9ee33b4604cbc0b13c21fa7f4808ba097b832b25f04097b51b9ab616c8` |
| `model_valid.parquet` SHA-256 | `1918af617df142328118ef479a76470aa7f319985c52636319115d239b2504e2` |
| `model_test.parquet` SHA-256 | `af857ec84dc8e303cdb99486ff211c1ca4add4cc3b7bd427849a592cbe837c49` |
| `phase3_manifest.json` SHA-256 | `61413373611672a2734c33f6e08bd78ba23973ecf6a78719e9dadd565e79b8e6` |
| Drive-type registry SHA-256 | `09422efd55147b151f9d31cbda350d8d097f458b480d19a3ed2db2c7f697b33e` |
| Splits | train 2016-01-01..2023-12-31, validation 2024-01-01..2024-12-31, test 2025-01-01..2026-06-30 |
| Rows (HDD) | train 25,332,594; valid 10,467,899; test 17,896,827 |
| Sampling | whole drives by hash; train = all drives failing by train end + 2% of others (weights 1 / 50); valid and test = 10% of drives (weight 10) |

## 2. Labels (fixed)

- Horizons H = 7, 14, 30 days; one model set per horizon.
- Positive (1): the drive's FIRST failure flag falls on day t+1 .. t+H.
- Healthy (0): no failure flag in (t, t+H] and the drive is observed again on or after t+H.
  (Deviation from methodology v2.0 section 16, which asked for H fully observed days. Reappearance proves the drive
  survived; a drive that fails during unobserved days never returns and is unknown. The share of healthy rows with
  unobserved days is reported: train 8.0% / 3.9% / 2.0%, valid 0.75% / 0.35% / 0.17% for 30 / 14 / 7 days.)
- Unknown (NULL): otherwise, including every window that crosses its split end. Unknown is never treated as healthy.
- Rows on and after the first failure day are removed.

## 3. Model inputs (fixed)

- 43 inputs: the 44 Phase 3 features minus `history_days` (left-censored at 2016-01-01 and drifts with calendar
  time; `age_days` from SMART 9 is the true age).
- Never inputs: drive_id, date, split, weight, eval_fail_date, model, manufacturer_group, dup_conflict, labels,
  baseline columns (smart_rule_flag / _score / _count), first_seen, last_seen, fail_date and other bookkeeping.
- No imputation or clipping in the data. LR and GLM: H2O mean imputation (training means). GBM and DRF: native
  missing-value handling.

## 4. Code and settings (fixed)

| Item | Value |
|---|---|
| Kit | `phase4_training_kit_v5.zip`, SHA-256 `949590be08a3b22a7a1cafa9fe9550faab669c972145366c78f804b404f82a0b` |
| `train_h2o.py` SHA-256 | `123b2cdfd1b002479ea9fe35c35f0ce50544eb13054d4e62754a5bcafe047fc5` |
| `metrics.py` SHA-256 | `7ca670a97a45e13874bf94db2a50bf0f6eae0587eeb1c7782fd9c8ca3ef79802` |
| Git | branch `phase4/review-fixes`, commit `f7004d4` (PR #3) |
| Software | Python 3.11.9, H2O 3.46.0.12, DuckDB 2.0.0, Java 17 |
| Machine | one machine for all three horizons (Mohammed Shuhail's MacBook Pro, Apple Silicon, 24 GB) |
| Seed | 42 |
| Healthy-row thinning | `--neg-keep 0.25` (deterministic md5 hash of drive_id and date; weights divided by 0.25) |
| LR | H2O GLM binomial, lambda = 0, standardize, remove collinear columns |
| GLM | binomial elastic net, alpha 0.5, lambda search (20 values) on validation |
| GBM | 200 trees, depth 6, learn rate 0.1, sample rate 0.8, column sample rate 0.8, min rows 50, max_abs_leafnode_pred 2.0, early stopping on validation AUCPR (5 rounds, tolerance 1e-3) |
| DRF | 100 trees, depth 12, sample rate 0.632, min rows 50, early stopping on validation AUCPR (3 rounds, tolerance 1e-3) |
| Baselines (not trained) | SMART rule flag (any of raw 5/187/188/197/198 > 0), SMART counter sum, SMART indicator count (0-5, rule R21) |
| Command | `python -m drivepriority.models.train_h2o --data <03_Phase3_FINAL_HDD> --horizon H --out <out>/hH` with default settings (all four models, models saved), only `--max-mem-size` and `--port` set for the machine. Not allowed: `--not-protocol`, `--strict-negatives`, `--neg-keep`, changed tree settings. |

## 5. How the data is used (fixed)

- Training data (2016-2023): fitting only.
- Validation data (2024): early stopping, GLM lambda choice, and the choice of the primary model. Nothing else.
- Test data (2025-2026 Q2): scored once per final run and reported. It is never used to fit, tune, stop, choose
  a model, set a threshold, or change this protocol.

## 6. Primary model choice (decided now, before results)

- For each horizon, the primary model is the trained model (LR, GLM, GBM or DRF) with the highest **validation
  average precision**. If two are within 0.001, the simpler one wins (order LR, GLM, DRF, GBM).
- All four models and all three baselines are reported on validation and test, whichever is primary.
- The primary model's scores feed Phase 5. Phase 5 parameters (queue sizes etc.) are also chosen on validation only.

## 7. Reported metrics (fixed)

Average precision (with the no-skill prevalence), ROC AUC, recall and precision in the top 0.1 / 0.5 / 1 / 5% of
drive-days (ties shared fairly), recall and precision at the SMART-rule alert volume, Brier and log loss
(not calibrated: training is re-weighted, so scores are for ranking). All are pooled known-label drive-days on the
10% evaluation sample. Per-day inspection, events caught and advance warning are Phase 5 results.

## 8. Acceptance checks for each final run (before results are used)

A final run is accepted only if: `run_info.json` shows kit v5, `protocol_run: true`, the data and code hashes
above, cohort HDD-only; `checks.json` shows no rows on or after failure, dates inside the splits, no forbidden
features, no leakage warning (AUC > 0.999 or AP > 0.95, single feature AUC > 0.99), no GBM saturation warning;
the score files contain every validation and test row; the four models are saved. A run that fails a check is
rerun with the same settings after the cause is fixed and logged here; results of a failed run are not used.

## 9. Disclosures that belong in the report

- 2026 data was seen during feasibility work before this protocol; the practice runs on 10 Nov printed test
  numbers on the console (not used for any decision).
- Practice runs (kit v5, GBM only, 50 trees, 10 Nov 2026) were used only to check the code on real data.
- Manual Level 2 approvals for 2016 Q3, 2018 Q3, 2019 Q3; duplicate day 2022-12-11 resolved by rule; source days
  missing: 2017-01-30, 2018-02-25, 2022-12-10; cause of the March-April 2023 spike unknown.
- HDD/SSD split by model-name patterns (not manufacturer-verified).
- `smart_194_mean7` is the mean over the last 7 days and today.
- Results are a decision-support simulation. They do not show that failures are prevented.

## 10. Deviations after freezing

(none yet)
