# DrivePriority AI

Early failure warning and risk-based inspection prioritization for hard-disk drives, evaluated with a
capacity-limited historical inspection replay on the public Backblaze Drive Stats data (Q1 2016 to Q2 2026).

ITE442 Data Science and Big Data Analytics, Abu Dhabi University.

**Scope.** This is a retrospective decision-support simulation. Nobody inspected these drives, a simulated
inspection changes nothing on a drive, and "failure" always means the failure flag recorded by Backblaze. The
results describe how inspection slots would have been allocated; they do not show that failures, downtime or
cost are prevented.

## What the study asks

- **RQ1 (primary).** Under the same fleet and the same daily inspection capacity, how do the re-inspection rule
  (next day vs 7-day cooldown) and the scheduling rule (fresh Top-K vs carry-forward queue) change the difference
  between a SMART-count rule and a machine-learning score, measured as the share of failures inspected 7 to 30
  days in advance?
- **RQ2.** What do alert-persistence rules (confirm, retain, EWMA) cost in timely capture for a steadier list?
- **RQ3.** How do 7-, 14- and 30-day warning horizons compare as predictors and as budgeted rankings?

## Pipeline

| Phase | What | Code | Key documents |
|---|---|---|---|
| 1 | Per-quarter cleaning and validation of the 42 archives (four outcomes: passed, passed_with_warnings, requires_review, failed) | `src/drivepriority/data/`, `RUN_C1_2.ps1` | `docs/C1_2_MASTER_GUIDE.md`, `docs/C1_TEAM_README.md` |
| 1b | Central incremental integration into one read-only snapshot (709,242,263 drive-days) | `src/drivepriority/integration/`, `RUN_INTEGRATION.cmd`, `RUN_CENTRAL.cmd` | `docs/CENTRAL_INTEGRATION_GUIDE.md`, `docs/START_HERE_INTEGRATION.txt` |
| 2-3 | Data profile; labels (7/14/30 days), past-only features, date splits, HDD cohort | `src/drivepriority/features/` (`build_dataset.py`, `hdd_filter.py`), `RUN_PHASE3.cmd` | `docs/data_pipeline_runbook.md` |
| 4 | H2O models (LR, GLM, GBM, DRF) and SMART baselines; protocol frozen before final runs | `src/drivepriority/models/train_h2o.py` | `docs/protocol_frozen_before_test.md` |
| 5 | Full-fleet scoring and the stateful inspection replay (RQ1-RQ3) | `src/drivepriority/simulation/` | `docs/phase5_protocol.md`, `configs/phase5_protocol_v1.json`, `docs/phase5_runbook.md` |

## Evidence for the manuscript

- Frozen replay protocol: `configs/phase5_protocol_v1.json` (SHA-256 `22110661...60af6`, checked by the code).
- Test authorization (single replay): `configs/phase5_test_authorization.json`.
- Run record of the single test replay, result tables and checks: `docs/phase5_test_run_record/`.
- Claim-to-evidence map and limitations (every reported number with its file and column):
  `docs/phase5_claims_and_limitations_2026-11-14.md`.
- Post hoc read-only analyses (no rerun): `tools/phase5_post_test/` (manufacturer audit, uncaptured failures,
  drive-model cluster bootstrap) and the notes `docs/phase5_finding_smart_attribute_availability_2026-11-14.md`,
  `docs/phase5_never_caught_breakdown_2026-11-15.md`, `docs/phase5_may12_gap_root_cause_2026-11-14.md`.
- Reviews and responses: `docs/phase5_review_*.md`, `docs/phase5_final_review_response_2026-11-15.md`,
  `docs/manuscript_v1_review_response_2026-11-18.md`.

Large files (saved models, scored data, service logs, the integrated snapshot) are not in this repository.

## Running the tests

```bash
python -m venv .venv && . .venv/bin/activate      # Windows: .venv\Scripts\activate
pip install -r requirements.txt
python -m pytest tests -q
```

The Windows launchers (`*.cmd`, `*.ps1`) wrap the same Python entry points for the team's lab machines. The H2O
test is opt-in because it needs Java and a local H2O cluster.

## Data

Backblaze, Inc., Hard Drive Test Data (Drive Stats), https://www.backblaze.com/cloud-storage/resources/hard-drive-test-data.
The raw data are not redistributed here.

## Licence

No licence has been chosen yet; until one is added, all rights are reserved by the authors.
