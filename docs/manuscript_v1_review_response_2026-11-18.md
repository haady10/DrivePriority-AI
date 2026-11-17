# Response to three AI reviews of IEEE manuscript V1 (18 Nov 2026)

Reviews (all of manuscript V1, dated 15 Nov 2026): (A) "IEEE Manuscript V1 Review" (prepared for M. Zahid),
(B) "Independent Review" (Claude), (C) "Strict Technical Review" (manuscript and repository audit, commit 99ccc5a).
All three: **major revision**. All three found the reported numbers consistent with the tables and result files.

Rules kept: no experiment was rerun, no result file was changed, the test period was not replayed. New numbers are
post hoc and read only saved files (`tools/phase5_post_test/model_cluster_bootstrap.py`, the drive-type registry).

## Implemented in manuscript V2

| Point | Review | Change |
|---|---|---|
| Test exposure and timeline | A1, B7 | New Section V-H with a dated table (profile and practice runs saw 2025-26; 10% test sample scored 11 Nov; queue rule for falling capacity added and implemented 12 Nov after the dry run, never triggered; single test replay 13 Nov). 2024 is called the development year; "replicated" is defined as consistency between development year and test period |
| Scope of the headline | A3, B1, B3 | Discussion restricted to the primary budget and tie rule; "+14.7 is against this particular count ... not superiority over well-designed SMART rules"; abstract states that the count reaches one manufacturer in that cell; budget 10/50 statements say "under frozen ties (rotating ties not run)" |
| Broader uncertainty | A5, B5, C4.1 | New table: drive-model cluster bootstrap for the four primary contrasts and both interactions, both periods, with cluster counts (42 test, 44 in 2024; not 136: only 42 models have eligible test events). Abstract gives both intervals for +14.7 |
| Ten-seed wording | A5, C6.5 | "Capture rates and contrasts are means over seeds; per-drive analyses use the saved logs of one seed" |
| Persistence and stability definitions | A4 | confirm-c described as ranking by the minimum of the last c observed scores (not "c consecutive alerts"); fewer than c observed scores = lowest priority; histories over observed days, reset after a gap > 7 days; stability measured on the watch-list (top K by priority before any re-inspection rule). "No cost" replaced by "similar observed capture; paired intervals not computed" |
| Queue algorithm and sensitivity queues | A6, B7 | Four-step queue (expire, cap after a fall of K, admit, schedule) and the 1K/2K/2 d and 2K/5K/7 d settings defined; both shown in the sensitivity tables |
| All pre-specified results | B7 | Capture at any lead added to the workload table; raw-SMART-sum and 1K/2K/2 d queue rows added to the sensitivity tables; contribution 4 reworded |
| Outcome partly defined by SMART counters | B6 | Section III-A: Backblaze names SMART 5/187/188/197/198 as its health indicators and schedules replacement when SMART 187 > 0 (Beach 2014, cited); repeated in limitations |
| Label misalignment, untuned model | B4, C4.2 | Section IV-D: settings fixed, not tuned; 30-day label also counts failures 1-6 days ahead; no model trained on the endpoint window. RQ3 gives this as a plausible, untested reason for the 7-day model's higher capture; GBM swap (4 to 9 points) in limitations |
| AP values 0.0845 / 0.091 / 0.065 | C6.3 | Table XIV has own-label AP for 2024; text says 0.0845 is the 10% validation sample, 0.091 the full fleet |
| HDD drive-days | A (smaller) | 703,142,666 HDD drive-days added (Table II, abstract) from the drive-type registry |
| Table II failures 28,875 vs 28,987 | B (minor) | Note: different selections, first-day failures of each replay excluded, remaining 112 not itemised |
| Capacity 631 vs 316,245 drives | B (minor) | Table XII drive counts come from a separate snapshot query over all drives of that day (319,304, no HDD filter); capacity 631 is from the 315,806 HDDs observed in the replay |
| Table VI too small | A (smaller) | Split into two tables (next day, cooldown) with larger type |
| Feature definitions | A (smaller) | Change windows, temperature mean, power-on age, previous-record gap and missing handling defined from `build_dataset.py` |
| Design choices | B (minor) | Budgets, cooldown and queue sizes stated as protocol design choices; scale: ~12.5 failures/day vs ~652 slots; an inspection = one unit of attention, outcome not modelled; window spans the shortest and longest horizon |
| Equations | C6.4 | Floor delimiters and upright words (ML, SMART, cooldown, next day) in Eqs. (3), (5), (6) |
| Uncaptured wording | A (smaller) | "captured by none of the eight primary policies" in abstract and conclusion |
| Repository link | A6, C6.1 | Availability statement names the public repository (branch phase5/simulation); large files on request |
| What the replay adds | B2 | Discussion: cancellations 0.4-1.6% of slots and the queue cap never triggered, so the stateful elements act mainly through re-inspection spacing; ranking-depth interpretation stated as untested |

## Implemented in the repository

- `README.md` rewritten (scope, research questions, pipeline, evidence map, tests, data, licence status) (C5).
- `docs/phase5_review_test_results_2026-11-14.md`: the PENDING line now records its resolution on 14 Nov (C5).
- Claims file: A37 (cluster bootstrap), A38 (HDD drive-days), B5 cross-reference.
- `tools/phase5_post_test/model_cluster_bootstrap.py` and its output `docs/phase5_test_run_record/model_cluster_bootstrap.json`.

## Not implemented, with reasons

| Point | Review | Reason |
|---|---|---|
| Fair baselines (within-manufacturer rank, 3-attribute count) as a post hoc annex | B1, C P3 | Needs new replays of the locked test period with new scores; the team decided not to rerun experiments. Stated as a limitation and as version-2 work |
| Train a model on the actionable 8-31 day label | B4 | New training and replay; same reason |
| Capture against list depth without a service model | B2 | Needs the full-fleet daily scores (178 M rows, not in the repository) and is a new analysis of the test period; stated as untested |
| Rotating ties at budgets 10 and 50 | B3 | New replay; the abstract and discussion now qualify the budget statement instead |
| Reconcile 6,834 vs 6,826 drive by drive | A2, C P1 | Needs a read-only snapshot query on the lab PC. One cause is now documented (the 6,834 query had no HDD filter). Still open |
| Aggregate service logs over ten seeds | C6.5 | Would need re-running the replay; wording now states the seed scope |
| LiteDiskGuard author list | A7, B | Not changed: the paper's own title page lists five authors (Zhiqiang Wang, Jinyu Lu, Sen Teng, Yongchao Liang, Ying Chen); the SSRN metadata lists four. The reference follows the paper |
| Author e-mail addresses | A, B, C | Needs the authors |
| Licence, Zenodo deposit of large files, cross-platform runner, dependency split | C5 | Owner decisions; README states that no licence has been chosen |
| `run_info.json` `input_sha256: null` | C5 | Already fixed after the test run (commit 78282d4: per-file input hashes); the test record keeps the original file and the separate `input_hashes.txt` |
