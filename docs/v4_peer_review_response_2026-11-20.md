# Response to the peer review of manuscript V4 (20 Nov 2026, evening)

Review: `DrivePriority_AI_V4_Peer_Review.md`. It reviewed the V4 PDF we sent (SHA-256 `115d7fef…a61a`, checked: same
file) and repository commit 32c763a. Recommendation: targeted revision. The review mentions an eight-page draft and
a ZIP of its own; we did not receive them. The eight-page manuscript V5 was written by us from V4, after checking
every point below against the data. A further constraint came from the course instructor: at most 8 pages including
references.

Labels: **REQUIRED** = needed for a correct paper; **RECOMMENDED** = improves it; **UNNECESSARY** = no change needed.

## Verdicts

| # | Review point | Verdict | Evidence (checked here) | Action |
|---|---|---|---|---|
| 1 | A1 differences change interpretation under drive-model resampling | **REQUIRED** | Own script `tools/phase5_post_test/a1_model_cluster_bootstrap.py` (same method as Table IX) first reproduces the frozen Table IX intervals exactly (max difference 9e-16 points, `A1_model_cluster_check.json`). M1 − S4 (next day, fresh, rotating, test) −7.6, model interval −22.3 to +8.7; 2024 −3.8, −18.1 to +12.8; M2 − S4 −5.0, −15.4 to +7.1; M2 − M1 +2.6, −4.3 to +8.9. All equal to the review. Of the 40 M1 − S*k* differences at B = 20 (test) only 5 exclude zero, all ML-favourable and next-day | V5 reports model intervals beside event intervals (Table VIII, Fig. 2, abstract, V-C, VI, VII). The quota-count lead is stated as a fact about the observed failures, not robust to the hardware mix |
| 2 | "Neither the model nor SMART is ahead in any policy" contradicted by individual cells | **REQUIRED** | e.g. M1 − S1 next/fresh +14.7 [13.8, 15.6]; M1 − S4 cool/fresh rotating −4.4 [−5.7, −3.1] | V5: "None of the four service rules met the criterion for a uniform advantage …"; the rule's failure is not equivalence; the multiplicity sentence now says the rule protects one joint claim per service rule, not the grid |
| 3 | Quota benefit for SMART reverses under the cooldown; S4 − S1 omitted | **REQUIRED** | S4 − S1 test, frozen: +3.2, +2.7, −1.5, −3.1; rotating: +19.4, +6.3, −1.9, −2.4 (all cooldown event intervals exclude zero) | Contribution 3 and Discussion corrected; S4 − S1 added to Table VIII. "Remove bias" replaced by "change who is inspected"; quotas called one allocation choice, not an optimum |
| 3b | "Level with S3" in the team note | **REQUIRED** (notes) | M1 − S3 cool rotating: fresh −1.1 [−2.5, +0.2], queue −2.1 [−3.3, −1.0] | Corrected in `phase5_amendment_A1_results` (clarification 6) and the project notes; the manuscript never said "level" |
| 4 | Webster quota does not guarantee one-slot deviation | **REQUIRED** | Reproduced with the repository's `QuotaRanker`: 73/9/9/9 drives, K = 7 → 4/1/1/1 (exact 5.11); same with 2,555/315/315/315 at B = 20 | V5 says "approximately proportional (it can miss the exact share by more than one slot)". Docstring and the old test assertion corrected (code unchanged); new test with the counterexample; dated clarification 1 in the results note |
| 5a | Spearman 0.95 mixes related settings; A1.6(5) also asks for completed inspections across budgets | **REQUIRED** (our own amendment asked for it) | Distinct / completed, test: B10 0.96 / 0.06, B20 0.95 / 0.26, B50 0.91 / 0.60; 2024: 0.96 / 0.43, 0.91 / 0.71, 0.82 / 0.83 (equal to the review) | V5 V-D reports both at the three budgets, says distinct drives are a policy outcome and that this is not a matched-breadth comparison |
| 5b | Horizons ranked by own-label APs of different prevalence | **REQUIRED** | V4 VI-H compared own-label APs across horizons | V5 ranks horizons only by capture and by AP on the common actionable target; the stability statement is scoped to the three definitions evaluated |
| 5c | "Retention rules are cheap" | **REQUIRED** (wording) | Cost was not measured | Removed; V5 says operator costs were not measured |
| 6 | A1 re-evaluated the overlapping S1/M1 policies | **REQUIRED** (wording) | The reproduction gate re-runs E1–E8 cells | V5 IV-C says so. Our V4 change log line "the test period was not rerun for the frozen policies" was wrong (clarification 2) |
| 7 | Lab PC clock cannot fix exact UTC times | **UNNECESSARY** (already done) | Clock note kept in the results note | None |
| 8 | Side-file loader does not compare drive IDs | **RECOMMENDED** | Confirmed by reading the loader (checked date and count only). The A1 runs are not affected: the side files used are byte-identical (SHA-256) to the files the build wrote after its (drive_id, date) join, in both periods | Loader now compares drive_id on every row (code change after the runs); test with two swapped drives |
| 9 | LiteDiskGuard authors wrong | **REQUIRED** | Crossref and SSRN: Zhiqiang Wang, Jinyu Lu, Teng Sen, Ying Chen (V4 had an extra "Y. Liang" and "S. Teng") | Fixed in V5 [10] |
| 10 | Wei et al. 2025 authors | **UNNECESSARY** | Crossref: Shuting Wei, Hongzhang Yang, Zhengguang Chen, Ping Wang; Sci. Data 12, 1125 — as in V4 | None |
| 11 | Pecht and Elburn volume and pages | **RECOMMENDED** | Publisher page: Circuit World 47(4), 408–426, 2021; online 24 Sep 2020 | Fixed in V5 [15] |
| 12 | Availability statement over-claims the generator | **REQUIRED** | Table II (data stages) and part of Table IV come from earlier evidence, not the generator | V5 names the tables the scripts rebuild (III, V–XI) and Fig. 2; `tools/manuscript_v5_fig2.py` rebuilds Fig. 2 byte-identically |
| 13 | 106 unplaced failures | **UNNECESSARY** now | Disclosed; no reason assigned | V5 Table II note no longer attributes a reason to all 112 |
| 14 | Eight-page version | **REQUIRED** (instructor's limit) | — | V5: 7 pages including references (21 references), IEEE conference layout |
| 15 | Author metadata | **REQUIRED** | Provided by the authors | Added in V5 (e-mails; ORCID for two authors) |

## Note on two files we did not write

At 19:50 GST two untracked files appeared in our working copy of this repository,
`tools/phase5_post_test/a1_model_cluster_bootstrap.py` and `published_results/phase5_A1/A1_model_cluster_bootstrap.csv`,
which we had not written. They were set aside unused. We wrote our own script (the one committed here) and only then
compared: the 432 rows are identical, as expected for the same method and seed.
