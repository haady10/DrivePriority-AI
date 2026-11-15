# Response to the independent final review of the Phase 5 test results (15 Nov 2026)

Review: "DrivePriority AI | Phase 5 — Independent final evidence review", 15 Nov 2026 (AI reviewer "Astra"; copy in
OneDrive `05_Phase5_RESULTS\03_reviews`). Verdict: **ACCEPT WITH CHANGES**. The computed test results stand, the
12 May input-completeness blocker is closed, and no replay of the locked test period is justified.

No number from the test run was changed and nothing was rerun. Every change below is wording, a stated
denominator, or a new read-only count from files saved before this review. Decision-support simulation on old
data; no prevention claim.

## Blockers

| # | Review point | What we did | Where |
|---|---|---|---|
| 1 | "Only Seagate" and the count ceiling are too absolute; one day does not prove E1 was Seagate on every day | Audited the saved E1 service log over all 545 decision days: all 355,069 scheduled and 352,686 completed inspections are Seagate (2,339 distinct drives), 0 days with any other make (tie seed 0). Rewrote "only" as "NULL in 100% of Toshiba/HGST and 99.6/99.4% of WDC rows on 2025-06-02"; the maximum count of 3 is stated as a one-day observation. Added: ML cells also give 78.9 to 85.8% of inspections to Seagate (about 34% of the fleet) | claims A27, A34, A35, B3; finding note sections 1-3; literature note C4; `tools/phase5_post_test/e1_make_by_day.py` |
| 2 | 6,834 failed drives against 6,826 replay events | Denominators now stated for every rate: 6,826 first-failure events (one per drive, 2025-01-02..2026-06-30) for the replay, 6,271 eligible for capture; 6,834 = earliest failure flag per serial number in the raw snapshot, make from the model name, for the SMART-availability shares. Not reconciled one by one: make-level differences go both ways (Toshiba +9, WDC +3, Seagate -3, HGST -1), so make assignment differs; candidates also include failures dated 2025-01-01 and drives whose first failure was before 2025. Recorded as limitation B20; a read-only snapshot query on the lab PC would close it | claims A24, A29, B20; finding note section 4 |
| 3 | "No ranking built on these SMART inputs can reach them" exceeds the evidence | Deleted. The breakdown is now descriptive and seed-0 only; low signal availability, ranking, budget, ties and timing are listed as possible explanations the data does not separate; the two-model link is called an association | claims A32, A33, B19; never-caught note "Reading" |

## Required changes

| # | Point | Status |
|---|---|---|
| 1 | +14.7 with the exact comparison, the rotating-tie +11.8, the per-make E1 row and the repeat share | Already in the corrected conclusions; the manuscript headline must read "ML 30-day DRF versus fleet-wide SMART count, frozen ties, next-day re-inspection, fresh Top-K, 20 per 10,000: +14.7 pp [13.8, 15.6]", with the per-make table and "95.5-99.3% repeats; an inspection has no effect on failure" beside it |
| 2 | Budget-specific cooldown conclusion | Kept as written (A4-A8): "no robust ML advantage established" at B20, not equality; sensitivity runs favour SMART; B10/B50 favour ML |
| 3 | 12 May: "gap in the archive analysed", 0.23 pp is an exclusion sensitivity, no publisher-confirmed outage | Done: gap note, run record, review note, claims A31 |
| 4 | Apply "replicated" mechanically; print the 2024 intervals for B10/B50; 52.8% is the highest observed, not the best; A5 B50 fresh is +3.3 | Done. 2024 intervals (fresh / queue): B10 +4.0 [+2.6,+5.3] / +2.9 [+1.6,+4.2]; B50 +4.0 [+3.1,+4.9] / +6.1 [+5.3,+6.8]; test B10 +9.8 [+8.8,+10.8] / +4.1 [+3.0,+5.2]; B50 +3.3 [+2.5,+4.1] / +4.4 [+3.7,+5.0]: all four meet the strict rule. B50 next-day queue does not (test +0.7 [-0.3,+1.7]). The 2024 rotating-tie cooldown/queue result (-1.2 [-2.4,+0.0]) is not called a replication. "Best" replaced in A9, run record item 4, review note finding 1 and conclusion 3 |
| 5 | Evidence and lock statements | A25 now lists the separate completeness checks (0 missing/0 different in six quarters, 18 parts, monthly 1e-6, daily K > 0); A26 says "documented authorization file" and "a hash is not a signature"; only the Phase 5 test-period policy replay ran once (the 10% Phase 4 test sample was scored on 11 Nov); recorded UTC times are not cited as an accurate chronology (B13) |
| 6 | Descriptive findings separate from explanations | Never-caught note rewritten; "schedule matters more than the score" stays qualified as partly mechanical (B1, B8); by-make reversals reported beside every headline (A22) |
| 7 | AP and uncertainty | B11: the label gap affects absolute AP levels and it is not shown that the order of scores is unaffected; B5: intervals are event resampling only, not model-cluster, retraining or fleet-mix uncertainty; RQ3 h7 vs h30 stays exploratory (B16) |
| 8 | Narrow novelty, exact credit, no "first" | Literature note header and C4 updated; manuscript wording: "a stateful inspection-service replay added to daily alert ranking, with a frozen evaluation discipline" |
| 9 | Journal-level caveats | B21 added (right-censoring at 2026-06-30, observed failures only, model mix change between 2024 and test); B6 (seed-0 logs only) and B1 (watch-list, not repair) kept |

## Optional improvements, status

1. One-page policy dashboard: for the Power BI work (Sulaiman) and the manuscript figure.
2. Read-only checks: E1 make audit done (blocker 1); the 8-failure crosswalk needs the snapshot (lab PC).
3. Reproducibility appendix: to be assembled from `04_provenance` and the claims file for the manuscript.
4. Human reviewer: still open.

## Version 2 (not done; needs a new frozen protocol and an untouched future period)

Fair cross-make baselines (within-make ranks or quotas, shared-attribute rules); inspection consequences and costs;
external or prospective validation; new tie seeds, other cadences and queue sizes, model-cluster and retraining
uncertainty. The period 2025-01-01..2026-06-30 is not reused to choose any of these.
