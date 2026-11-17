# Human review of Phase 5 and of manuscript V1 (summary dated 18 Nov 2026)

Source: `DrivePriority_AI_Human_Review_Anonymous.pdf`, one-page summary (OneDrive
`05_Phase5_RESULTS\03_reviews\2026-11-18_human_review_summary_anonymous.pdf`, SHA-256
c3f942dae2306ec329a361c94d1ed1e9bb95f1a3b3a7f61172532d648f2bf7d0).

- **Reviewer:** one anonymous human reviewer; name and role are not disclosed in the summary. The summary does
  not state the reviewer's relation to the team; the coordinator reports the review as independent.
- **Scope:** the Phase 5 experimental protocol, code and repository review findings, simulation results and
  numerical comparisons, and IEEE manuscript V1, using the review materials supplied by the team.
- **Date:** 18 Nov 2026 is the date of the summary; the date of any earlier individual review is not specified.
- **Verdict:** retain the computed Phase 5 simulation results with appropriate qualifications; **major revision of
  the manuscript is required before submission.**

This closes the open item "no human review" (claims B17). It does not change any number and nothing was rerun.

## Requested changes and their status

| # | Requested change | Status on 18 Nov 2026 |
|---|---|---|
| H1 | Clarify SMART manufacturer bias and tie-breaking bias | Evidence exists (claims A22, A27, A34, A35, B3, B23; manuscript V1 sections VI-C and VI-E); the manuscript text must make both biases explicit next to every headline number. Open |
| H2 | Report broader statistical uncertainty | Event bootstrap only in the primary tables (B5). A post hoc drive-model cluster bootstrap exists (42 models: next/fresh +14.7 [9.6, 21.4]; cool/fresh +0.6 [-7.9, 10.5]; cool/queue -0.3 [-7.2, 8.0]). Retraining and fleet-mix uncertainty not estimated. Open |
| H3 | Reconcile the eight-event difference (6,834 against 6,826) | Not reconciled (B20); needs a read-only query of the snapshot on the lab PC. Open |
| H4 | Explain the 12 May 2025 data-gap reporting and the evaluation timeline | Gap evidence in `docs/phase5_may12_gap_root_cause_2026-11-14.md`; timeline (protocol frozen, Phase 4 test-sample scoring 11 Nov, reading plan, single test replay 13 Nov, gap found and explained 14 Nov, reviews) to be written into the manuscript. Open |
| H5 | Improve manuscript and repository documentation | Open |
| H6 | Do not present the simulation as proof that failures were prevented | Already the wording everywhere (B1); keep |

Next: manuscript V2 addressing H1 to H5.
