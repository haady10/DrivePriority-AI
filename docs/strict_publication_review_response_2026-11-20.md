# Response to the strict publication review of 19 November 2026 (manuscript V3)

Review: `DrivePriority_AI_Strict_Publication_Review_2026-11-19.md` (Astra), verdict "major revision before
submission", reviewed commit `ce724b0`. This note gives an independent verdict on every correction, what was
changed, and what is still open. Written 20 November 2026 (Gulf Standard Time).

**Rules kept.** No model was retrained and no policy was replayed; the frozen protocol, the run records and the
two result archives are unchanged (the archives in `published_results/phase5/` are byte-identical to the OneDrive copies;
SHA-256 as fingerprinted by the review). New numbers come only from read-only analyses of the saved results and
the frozen daily replay inputs (`tools/phase5_post_test/`, outputs in `published_results/phase5_post_test/`). Stale values
in the claims map are corrected with a dated note, not overwritten silently.

**Labels.** REQUIRED = needed for an honest IEEE submission of the paper as framed; RECOMMENDED = strengthens it,
not needed for correctness; UNNECESSARY = not needed now. "Done" means implemented in manuscript V3 and/or this
repository.

## 1. Verdict per correction

| # | Review point | Verdict | Independent evidence | What was done |
|---|---|---|---|---|
| F1a | The +14.7 headline mixes ranking quality, attribute availability, allocation and ties | Agree; the evidence is stronger than stated | On every day of both periods the drives with a SMART count of 4 or 5 outnumber the slots (smallest margin 171 test, 11 in 2024) and all are Seagate, so E1 schedules only Seagate drives **under every tie seed and every tie rule**, not only in the seed-0 log (A40) | Stated beside the confirmatory result (VI-B), in VI-E, abstract, contributions, discussion |
| F1b | Show the rotating-tie runs prominently | RECOMMENDED; done | `cell_results.csv`, group `sens_tie`: SMART 14.6 / 32.8 / 50.4 / 52.8; ML changes < 0.1 points (A39) | Table VI column, Fig. 3 triangles, abstract, discussion. New in text: with rotating ties the next-day queue advantage falls from +11.2 to +2.4 |
| F1c | Common-attribute and manufacturer-aware SMART baselines, rotating ties at budgets 10 and 50 | REQUIRED for any general ML-versus-SMART claim; RECOMMENDED for the narrowed evaluation paper. **Not done** | Needs per-attribute values from the snapshot (the replay input holds only the count and the sum) and new replays; the standing instruction for this revision was not to rerun replays | Claims narrowed; the missing baselines are named as the main open control (VII, VIII, IX); plan in section 4 |
| F1d | "The lock prevents further experiments" is not a scientific answer | Agree (REQUIRED wording) | V2 limitation said so | Removed; V3 says nothing in the protocol prevents labelled post hoc runs |
| F2a | The replay measures allocation, not maintenance value | REQUIRED framing; done | 95.5–99.3 per cent repeat inspections; each failure counted once | Introduction, new discussion paragraph "What the replay measures", construct limitation |
| F2b | Matched-effort comparison separating ranking depth from the service rule | RECOMMENDED (REQUIRED only for operational claims, which V3 does not make). Not done | needs new policy evaluations | Stated as not run (VI-D, IX) |
| F2c | 652 slots against 12.5 failures a day is a design assumption | REQUIRED; done | `daily_capacity.csv`, 6,826 events / 545 days | V-A, VIII |
| F3a | Actionable-AP restrictions missing from the manuscript | REQUIRED; done | `prediction.py` lines 33–40 implement protocol section 14 exactly; the manuscript did not describe it | V-F and Table XIV note give the exact population, counts and prevalence |
| F3b | Recompute with full follow-up and with imminent failures as negatives | RECOMMENDED; done (read-only) | D0 reproduces `rq3_horizons.csv` to the last digit in both periods; D2 and D1 keep every score's place in both periods (A44) | Table XVII (Appendix), VI-H, limitation |
| F3c | Positives kept with unknown 30-day label, negatives not | Correct; effect small | Test AP falls by at most 0.0009 (D0 to D2), 2024 by at most 0.0044 | Described; no conclusion changes |
| F4a | Off-by-one: failures 1–7 (not 1–6) days after the decision day are unrewarded; label omits day 31 | Verified; REQUIRED; done | label `t < f <= t+30`, endpoint `7 <= f-(t+1) <= 30` | IV-D rewritten |
| F4b | Unsupported mechanism for the 7-day model | Verified; REQUIRED; done | the 7-day label (days 1–7) lies wholly outside the rewarded days 8–31 | Causal sentence removed; candidate explanations listed as untested (VI-H) |
| F5a | 6,834 vs 6,826: different populations | REQUIRED; resolved for the manuscript | Replay-cohort shares (6,271 eligible events; serial + model; HDD) differ from the old query by under one point per make (A41) | Table XII uses the replay cohort; drive counts, highest count and the 850 Seagate drives now also from the replay population (A46) |
| F5b | 112 failures unitemised | RECOMMENDED (no reported rate uses them; disclosed). Partly done | 6 are drives seen in 2024 that failed on 2025-01-01 (A47); a failure enters a selection only with an earlier record in the same period | Table II note, limitation, B24. The other 106 need a snapshot query |
| F6a | Carry model-cluster uncertainty into abstract and conclusions | REQUIRED; done | `cluster.json` | Abstract, VI-B, IX: cooldown −7.9 to 10.5 |
| F6b | Sensitivity grid and subgroups are descriptive; multiplicity | REQUIRED; done | | V-E, VIII |
| F6c | Temporal or grouped sensitivity | RECOMMENDED; done | Calendar-week block bootstrap: interval ends move by at most 0.4 points; no conclusion changes (A45) | Table IX column, VI-C |
| F6d | 703 million drive-days are not the inferential sample | REQUIRED; done | | V-E, VIII |
| F6e | Save per-event results for every policy in future runs | RECOMMENDED (future) | only primary per-event results exist | IX |
| F7a | Section V opening contradicts the post-dry-run queue rule | Verified; REQUIRED; done | protocol section 19: JSON unchanged, C-2 behaviour added, C-1/C-5/C-8 corrections | Section V intro |
| F7b | Confirmatory contrast declared after the 2024 replay | Verified; REQUIRED; done | reading plan, commit `0b83f55`, 13 Nov 16:16 | V-E, Table V, VIII |
| F7c | "One pre-specified test evaluation batch" | REQUIRED; done | | Abstract, contributions, V-H |
| F7d | "Consistent across periods" instead of "replicated" | REQUIRED; done | | Throughout |
| F8a | Availability statement: archives are small | Verified; REQUIRED; done | 8,427,600 and 5,198,218 bytes | `published_results/phase5/` with checksums; availability statement |
| F8b | H2O test not opt-in; Windows test unguarded | Verified; REQUIRED; done. Also found: the README command failed without `PYTHONPATH` | before: 403 passed, 4 skipped, 1 failed (H2O excluded); now `python -m pytest tests -q`: 405 passed, 6 skipped | `conftest.py`, guards, README |
| F8c | Licence, versioned release, pinned commit | Commit pinned: done. Licence and release: **owner decision, REQUIRED before submission** | | README states the status |
| F8d | Lightweight verification command; split dependencies | Verification: done (`tools/verify_results.py`, pandas + pyarrow only, with a test). Requirements file not split: RECOMMENDED | | README |
| F8e | Do not edit old run records | Agree | | Archives unchanged; claims corrections dated |
| P1 | Author e-mail placeholder | REQUIRED; owner | | Open |
| P2 | Tie comparison and manufacturer allocation beside the primary result | REQUIRED; done | | Table VI |
| P3 | Shorter title; move audits and grid to a supplement | Title: RECOMMENDED, done. Supplement: UNNECESSARY until a venue and page limit are chosen | | New title |
| P4 | "All pre-specified sensitivity contrasts are reported" | Verified as overstated; REQUIRED; done | protocol sections 10, 12, 13 | Appendix Tables XV–XVI; wording narrowed; complete files in the repository |
| P5 | Claims map: A28 626 vs 631, A20 0.092 vs 0.091 | Verified; REQUIRED; done | 315,806 drives → 631; 0.09145 → 0.091 | Dated corrections |
| P6 | Statistical limitation wording | REQUIRED; done | | VIII |
| P7 | Reference-by-reference audit | RECOMMENDED; partly done | | Li et al. 2016 (SRDS) added as the original of [li18]; HDFPM pages added; seven references re-checked against publisher or primary records in this session; see section 3 |

## 2. Where this response differs from the review

- **F1c and F2b** are required for a paper that claims ML is better or worse than SMART triage, or claims
  operational value. V3 claims neither: its central statement is the conditional one the review proposes. They
  remain the most likely reviewer requests, which is why the final verdict is still "not ready".
- **F5b** is recommended, not required: the 112 failures enter no reported rate, the reason category is now
  documented and six are itemised.
- **F3** is not an implementation error: the code follows the frozen protocol (section 14). The defect was that
  the manuscript did not describe the definition. The stress test shows the order of the scores is robust.
- **Not raised by the review:** the by-make table with intervals (manuscript Table XI, Fig. 5) came from a post hoc
  script of 14 November that was not kept. It is now generated by `tools/phase5_post_test/by_make_table.py` with
  the engine's bootstrap: identical estimates and never-captured shares, interval ends within 0.3 points; V3 uses
  the reproducible table. `tools/manuscript_tables_figures.py` rebuilds the data of every table and all figures
  from the published archives (checked identical to the manuscript build).
- **F1a** understated the evidence: the all-Seagate allocation of E1 is a property of the data on every day, not
  a one-seed observation. Conversely, V2's abstract attributed it partly to frozen ties, which was wrong: it holds
  under any tie order.

## 3. Reference checks in this session

Checked against publisher or primary-source pages: Li et al. 2016 (new; SRDS 2016, pp. 71–80; the DOI printed in
the PDF could not be confirmed against the registry, so none is given), Siemroth and Park 2026, Jing et al. 2026
(pages 1–25 added), Wei et al. 2025 (DFPoLD), Ahmed and Green 2025, Zhang et al. 2025 (LWCM), Pecht and Elburn
2020 (no volume or pages listed by the publisher's abstract page). Not re-checked here: the author list of Wei et
al. 2025 (Scientific Data), the PawsPro and LiteDiskGuard records (checked by the review and earlier), and the
remaining references checked in earlier sessions.

## 4. Open work before submission

Owner decisions: author e-mails; licence; a versioned release or tag that the manuscript cites; venue, template
and page limit (V3 is 15 pages; the Appendix and Tables VII, VIII and XV–XVII can move to a supplement).

Analyses (read-only, lab PC): itemise the 106 remaining failures from the snapshot; re-check the reference list.

**Update, 20 Nov 2026 (later the same day).** Amendment A1 (`docs/phase5_amendment_A1_fair_baselines_2026-11-20.md`)
fixes experiment 1 below and the itemisation of the unplaced failures before either is run, and its code is in
`src/drivepriority/posthoc/` with a step-by-step guide (`docs/posthoc_A1_lab_pc_guide_2026-11-20.md`). Correction to
the line below: the single SMART values are also in the full-fleet feature files (`features_valid_*`,
`features_test_*`), so experiment 1 needs those files and the frozen replay inputs, not the snapshot; only the
unplaced failures need the snapshot. A1 runs both tie orders, not only rotating ties, and keeps the per-event
results (mean over seeds) of every A1 cell. Experiment 2 (a matched-effort service rule) is not part of A1; A1
reports equal effort descriptively only.

Experiments, to be recorded first as a dated protocol amendment and reported as post hoc (they use periods that
have been seen):

1. Fairer SMART baselines on 2024 and the test period: count of SMART 5, 197 and 198 only; within-manufacturer
   percentile rank of the five-indicator count; proportional manufacturer quotas applied to both scores; all with
   rotating ties, at budgets 10, 20 and 50. Needs the per-attribute values from the snapshot.
2. Matched-effort comparison: a cyclic or episode-based service rule matched to the cooldown policies on completed
   inspections and distinct drives inspected.
3. Save per-event results for every evaluated policy and seed, so that model-cluster intervals exist for all.

For scale, the complete test batch took 40 minutes and the 2024 batch 23 minutes on the lab PC (`run_info.json`).
A confirmatory claim would need an untouched later period or a second fleet, with the analysis fixed before it
is opened. An actionable-label model (new training) is needed only for mechanism claims about horizons.

## 5. Files changed

- Manuscript V3 (not in this repository): title, abstract, Sections I, III-D, IV-D, V (intro, A, D, E, F, H),
  VI-B to VI-H, VII, VIII, IX, Appendix, availability statement, references; Tables II, V, VI, IX, XII, XIV
  notes and content; new Tables XV–XVII; Fig. 3.
- `published_results/phase5/` (archives, `SHA256SUMS.txt`), `published_results/phase5_post_test/`, `published_results/README.md`.
- `tools/verify_results.py`, `tests/test_verify_results.py`, `tools/manuscript_tables_figures.py`.
- `tools/phase5_post_test/`: `actionable_ap_reanalysis.py`, `signal_share_replay_cohort.py`,
  `time_block_bootstrap.py`, `smart_count_day_facts.py`, `first_day_failures.py`, `by_make_table.py`.
- `conftest.py`, `tests/test_h2o_setup.py`, `tests/test_integration_operator.py`, `.gitignore`, `README.md`.
- `docs/phase5_claims_and_limitations_2026-11-14.md`: A20, A28, A29, A33, B5, B11, B20 updated with dated notes;
  new A39–A48, B23, B24.
