# Review of the full-fleet VALIDATION (2024) results, before the test run

Date: 13 Nov 2026. Reviewer: a fresh AI reviewer session that had not taken part in building Phase 5 (it was given the frozen protocol, the code and the result tables, read-only). It is NOT a human reviewer. A human independent review is still open and is listed as a limit in the report.

## Verdict
AUTHORIZE TEST RUN. No blocking finding.

## Verified numerically by the reviewer (own code, raw tables)
- Event counts: 4,210 eligible + 346 burn-in + 51 no-opportunity = 4,607, one row per drive.
- E1..E8 coverage equals cell_results.csv to 6 decimals; paired contrasts match; reviewer bootstrap (other seed) within about 0.002, same sign of "excludes zero" everywhere.
- Capacity K_t = floor(20 x N_t / 10,000): scheduled = K on all 365 decision days; no drive twice a day; cooldown gaps >= 7 in E3/E4/E7/E8; queue admit <= 2K, size <= 3K, expiry exactly 3 days, no overlaps, no cap-removals.
- run_info protocol equals configs/phase5_protocol_v1.json; hash 22110661... matches; split valid, rows 2024-01-01..2024-12-31.
- Code read: rankings use one day's rows only; failure dates enter only after the decision; event eligibility does not depend on scores; persistence transforms are causal.

## Findings
1. Should-fix: ML versus SMART under the 7-day cooldown depends on the tie rule (frozen drive ties: ML +1.8 points; day-rotating ties: SMART ahead; with the queue the difference is -0.6, interval includes zero). Any such claim needs this caveat. Next-day ML advantage is stable.
2. Should-fix: the large next-day ML advantage is partly a weak-baseline effect (SMART count captures almost only Seagate; 99% repeat inspections). GBM reaches 29.2% vs DRF 26.4% next-day fresh.
3. Wording: say "simulated inspection slots" and "recorded failure flag"; never "prevented", "avoided", "saved".
4. Should-fix: run_info has input_sha256 null and prepare_manifest null. Record the replay input hashes for the test run.
5. Note: full_fleet.py / replay_input.py differ from the hashes in the validation run (commits 87d0813, 991fff0; label-unknown rule and SQL path quoting; no change to decisions, engine or events). The test is run from a clean commit that contains them.
6. Note: queue-cap rule C-2 fired 0 times in 2024. The test report must print queue_removed_cap_fell.
7. Note: disclosed reading boundary C-1 (labels only).

## Not verified by the reviewer
Replay input and models (training cutoff), a re-run of the no-future check (taken from checks.json), bootstrap reproduction, the wording of the summary note, per-make table beyond a crude cut.
