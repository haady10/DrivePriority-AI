# Review of the Phase 5 test results and of the project (14 Nov 2026)

Reviewer: the same AI assistant session that worked on Phase 5 with the coordinator, asked to review its own
project. It is therefore a SECOND PASS, NOT an independent review. Strong part: every number below was
recomputed from the raw result files with new code. Weak part: the code review and the judgement of the claims
(the reviewer already knew both). An independent AI review (other model) and a human review remain open.

The study is a decision-support simulation on old data. It does not show that any failure was prevented.
The original test results are unchanged; this review changes only how they are read.

## Addendum after the independent AI review of 14 Nov (the reviewer here was too lenient on two points)

1. The one-day drop of 2025-05-12 (69,565 drives absent for one day) was called "negligible" below. The reading
   plan, written before the test, says the results are accepted only if no sudden drop is left unexplained. The
   independent review is right: acceptance is PENDING until the cause is reconciled with the upstream records.
   Bound computed since: leaving out the 281 eligible events whose window touches 12 or 13 May changes the four
   ML-minus-SMART contrasts by at most 0.23 points (confirmatory: +14.66 against +14.67).
2. Finding 1 below must be read for the primary budget only. At budgets 10 and 50 the cooldown contrasts with frozen
   ties favour ML in both years and pass the strict replication rule; no rotating-tie runs exist for those budgets.
   Correct wording: "At the primary budget of 20 per 10,000 no robust ML advantage was established under the 7-day
   cooldown; the tie, stronger-baseline and GBM sensitivity runs favour SMART on test." Not "no difference", not
   "SMART is better" without the budget.
3. "Because SMART fell" was a causal phrase for arithmetic: SMART with the queue is 4.6 points lower than in 2024
   and ML 2.0 points higher.

## Verdict: accept with changes

The official numbers are correct and the one-time test run followed the rules. No bug and no use of future
information was found. Three findings change what the report may claim.

## Findings

### 1. (important) Under the 7-day cooldown a simple SMART rule beat the ML model, in both years

Timely capture, 7-day cooldown, budget 20 per 10,000:

| Policy | Test, fresh Top-K | Test, queue | 2024, fresh Top-K | 2024, queue |
|---|---|---|---|---|
| SMART count, day-rotating ties | 50.4 | 52.8 | 51.2 | 52.0 |
| SMART count + sum (`smart_count_sum`), frozen ties | 47.3 | 49.9 | 47.2 | 51.1 |
| ML 30-day DRF (primary) | 44.1 | 47.6 | 47.5 | 50.8 |
| SMART count, frozen ties (primary) | 43.5 | 47.9 | 45.8 | 51.4 |
| ML 30-day GBM | 40.1 | 38.8 | 44.2 | 49.4 |

ML minus SMART with day-rotating ties: test -6.3 [-7.4,-5.2] (fresh) and -5.2 [-6.2,-4.2] (queue); 2024 -3.7
[-4.9,-2.4] and -1.2 [-2.4,+0.0]. For fresh Top-K this passes the strict "replicated" rule in SMART's favour.
ML minus `smart_count_sum` on test: -3.1 [-4.1,-2.2] and -2.3 [-3.2,-1.4] (2024: +0.4 and -0.3, both include zero).
The best policy of the whole study on the test period is SMART count with rotating ties, cooldown and queue: 52.8%.
"No reliable difference" understates this. Correct statement: with spaced re-inspection the ML model did not beat
simple SMART rules, and a SMART rule with rotating ties was ahead in both years. These are pre-specified
sensitivity runs, not the primary analysis, and must be reported as such.

### 2. (important) The frozen tie rule works against SMART

SMART count has few distinct values, so many drives tie. With ties frozen per drive the same tied drives win every
day: on test SMART reaches 10,446 distinct drives under the cooldown, with rotating ties 14,552. This costs SMART
about 7 points under the cooldown and about 9 in the next-day queue cell. With rotating ties the ML-minus-SMART
picture is stable across the two years; with frozen ties it is not:

| ML minus SMART | 2024 frozen | Test frozen | 2024 rotating | Test rotating |
|---|---|---|---|---|
| next day, fresh | +14.1 | +14.7 | +13.0 | +11.8 |
| next day, queue | +4.6 | +11.2 | +3.0 | +2.4 |
| cooldown, fresh | +1.8 | +0.6 | -3.7 | -6.3 |
| cooldown, queue | -0.6 | -0.3 | -1.2 | -5.2 |

The confirmatory result (next day, fresh) stays clearly positive in both tie modes. Both tie modes must be shown
side by side.

### 3. (important) "The schedule matters more than the score" is partly true by construction

In every policy 95.5 to 99.3% of inspections are repeats: about 352,000 completed inspections reach only 2,300 to
15,800 distinct drives out of 368,720. An inspection has no consequence in the simulation (the drive stays and is
inspected again). "Timely capture" therefore means "the drive was on the watch-list 7 to 30 days before it
failed". The cooldown gain (+31.7 points for SMART) comes mainly from a larger watch-list at the same daily effort
(about 7 times as many drives). Next-day re-inspection is not a realistic practice, and the one confirmatory
contrast sits in that cell. Report the schedule effect as an effect of watch-list size and state that inspections
have no outcome in this simulation.

### 4. The test period was not completely unseen

Phase 4 scored the 10% test sample on 11 Nov 2026 and its test metrics were checked then. The Phase 5 policy
results on the test period were computed once (verified). Do not write "data never looked at"; write "the policy
results on the test period were computed once".

### 5. The cooldown average hides opposite effects by make

ML minus SMART, cooldown, fresh Top-K, test: HGST -21.7, Toshiba -2.2, Seagate +9.7, WDC +12.1 (events 1,163 /
1,865 / 2,488 / 755). Resampling whole drive models (42 models) instead of events gives about -8 to +10 for the
cooldown contrasts; the confirmatory next-day contrast survives it (9.5 to 21.4).

### 6. Notes

- Prediction quality fell on test: ML 30-day AP 0.060 (2024: 0.092); SMART count 0.023 (0.032). The fleet changed.
- The lab PC clock is 4 hours behind: recorded `started_utc 13:32` was about 17:32 UTC. Order of events unaffected:
  the reading plan was committed before any test score or replay existed.
- 2025-05-12: about 69,600 drives missing for one day; 86 to 154 cancelled inspections; negligible effect.
- The retain rules cost nothing but gain little (list overlap 0.907 to 0.924 / 0.933).
- The 7-day ML model (50.6% under the cooldown) roughly equals the best SMART rule (50.4%); exploratory, no
  paired comparison.
- A random policy captures 4.0 to 4.1%; every policy is far above chance.

## Verified by recomputation

Event counts (6,271 + 368 + 187 = 6,826; burn-in = failures before 2025-02-01); the eight primary coverages; the
four ML-minus-SMART contrasts with an own bootstrap (other seed, 4,000 resamples, within 0.1 point); engine rules
in all 8 service logs (scheduled = capacity on all 545 decision days, no drive twice a day, service always the next
day, cooldown gaps >= 7 days between completed inspections, no inspection completed on or after a failure day);
queue rules (admissions <= 2K, length <= 3K, expiry exactly 3 days, no overlaps, no cap removals); all 14
simulation code hashes equal commit 68a8e3e; authorization and protocol hashes; no code change after the test
commit; tests 50 passed, 1 skipped.

## Not verified

The replay input, the features and the saved models (hashes only); Phases 1 to 3 data and the Phase 4 training
(notes only); the no-future-information check (taken from checks.json, code read); the ten tie-seed runs (only
seed 0 logs are saved); RQ2 and RQ3 beyond the tables.

## Corrected conclusions for the report

1. Confirmed (pre-declared): with next-day re-inspection and fresh Top-K the ML score captures 14.7 points more
   failures than SMART count (11.8 with rotating ties). This setting re-inspects the same drives daily and is not
   a realistic practice.
2. At the primary budget (20 per 10,000) no robust ML advantage was established under the 7-day cooldown. SMART count
   with rotating ties was ahead in both years (by 3.7 and 6.3 points with fresh Top-K); SMART count+sum was ahead on
   test. At budgets 10 and 50 the frozen-tie contrasts favour ML in both years.
3. At budget 20 the best policy on the test period is a SMART rule: SMART count, rotating ties, 7-day cooldown, queue (52.8%).
4. Spacing re-inspections (a larger watch-list at the same daily effort) raises capture far more than the choice
   of score; this follows partly from how the simulation counts a capture.
5. The ML-versus-SMART difference depends on the drive make (ML ahead on Seagate and WDC, SMART far ahead on HGST).
6. Retain rules make ML lists slightly steadier at no cost. The 7-day model works better for action than the
   30-day model (exploratory).
7. About 36% of failures are never captured by any of the eight primary policies; Toshiba and WDC least. Cause open.
