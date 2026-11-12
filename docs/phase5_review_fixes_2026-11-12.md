# Phase 5: answer to the independent review of 12 November 2026

Review: `DrivePriority_AI_Phase5_Independent_Review.pdf`, verdict **do not accept** until the blocking problems
are fixed. This file says what was done for every point. Every point of the review was checked against the
code and was found correct; none is disputed.

Rules kept while fixing:

- No experimental setting was changed. `configs/phase5_protocol_v1.json` is the file of the first commit.
- Nothing was chosen by looking at a result. The fixes are corrections and clarifications, logged as C-1 to
  C-6 in `docs/phase5_protocol.md` section 19.
- The test split (2025-01-01 to 2026-06-30) was not opened, not to test the lock either. The lock tests use
  small fake fleets.

## Blocking problems

| Review | What was wrong | Fix | Proof (test) |
|---|---|---|---|
| **B1** validation build read past the validation boundary | `full_fleet.py` built the drive table (first seen, last seen, failure date) from snapshot rows up to the end of the test period | The build reads rows only up to `valid_end + 31 days` (31 January 2025). These 31 days are the outcome window of the last validation day and were already inside the Phase 3 validation labels. The replay loader also drops any failure date after that window, so a validation replay cannot hold a later failure date | `test_validation_build_does_not_depend_on_rows_after_its_outcome_window`: every snapshot row after the boundary is rewritten (all drives "fail", counters changed) and the output is identical. `test_validation_replay_holds_no_failure_date_after_its_outcome_window` |
| **B2** queue could exceed 3K when K falls | Old unexpired entries stayed even when they were more than today's cap, and the rule checker allowed it | New step 1b in protocol section 8: keep the first `3 x K_t` entries in service order (oldest admission, then present drives, then today's ranking, then the tie key); the rest leave with the reason `removed_cap_fell`; nothing is admitted that day. The checker now counts any queue above the cap | `test_queue_never_exceeds_the_cap_when_capacity_falls_sharply` (K falls from 10 to 2). The slow reference simulator has the same rule and still equals the engine on random fleets |
| **B3** the no-future check was too weak | It cut the replay instead of scrambling, covered 2 cells, compared event counts | New module `checks.py`: all scores and labels after the middle day are shuffled between rows and all failure dates removed; every policy family (both scores, all budgets, all queue settings, both tie modes, all sensitivity scores, all persistence rules, all horizons, urgency-first) is replayed on both copies and the service logs must be equal row by row; the eight primary logs must equal the first part of the full replay. Events are compared by identity (SHA-256 of drive id and failure day) and must not depend on any score | `test_scramble_check_passes_for_honest_scores_and_catches_a_look_ahead` (a deliberately broken score that uses the whole period is caught), `test_event_identity_key_and_independence_from_scores` |
| **B4** test lock was not one-time | Lock file lived in the output folder; `--quick` was accepted | The test split now needs: `--final-test-run`; the frozen protocol (no `--config`); no `--quick`; a committed authorization file `configs/phase5_test_authorization.json` (protocol hash, review document, who authorized, replays per population); a free place in a register outside the output folder (`~/.drivepriority/`). The full-fleet build needs the same authorization | `test_test_split_needs_authorization_and_runs_once_whatever_the_output_folder`: seven refusals, and a new output folder does not give a second run |

Limit of B4, stated openly: a register on one machine cannot see another machine. Across machines the control
is the committed authorization file and the committed run record. The test replay must be run on one machine.

## Non-blocking problems

| Review | Fix |
|---|---|
| **C1** JSON not tied to the written protocol | The SHA-256 of the frozen JSON is printed in the protocol document and held in the code; the code stops when the default file has another hash; a test compares document, code and file. The hash is taken after Windows line endings are turned into LF (see the note below) |
| **C2** repeat share "98-99%" | Report corrected: 99.0% and 98.2% (next day, fresh), 97.7% and 96.9% (next day, queue), 94.0% to 95.8% (cooldown) |
| **C3** "41% cannot be caught" | Report corrected: 182 of 464 events (39.2%) had no timely capture in any of the eight primary cells under any tie seed; 188 (40.5%) with seed 0 alone. No statement about signals or about other methods |
| **C4** "each model is best on its own label" | Claim removed. The report gives the three own-label numbers and says they are on different labels and do not rank the models |
| **C5** `smart_count_sum` used a period-wide rank | Now a fixed row-wise formula (count x 2^49 + sum + 1), exact in float64, same order (count first, sum inside equal counts). Test: `test_smart_count_sum_is_row_wise_and_orders_count_first` |
| **C6** "days in a row" | Report and protocol now say consecutive observed records, with the reset after a gap of more than 7 days |
| Tie sensitivity | Moved next to the main RQ1 result in the report, with both intervals |
| Queue 1K/2K/2d equals fresh Top-K | Kept as predeclared; test `test_small_queue_1k_2k_2d_equals_fresh_topk` proves the equivalence with and without cooldown |

## A new point found while fixing (not in the review)

The dry run on the Windows laptop recorded the protocol hash `065900cc…`. The committed file has the hash
`22110661…`. The content is the same: git wrote the file with Windows line endings on the laptop. A raw-byte
hash guard would have stopped every Windows run, so the hash is now taken after line endings are normalised.
A test proves that the Windows hash is the committed file with CR LF line endings.

## Tests

`tests/test_phase5_simulation.py`: 42 tests (33 before). `tests/test_phase5_full_fleet.py`: 4 tests plus the
opt-in H2O test (3 plus 1 before). All pass.

## Does any reported number move?

Expected from the logic: no number of the eight primary cells moves.

- C-1: the 10% dry run was built from the Phase 4 score files, not by `full_fleet.py`. Dropping failure dates
  after 31 January 2025 cannot change an event of 2024.
- C-2: the 2024 capacity is between 53 and 60 every day and the queue length of the day before was never above
  3 x K of the day (checked in `daily_capacity.csv` for E5 to E8), so step 1b never applied.
- C-3, C-4, C-6: checks and locks only.
- C-5: same order of rows, so the `smart_count_sum` sensitivity cells should be the same.

**Still to do: repeat the 10% validation dry run with the fixed code and compare file by file.** The laptop
was not reachable when the fixes were finished, so this comparison is not yet done. The result will be added
here. Until then the statement above is an expectation, not a measured fact.

## What the reviewer should check again

1. B1 to B4 against the new code and tests (this file names each test).
2. Protocol section 19: are C-1 to C-6 clarifications, or does any of them change an experimental setting?
3. The repeat of the validation dry run, once it is added here.
4. Still not checked by anybody outside the team: a rebuild of the Phase 4 scores, and the ten-seed decisions
   (only seed 0 logs are saved).
