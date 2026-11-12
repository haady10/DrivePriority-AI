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

### Measured: the dry run was repeated on 12 November 2026 with the fixed code (commit `1e9d54f`)

Same replay input as the first run (`replay_valid.parquet`, SHA-256 `507a44c7…e558`, 10,467,899 rows), run in
192 seconds on a Linux machine. All checks passed. Compared with the first run file by file:

| File | Result |
|---|---|
| `cell_results.csv` (126 cells, every numeric column) | identical; largest difference 2e-16 (rounding between two machines) in interval ends |
| `rq1_contrasts.csv` (238 contrasts) | estimates identical; interval ends within 3e-16 |
| `rq2_stability.csv`, `rq3_horizons.csv`, `daily_capacity.csv` | identical (largest difference 3e-14) |
| `event_coverage.parquet` (495 events, per-event result in each of the eight cells) | identical |
| Queue entries removed by the new step 1b | 0 in every cell |

One small difference: the column `share_of_resamples_above_zero` in `rq1_contrasts.csv` moves by at most 0.0035
(7 of 2,000 resamples) in 36 rows. The estimates and intervals of the same rows are equal, so the likely cause is
resamples whose difference is exactly zero and falls on one side or the other by rounding on another machine.
This column is not used in any reported result. The reviewer may want to confirm the cause.

Not repeated: the step that builds `replay_valid.parquet` from the Phase 4 score files (those files are on the
laptop). That step's code changed only in one place (failure dates after 31 January 2025 are dropped at load).

New check results on the real data: the no-future check compared 86 service logs (959,508 rows) from 23 policy
families after changing 18.2 million later score values: no log changed. The eligible event set has the same
SHA-256 in every cell and does not depend on scores.

## What the reviewer should check again

1. B1 to B4 against the new code and tests (this file names each test).
2. Protocol section 19: are C-1 to C-6 clarifications, or does any of them change an experimental setting?
3. The repeat of the validation dry run recorded above.
4. Still not checked by anybody outside the team: a rebuild of the Phase 4 scores, and the ten-seed decisions
   (only seed 0 logs are saved).

---

# Re-review of 12 November 2026 (commit `6fabff1`): verdict "accept after small fixes"

Re-review: `DrivePriority_Phase5_Independent_ReReview_2026-11-12.pdf`. It asks for three small actions before the
test split may be opened. All three are done. No experimental setting was changed and the test split was not opened.

## Action 1: C-2 is a behaviour change, and validation outcomes had been seen

Agreed. Protocol section 19 now has a column "Kind" (Behaviour, Correction, Control) and a paragraph on C-2:

- The first protocol said only "the queue never exceeds 3 x K". It did not say what happens when K falls below
  a queue that is already longer. Step 1b decides that. It is a change to the method.
- It was written on 12 November 2026, **after** the outcomes of the 10% validation dry run were observed.
- In the 2024 validation replay the case did not occur: 0 entries removed in every cell. No validation number
  depends on the rule.
- On the test period it may occur. The report must then give the number of removed entries
  (`queue_removed_cap_fell` in `cell_results.csv`).

All eight entries (C-1 to C-8) are marked "Validation sample: yes" for results seen before.

## Action 2: overrides of the test lock

The re-review is right that `--authorization` and `--test-lock-dir` made the one-time control easy to pass.

| Before | Now (C-7) |
|---|---|
| `--authorization <any file>` | Option removed. One fixed place: `configs/phase5_test_authorization.json` |
| `--test-lock-dir <any folder>` | Option removed. One fixed place: `~/.drivepriority/` |
| "Committed" was only a word in the documentation | The code asks git: the authorization file must be committed, and `configs/` and the simulation code must be exactly as in the commit. Otherwise the test split is refused |
| Register entry: time, output folder, authorization hash | Also: git commit, who authorized, machine, user |

Tests: `test_test_split_needs_authorization_and_runs_once_whatever_the_output_folder` (the two options are now
unknown to the command line; an authorization that is not committed is refused) and
`test_authorization_must_be_committed_and_the_checkout_clean` (a real temporary git repository: not committed,
committed, code edited after the commit).

What this does **not** do, stated openly: a person who edits the code or deletes the register can still run
again, and the register cannot see another machine. No software lock on the team's own machine can prevent
that. The control is therefore the second choice the re-review offers: **coordinator-controlled execution with
an auditable run record.** The coordinator runs the test replay, on one machine, from a clean commit, and
commits `run_info.json` (protocol hash, code hashes, authorization, input hashes, machine) and the register entry.

## Action 3: the 36 differing rows of `share_of_resamples_above_zero`

Confirmed: the cause is resampled differences that are zero up to float rounding.

How it was checked: the replay was repeated with a probe that, for each of the 238 contrasts, counted the
resamples with an absolute difference below 1e-12.

| Fact | Value |
|---|---|
| Rows where the laptop and the repeat differ | 36 |
| Of these, rows that have at least one near-zero resample | 36 (all) |
| Rows with no near-zero resample that differ | 0 |
| Largest absolute value among the near-zero resamples | 4.4e-16 |
| Smallest absolute value among all other resamples | 2.2e-4 |
| Laptop value inside the range [count above +1e-12, count above -1e-12] | all 238 rows |
| Difference never larger than the number of near-zero resamples of the row | all 238 rows |

So there is a clean gap of twelve orders of magnitude between "zero with rounding noise" and the smallest real
difference, and every differing row is explained by which side of zero the noise fell on. A resample of
exactly zero happens when the resampled events are captured equally in both cells; it is most common where two
cells are almost the same policy (for example queue 1K/2K/2d against fresh Top-K).

Fix (C-8): the column counts a difference as above zero only when it is larger than 1e-12. Test:
`test_share_above_zero_treats_rounding_noise_as_a_tie`. The repeat with this fix gives, in all 238 rows, exactly
the value the probe predicted; estimates, interval ends and "interval excludes zero" are unchanged (difference
0.0), and all 126 cells are unchanged.

Not done: the laptop run was not repeated with the fix, so "both machines now give the same column" is expected
from the logic above, not measured. The column is not used in any reported result.

## Tests after the re-review fixes

48 pass (46 before), plus the opt-in H2O test.

## Limits the re-review names, which stay open

- Nobody outside the team has rebuilt the Phase 4 scores or the full-fleet scores.
- Only the tie seed 0 service logs are saved; the ten-seed decisions were not reproduced independently.
- The reviewer did not run the tests or the repeat; they are reported by the team.
- The final conclusions need a separate review of the test replay.

