# DrivePriority AI: Phase 5 protocol (inspection simulation), fixed before any replay result

Written: 11 November 2026, before any Phase 5 code was run on real data.
Status: FIXED for the validation dry run. It becomes FROZEN for the test year after the independent review
(section 16). Any later change is a **deviation**: it is added in section 19 with the date, the reason, and
whether validation or test results had been seen.

Plain-language summary: Phase 4 gave every drive a risk score for every day. Phase 5 replays history one day at
a time. Each day a fixed, small number of drives is picked for a *simulated* inspection. Afterwards we count how
many drives that later had a recorded failure were picked early enough (7 to 30 days before). We compare the
old SMART rule with the machine-learning score under different inspection rules.

**This is a decision-support simulation. Nobody inspected these drives. A simulated inspection changes nothing
on the drive. The results describe how inspection slots would have been allocated. They do not show that
failures, downtime or costs are prevented.** "Failure" always means the failure flag recorded by Backblaze.

Machine-readable copy of every number in this file: `configs/phase5_protocol_v1.json`. The code reads the JSON.
If the two ever differ, the run must stop and the difference is a deviation.
SHA-256 of the frozen JSON: `22110661c20a838c709c9712249a36dfba6feef84291fd9316cbe66b8fa60af6` (taken after
Windows line endings are turned into LF, so every machine gets the same value). The code holds the same value
and stops when the default file has another hash; a test compares this line, the code and the file. The 10%
validation dry run of 11 November ran on a Windows checkout and recorded
`065900ccb5e01ab53754b5505555c423fa4484aac8c7e1deba0e56c661d4f2d2`: that is the hash of the same file with Windows
line endings (a test proves it).

---

## 1. What is fixed from earlier phases (not changed here)

| Item | Value |
|---|---|
| Data | Snapshot-0010, HDD-only cohort (rule R13), Phase 3 v3 features, labels and splits |
| Splits | validation 2024-01-01..2024-12-31; test 2025-01-01..2026-06-30 |
| Primary ML score | DRF, 30-day horizon (Phase 4 frozen protocol, chosen on 2024 validation average precision) |
| Other frozen scores | DRF 7-day and 14-day (RQ3); GBM 30-day (runner-up, sensitivity only) |
| SMART counters | raw 5, 187, 188, 197, 198 |
| Rows on and after a drive's first failure day | already removed in Phase 3 (a failed drive is never a candidate) |

## 2. Inputs and population

- **Replay input**: one table per split with one row per drive per observed day: `drive_id`, `date`,
  `fail_date` (first recorded failure of the drive, empty if none; used ONLY after the decisions, to count
  outcomes), `label_30d` (1 / 0 / empty; used ONLY to describe inspections afterwards), the SMART baseline
  columns and one column per frozen model score. It is built from the Phase 4 `scores_<split>.parquet` files by
  `prepare_input.py` without changing any value.
- **Every scored row is a candidate**, including rows whose outcome is unknown. Candidates are never filtered
  by labels or by what happens later (rule R18).
- **Population tag**. `sample10` = the 10% whole-drive evaluation sample from Phase 4 (about 28,600 drives per
  day in 2024). Results on it are **exploratory** (rule R18). `full_fleet` = all eligible HDDs; this needs the
  full-fleet scoring job on the lab PC and gives the **primary** results. The replay code is the same for both.
  Because K is a fixed share of the fleet (section 4) the sample estimates the same quantity, but with about ten
  times fewer failure events and therefore much wider uncertainty.

## 3. Calendar and time rule: decide today, inspect tomorrow

- Decision day `t`: at the end of day `t` the policy sees only data with date <= `t`.
- Service day `s = t + 1`: the simulated inspection happens the next calendar day.
- Replay window of a split: decision days from the first day `D0` to the day before the last day `D1`;
  service days `D0+1 .. D1`. All state (queues, last inspection dates, smoothed scores) starts empty at `D0`
  (cold start). Validation and test are replayed separately.
- **Observable fleet** `O_t` = the drives that have a row on day `t`. `N_t` = their number.
- A scheduled inspection is **completed** if the drive is in `O_s` (it has a row on the service day). Otherwise
  it is **cancelled** (`absent`, or `no_source_data` when the whole fleet has no rows that day). A cancelled
  inspection wastes its slot: the slot is never given to another drive, and it does not start a cooldown.
- A drive's failure day has no row in the input, so an inspection scheduled for the failure day is cancelled.
  It could never count as early warning anyway.
- If day `t` has no data at all, no decision is made that day.

## 4. Daily capacity K

`K_t = floor(B x N_t / 10,000)`, computed **before** any eligibility rule, queue or outcome filter.

- Primary budget `B = 20` inspections per 10,000 observed drives per day.
- Sensitivity budgets `B = 10` and `B = 50` (all eight RQ1 cells are rerun; nothing is chosen from them).
- No minimum of 1 is forced. If `K_t = 0` the day has no inspections.
- At most `K_t` distinct drives are scheduled per decision day. Unused slots are counted, never carried over.

## 5. Score sources

Higher score = inspected first. A missing score ranks below every real score.

| Name | Meaning | Role |
|---|---|---|
| `smart_count` | number of the five SMART counters above 0 (0 to 5) | **primary SMART** (rule R21) |
| `ml_h30` | DRF 30-day score | **primary ML** |
| `smart_count_sum` | `smart_count`, ties ordered by the raw counter sum | sensitivity (stronger-baseline check) |
| `smart_sum` | raw counter sum | sensitivity ("legacy", rule R21) |
| `gbm_h30` | GBM 30-day score | sensitivity (second ML model) |
| `ml_h7`, `ml_h14` | DRF 7-day and 14-day scores | RQ3 only |
| `random` | a fixed pseudo-random number per drive and day | reference floor ("no skill") |

**Stronger-baseline check**: if a sensitivity SMART variant has higher timely coverage than `smart_count` on
validation, the report also shows ML against that variant. The primary cells are not changed.

Scores are used as ranks only. They are not calibrated probabilities (rule R25).

## 6. Tie rule (frozen)

Many drives share the same score (SMART count has only six values). Order within a tie:

1. A **tie key** `u(drive, seed)` = a 64-bit hash of the drive id and a seed. Smaller key first. The key does not
   depend on the day, on row order or on the machine, so a tied drive keeps its place from day to day
   ("frozen tie order", methodology section 27).
2. The replay is repeated for **10 tie seeds (0..9)**. Every reported number is the **mean over the 10 seeds**
   (the expected value over random tie orders). The lowest and highest seed are also reported.

Sensitivity `tie_mode = day`: the key is re-drawn every day, `u(drive, day, seed)`. This makes tied drives
rotate. It is reported for the eight RQ1 cells and never used to choose anything.

## 7. Reinspection rules (factor 2 of RQ1)

- **Next day**: a completed inspection does not block the drive. It can be inspected again the next day.
- **7-day cooldown**: after a completed inspection on day `d` the drive cannot be inspected on `d+1 .. d+6`.
  It is eligible again on `d+7`. Rule: eligible for service day `s` if `s - d >= 7`.

Eligibility is checked at decision time for service day `t+1`. Cancelled inspections never count.

## 8. Scheduling rules (factor 3 of RQ1)

**Fresh Top-K.** Walk the day-`t` ranking from the top and schedule the first `K_t` drives of `O_t` that are
eligible. Drives that were not picked are forgotten.

**Carry-forward queue** (primary setting `admit = 2K`, `cap = 3K`, `expire = 3 days`, fixed now, not tuned):

1. *Expire*: remove queue entries admitted on day `a` when `t - a >= 3`.
   *1b. Cap after a fall of K (clarification C-2, section 19)*: if the entries that are left are more than
   `3 x K_t` (possible only when today's K is much smaller than before), keep the first `3 x K_t` in service
   order (oldest admission day first, then drives observed today before absent ones, then today's ranking, then
   the tie key) and remove the rest. They are logged with the reason `removed_cap_fell`. Nothing is admitted
   on such a day.
2. *Admit*: walk the day-`t` ranking from the top. A drive is admitted if it is in `O_t`, is not already in the
   queue and is eligible for service day `t+1`. Stop after `2 x K_t` new admissions or when the queue holds
   `3 x K_t` entries. One entry per drive.
3. *Schedule*: from the queue entries whose drive is in `O_t` and eligible, take up to `K_t` in this order:
   oldest admission day first, then today's ranking. Scheduled entries leave the queue (also if the inspection
   is cancelled later).
4. Everything else waits for the next day.

Queue sensitivity settings (reported, never chosen from): `1K / 2K / 2 days` and `2K / 5K / 7 days`.

Methodology v2.0 said the queue numbers would be finalised on validation. This protocol is stricter: the
illustrative values are fixed as primary before any result, so nothing is selected at all.

## 9. The eight primary RQ1 cells

Budget `B = 20`, scores `smart_count` and `ml_h30`, 10 tie seeds, same fleet, same days, same events.

| Cell | Score | Reinspection | Scheduling |
|---|---|---|---|
| E1 | SMART | next day | fresh Top-K |
| E2 | ML | next day | fresh Top-K |
| E3 | SMART | 7-day cooldown | fresh Top-K |
| E4 | ML | 7-day cooldown | fresh Top-K |
| E5 | SMART | next day | carry-forward queue |
| E6 | ML | next day | carry-forward queue |
| E7 | SMART | 7-day cooldown | carry-forward queue |
| E8 | ML | 7-day cooldown | carry-forward queue |

All eight cells are reported, also when a result is unfavourable.

## 10. Failure events and the main result (rule R19)

- **Event** = a drive's first recorded failure, date `f`. One event per drive at most.
- **Lead** of an inspection = `f - s` in calendar days (`s` = service day).
- **Eligible events** (the same set for every cell and every score):
  1. `D0 + 31 <= f <= D1`: the whole window `f-30 .. f-7` lies inside the replay. Earlier events are excluded as
     "burn-in" because the simulation had not been running for their whole window.
  2. The drive had at least one **opportunity day**: a service day `s` in `f-30 .. f-7` with the drive observed
     on `s-1` and on `s`. An event with no opportunity cannot be called a miss.
  Excluded events are counted with their reason. Sensitivity: only events with all 24 opportunity days.
- **Timely capture (PRIMARY ENDPOINT)**: the event has at least one *completed* inspection with
  `7 <= lead <= 30`. An event is counted once, however many inspections it received.
  `C(cell) = captured eligible events / eligible events`, mean over the 10 tie seeds.
- Secondary event results: any-prior capture (`lead >= 1`), `14 <= lead <= 30` capture, window `7..29`
  (boundary sensitivity), earliest timely lead (median and quartiles), events inspected only late
  (`1 <= lead <= 6`), events never inspected.

## 11. Workload and queue results

Per cell: slots available (sum of `K_t`), scheduled, completed, cancelled, unused; distinct drives inspected;
repeat share (`1 - distinct / completed`); completed inspections per timely capture; inspections by their
30-day outcome at the service day (failure within 30 days / no failure / unknown). "Unnecessary" = completed
inspection with a known no-failure outcome. Unknown outcomes are **not** counted as unnecessary.
Queue cells also: waiting days (schedule day minus admission day), expired entries, mean and maximum queue length.

Units (rule R25): coverage is per **event**; workload is per **inspection**; nothing in Phase 5 is weighted,
because the replay itself runs on the population given (sample or full fleet).

## 12. RQ1 statistics: does the ML advantage depend on the policy?

For reinspection `r` and scheduling `g`: `Delta(r, g) = C(ML; r, g) - C(SMART; r, g)`.

- Reinspection interaction: `Delta(cooldown, g) - Delta(next day, g)` for each `g`.
- Scheduling interaction: `Delta(r, queue) - Delta(r, fresh)` for each `r`.
- Three-way contrast: reinspection interaction under queue minus under fresh Top-K.
- Also reported: the plain effect of cooldown and of the queue for each score.

A finding of "no interaction" or "SMART is as good as ML" is a valid result and is reported as it is.

## 13. RQ2: alert stability against timeliness

Objects kept separate (methodology section 33): the **watchlist** (drives with an active warning), the
**queue** (section 8) and the **service log** (inspections). RQ2 uses fresh Top-K only, `B = 20`.

A persistence rule turns the daily score into a **priority**. Three standard rules, written as running
statistics over a drive's last *observed* days (history restarts when a drive was unobserved for more than 7
calendar days):

| Rule | Priority on day t | Equal to the classic rule |
|---|---|---|
| `daily` | today's score | no persistence (baseline) |
| `confirm_c` (c = 2, 3) | **minimum** of the last c scores (missing history = lowest priority) | "warning on c consecutive days" |
| `ewma_a` (a = 0.5, 0.2) | `a x today + (1 - a) x previous priority` | exponentially smoothed score |
| `retain_h` (h = 2, 6) | **maximum** of the last h+1 scores | "keep the warning h more days" |

- **Watchlist** `L_t` = the top `K_t` drives of `O_t` by priority (tie rule of section 6), before any
  reinspection rule. A second, wider list uses `5 x K_t`. Because every rule fills the same number of places,
  **occupancy is matched exactly by construction** (methodology section 36 asked for matched occupancy).
- **Inspections** = fresh Top-K on the priority, under next-day reinspection (primary, as in the proposal) and
  under the 7-day cooldown.
- Stability results per rule: mean day-to-day Jaccard overlap `|L_t and L_t-1| / |L_t or L_t-1|`; entries per
  day; turnover `entries / list size`; exits split into "drive not observed today" and "outranked"; mean
  number of consecutive days on the list.
- Timeliness results per rule: timely coverage (section 10), earliest lead, events only seen late.
- Runs: 7 rules x 2 scores (`smart_count`, `ml_h30`) x 2 reinspection rules. All 28 are reported on validation
  and on test. The coverage-against-turnover picture and its non-dominated settings (Pareto set) are described
  on validation; the test year shows whether the same picture repeats. No setting is picked from the test year.

## 14. RQ3: warning horizons

- Prediction quality per horizon comes from Phase 4 (average precision with prevalence, ROC AUC) on each
  horizon's own rows, plus here on the **common rows** where all three labels are known.
- **Actionable average precision** (new, descriptive): each score is also judged on the label "the drive fails
  8 to 31 days after the decision day". With inspection on the next day this is exactly a lead of 7 to 30 days,
  the part of a warning that leaves time to act. Rows 1 to 7 days before the failure are left out of this
  metric (they are neither a useful warning nor a false one). Negatives are rows with a known healthy 30-day label.
- **Budgeted coverage per horizon**: fresh Top-K, `B = 20`, both reinspection rules, with `ml_h7`, `ml_h14`,
  `ml_h30`, and same endpoint (section 10).
- **Urgency-first** (declared secondary, methodology section 38): slots are filled round-robin from the 7-day,
  14-day and 30-day rankings (rank 1 of each, then rank 2 of each, ...), skipping drives already taken.

## 15. Uncertainty

- **Drive-cluster bootstrap** (rule R26): one event per drive, so whole eligible events are resampled with
  replacement, 2,000 times, seed 20261111. Each event carries its capture result for every cell (mean over tie
  seeds), so differences and interactions are **paired**. 95% percentile intervals.
- These intervals are conditional on the replay that happened. They do not cover retraining or a different
  fleet (methodology section 39).
- **Fleet-competition sensitivity**: the eight cells are rerun on 5 random halves of the drives (whole drives,
  by hash).
- Subgroups (drive model seen or not seen in training, manufacturer, rules R15 to R17) are reported only with
  their event counts, and no claim is made for groups with fewer than 30 eligible events.

## 16. How validation and test are used

- **Nothing in this protocol is tuned.** Budgets, queue numbers, persistence grids, seeds and tie rule are fixed
  above before any replay result.
- **Validation (2024)**: dry run, debugging, acceptance checks (below), first look at the results.
- **Test (2025-01-01..2026-06-30)**: replayed **once**, after the independent review, with the frozen JSON
  config (its SHA-256 is recorded). The code refuses the test split unless all of these hold (clarification
  C-4): the switch `--final-test-run`; the frozen protocol file (no `--config`); no `--quick`; an authorization
  file `configs/phase5_test_authorization.json` committed to the repository after the review, naming the
  protocol hash, the review document, who authorized it, and the number of test replays per population
  (1 for `sample10`, 1 for `full_fleet`); a free place in the lock register of the machine
  (`~/.drivepriority/`, outside the output folder, so a new output folder does not give a second run); and no
  lock file in the output folder. The register cannot see other machines: across machines the control is the
  committed authorization and the committed run record. A repeat needs a logged deviation and a changed,
  committed authorization.
- **Reading boundary (clarification C-1)**: a validation run uses no row and no failure date later than
  `valid_end + 31 days`. Those 31 days are the outcome window of the last validation day (a row of day `t` is
  labelled by failures up to `t + 30`; a decision of day `t` is judged by failures up to `t + 31`). This window
  was already part of the Phase 3 validation labels. No score, feature or decision uses it.
- Validation and test are two separate periods. A pattern is called "replicated" only if it has the same sign
  in both and the test interval excludes zero.
- Disclosure carried over from Phase 4: 2026 data was seen during early feasibility work; Phase 4 printed
  test-year model metrics that were not used for any decision.

**Acceptance checks for every replay** (in `checks.json`; a failed check stops the use of the results):
scheduled inspections never exceed `K_t`; scheduled = completed + cancelled; no drive scheduled twice on one
day; the cooldown is never violated; the queue has one entry per drive, never exceeds its cap and holds no
expired entry; the eligible-event set is identical in every cell; the replay input has no row on or after a
failure day and no duplicate drive-day; a replay with all future rows scrambled gives identical decisions up to
that day (no use of future information). How the last check is done (clarification C-3): scores and labels of
all rows after the middle day are shuffled between rows and all failure dates are removed; every policy family
of the run (both scores, all budgets, all queue settings, both tie modes, every sensitivity score, every
persistence rule, every horizon, urgency-first) is replayed on both copies up to that day and the service logs
must be equal row by row; the eight primary logs must also equal the first part of the full replay. The
eligible events are compared by identity (a hash of drive id and failure day), not by count.

## 17. Outputs

`cell_results.csv` (every cell: coverage, secondary endpoints, workload; mean / min / max over seeds),
`rq1_contrasts.csv` (differences and interactions with intervals), `event_coverage.parquet` (one row per event:
eligibility, capture share per cell, earliest lead), `service_log_<cell>.parquet` and `queue_ledger_<cell>.parquet`
(seed 0), `daily_capacity.csv`, `rq2_stability.csv`, `rq3_horizons.csv`, `checks.json`, `run_info.json`
(config hash, input and code hashes, versions, population tag, split).

## 18. Limitations that belong in the report

- Simulated inspections only; no real inspection outcome exists; no prevention claim.
- A recorded failure flag can mean a suspected failure or a planned removal, not always a breakdown.
- An inspection does not change later scores or later failures in the replay.
- Cold start at the first day of each split; events in the first 31 days are excluded.
- `sample10` results are exploratory; intervals are conditional on the realised replay.
- The cooldown length (7 days), the budgets and the queue numbers are design choices, not optimised values.
- Everything already disclosed in `docs/protocol_frozen_before_test.md` section 9 still applies.

## 19. Deviations after fixing

No experimental setting has been changed. `configs/phase5_protocol_v1.json` is byte for byte the file of the
first commit (same SHA-256).

The entries below are **clarifications and implementation corrections** asked for by the independent review of
12 November 2026 (`DrivePriority_AI_Phase5_Independent_Review.pdf`, verdict "do not accept" until fixed). They were
written after the 10% validation dry run had been seen (test split never opened). None of them was chosen by
looking at a result. The validation dry run is repeated with the fixed code and compared file by file; the
comparison is recorded in `docs/phase5_review_fixes_2026-11-12.md`. The last column below is what the logic
predicts until that comparison is recorded.

| No. | Date | What | Why | Results seen before? | Effect on the validation dry run |
|---|---|---|---|---|---|
| C-1 | 12 Nov 2026 | Reading boundary `valid_end + 31 days` for the full-fleet build and for failure dates in any replay input (sections 2 and 16) | Review B1: the full-fleet validation build read drive first/last/failure dates up to the end of the test period | Validation sample: yes. Test: no | None on any reported number (later failure dates were never used for a result) |
| C-2 | 12 Nov 2026 | Queue step 1b: what happens when the waiting entries exceed `3 x K_t` after K falls (section 8) | Review B2: the protocol says the queue never exceeds its cap; the code let old entries stay | Validation sample: yes. Test: no | None: the case did not occur (K moved between 53 and 60) |
| C-3 | 12 Nov 2026 | The no-future-information check is done as written in section 16 (scramble, all families, exact logs); events compared by identity | Review B3: the first implementation cut the replay instead of scrambling and covered two cells | Validation sample: yes. Test: no | None on results; `checks.json` now holds the complete check |
| C-4 | 12 Nov 2026 | Stronger test lock (section 16) | Review B4: the lock was only in the output folder and `--quick` was accepted | Validation sample: yes. Test: no | None |
| C-5 | 12 Nov 2026 | `smart_count_sum` is computed row by row (count x 2^49 + sum + 1) instead of ranking the sums over the whole period | Review C5: a period-wide rank touches later rows, even if the order is the same | Validation sample: yes. Test: no | None expected: the order of rows inside every day is the same |
| C-6 | 12 Nov 2026 | The code stops when the default protocol JSON does not have the frozen SHA-256; the hash is printed in this file | Review C1 | Validation sample: yes. Test: no | None |

Wording, not a rule change: in section 13 "c days in a row" means c consecutive **observed** records of the
drive; the history starts again after a gap of more than 7 calendar days.
