# Why the SMART baseline inspects almost only Seagate drives (14 Nov 2026)

Read-only checks on the integrated snapshot (`C:\DP-out\smart_by_make.log`, `smart_by_make2.log`,
`smart_count_dist.log`, copies in OneDrive `05_Phase5_RESULTS\04_provenance`). Nothing was changed or rerun.
Decision-support simulation on old data; no prevention claim.

## The question
In the Phase 5 test replay, cell E1 (SMART count, next-day re-inspection, fresh Top-K) captures 29.8 per cent of
Seagate failure events and 0.0 per cent of HGST, Toshiba and WDC events (0 of 3,783 non-Seagate eligible events).
About 36 per cent of all eligible failures are never captured by any of the eight primary policies, and Toshiba
(55.8 per cent never captured) and WDC (48.6 per cent) are the worst. The working hypothesis was that those makes
do not report the SMART attributes the baseline uses.

## What the data shows

### 1. Two of the five indicators are reported only by Seagate
Share of rows where the attribute is NULL, fleet of 2025-06-02 (319,304 drives):

| Make | drives | smart_5 | smart_187 | smart_188 | smart_197 | smart_198 |
|---|---|---|---|---|---|---|
| Seagate | 109,456 | 0.000 | 0.001 | 0.001 | 0.000 | 0.000 |
| Toshiba | 104,407 | 0.000 | **1.000** | **1.000** | 0.058 | 0.000 |
| WDC | 75,080 | 0.000 | **0.996** | **0.994** | 0.004 | 0.004 |
| HGST | 27,302 | 0.000 | **1.000** | **1.000** | 0.000 | 0.000 |

`smart_187` (reported uncorrectable errors) and `smart_188` (command timeout) are Seagate-only in this fleet.
The Phase 4 baseline counts how many of the five indicators are above zero and reads a missing value as zero
(rule R21 with the NULL-as-0 convention recorded in the Phase 4 review fixes).

### 2. The baseline therefore has a different ceiling per make
Distribution of the indicator count on 2025-06-02:

| Count | Seagate | Toshiba | WDC | HGST |
|---|---|---|---|---|
| 0 | 93,899 (85.8%) | 100,243 (96.0%) | 73,690 (98.2%) | 24,749 (90.7%) |
| 1 | 11,348 | 3,640 | 1,051 | 848 |
| 2 | 2,656 | 459 | 298 | 733 |
| 3 | 698 | 65 | 41 | 972 |
| 4 | **749** | 0 | 0 | 0 |
| 5 | **106** | 0 | 0 | 0 |
| max reached | **5** | 3 | 3 | 3 |

### 3. The budget is smaller than the Seagate-only top band
The daily capacity is `floor(20 x N/10,000)`, about 626 drives for a fleet of about 313,000 HDDs. On 2025-06-02
there were 855 Seagate drives at count 4 or 5 — more than the entire daily budget. A fleet-wide Top-K on this
score therefore fills every slot from the Seagate-only band and never reaches a non-Seagate drive, whatever its
condition. With ties frozen per drive the same drives win every day, which is also why E1's repeat share is
99.3 per cent and it reaches only 2,345 distinct drives in 18 months.

### 4. The other makes are not short of warning signs
Share of failed drives with at least one indicator above zero in the 30 days before failure (6,834 failed drives,
test period):

| Make | failed drives | any indicator nonzero | smart_5 | smart_197 | smart_198 |
|---|---|---|---|---|---|
| HGST | 1,292 | **0.906** | 0.746 | 0.891 | 0.793 |
| Seagate | 2,702 | 0.830 | 0.712 | 0.487 | 0.487 |
| WDC | 829 | 0.765 | 0.469 | 0.674 | 0.062 |
| Toshiba | 2,011 | 0.642 | 0.447 | 0.515 | 0.245 |

HGST has the highest signal availability of any make and still receives no inspections under E1. The hypothesis
that these makes lack SMART evidence is wrong; what they lack is access to the top of a shared ranking.

### 5. It is a model-level effect too
Share of failed drives with any indicator nonzero before failure, by model (at least 40 failures): HGST
HUH721212ALN604 0.945, ST8000NM0055 0.927, HGST HUH721212ALE604 0.917, ST12000NM0008 0.895, WDC WUH721816ALE6L4
0.817, TOSHIBA MG07ACA14TA 0.808, WDC WUH722222ALE6L4 0.743, TOSHIBA MG08ACA16TA 0.654, **TOSHIBA MG08ACA16TEY
0.254**, **ST24000NM002H 0.131**. The two weakest are one Toshiba and one Seagate model, so the effect is not
purely a manufacturer property.

## What this means
1. The SMART-count baseline is biased by attribute availability, not only by drive health. Any fleet-wide
   threshold or Top-K on a count of heterogeneous attributes inherits that bias.
2. Part of the headline next-day ML advantage (+14.7 points) is this degeneracy: the baseline it beats can only
   ever select one manufacturer in that cell. The advantage must be reported with the per-make table beside it.
3. A fair cross-manufacturer SMART baseline needs per-make normalisation (rank within make, or use only the
   attributes every make reports), or per-make inspection quotas. Not done here; it is a protocol change and
   belongs to a version 2 study with its own untouched test period.
4. It does not explain everything. Under the 7-day cooldown, SMART reaches 70.5 per cent of HGST events against
   48.8 for ML, so the spaced-inspection policies do reach the other makes. Toshiba remains the weakest on both
   scores (20.8 and 18.6 per cent), and its lower indicator availability (0.642) is part of that.

## Status
Established from the snapshot by read-only queries. The mechanism in section 3 is an arithmetic consequence of
sections 1 to 3 and of the frozen capacity rule; it was not re-derived from the service logs. No result file was
changed and nothing was rerun.
