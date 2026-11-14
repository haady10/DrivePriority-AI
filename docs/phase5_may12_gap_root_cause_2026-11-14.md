# Root cause of the 2025-05-12 observation gap (14 Nov 2026)

Conclusion: the missing observations are absent from the ORIGINAL Backblaze archive. They were not lost in our
cleaning, integration or Phase 5 input preparation. The acceptance condition of
`docs/phase5_test_reading_plan_2026-11-13.md` section 8 ("no sudden drop in observed drives left unexplained")
is met. The test results stand as computed; nothing is rerun and no file is changed.

## What was observed
The Phase 5 test replay reports 243,354 observed HDDs on 2025-05-12 against about 313,000 on the surrounding
days (capacity K 486 against 625). An independent AI review (Astra, 14 Nov) made this a blocking item.

## Evidence

### 1. The gap is one Backblaze data center, for exactly one day
Rows per day per data center in the integrated snapshot (read-only check, `C:\DP-out\check_gap.log`,
`check_gap2.log`):

| Data center | 11 May | 12 May | 13 May |
|---|---|---|---|
| ams5 | 22,141 | 22,140 | 22,142 |
| iad1 | 42,001 | 42,000 | 42,002 |
| phx1 | 100,280 | 100,278 | 100,267 |
| sac0 | 78,048 | 77,979 | 77,979 |
| **sac2** | **70,380** | **absent (no rows at all)** | **70,380** |
| yyz1 | 3,600 | 3,600 | 3,600 |

70,414 drives are present on 11 and 13 May but absent on 12 May: 70,374 of them in sac2 (of 70,380), 38 in sac0,
2 in phx1. Whole vaults (1,220 of 1,220) and the whole of cluster `020` (18,120 of 18,120) are missing.
Since 2024-01-01 this is the only day whose total falls below 90 per cent of the previous day.
(Limitation of that scan: a data center that is entirely absent produces no group row, so the per-data-center
"under 20 per cent of usual" query cannot see it; the total-based query is what caught the day.)

### 2. The raw Backblaze CSV for that day is itself short
`Cleaned/source_manifest.json` of the 2025Q2 submission (`2025Q2_Zahid.zip`) records the members of the original
`data_Q2_2025.zip`:

| Raw member | compressed | uncompressed | crc32 | rows in our data | bytes/row |
|---|---|---|---|---|---|
| `data_Q2_2025/2025-05-11.csv` | 11,733,238 | 124,329,571 | 951681f5 | 316,450 | 392.9 |
| `data_Q2_2025/2025-05-12.csv` | 9,315,405 | **97,052,675** | 0509a721 | 245,997 | 394.5 |
| `data_Q2_2025/2025-05-13.csv` | 11,727,009 | 124,302,283 | a61951a6 | 316,370 | 392.9 |

The 12 May member is 27,276,896 bytes smaller than 11 May. At the bytes-per-row of the neighbouring days that
size predicts about 247,016 rows, within 0.4 per cent of the 245,997 we hold. All three members share the same
`header_signature` (8a517d6f...), so the schema is unchanged.

### 3. No row loss in our processing
2025Q2 cleaning: 28,808,042 input rows, 28,808,042 retained, 0 quarantined (Zahid's preliminary report,
14 Nov 2026, from `Evidence/cleaning_result.json` and the revalidation run-002 report, verdict
`passed_with_warnings`, no structural errors). The cleaned daily counts (316,450 / 245,997 / 316,370) equal the
snapshot counts exactly, and all three days sit in the same snapshot parquet part file, so no file was dropped
during integration. Phase 5 features reported 0 missing and 0 different rows against the Phase 3 test sample in
all six quarters.

### 4. Our pipeline cannot produce this shape
No cleaning, integration or Phase 5 step partitions work by data center, vault or cluster. A loss in our
processing would follow file or row boundaries, not Backblaze's physical site layout.

## Effect on the results (bound, computed 14 Nov)
Inspections due on 2025-05-12 that were cancelled (tie seed 0): E1 94, E2 86, E3 154, E4 34, E5 211, E6 72,
E7 169, E8 33. The decisions of that day had 139 fewer slots per cell. Leaving out all 281 eligible events whose
7..30-day capture window touches 12 or 13 May:

| ML minus SMART (points) | all 6,271 events | without those 281 |
|---|---|---|
| next day, fresh (confirmatory) | +14.67 [+13.76,+15.57] | +14.66 [+13.74,+15.58] |
| next day, queue | +11.23 | +11.32 |
| cooldown, fresh | +0.61 [-0.45,+1.63] | +0.78 [-0.27,+1.81] |
| cooldown, queue | -0.26 | -0.03 |

Leaving out every event failing between 13 May and 12 July gives +14.77, +11.56, +1.07, +0.24. No conclusion
changes.

## Decision
Path A of the plan: a source observation gap. The replay is a faithful replay of the observations that exist in
the published data. The test results are accepted as computed and the day is disclosed in the report as a
one-day source outage at one data center, with the bound above. No logged deviation and no corrective replay.

## Still open (confirmation, not a condition)
A direct line count of the three raw CSV members inside
`C:\Users\1093813\Downloads\data_Q2_2025.zip` (Zahid's machine), with the count of lines containing `sac2`.
Expected: about 245,997 rows and near zero sac2 rows on 12 May. This would replace the size-based inference with
a direct count. The archive's publisher checksum was never independently established (noted at Level 2 approval
for 2025Q2); the crc32 values above pin the members we actually read.

## Evidence files
`C:\DP-out\check_gap.log`, `C:\DP-out\check_gap2.log`, `C:\DP-out\Cleaned_source_manifest.json` (extracted from
`2025Q2_Zahid.zip`, package sha256 C518132BB23774AB4434C8359EDB5E6EEE0883DBCC40CC797498AB9C208C6DCF);
Zahid's preliminary investigation report of 14 Nov 2026. Copies in OneDrive
`05_Phase5_RESULTS\04_provenance` and `03_reviews`.
