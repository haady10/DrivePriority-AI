# Why 36.2 per cent of test failures are never captured (15 Nov 2026)

Post-test, read-only, descriptive. Inputs: the saved test results `event_coverage.parquet` and
`service_log_E1.parquet`..`service_log_E8.parquet` (tie seed 0; OneDrive `05_Phase5_RESULTS/02_test_2025_2026_fullfleet`).
Scripts: `tools/phase5_post_test/never_caught_logs.py`, `tools/phase5_post_test/never_caught_test.py`.
Nothing was rerun and no result was changed. Nothing here is used to choose any setting.
Decision-support simulation on old data; it does not show that any failure was prevented.

## Result

2,270 of 6,271 eligible test events (36.2 per cent) are never timely-captured by any of the eight primary cells
under any tie seed.

Inspection history of these 2,270 drives (completed inspections in the seed-0 logs of all eight cells, before the
failure day):

| History | Events | Share |
|---|---|---|
| A. Never inspected in any policy | 2,003 | 88.2% |
| C. Inspected only 1-6 days before failure (too late) | 186 | 8.2% |
| B. Inspected only more than 30 days before failure (too early) | 64 | 2.8% |
| D. Too early and too late, never 7-30 days before | 17 | 0.7% |

By make (never captured / eligible): Toshiba 1,041 / 1,865 (55.8%), WDC 367 / 755 (48.6%), Seagate 661 / 2,488
(26.6%), HGST 201 / 1,163 (17.3%). Group A: Toshiba 968, Seagate 549, WDC 310, HGST 176.

Models with at least 40 events and the highest never-captured share: TOSHIBA MG08ACA16TEY 310 / 364 (85.2%),
ST24000NM002H 239 / 297 (80.5%), WDC WUH722222ALE6L4 137 / 241 (56.8%), TOSHIBA MG08ACA16TA 297 / 520 (57.1%),
TOSHIBA MG10ACA20TE 84 / 166 (50.6%), WDC WUH721816ALE6L4 142 / 290 (49.0%). The two worst are the two models with
the lowest share of failed drives showing any SMART indicator above zero before failure (0.254 and 0.131,
`docs/phase5_finding_smart_attribute_availability_2026-11-14.md` section 5). Together they are 549 of the 2,270.

The median number of observed days in the capture window is 24 for both captured and never-captured events, so
missing observations do not explain the misses (48 never-captured events have fewer than 10).

## Reading

The misses are mainly a ranking effect, not a timing effect: about nine in ten never-captured drives never ranked
high enough under either the ML score or the SMART count to receive a single inspection at about 626 inspections
per day. About one in eleven was reached only in its last week. The worst models are the ones that show little or
no SMART warning before failure; no ranking built on these SMART inputs can be expected to reach them in advance.

## Limits and open part

- Only tie-seed-0 logs exist; the history columns describe seed 0, the "never captured" definition uses all seeds.
- Make is derived from the model name (12 WUH-prefixed models counted with WDC).
- Open: for group A, the share with no SMART indicator above zero in the 30 days before failure, and their ML rank.
  Needs the test features or snapshot (OneDrive `06_features` / `07_snapshot`).
