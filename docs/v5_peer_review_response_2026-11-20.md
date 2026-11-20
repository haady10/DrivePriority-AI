# Response to the pre-submission review of manuscript V5 (20 Nov 2026, evening)

Review: `DrivePriority_AI_V5_Peer_Review.md`, recommendation **minor revision**. It reviewed the V5 PDF we sent
(SHA-256 `1eb07c0c…945d`, checked: same file) and commit 27e7e9d. Every point was checked against the result
archives and the code before anything was changed. The result is manuscript V6 (7 pages including references).

| # | Review point | Verdict | Evidence (checked here) | Action in V6 |
|---|---|---|---|---|
| 1 | Contribution 2 and the discussion overstate which baselines reach which manufacturers; the largest ML lead is not the S1 contrast | **REQUIRED** | Largest of the 40 test M1 − S*k* at B = 20: **+17.3 [16.3, 18.3] vs S2** (frozen ties, next day, fresh), drive-model interval **6.8 to 29.7**; S2 inspects all four makes there (Seagate 47.7%, HGST 46.0%, Toshiba 4.5%, WDC 1.8%). S3 gives **HGST 0%** under next-day re-inspection with fresh *and* queue scheduling, both tie orders. All-Seagate holds only for S1 with fresh next-day selection (queue: 71.4%) | Contribution 2, abstract, V-C, VI and VII now name the score and scheduling rule; "reach every manufacturer" is used only for the quota counts; S2/S3 reach given with shares; the +17.3 lead and its positive model interval are reported; the conclusion names the quota-count reversal as the result whose interval crosses zero; the rule is stated as covering S1 to S5 |
| 2 | Quota claims lose budget and period qualifiers; "every observed setting" is false; "54.3% in any run" is false | **REQUIRED** | M2 − M1, test, B = 50, cooldown + queue: **−0.5 / −0.7**; S4 − S1, test, B = 10, cooldown + fresh, rotating: **+5.5**; S4 − S1, 2024, B = 20, next day + queue, frozen: **−1.7**. Highest B = 20 capture: test 54.3% (A1 M2) above every frozen test cell (max 52.8%); 2024: the 7-day forest 54.8% | Allocation paragraph and Discussion now say "at budget 20 in the test period" and give the exceptions; 54.3% is "the highest test-period capture observed at B = 20", with the 2024 54.8% |
| 3 | Abstract: name comparator, tie order and the direction of the difference | **REQUIRED** | Rotating ties, cooldown, fresh: M1 − S1 −6.3 [−7.4, −5.2], model −14.8 to 3.4 | Abstract names frozen ties for the cooldown result, gives the quota-count result as "forest minus count −7.6 points (95% drive-model interval −22.3 to 8.7)", and states the 11.8% / 34.1% comparator range; the model interval for the rotating cooldown contrast is added in V-C |
| 4 | Label definition lost the split-boundary precedence | **REQUIRED** | `build_dataset.py` (label SQL): `CASE WHEN date + h > split_end THEN NULL` comes before the positive rule | III-B states that a label is unknown whenever the window crosses the end of its split, even with a failure inside the partial window |
| 5 | One sentence mixed two different sensitivity checks | **REQUIRED** | Claims map A31 (281-event exclusion: point estimates ≤ 0.23) and A45 (calendar-week bootstrap: interval ends ≤ 0.4) | V-D separates them |
| 6 | IEEE AI-use disclosure should name the system, sections and extent | **REQUIRED by the IEEE policy** (system, sections and level must be identified; editing-only use is exempt) | IEEE conference submission policies page | **Not changed at the authors' decision** (20 Nov): the V5 sentence is kept. Before submission to an IEEE venue the authors must add the system name, the sections and the level of use |
| 7 | Keep Fig. 2 and the equal-allocation comparison | **UNNECESSARY** (already) | — | Kept |
| 8 | Table VIII legibility; move the 2024 grid if space is needed | **RECOMMENDED** | 6-point type in V5 Table VIII | Differences table rebuilt with group rows and 7.5/7-point type; the 2024 A1 grid moved to the repository (2024 values stay in the text) to keep one-third of a page in hand under the 8-page limit |
| 9 | Give the prevalence of the actionable target | **RECOMMENDED** | 143,875 / 167,181,101 = 0.0861% (test); 0.1143% (2024) | Added to the horizons table note |
| 10 | Keep the scope a single-fleet allocation study | **UNNECESSARY** | — | No change |

New qualitative claims in V6 are checked at build time (`must()` guards: S3 gives HGST no next-day inspection;
S2 reaches all four makes; the largest frozen next-day lead is against S2 with a positive model interval; the
three quota exceptions; the 2024 7-day forest above the A1 2024 maximum; 2024 frozen next-day lead over all five).
Independent check of the built V6 against the raw archives: 264 table values, 0 mismatches.

Repository change: `tools/a1_tables.py` also exports every make's share of completed inspections (used for the
S2/S3 reach statements).
