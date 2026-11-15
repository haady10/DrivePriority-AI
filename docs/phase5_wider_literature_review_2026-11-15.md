# Wider literature and novelty review (AI reviewer "Astra", 15 Nov 2026): outcome and how we use it

Source: `DrivePriority_AI_Wider_Literature_Novelty_Review_2026-11-15.pdf` (OneDrive `05_Phase5_RESULTS\03_reviews`,
saved as `2026-11-15_wider_literature_novelty_review_AI_Astra.pdf`). Targeted cross-field public-web search, 26 new
papers, 19 shortlisted; not a registered systematic review; our numbers were not re-checked by the reviewer.

## Verdict per claim

| Claim | Verdict | Closest prior work | Manuscript wording |
|---|---|---|---|
| C1 stateful inspection replay | Partly covered | Hoffman et al. 2022 (maintenance queue, limited capacity, MCTS); Ong et al. 2022 a/b (resource-aware predictive maintenance, DRL); Omoleye et al. 2019 (DES, finite resources); Lin et al. 2002 (field-service DES) | Describe the HDD replay (day t decision, t+1 service, cancellation, bounded expiring queue, cooldown, 7-30 d timely window); never "new prediction-to-maintenance simulation" |
| C2 ML advantage conditional on re-inspection, budget, ties, make | Partly covered (exact HDD ML-vs-SMART interaction not found) | Frantzen et al. 2022 (prioritisation gain depends on staff count); PCAM (Chen et al. 2022); Li et al. 2018 | Conditional result with estimates and intervals; E1/E2/E5/E6 are the no-cooldown (next-day) comparators and must be named as such |
| C3 frozen protocol, single-use test | Covered (established practice) | Gallitto et al. 2025; Kjell et al. 2026; Kapoor and Narayanan 2023; Cawley and Talbot 2010 | Integrity safeguard, "pre-specified/frozen", not "pre-registered"; cite the hash/authorization record |
| C4 SMART-count bias under a shared budget | Partly covered | Pecht and Elburn 2020 (Backblaze SMART reporting asymmetry); Oakley et al. 2024 (five critical counters); Wei et al. 2025 (SMART-Z); Murray et al. 2005 | Quantified consequence on slot allocation only (A27, A34, A35); not "reporting asymmetry is new" |

## Pre-submission checks raised and our status

1. Test-lock proof: protocol sha256, authorization file, run record (A25, A26). Hash is not a signature; no external registration.
2. Right-censoring: labels purged at split ends; B21.
3. Cooldown versus daily re-inspection: next-day cells are the no-cooldown comparator; report unique drives, repeats (95.5-99.3%) and completed inspections separately.
4. Queue event audit: describe ordering (failure/removal vs inspection at day boundary, cancellation, tie sort, admission, expiry) in methods from the frozen protocol.
5. Missing SMART counted as zero: confirmed in code; new limitation **B23**.
6. Outcome interpretation: timely encounter, not prevention (B1).
7. Statistics: paired drive-cluster bootstrap, tie sensitivity, number of policies fixed before test.
8. Related-work transparency: "broad, cross-domain, non-exhaustive search".

## Cautions before citing
- Not yet DOI-checked by us; check each while writing. Recent or non-refereed: Siemroth and Park (arXiv 2606.29078, preprint), Diallo et al. 2026, Kjell et al. 2026; Darrell Long blog (19 Oct 2026) is background only.
- Do not write "as cited by PawsPro" for Ong et al.; the reviewer could not confirm which Ong paper PawsPro cites.
