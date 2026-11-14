# Literature check against the ten supplied papers, and what DrivePriority AI can claim as new (14 Nov 2026)

Method: the ten PDFs were converted to text and read for (a) what resource or action each one models, (b) how
it evaluates, (c) anything that overlaps our Phase 5 claims. LiteDiskGuard, Li et al. (2018), the ATC 2018 paper
and PawsPro were read closely; the other six were read for abstract, contributions and the relevant sections.
Three short web searches were added. This is NOT a systematic literature search: no "first" claim is safe on
this basis. Written by the assistant that worked on Phase 5; an independent reader should repeat the check.

## 1. Corrections to the earlier ten-paper comparison report

1. **Citation of paper 6.** The published paper is Y. Xu et al., "Improving **Service** Availability of Cloud
   Systems by Predicting Disk Error", USENIX ATC 2018, pp. 481-494 (first author Yong Xu; M. Chintalapati and
   D. Zhang are the last authors). The supplied PDF is a re-typeset copy whose title drops "Service" and whose
   author line is cut off, which is where "Chintalapati et al." came from. Cite the published version.
2. **LiteDiskGuard overlaps more than the report says.** See section 2. It is also a non-peer-reviewed SSRN
   preprint (2026) and must be cited as such.
3. **"SMART attributes differ by manufacturer" is known.** HDFPM states it in its abstract and analyses shared
   attribute ratios; HDDse and LiteDiskGuard both discuss manufacturer/model heterogeneity. Our finding of
   14 Nov is a specific consequence, not the general fact (section 3, claim C4).
4. **"Cost-aware" in Ahmed and Green** means class-weighted loss functions (weighted cross-entropy, focal
   loss), not operational or maintenance cost. It is a 1D-CNN paper with a Cox survival analysis.
5. **DFPoLD's missing data** is random deletion of 10 to 99 per cent of records, not a site outage. It is only
   loosely related to our 2025-05-12 gap.

## 2. What each paper does, checked against the PDF

| # | Paper | Resource or action modelled | Evaluation | Overlap with Phase 5 |
|---|---|---|---|---|
| 1 | Jing et al., HDFPM, CMC 2026 (online 2025) | none | F1/TPR/FPR across Seagate models; shared-attribute ratio | States that SMART attributes differ across manufacturers; shared attributes improve transfer |
| 2 | Ahmed and Green, Neural Comput. Appl. 2025 | none (class-weighted loss) | G-mean, FDR/FAR; Cox regression | None beyond SMART prediction |
| 3 | Li et al., IEEE Access 2018 | Data migration with a bandwidth-limited transfer rate set per disk by predicted residual life | Migration rate, mismigration rate, migration time | **Closest conceptual precedent.** Concludes that prediction accuracy "is sometimes misleading": the most accurate model (RNN) is sometimes beaten by simpler trees on the resource-dependent metrics |
| 4 | Xia et al., PawsPro, FGCS 2026 | Per-disk Keep / Replace chosen by a DQN in a simulated degradation environment | Recall, F1, warning window | Links prediction to an action with a consequence, per disk; no shared fleet capacity. Cites Ong et al. for budget-constrained maintenance (not among the ten; to follow up) |
| 5 | Zhang et al., LWCM, JCST 2025 | none | TPR/FPR stability across the lookahead window | Lead-time windows only |
| 6 | Xu et al., USENIX ATC 2018 | Live VM migration; picks the top r ranked disks that minimise a cost of false positives and false negatives | Online (time-ordered) evaluation; argues cross-validation is wrong for this task; deployed in Azure | Risk ranking, top-r selection and time-ordered evaluation are established here |
| 7 | Zhang et al., HDDse, USENIX ATC 2020 | none | TPR/FPR across manufacturers | Heterogeneous disks; notes SMART differs by manufacturer |
| 8 | Wei et al., DFPoLD, Informatics 2025 | none | TPR/FPR under random record deletion | Missing data, different kind |
| 9 | Lu et al., FAST 2020 | none | F-measure, MCC; SMART + performance + location | Notes the FPR/FNR cost trade-off; tree models suffice on SMART alone |
| 10 | Wang et al., LiteDiskGuard, SSRN preprint 2026 | Daily Top-K alert budget (500 / 1,000 / 3,000 records per day); FPR thresholds chosen on validation; a persistence gate requiring consecutive alert days | Backblaze 2024, ten most frequent HDD models; 7/14/30-day labels with a 30-day purge; past-only features; full-fleet scoring; drive recall, alerted drives, drive precision, first-alert lead time, repeated-alert ratio; per-model PR-AUC ranges; paired drive-cluster bootstrap | **Closest evaluation precedent**, see below |

### LiteDiskGuard in detail
Already there: leakage-controlled multi-horizon labels (7/14/30 days), past-only features, failure-day and
post-failure rows removed, full-fleet scoring, daily Top-K alert budgets, drive-level recall and precision,
lead time, repeated-alert ratio, a persistence rule (consecutive alerts), per-model heterogeneity, paired
drive-cluster bootstrap intervals, and the statement that the ranking feeds "a ranked triage queue whose capacity
and persistence policy is set by the storage operator".

Not there (checked by search of the full text):
- no SMART-rule baseline; all comparisons are ML against ML (LR, RF, MLP, XGBoost, GRU, LightGBM);
- an alert is a scored record, not a completed inspection: no service delay, no cancellation when the drive is
  absent, no rule for spacing repeat visits (cooldown), no carry-forward queue with admission and expiry;
- "detected" means at least one alert inside the positive window; there is no 7-to-30-day action window;
- one calendar year (2024) split in time inside the year, ten drive models; no out-of-year test period;
- no pre-registered reading rules and no single-use lock on the test period.

## 3. Claim by claim: what we may say

| Claim | Status | Wording to use |
|---|---|---|
| C1. A stateful replay of inspection service: decision on day t, service on day t+1, cancellation, a re-inspection cooldown, a bounded queue with expiry, capacity tied to fleet size, capture counted only for a completed inspection 7 to 30 days before failure | Not found in the ten papers. LiteDiskGuard stops at the alert list; Li et al. and PawsPro model per-disk actions | "To our knowledge among the works we reviewed" — never "first" |
| C2. The ML advantage over a SMART rule is large only when the same drives are re-inspected daily; with spaced re-inspection it is not robust at the main budget and depends on budget, tie handling and manufacturer | Not found in the ten. Li et al. (2018) reached the same kind of conclusion for migration (accuracy can mislead) and must be credited as the precedent | Present as extending Li et al.'s point from migration bandwidth to inspection scheduling |
| C3. A frozen protocol, reading rules written before the test, a locked single-use out-of-year test period (18 months), hashes of code and input | Stronger than the ten: LiteDiskGuard has leakage control and validation-selected thresholds but no out-of-year test or lock; Xu et al. argue for time-ordered evaluation | Method contribution; state it plainly, without ranking it against others |
| C4. A count of SMART indicators has a ceiling of 5 for Seagate and 3 for other makes because smart_187 and smart_188 are Seagate-only, so a fleet-wide Top-K fills the budget from one manufacturer | The general fact (attributes differ by manufacturer) is known: HDFPM, HDDse. The specific consequence for a count baseline under a shared budget was not found in the ten | "A known heterogeneity has an unreported consequence for count-based baselines under a shared budget" |
| C5. Persistence rules steady the alert list at little cost (RQ2) | **Overlaps LiteDiskGuard**, which has a consecutive-alert persistence gate and reports its recall/workload trade-off | Report as a result, not as a novelty; cite LiteDiskGuard |
| C6. Three warning horizons, and the 7-day model working better for the action (RQ3) | Multi-horizon evaluation overlaps LiteDiskGuard. The observation that the horizon that scores best on its own label is not the best for a 7-to-30-day action window was not found; ours is exploratory (no paired comparison) | Exploratory observation |
| Daily Top-K budgets, drive-level recall, drive-cluster bootstrap, past-only features, purged labels, time-ordered splits, risk ranking, resource-aware metrics | **Not ours.** LiteDiskGuard, Xu et al., Li et al. | Cite; do not claim |

## 4. Verdict

Three contributions survive the check (C1, C2, C3) and one more is a solid supporting finding (C4). RQ2 and RQ3
are results, not novelties. The paper is an evaluation paper: "when does a risk ranking help once inspections are
limited, delayed and spaced?" It should position itself directly against LiteDiskGuard (alert lists under a
daily budget) and Li et al. 2018 (accuracy is not enough), and say what the service replay adds: the conclusion
about ML against SMART changes once re-inspection spacing is modelled.

Two things would weaken this and are not excluded: a peer-reviewed version of LiteDiskGuard that adds service
mechanics, and prior work outside these ten (for example the budget-constrained maintenance model of Ong et al.
cited by PawsPro, or operations-research work on inspection scheduling). A proper search is still needed before
submission.

## 5. Related-work sentences that are safe to write

- "Li et al. showed that the most accurate failure predictor is not always the one that protects the most data
  once migration bandwidth is limited [Li 2018]. We ask the corresponding question for inspection capacity."
- "Xu et al. rank disks by error-proneness and act on the top-ranked ones, and argue for time-ordered rather than
  cross-validated evaluation [Xu 2018]."
- "LiteDiskGuard (a preprint) evaluates multi-horizon rankings under fixed daily Top-K alert budgets with
  leakage-controlled labels [Wang 2026]. It treats an alert as the outcome; we additionally replay the service
  that follows the alert, and compare against a SMART-rule baseline on a later, untouched period."
- "PawsPro couples prediction to a per-disk keep-or-replace policy learned by reinforcement learning
  [Xia 2026]; it does not model a capacity shared across the fleet."
- "SMART attributes differ across manufacturers [Jing 2026; Zhang 2020]; we show one consequence for
  count-based baselines under a shared budget."

Sources for the citation check: USENIX presentation page
https://www.usenix.org/conference/atc18/presentation/xu-yong and Microsoft Research
https://www.microsoft.com/en-us/research/publication/improving-service-availability-cloud-systems-predicting-disk-error/
