# DrivePriority AI central integration 1.1.5

Version 1.1.5 makes the exceptions.parquet rule match the cleaning pipeline's own rule. The file is required
and must be listed in the manifest only when `processing.exception_rows` is greater than 0. With 0 exception
rows the file must be absent and unlisted, and a package that lists it anyway is rejected. A missing or
non-integer `exception_rows` keeps the old strict behaviour (the file must be listed). All other checks are
unchanged. The coordinator approved this rule change (Option A).

Version 1.1.4 adds a central research-use approval list (`central_research_use_approvals` in
`configs/central_policy.json`). An entry names one quarter, the exact source-archive SHA-256, the evidence level
(`level2_official_download_supported` or `level3_publisher_bytes_verified`), who approved it and when. When a
package has no approval record inside its revalidation report, an entry that matches its quarter and archive hash
is accepted instead, and the admission records `central_ledger_approval_<level>` as the authenticity basis. It
replaces only the missing in-package approval record. It does not relax any other check: the five evidence
files, the exceptions.parquet manifest entry, the revalidation status, the pinned pipeline release and the
one-manifest rule all still apply. This is documented manual approval of the official download, not
publisher-checksum verification. Remove an entry to restore strict behaviour for that quarter. Reports and the
manuscript must disclose these quarters.

Version 1.1.3 fixes incremental rebuilds: a quarter that was admitted through a bound revalidation now keeps
its revalidation evidence when later quarters are added. Before, the rebuild re-checked it without that
evidence and blocked it. If an earlier run left an `.Snapshot-NNNN.incremental-checkpoint` folder behind,
delete that folder (it only holds a planning state file) before running again.

Version 1.1.2 adds a recorded coordinator policy (`configs/central_policy.json`). When it is enabled, a
passed or passed_with_warnings revalidation whose research-use approval is bound to the exact archive SHA-256
at evidence level `level2_official_download_supported` or `level3_publisher_bytes_verified` satisfies the
source-authenticity requirement even if the cleaning manifest was written with authenticity unverified. The
basis is stored per quarter as `authenticity_basis`. This is documented manual-review approval, not publisher
checksum verification, and reports must say so. Set the policy flag to false to restore the strict rule.
Structural checks (evidence file counts, `exceptions.parquet` in the manifest, hashes, schema) are unchanged.

Version 1.1.1 shortens hash-addressed extraction and staging paths for Windows
systems where `LongPathsEnabled=0`. The complete package SHA-256 remains in the
cache marker and prefix collisions fail closed. Version 1.1.0 added the automatic
incremental ZIP operator and Windows launcher. Admission and integration contracts
remain unchanged from 1.0.1 and accept DrivePriority cleaning revalidation 1.1.0.

Use only immutable quarter outputs that pass the admission gate. Git checkouts may be pinned by commit; Git-free cleaning releases must be pinned by both the published release ZIP SHA-256 and the runtime-source SHA-256 recorded in the source manifest. A bound revalidation report may resolve quality-review findings, but it cannot override source-authenticity, structural-integrity, or authorization failures.

## Commands

```text
RUN_CENTRAL.cmd admit --output-root "D:\Quarter" --quarter 2016Q1 --trusted-archive-sha256 HASH --trusted-runtime-source-sha256 HASH --trusted-release-sha256 HASH --revalidation-root "D:\Quarter-Revalidation"
RUN_CENTRAL.cmd integrate --sources-json "D:\Control\sources.json" --output "D:\Central\Snapshot-01"
RUN_CENTRAL.cmd incremental --existing "D:\Central\Snapshot-01" --new-sources-json "D:\Control\new-quarter.json" --output "D:\Central\Snapshot-02"
RUN_CENTRAL.cmd validate --root "D:\Central\Snapshot-02" --report "D:\Central\Snapshot-02-validation.json"
```

Every snapshot is immutable. Incremental addition writes a new snapshot, rejects duplicate quarters, validates the existing snapshot first, stores a durable identity-bound checkpoint, and independently validates the new snapshot before removing that checkpoint. Use `--resume` after an interruption or `--restart` to discard only the matching incomplete incremental checkpoint.

Different quarterly SMART schemas are unioned by field name. Historically absent fields receive typed nulls; decimal128(38,0) values, including negatives, remain exact. Cross-quarter drive identity is not inferred or merged by this component.

## Simple incremental Windows workflow

For normal operation, put completed quarter ZIP packages in `Input_Quarters` and
double-click `RUN_INTEGRATION.cmd`. Do not extract them. The launcher creates
`Integrated_Data` and `Reports`, verifies every ZIP and its CRC, safely extracts a
hash-addressed working copy, locates the cleaning, anomaly-analysis, and final
revalidation evidence regardless of their containing folders, and calls the same
approved admission and integration functions described above.

Each package must contain exactly one `source_manifest.json`,
`analysis_manifest.json`, `anomaly_report.json`, `revalidation_manifest.json`, and
`revalidation_report.json`. The final revalidation must bind the cleaned and analysis
manifests, include exact-hash Level 2 or Level 3 research-use source approval, and end
in an outcome the existing admission gate accepts. `requires_review`, `failed`,
missing evidence, corrupted evidence, and unknown Git-free cleaning releases are
blocked. Git-free v0.3.0 is pinned to the separately verified master-release hash in
`configs/central_release_trust.json`; the program never invents a release identity.

Every successful change creates a new immutable `Snapshot-NNNN`. Re-running with
unchanged ZIPs validates and skips them. A different package hash for an already
integrated quarter is reported as a conflict and is never substituted. The durable
inventory is `Reports/integration_inventory.json`; beginner-readable and JSON run
reports are written beside it. Short-directory, full-hash-verified evidence under
`.Integration_Work` must be retained because immutable central manifests preserve
references to the admitted source evidence.

The workflow uses bounded Parquet batches and never loads all quarters into Pandas.
It performs a free-disk preflight, but synthetic tests do not prove full-scale memory
or runtime suitability. Run the first controlled two-quarter trial on the 32 GB
university PC and retain the original ZIPs, reports, working evidence, and snapshots.
