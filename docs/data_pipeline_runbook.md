# DrivePriority AI Backblaze Ingestion Runbook

## Scope

This reference pipeline converts one existing local Backblaze quarterly ZIP archive into an audited quarterly submission. It covers 2016 Q1 through 2026 Q2 and never downloads source data.

The quarterly output is not the final integrated `canonical_drive_day` dataset. Stable device keys, cross-quarter duplicate reconciliation, chronological drive histories, future-failure labels, feature engineering, model training, and RQ1/RQ2/RQ3 simulations are central downstream responsibilities.

## Existing environment

Run every Python command from the repository root with the existing environment:

```powershell
$env:PYTHONPATH = "src"
.\.venv\Scripts\python.exe --version
```

Do not create a replacement environment or install packages. The pipeline uses the pinned PyArrow, DuckDB, PyYAML, and pytest versions already present.

## Inputs

Each run requires:

- One assigned quarter in `YYYYQn` form.
- One existing local official Backblaze ZIP archive.
- The official archive or dataset URL for provenance. The pipeline records but never opens or downloads this URL.
- The shared `configs/ingestion.yaml` file.
- A final output directory that does not contain unrelated files.

Keep original ZIP files unmodified in approved raw evidence storage. Store ZIPs, Parquet data, generated reports, and models outside GitHub.

The local ZIP is hashed only while its size and modification metadata remain stable. Its byte size and SHA-256 are rechecked after archive inspection, after member processing, and immediately before manifest publication. A replacement or mutation at any of those boundaries leaves staging evidence intact and refuses final publication; the pipeline never associates output with an earlier source hash.

### Archive member selection

Archive member-selection contract `1.1.0` accounts for every non-directory ZIP member as a processed CSV, verified AppleDouble metadata, or an inventoried non-CSV member. AppleDouble exclusion is mandatory and deliberately narrow: the member must be below the top-level `__MACOSX/` directory, its basename must begin `._`, and its binary header must contain AppleDouble magic `00051607` and version `00020000`. The complete RFC 1740 version-2 descriptor table must be available and contain at least one entry. Verification rejects entry ID zero, the prohibited data-fork entry, the unused version-2 entry ID 7, unassigned standard-range IDs, duplicate IDs, payload offsets inside the header or table, out-of-bounds or truncated payload declarations, and overlapping nonempty payload ranges. Application-defined high-bit IDs remain valid. The 16-byte header environment field is retained as evidence rather than treated as a filename substitute or silently discarded.

Configured archive-member size and compression-expansion limits apply to every member before any metadata candidate is opened or decompressed. Verified metadata is then streamed through EOF to verify its ZIP CRC and record its SHA-256, name, archive index, compressed and uncompressed sizes, reason code, header evidence, and parsed descriptors. These members are never presented as source records, but their contents remain unchanged inside the original ZIP.

A path or filename match without the verified binary signature is not excluded. A candidate with recognized version-2 AppleDouble magic but an invalid structure fails inspection and is never silently excluded. Other members remain subject to normal CSV header parsing and fail safely if they are not valid CSV. Non-CSV members are separately inventoried rather than silently conflated with excluded metadata. The category counts must sum exactly to the ZIP file count. Contract `1.1.0` inventories every selected CSV by archive index, name, CRC, compressed and uncompressed size, and header signature in addition to the excluded and non-CSV inventories. This deterministic selection is reused by dry run, processing, checkpoint recovery, manifest construction, and completed-output verification; it is not configurable.

Completed-output reuse, marker-present recovery, and complete-manifest pre-marker recovery freshly inspect the unchanged source ZIP and require exact equality with the manifest's versioned selection evidence. Missing evidence, altered counts, or any changed member identity refuses recovery. Pre-marker recovery does not create a publication marker until this reconciliation and all existing artifact checks have passed.

During real-data acceptance, the official Backblaze page described the 2016 Q1 download as containing 92 files, while the ZIP central directory contained 182 file entries: 91 complete daily CSVs and 91 verified AppleDouble sidecars. No separate 92nd genuine daily CSV was observed. Preserve this as an unresolved external-documentation discrepancy; do not infer a missing day, fabricate a record, or alter the source archive.

### Approved source inventory

`configs/ingestion.yaml` contains one evidence-backed inventory entry for 2016 Q1. The official Backblaze listing and direct archive URL are recorded, together with the locally observed filename, exact byte size, SHA-256, complete ZIP CRC result, archive-member counts, full expected quarter calendar, and an empty approved-gap list. Backblaze does not publish an independent SHA-256 or exact byte count for this historical archive, so the independent expectation fields remain null and this quarter remains `requires_review`. No other quarter is populated or implied.

A verified inventory requires provenance reference, verifier, verification timestamp, and an approved quarter entry containing:

- The canonical source URL and original archive filename.
- The complete expected calendar-date list for the quarter.
- Any explicitly approved missing dates.
- Expected archive byte size when independently available.
- Expected SHA-256 when independently available.

Available evidence is enforced exactly. A URL, filename, independently expected size/checksum, or locally observed size/checksum mismatch stops before staging. Locally observed bytes prevent silent substitution of the reviewed download but do not authenticate what Backblaze originally published. If an independent checksum is unavailable, the manifest records that fact and the run remains `requires_review`; the pipeline never invents a checksum or gap approval. Add another quarter only after recording its official URL, exact local observations, full calendar, evidence provenance, and any explicitly reviewed gaps.

## Dry run

A dry run verifies the local archive hash, ZIP inventory, CSV headers, quarter, configuration, and intended output without creating an output directory:

```powershell
$env:PYTHONPATH = "src"

.\.venv\Scripts\python.exe -m drivepriority.data.ingest `
    --config configs/ingestion.yaml `
    --quarter 2016Q1 `
    --archive "D:\DrivePriority-Acceptance\Raw\data_Q1_2016.zip" `
    --output "D:\BackblazeProcessed\2016Q1" `
    --source-url "https://f001.backblazeb2.com/file/Backblaze-Hard-Drive-Data/data_Q1_2016.zip" `
    --dry-run
```

The archive filename is not used to infer its quarter. The explicit quarter controls the permitted calendar boundaries.

## Normal processing

After the dry run is reviewed, omit `--dry-run`:

```powershell
$env:PYTHONPATH = "src"

.\.venv\Scripts\python.exe -m drivepriority.data.ingest `
    --config configs/ingestion.yaml `
    --quarter 2016Q1 `
    --archive "D:\DrivePriority-Acceptance\Raw\data_Q1_2016.zip" `
    --output "D:\BackblazeProcessed\2016Q1" `
    --source-url "https://f001.backblazeb2.com/file/Backblaze-Hard-Drive-Data/data_Q1_2016.zip"
```

Processing occurs in a sibling directory ending in `.staging`. The final output name appears only after structural validation succeeds and the staging directory is atomically renamed.

The CSV reader processes bounded row batches. It does not materialize a full quarter in Pandas. Canonical buffers flush at either `batch_rows` or the independent estimated `canonical_batch_bytes` ceiling; one exceptionally large source row is preserved intact and can exceed the byte target only for its own batch. DuckDB stores duplicate-tracking state on disk with the configured memory limit. Serial-collision classification uses bounded per-serial first/last identity summaries rather than a same-serial pairwise self-join. Exception evidence has independent `exception_batch_rows` and estimated `exception_batch_bytes` limits, and final exception compaction streams Parquet record batches instead of reading whole part files into memory. A single evidence item larger than the byte target is written intact in its own batch; evidence is never sampled, truncated, or discarded.

Before staging, the pipeline validates batch and Parquet row-group sizes, UTF-8-with-BOM-compatible decoding, the immutable empty-field missing-value contract, the DuckDB memory-limit syntax, Parquet boolean options and codec-specific compression level, ZIP member/count/expansion limits, the staging suffix, mandatory integrity flags, and a free-disk-space estimate. The disk estimate is the greater of the configured fixed minimum and the archive's total uncompressed size multiplied by the configured working-space factor. Unsafe, mistyped, or disabled safeguards stop the run. In particular, literal `"0"` can never be configured as missing.

## Output layout

```text
2016Q1/
├── data/
│   └── 2016Q1/
│       ├── part-m00000-b000000.parquet
│       └── ...
├── source_manifest.json
├── schema_report.json
├── quality_report.json
├── validation_results.json
├── publication_ready.json    # durable completed-publication recovery marker
└── exceptions.parquet        # present only when exceptions exist
```

The non-Hive `data/2016Q1/` directory is the physical quarter partition. Every Parquet file also retains the required `source_quarter` field. This prevents partition readers from inferring a second incompatible copy of that field.

### Source manifest

`source_manifest.json` records the source URL and local archive identity, byte size, SHA-256, approved-inventory assessment and provenance, complete archive-member selection and exclusions, processed member inventory, original headers, CRC results, configuration and hash, the installed Python implementation/version/cache tag and exact DuckDB/PyArrow/PyYAML versions, Git provenance when available, pipeline version, deterministic runtime-source hash, requirements hash, pipeline-scoped dirty state, disk-space preflight, row counts, output checksums, validation status, and processing timestamps. Manifest version `1.6.0` and pipeline version `0.2.0` include the evidence-based source-approval contract, bounded historical header support, exact numeric-token evidence, comparison-only identity normalization, and immutable anomaly/revalidation tooling while preserving archive-member-selection contract `1.1.0`; they do not change the canonical SMART data types. Startup requires the installed core dependency versions to match their exact `requirements.txt` locks. The installed runtime identity is hashed into the recovery identity, so a runtime-version change cannot resume staged work or claim completed-output reuse under the prior environment.

In a Git checkout, `git_commit`, `pipeline_scoped_dirty`, and `git_metadata_available: true` record the scoped repository state. In a code-only ZIP, no commit is invented: the first two fields are null and `git_metadata_available` is false. ZIP users verify the separately published package SHA-256, the release file-hash inventory, `runtime_source_sha256`, `requirements_sha256`, the configuration SHA-256, and the installed-runtime hash. Missing Git metadata never converts an unavailable source identity into a clean checkout.

### Reproducibility scope

Reproducibility method `1.0.0` claims semantic equivalence only when archive, configuration, source URL, assigned quarter, schema, pipeline recovery identity, and dependency environment are appropriately matched. The comparison covers canonical row values and Arrow types, Arrow/Parquet schemas, schema/quality/validation reports, source-member and exception inventories, failure counts, and source-to-output accounting.

Byte-for-byte equality of independently written Parquet files is not claimed. Physical bytes can vary with PyArrow, compression libraries, platform behavior, or writer metadata even when the scientific content is equivalent. Processing timestamps, observed free disk space, absolute local paths, and host metadata are intentionally variable and must be separated from scientific comparisons. Each manifest and validation report records the method version, comparison scope, variable fields, and limitations. Manifest checksums still certify the immutable files from one specific published run; semantic comparison does not replace those integrity checks.

### Schema report

`schema_report.json` records original and normalized headers, per-member schema fingerprints, source CSV token types, canonical Parquet types, SMART availability by hardware model, unexpected SMART-shaped fields, and unsupported non-SMART source field names. Every value from an unsupported field is preserved in provenance-linked exception evidence and forces `requires_review`; ingestion never silently discards or guesses a canonical meaning for it.

Each quarter retains its own observed SMART-column union; ingestion does not impose a fixed global SMART schema. Schema contract `1.0.0` is calculated only after canonical Parquet parts are written and their Arrow schemas are read back. The contract records the complete canonical column inventory, nullable flags, exact Arrow types, decimal precision and scale, recognized numeric SMART fields, unexpected SMART string extensions, Arrow metadata, per-file schema fingerprints, source-column availability, and per-member schema-drift evidence. The canonical fingerprint uses method `sha256_arrow_schema_ipc_v1`, meaning SHA-256 over the serialized Arrow schema actually present in canonical Parquet, including its schema metadata.

The same schema contract and fingerprint appear in `schema_report.json`, `validation_results.json`, and `source_manifest.json`. Canonical Parquet metadata includes both `drivepriority.schema_version` and `drivepriority.schema_contract_version`. Publication fails if data-file schemas disagree, differ from the planned within-quarter union, cannot be read, or carry inconsistent contract metadata. Completed-output reuse recalculates the published schema evidence, while member checkpoint recovery verifies each recorded Parquet schema fingerprint before reuse.

Before publication, validation streams every canonical and exception Parquet record batch so data pages—not only footers—are decoded under bounded batch size. It independently reconstructs retained failure counts from canonical Parquet and quarantined failure counts from single-copy source evidence, then reconciles both totals and daily counts with processing statistics. Malformed-width evidence is identified by its controlled `malformed_column_count` exception code, never by a source-controlled field name. Any readback error or mismatch is structural failure.

### Quality report

`quality_report.json` separates:

- Column absent from a source member.
- Column present with a blank value.
- Column present with an invalid token.
- Genuine numeric zero.
- Canonical null.

It also records expected and observed calendar dates, approved and unexplained gaps, out-of-quarter dates, separated source/retained/quarantined failure counts, separately disclosed safely identifiable and unclassifiable flags from malformed-width rows, duplicate classes, candidate serial collisions across dates, negative SMART aggregates, unexpected-extension source states, issue severities, model-level availability, complete row accounting, and an explicit zero count for fabricated rows.

### Validation and admission outcomes

Validation outcomes are ordered by precedence:

1. `failed` — a structural requirement failed. Examples include incomplete member processing, row-accounting failure, unreadable or incompatible Parquet, row-count mismatch, or zero retained records. The output remains in staging and is not published.
2. `requires_review` — structure is intact, but the archive cannot be admitted centrally without explicit review. Causes include missing source-authenticity evidence, unavailable independent checksum, unexplained calendar gaps, conflicting duplicates, serial collisions, or error-severity exception evidence.
3. `passed_with_warnings` — structure and admission checks pass, but warning evidence or documented approved gaps remain for review.
4. `passed` — all structural, authenticity, coverage, and quality gates pass without findings.

Only `passed` and `passed_with_warnings` are marked eligible for central integration. `requires_review` remains an immutable evidence output but is not an admission decision. The quarterly pipeline never resolves conflicting identities or fabricates missing dates.

### Exception evidence

`exceptions.parquet` retains the source member, record and line location, raw identifier/date evidence, original header, exact unsafe token, exception code, severity, disposition, record SHA-256, first-seen location, and first/last observation dates for collision evidence. Schema version `1.1.0` adds `row_evidence_id`: all field exceptions from one source row carry the same stable identifier, while the complete `raw_record_json` is stored once on the first linked exception rather than repeated for every invalid SMART field. The identifier incorporates the quarter, source member, record/line location, and raw-record SHA-256, so identical records at different source locations remain separately traceable.

## Exact SMART representation

All recognized `smart_*_raw` and `smart_*_normalized` fields use nullable Parquet `decimal128(38,0)`.

The exactly representable range is:

```text
minimum = -(10^38 - 1)
maximum = +(10^38 - 1)
```

Base-10 integer tokens within this range are preserved exactly, including suspicious negative values. Every negative SMART value remains signed `decimal128(38,0)` and also produces field-level warning evidence; counts are reported by quarter, source member, hardware model, and SMART field. Negative, zero, blank, absent, and invalid states remain distinct. Blank is null and is never converted to zero. Fractional values, scientific notation, nonnumeric tokens, and integers outside the documented range produce a null canonical value plus complete exception evidence. No floating-point intermediate is used.

Normalized and raw SMART identifiers remain manufacturer-specific. The ingestion stage does not interpret SMART 188 or any other attribute as a universal physical measurement.

Unexpected `smart_*` extensions that do not match the approved numeric naming contract remain nullable strings. Their original non-missing tokens are preserved, including literal `"0"`. The quality report separately records absent-column, blank, literal-zero, other-nonempty, and canonical-null states by quarter, member, model, and field. These extensions are not treated as validated numeric measurements without a separately approved explicit contract.

## Failure flags and row accounting

The only valid canonical failure values are source literals `0` and `1`. They are preserved without relabeling. A row with an invalid or missing failure flag is kept in quarantine evidence and excluded from typed drive-day Parquet because it cannot satisfy the canonical contract.

Failure accounting is reported separately for all valid well-formed source flags, flags retained in canonical Parquet, and valid flags belonging to ordinary quarantined rows. Each category includes daily `0`/`1` totals and source-to-output reconciliation. A readable positional flag in a malformed-width record is disclosed in a separate evidence category; it is never promoted into valid-source, quarantined-drive-day, or retained-drive-day counts, and an unreadable position is explicitly counted as unclassifiable. Ingestion preserves every retained failure-day record and does not select a first failure event.

Every parsed source row has exactly one disjoint disposition:

```text
input rows = typed output rows + quarantined rows
```

Recoverable field exceptions, exact duplicates, and candidate identity collisions remain in typed output and are counted separately. Failure-day and later source observations are not removed during ingestion.

Every CSV member must contain `date`, `serial_number`, `model`, `capacity_bytes`, and `failure` before row processing begins. Empty archives, empty CSV members, missing required headers, and runs producing zero retained canonical records are structural failures.

### Line terminators and the missing-value contract

CSV members may use LF, CRLF, or CR-only record terminators. Archive inspection reads only the first CSV record's bytes (bounded to 1 MiB) and parses it with the same newline-preserving `csv` semantics used during member processing, so inspection and processing always agree on the header. Pipeline version `0.1.5` fixes a defect in earlier versions where header inspection used binary `readline()`, which recognizes only LF: a CR-only member, such as `2018-02-25.csv` in the 2018 Q1 archive, was read whole as a single line and failed with `new-line character seen in unquoted field`.

The missing-value contract is applied exactly: only an empty field is source-missing. A whitespace-only field is a nonempty source token. For SMART and capacity fields it becomes a canonical null with `invalid_smart_value` or `invalid_capacity_bytes` evidence that retains the exact token; a whitespace-only failure flag is quarantined as `invalid_failure_flag`; text fields and unexpected SMART extensions keep the token unchanged. Earlier versions silently reported whitespace-only fields as blank.

A corrupt compressed member stream is reported as a pipeline integrity error (exit code 2) before staging publication, never as an unhandled traceback. A dry run decompresses only each member's header region, so it does not certify full member integrity; the full run checks every member's CRC before processing.

## Duplicate and identity audit

Within an assigned quarter, the pipeline reports:

- Exact repeated source records.
- Same-date/raw-serial records with different measurements.
- Same-date/raw-serial records with conflicting model, manufacturer, or capacity evidence.
- The same original serial appearing on different dates, members, or processing batches with different model, supplied manufacturer, or capacity signatures.

The disk-backed audit retains the first source location plus first and last observation dates for collision evidence. Blank and absent manufacturer values are both treated as unavailable identity evidence, while supplied manufacturer values remain part of the signature. Original serial strings are preserved exactly. All records remain present. The pipeline does not normalize or merge serials, declare that a raw serial is a globally stable device key, or choose a winning conflicting row.

The project lead performs stable identity mapping, deterministic conflict resolution, cross-quarter duplicate detection, boundary continuity checks, and drive-history integration centrally after all quarterly manifests and partitions have been verified.

The later central integration stage must union verified quarterly columns by field name, insert correctly typed nulls where a field was historically absent, and reject incompatible Arrow types unless an explicit reviewed conversion is approved. It must preserve original source values, source-member provenance, quarterly contract fingerprints, and exception evidence. Quarterly outputs marked `requires_review` must be rejected from automatic admission or explicitly approved with documented evidence. Cross-quarter duplicate auditing, stable drive-identity resolution, and chronological history integration remain central responsibilities; none is performed by this quarterly pipeline.

## Interrupted runs

If processing stops, the final output directory remains absent and the matching `.staging` directory retains transactional member checkpoints.

A CSV parsing or Unicode decoding failure atomically writes `processing_failure.json` in staging and marks `run_state.json` as failed. Report version `1.1.0` records the exact attempted logical record number separately from the last successfully decoded record, labels available physical line/byte positions as approximate, and includes archive SHA-256, member and processing sequence, sanitized exception type and description, completed-member state, and recovery requirement. Failed parser staging cannot be resumed or published; inspect the report and use an explicit `--restart`. Restart removes only the verified matching staging directory and does not touch sibling or completed outputs.

Resume only with the same archive SHA-256, source URL, quarter, schema version, configuration hash, Git state when available (or the same explicit unavailable state in a ZIP), pipeline version, deterministic runtime-source hash, requirements hash, installed Python and core dependency identity, and pipeline-scoped dirty state:

```powershell
.\.venv\Scripts\python.exe -m drivepriority.data.ingest `
    --config configs/ingestion.yaml `
    --quarter 2016Q1 `
    --archive "D:\DrivePriority-Acceptance\Raw\data_Q1_2016.zip" `
    --output "D:\BackblazeProcessed\2016Q1" `
    --source-url "https://f001.backblazeb2.com/file/Backblaze-Hard-Drive-Data/data_Q1_2016.zip" `
    --resume
```

Completed member transactions are reused only after every recorded Parquet artifact path, byte size, SHA-256, row count, and schema fingerprint is verified. This includes exception parts using the linked row-evidence schema. Checkpoint verification also independently reads the listed parts and reconciles canonical rows, exception-code and severity counts, quarantined source-record evidence, field issues, stable evidence links, and exactly one complete raw-row copy per evidence identifier against the member statistics. High-cardinality evidence identifiers are aggregated through disk-backed DuckDB temporary state under the configured memory limit rather than retained in process-wide Python sets. A missing, omitted, modified, or inconsistent checkpoint stops recovery before incomplete-file cleanup and requires an explicit `--restart`; checkpoint metadata is never recalculated to certify damaged evidence. These local cross-checks detect accidental damage and inconsistent metadata, but do not claim cryptographic authenticity against an actor able to rewrite every local checkpoint record.

After the complete manifest is written, `publication_ready.json` binds its SHA-256 and recovery identity. This marker remains valid through runtime cleanup and the atomic staging rename, so an interruption after `run_state.json` deletion or a rename failure can be safely resumed without reprocessing or touching unrelated files.

If interruption occurs in the narrower boundary after the complete manifest is durable but before `publication_ready.json` exists, `--resume` does not trust the manifest alone. It requires the original matching `run_state.json`, re-verifies every checkpoint artifact and evidence statistic, reconciles aggregate processing counts and quality/validation reports, verifies the marker-free manifest inventory and actual schema contract, and only then recreates publication readiness. Any inconsistency requires explicit `--restart`.

Use `--restart` only when intentionally discarding a verified matching staging run. Restart refuses staging state belonging to any other archive, URL, quarter, or configuration. It never deletes an existing completed output.

## Safe reruns

If the final output already exists, the pipeline verifies its identity, manifest version, publication marker, required artifact set, unique safe paths, exact output membership, and every recorded file size and SHA-256. It also reconciles the schema contract across Parquet, schema report, validation report, and manifest. A matching completed submission returns `verified_existing` without rewriting it. Missing, unlisted, duplicate, unexpected, or inconsistent artifacts stop verification.

Publish a new immutable output directory for a changed archive, configuration, or pipeline revision. Do not overwrite a teammate's submission.

## Team submission procedure

For every assigned quarter:

1. Confirm the assignment with the project lead.
2. Use the same approved Git commit or tagged pipeline release.
3. Run the focused automated tests.
4. Perform and review a dry run.
5. Process the local archive.
6. Require `passed` or `passed_with_warnings` and `central_integration_admission.eligible: true` in `validation_results.json`.
7. Review every warning and exception; do not silently correct or filter it.
8. Verify archive and output checksums from `source_manifest.json`.
9. Upload large artifacts to approved shared artifact storage, not GitHub.
10. Submit the manifest, quarter ID, pipeline revision, validation result, and artifact location to the project lead.

The project lead builds the 42-archive inventory and independently reconciles expected quarters, calendar coverage, row totals, raw serial/model/capacity evidence, and failure counts before central history integration.

## Automated tests

Run only the ingestion tests for this stage:

```powershell
$env:PYTHONPATH = "src"
.\.venv\Scripts\python.exe -B -m pytest `
    tests/test_schema_validation.py `
    tests/test_ingestion.py `
    -q
```

Fixtures and outputs are created only in pytest temporary directories. The tests cover schema drift, required headers, empty and zero-retained archives, exact SMART precision, blank/zero/negative/invalid/absent states, unexpected SMART extension states, separated failure-flag accounting, quarantines, malformed records, wide invalid rows with linked single-copy raw evidence, independent row/byte exception batching, streaming exception compaction, parser and decoding failure reports, same-date and cross-date collision classes across members and batches, original serial preservation, approved and unexplained calendar gaps, source-inventory provenance and mismatch handling, ordered validation outcomes, central admission, configuration safeguards, ZIP and disk limits, row accounting, bounded batches, deterministic validation, corrupt archives, immutable reruns, pipeline identity changes, checkpoint integrity, interruption, resume, and scoped restart.

### Historical compatibility evidence

The historical-layout suite uses explicitly synthetic fixtures representing contract variations relevant to 2016–2017, the 2018 transition period, a middle period, 2025, and January–June 2026. It exercises optional manufacturer data, changing SMART-column presence across members, case/spacing/underscore/BOM header forms, blank/signed/unusual/invalid tokens, nested and date-oriented member names, and quarter-boundary dates including 2026-06-30. These fixtures are invented test inputs, not transcriptions of authenticated Backblaze records, and they do not prove real historical compatibility.

Release acceptance still requires independently verified real-archive tests across the approved January 2016–June 2026 inventory, with observed headers and tokens reconciled against authentic source evidence. It also requires measured peak-RAM benchmarks on representative real quarterly archives. Neither evidence is fabricated or inferred from the synthetic suite.

The existing H2O smoke test is separate. It verifies software startup with synthetic model data and is not evidence that the ingestion or research methodology is correct.

## Common errors

- **Header normalization collision:** two source headers would map to one canonical name. Stop and escalate; never choose one silently.
- **CRC failure or malformed ZIP:** replace the local copy only after verifying the official source and expected SHA-256.
- **Date outside assigned quarter:** review the member and assignment. The row remains in quarantine evidence.
- **Missing source dates:** confirm the official archive inventory and disclose the gap; never fabricate rows.
- **Independent source checksum unavailable:** enforce the recorded local size/SHA evidence, retain `requires_review`, and obtain independent publisher or research-lead authenticity evidence before admission. A local hash alone is not publisher authentication.
- **Trusted source mismatch:** stop and escalate. Do not edit the inventory or archive merely to make the values agree.
- **Insufficient disk space:** use an approved output volume with enough free space or revise safeguards through reviewed configuration; never disable the check silently.
- **Invalid SMART token:** inspect the exact raw token in `exceptions.parquet`; do not round, clip, or substitute zero.
- **CSV parser or decoding failure:** inspect the atomic staging `processing_failure.json`; correct or replace the verified source input, then use `--restart`. Do not use `--resume` or manually publish failed staging.
- **Candidate serial collision:** preserve all records and defer identity resolution to central integration.
- **Staging identity mismatch:** use the correct output location or matching archive/configuration; never delete unrelated staging data.
- **Completed output mismatch:** treat it as an integrity failure and publish a separately named corrected version after review.

## Prohibited work in this stage

Do not create future-failure labels, infer removals from disappearance, construct rolling features, sample by outcome, perform imputation, train models, select RQ scores, calculate service capacity, or run inspection/watchlist/queue simulations. Those operations require the centrally integrated chronology and their separately approved methodology stages.
