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

## Dry run

A dry run verifies the local archive hash, ZIP inventory, CSV headers, quarter, configuration, and intended output without creating an output directory:

```powershell
$env:PYTHONPATH = "src"

.\.venv\Scripts\python.exe -m drivepriority.data.ingest `
    --config configs/ingestion.yaml `
    --quarter 2016Q1 `
    --archive "D:\Backblaze\2016Q1.zip" `
    --output "D:\BackblazeProcessed\2016Q1" `
    --source-url "https://www.backblaze.com/cloud-storage/resources/hard-drive-test-data" `
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
    --archive "D:\Backblaze\2016Q1.zip" `
    --output "D:\BackblazeProcessed\2016Q1" `
    --source-url "https://www.backblaze.com/cloud-storage/resources/hard-drive-test-data"
```

Processing occurs in a sibling directory ending in `.staging`. The final output name appears only after structural validation succeeds and the staging directory is atomically renamed.

The CSV reader processes bounded row batches. It does not materialize a full quarter in Pandas. DuckDB stores duplicate-tracking state on disk with the configured memory limit.

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
└── exceptions.parquet        # present only when exceptions exist
```

The non-Hive `data/2016Q1/` directory is the physical quarter partition. Every Parquet file also retains the required `source_quarter` field. This prevents partition readers from inferring a second incompatible copy of that field.

### Source manifest

`source_manifest.json` records the source URL and local archive identity, byte size, SHA-256, member inventory, original headers, CRC results, configuration and hash, Python/dependency versions, exact Git commit, pipeline version, deterministic runtime-source hash, requirements hash, pipeline-scoped dirty state, row counts, output checksums, validation status, and processing timestamps. If Git metadata is unavailable, the commit and scoped dirty state are recorded as unknown rather than being reported as clean; the exact source and requirements hashes remain mandatory.

### Schema report

`schema_report.json` records original and normalized headers, per-member schema fingerprints, source CSV token types, canonical Parquet types, SMART availability by hardware model, and unexpected SMART-shaped fields.

### Quality report

`quality_report.json` separates:

- Column absent from a source member.
- Column present with a blank value.
- Column present with an invalid token.
- Genuine numeric zero.
- Canonical null.

It also records daily coverage, missing source dates, out-of-quarter dates, original 0/1 failure counts, malformed rows, duplicate classes, candidate serial collisions, model-level availability, and complete row accounting.

### Exception evidence

`exceptions.parquet` retains the source member, record and line location, raw identifier/date evidence, original header, exact unsafe token, exception code, severity, disposition, complete raw record JSON, record SHA-256, and first-seen location for duplicates.

## Exact SMART representation

All recognized `smart_*_raw` and `smart_*_normalized` fields use nullable Parquet `decimal128(38,0)`.

The exactly representable range is:

```text
minimum = -(10^38 - 1)
maximum = +(10^38 - 1)
```

Base-10 integer tokens within this range are preserved exactly, including suspicious negative values. Blank is null and is never converted to zero. Fractional values, scientific notation, nonnumeric tokens, and integers outside the documented range produce a null canonical value plus complete exception evidence. No floating-point intermediate is used.

Normalized and raw SMART identifiers remain manufacturer-specific. The ingestion stage does not interpret SMART 188 or any other attribute as a universal physical measurement.

## Failure flags and row accounting

The only valid canonical failure values are source literals `0` and `1`. They are preserved without relabeling. A row with an invalid or missing failure flag is kept in quarantine evidence and excluded from typed drive-day Parquet because it cannot satisfy the canonical contract.

Every parsed source row has exactly one disjoint disposition:

```text
input rows = typed output rows + quarantined rows
```

Recoverable field exceptions, exact duplicates, and candidate identity collisions remain in typed output and are counted separately. Failure-day and later source observations are not removed during ingestion.

## Duplicate and identity audit

Within an assigned quarter, the pipeline reports:

- Exact repeated source records.
- Same-date/raw-serial records with different measurements.
- Same-date/raw-serial records with conflicting model, manufacturer, or capacity evidence.

All records remain present. The pipeline does not declare that a raw serial is a globally stable device key and does not choose a winning conflicting row.

The project lead performs stable identity mapping, deterministic conflict resolution, cross-quarter duplicate detection, boundary continuity checks, and drive-history integration centrally after all quarterly manifests and partitions have been verified.

## Interrupted runs

If processing stops, the final output directory remains absent and the matching `.staging` directory retains transactional member checkpoints.

Resume only with the same archive SHA-256, source URL, quarter, schema version, configuration hash, exact Git commit, pipeline version, deterministic runtime-source hash, requirements hash, and pipeline-scoped dirty state:

```powershell
.\.venv\Scripts\python.exe -m drivepriority.data.ingest `
    --config configs/ingestion.yaml `
    --quarter 2016Q1 `
    --archive "D:\Backblaze\2016Q1.zip" `
    --output "D:\BackblazeProcessed\2016Q1" `
    --source-url "https://www.backblaze.com/cloud-storage/resources/hard-drive-test-data" `
    --resume
```

Completed member transactions are reused only after every recorded Parquet artifact path, byte size, SHA-256, row count, and schema fingerprint is verified. A missing, modified, or inconsistent checkpoint stops recovery and requires an explicit `--restart`; checkpoint metadata is never recalculated to certify a damaged file. Incomplete member files are removed only from the verified matching staging boundary and that member is replayed from its source stream.

Use `--restart` only when intentionally discarding a verified matching staging run. Restart refuses staging state belonging to any other archive, URL, quarter, or configuration. It never deletes an existing completed output.

## Safe reruns

If the final output already exists, the pipeline verifies its identity and every manifest-recorded file size and SHA-256. A matching completed submission returns `verified_existing` without rewriting it. Any mismatch stops with an error.

Publish a new immutable output directory for a changed archive, configuration, or pipeline revision. Do not overwrite a teammate's submission.

## Team submission procedure

For every assigned quarter:

1. Confirm the assignment with the project lead.
2. Use the same approved Git commit or tagged pipeline release.
3. Run the focused automated tests.
4. Perform and review a dry run.
5. Process the local archive.
6. Require `passed` or `passed_with_warnings` in `validation_results.json`.
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

Fixtures and outputs are created only in pytest temporary directories. The tests cover schema drift, exact SMART precision, blank/zero/invalid/absent states, failure flags, quarantines, malformed records, duplicate classes, source collisions, calendar coverage, row accounting, bounded batches, deterministic validation, corrupt archives, immutable reruns, pipeline identity changes, checkpoint integrity, interruption, resume, and scoped restart.

The existing H2O smoke test is separate. It verifies software startup with synthetic model data and is not evidence that the ingestion or research methodology is correct.

## Common errors

- **Header normalization collision:** two source headers would map to one canonical name. Stop and escalate; never choose one silently.
- **CRC failure or malformed ZIP:** replace the local copy only after verifying the official source and expected SHA-256.
- **Date outside assigned quarter:** review the member and assignment. The row remains in quarantine evidence.
- **Missing source dates:** confirm the official archive inventory and disclose the gap; never fabricate rows.
- **Invalid SMART token:** inspect the exact raw token in `exceptions.parquet`; do not round, clip, or substitute zero.
- **Candidate serial collision:** preserve all records and defer identity resolution to central integration.
- **Staging identity mismatch:** use the correct output location or matching archive/configuration; never delete unrelated staging data.
- **Completed output mismatch:** treat it as an integrity failure and publish a separately named corrected version after review.

## Prohibited work in this stage

Do not create future-failure labels, infer removals from disappearance, construct rolling features, sample by outcome, perform imputation, train models, select RQ scores, calculate service capacity, or run inspection/watchlist/queue simulations. Those operations require the centrally integrated chronology and their separately approved methodology stages.
