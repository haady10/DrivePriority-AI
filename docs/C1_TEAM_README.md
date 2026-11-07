# DrivePriority AI C1.1 — Gitless code-only distribution

This package repairs Gitless test compatibility and adds evidence-backed 2016 Q1 source observations. It does not authenticate the Backblaze ZIP independently and does not authorize full ingestion, central integration, labels, features, or model work.

## Integrity model

- In a Git checkout, the manifest records the commit and pipeline-scoped dirty state.
- In this code-only ZIP, `git_commit` and `pipeline_scoped_dirty` are null and `git_metadata_available` is false.
- Verify the ZIP SHA-256 against the separately supplied `C1_1_release_gate.json`.
- The runtime still verifies code hashes, requirements hash, configuration hash, installed dependency identity, source archive SHA-256, member CRCs, checkpoints, and output checksums.

## Approved 2016 Q1 observation

- Official listing: `https://www.backblaze.com/cloud-storage/resources/hard-drive-test-data`
- Direct source URL: `https://f001.backblazeb2.com/file/Backblaze-Hard-Drive-Data/data_Q1_2016.zip`
- Filename: `data_Q1_2016.zip`
- Locally observed bytes: `270281914`
- Locally observed SHA-256: `768BD8E2E95564012B1467C0F9C23E8C591526CA79A2C6CDADF707CBD8850699`
- Expected calendar: 2016-01-01 through 2016-03-31, with no approved gaps.

Backblaze does not publish an independent SHA-256 for this historical file. The local hash is enforced against substitution, but the validation result remains `requires_review` and real ingestion is not yet authorized.

## Windows verification

Set `$python` to the existing approved DrivePriority Python 3.11 environment. Do not substitute dependencies or install packages without project-owner approval.

```powershell
$ErrorActionPreference = "Stop"
$python = "C:\Path\To\Approved\DrivePriority\.venv\Scripts\python.exe"
$root = (Get-Location).Path
$env:PYTHONPATH = Join-Path $root "src"
$env:PYTHONDONTWRITEBYTECODE = "1"

& $python -B -m pytest -p no:cacheprovider -q `
    tests\test_schema_validation.py tests\test_ingestion.py
if ($LASTEXITCODE -ne 0) { throw "C1.1 tests failed." }
```

## Authorized dry run only

Use a teammate-specific output path that does not already exist. `--dry-run` must not create it.

```powershell
$archive = "C:\DrivePriority\Raw\data_Q1_2016.zip"
$output = "C:\DrivePriority\DryRuns\REPLACE_WITH_TEAMMATE\2016Q1"

if ((Get-FileHash -Algorithm SHA256 -LiteralPath $archive).Hash -ne `
    "768BD8E2E95564012B1467C0F9C23E8C591526CA79A2C6CDADF707CBD8850699") {
    throw "2016 Q1 source hash mismatch."
}

& $python -B -m drivepriority.data.ingest `
    --config configs\ingestion.yaml `
    --quarter 2016Q1 `
    --archive $archive `
    --output $output `
    --source-url "https://f001.backblazeb2.com/file/Backblaze-Hard-Drive-Data/data_Q1_2016.zip" `
    --dry-run
if ($LASTEXITCODE -ne 0) { throw "C1.1 dry run failed." }
if (Test-Path -LiteralPath $output) { throw "Dry run unexpectedly created output." }
```

Use `Faheem`, `Zahid`, `Sulaiman`, or `Shuhail` for `REPLACE_WITH_TEAMMATE`. Do not remove `--dry-run` until independent source-authenticity evidence and project-owner authorization are recorded.
