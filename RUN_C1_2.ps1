[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)][string]$PackagePath,
    [Parameter(Mandatory = $true)][ValidatePattern('^[0-9A-Fa-f]{64}$')][string]$ExpectedPackageSha256,
    [Parameter(Mandatory = $true)][ValidateSet('Faheem', 'Zahid', 'Sulaiman', 'Shuhail')][string]$Teammate,
    [Parameter(Mandatory = $true)][string]$ArchiveDirectory,
    [Parameter(Mandatory = $true)][string]$EvidenceRoot,
    [Parameter(Mandatory = $true)][string]$OutputRoot,
    [Parameter(Mandatory = $true)][string]$PythonPath,
    [string]$AssignmentsPath = (Join-Path $PSScriptRoot 'configs\teammate_assignments.yaml'),
    [string]$ApprovalsPath,
    [switch]$ExecuteCleaning,
    [switch]$DryRunOnly
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

function Assert-File([string]$Path, [string]$Label) {
    if (-not (Test-Path -LiteralPath $Path -PathType Leaf)) {
        throw "$Label not found: $Path"
    }
}

function Get-Sha256([string]$Path) {
    $algorithm = [System.Security.Cryptography.SHA256]::Create()
    try {
        $stream = [System.IO.File]::OpenRead($Path)
        try {
            return ([System.BitConverter]::ToString($algorithm.ComputeHash($stream))).Replace('-', '')
        } finally {
            $stream.Dispose()
        }
    } finally {
        $algorithm.Dispose()
    }
}

function Write-JsonAtomic([string]$Path, [object]$Value) {
    $parent = Split-Path -Parent $Path
    if (-not (Test-Path -LiteralPath $parent)) {
        New-Item -ItemType Directory -Path $parent | Out-Null
    }
    $temporary = Join-Path $parent ('.' + (Split-Path -Leaf $Path) + '.tmp')
    $json = $Value | ConvertTo-Json -Depth 12
    [System.IO.File]::WriteAllText(
        $temporary,
        $json + [Environment]::NewLine,
        (New-Object System.Text.UTF8Encoding($false))
    )
    Move-Item -LiteralPath $temporary -Destination $Path
}

Assert-File $PackagePath 'Master package'
Assert-File $PythonPath 'Approved Python interpreter'
Assert-File $AssignmentsPath 'Teammate assignment file'
if ($ApprovalsPath) { Assert-File $ApprovalsPath 'Source approval file' }

$actualPackageSha256 = Get-Sha256 $PackagePath
if ($actualPackageSha256 -ne $ExpectedPackageSha256.ToUpperInvariant()) {
    throw "Master package SHA-256 mismatch. Expected $ExpectedPackageSha256; observed $actualPackageSha256"
}

$archiveRoot = (Resolve-Path -LiteralPath $ArchiveDirectory).Path
$packageRoot = (Resolve-Path -LiteralPath $PSScriptRoot).Path
$runName = '{0}-{1}' -f $Teammate, (Get-Date).ToUniversalTime().ToString('yyyyMMddTHHmmssZ')
$evidenceDirectory = Join-Path $EvidenceRoot $runName
if (Test-Path -LiteralPath $evidenceDirectory) {
    throw "Evidence directory already exists: $evidenceDirectory"
}

$env:PYTHONPATH = Join-Path $packageRoot 'src'
$env:PYTHONDONTWRITEBYTECODE = '1'
$inventoryArguments = @(
    '-B', '-m', 'drivepriority.data.source_inventory',
    '--teammate', $Teammate,
    '--assignments', $AssignmentsPath,
    '--archive-dir', $archiveRoot,
    '--evidence-dir', $evidenceDirectory,
    '--base-config', (Join-Path $packageRoot 'configs\ingestion.yaml')
)
if ($ApprovalsPath) { $inventoryArguments += @('--approvals', $ApprovalsPath) }

& $PythonPath @inventoryArguments | Out-Host
if ($LASTEXITCODE -ne 0) { throw 'Source inventory generation failed.' }

$planPath = Join-Path $evidenceDirectory 'execution_plan.json'
Assert-File $planPath 'Execution plan'
$plan = Get-Content -LiteralPath $planPath -Raw | ConvertFrom-Json
$summaryRows = [System.Collections.Generic.List[object]]::new()

foreach ($quarter in $plan.quarters) {
    $name = [string]$quarter.quarter
    if (-not $quarter.processing_authorized) {
        $summaryRows.Add([ordered]@{
            quarter = $name
            outcome = 'NOT_AUTHORIZED'
            validation_status = 'not_run'
            reason = [string]$quarter.reason
        })
        continue
    }

    if (-not $ExecuteCleaning -and -not $DryRunOnly) {
        $summaryRows.Add([ordered]@{
            quarter = $name
            outcome = 'AUTHORIZED_NOT_EXECUTED'
            validation_status = 'not_run'
            reason = 'Use -DryRunOnly or -ExecuteCleaning after review.'
        })
        continue
    }

    $pendingRoot = Join-Path (Join-Path $OutputRoot 'pending') $name
    foreach ($category in @('approved', 'requires_review', 'failed')) {
        $existing = Join-Path (Join-Path $OutputRoot $category) $name
        if (Test-Path -LiteralPath $existing) {
            throw "Refusing to overwrite existing quarter output: $existing"
        }
    }
    if ((Test-Path -LiteralPath $pendingRoot) -or (Test-Path -LiteralPath ($pendingRoot + '.staging'))) {
        throw "Pending output already exists: $pendingRoot"
    }

    $arguments = @(
        '-B', '-m', 'drivepriority.data.ingest',
        '--config', [string]$quarter.configuration_path,
        '--quarter', $name,
        '--archive', [string]$quarter.archive_path,
        '--output', $pendingRoot,
        '--source-url', [string]$quarter.source_url
    )
    if ($DryRunOnly) { $arguments += '--dry-run' }
    & $PythonPath @arguments | Out-Host
    $exitCode = $LASTEXITCODE

    if ($DryRunOnly) {
        if ($exitCode -ne 0) { throw "Dry run failed for $name" }
        if ((Test-Path -LiteralPath $pendingRoot) -or (Test-Path -LiteralPath ($pendingRoot + '.staging'))) {
            throw "Dry run created output for $name"
        }
        $summaryRows.Add([ordered]@{
            quarter = $name
            outcome = 'DRY_RUN_PASS'
            validation_status = [string]$quarter.expected_validation_status
            reason = $null
        })
        continue
    }

    if ($exitCode -ne 0) {
        $failedParent = Join-Path $OutputRoot 'failed'
        if (-not (Test-Path -LiteralPath $failedParent)) {
            New-Item -ItemType Directory -Path $failedParent | Out-Null
        }
        $failedEvidence = $null
        if (Test-Path -LiteralPath ($pendingRoot + '.staging')) {
            $failedEvidence = Join-Path $failedParent ($name + '.staging')
            Move-Item -LiteralPath ($pendingRoot + '.staging') -Destination $failedEvidence
        } elseif (Test-Path -LiteralPath $pendingRoot) {
            $failedEvidence = Join-Path $failedParent $name
            Move-Item -LiteralPath $pendingRoot -Destination $failedEvidence
        }
        $summaryRows.Add([ordered]@{
            quarter = $name
            outcome = 'FAILED'
            validation_status = 'failed'
            reason = 'Pipeline exited nonzero; inspect matching staging evidence.'
            output = $failedEvidence
        })
        continue
    }

    $validationPath = Join-Path $pendingRoot 'validation_results.json'
    Assert-File $validationPath "Validation result for $name"
    $validation = Get-Content -LiteralPath $validationPath -Raw | ConvertFrom-Json
    $status = [string]$validation.status
    if ($status -in @('passed', 'passed_with_warnings')) {
        $category = 'approved'
    } elseif ($status -eq 'requires_review') {
        $category = 'requires_review'
    } else {
        $category = 'failed'
    }
    $destination = Join-Path (Join-Path $OutputRoot $category) $name
    $destinationParent = Split-Path -Parent $destination
    if (-not (Test-Path -LiteralPath $destinationParent)) {
        New-Item -ItemType Directory -Path $destinationParent | Out-Null
    }
    Move-Item -LiteralPath $pendingRoot -Destination $destination
    $summaryRows.Add([ordered]@{
        quarter = $name
        outcome = $category.ToUpperInvariant()
        validation_status = $status
        output = $destination
        reason = $null
    })
}

$summary = [ordered]@{
    version = '1.0.0'
    teammate = $Teammate
    package_sha256 = $actualPackageSha256
    generated_at = (Get-Date).ToUniversalTime().ToString('yyyy-MM-ddTHH:mm:ssZ')
    execute_cleaning = [bool]$ExecuteCleaning
    dry_run_only = [bool]$DryRunOnly
    quarters = $summaryRows
}
$summaryPath = Join-Path $evidenceDirectory 'teammate_summary.json'
Write-JsonAtomic $summaryPath $summary
Write-Host "C1.2 workflow complete. Summary: $summaryPath"
