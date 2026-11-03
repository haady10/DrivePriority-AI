[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)][string]$PackagePath,
    [Parameter(Mandatory = $true)][ValidatePattern('^[0-9A-Fa-f]{64}$')][string]$ExpectedPackageSha256,
    [Parameter(Mandatory = $true)][ValidateSet('Haady Abdul Jabar', 'Faheem', 'Zahid', 'Sulaiman', 'Shuhail')][string]$Operator,
    [Parameter(Mandatory = $true)][ValidatePattern('^20\d{2}Q[1-4]$')][string]$Quarter,
    [Parameter(Mandatory = $true)][string]$ArchivePath,
    [Parameter(Mandatory = $true)][string]$SourceUrl,
    [Parameter(Mandatory = $true)][string]$EvidenceRoot,
    [Parameter(Mandatory = $true)][string]$OutputRoot,
    [Parameter(Mandatory = $true)][string]$PythonPath,
    [Parameter(Mandatory = $true)][string]$ApprovalsPath,
    [string]$OriginalOperator,
    [switch]$InspectOnly,
    [switch]$ExecuteCleaning
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

function Assert-File([string]$Path, [string]$Label) {
    if (-not (Test-Path -LiteralPath $Path -PathType Leaf)) { throw "$Label not found: $Path" }
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

Assert-File $PackagePath 'Master package'
Assert-File $ArchivePath 'Source archive'
Assert-File $PythonPath 'Approved Python interpreter'
Assert-File $ApprovalsPath 'Leader approval store'

$actualPackageSha256 = Get-Sha256 $PackagePath
if ($actualPackageSha256 -ne $ExpectedPackageSha256.ToUpperInvariant()) {
    throw "Master package SHA-256 mismatch. Expected $ExpectedPackageSha256; observed $actualPackageSha256"
}

$packageRoot = (Resolve-Path -LiteralPath $PSScriptRoot).Path
$archive = (Resolve-Path -LiteralPath $ArchivePath).Path
$runId = 'run-{0}-{1}-{2}' -f $Quarter, $Operator.Replace(' ', '-'), (Get-Date).ToUniversalTime().ToString('yyyyMMddTHHmmssZ')
$evidence = Join-Path $EvidenceRoot $runId
$output = Join-Path (Join-Path $OutputRoot $Quarter) $runId
if ((Test-Path -LiteralPath $evidence) -or (Test-Path -LiteralPath $output)) {
    throw "Refusing to overwrite an existing run: $runId"
}

$env:PYTHONPATH = Join-Path $packageRoot 'src'
$env:PYTHONDONTWRITEBYTECODE = '1'
$base = Join-Path $packageRoot 'configs\ingestion.yaml'

& $PythonPath -B -m drivepriority.data.workflow inspect --operator $Operator --quarter $Quarter --archive $archive --source-url $SourceUrl --config $base | Out-Host
if ($LASTEXITCODE -ne 0) { throw 'Archive inspection failed.' }
if ($InspectOnly -or -not $ExecuteCleaning) {
    Write-Host 'Inspection complete. The leader must authorize this exact archive before cleaning.'
    exit 0
}

$arguments = @(
    '-B', '-m', 'drivepriority.data.workflow', 'clean',
    '--operator', $Operator,
    '--quarter', $Quarter,
    '--archive', $archive,
    '--output', $output,
    '--evidence', $evidence,
    '--approvals', (Resolve-Path -LiteralPath $ApprovalsPath).Path,
    '--config', $base,
    '--run-id', $runId
)
if ($OriginalOperator) { $arguments += @('--original-operator', $OriginalOperator) }
& $PythonPath @arguments | Out-Host
if ($LASTEXITCODE -ne 0) { throw 'Cleaning failed or this archive is not authorized.' }
Write-Host "Cleaning complete. Output: $output"
Write-Host "Evidence: $evidence"
