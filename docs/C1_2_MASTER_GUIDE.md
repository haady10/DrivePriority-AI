# DrivePriority AI master cleaning release 0.3.0

This Git-free Windows package lets Haady Abdul Jabar, Faheem, Zahid, Sulaiman,
or Shuhail process any quarter from 2016Q1 through 2026Q2. Operator selection is
traceability, not identity authentication. Planning assignments never restrict access.

## Setup

```powershell
py -3.11 -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
$env:DRIVEPRIORITY_PYTHON = "$PWD\.venv\Scripts\python.exe"
.\RUN_ME.cmd help
```

## Operator preflight

```powershell
.\RUN_ME.cmd inspect --operator "Faheem" --quarter 2016Q1 --archive "D:\Backblaze\Raw\data_Q1_2016.zip" --source-url "https://f001.backblazeb2.com/file/Backblaze-Hard-Drive-Data/data_Q1_2016.zip" --evidence-reference "manual Backblaze download declaration"
```

The command calculates size and SHA-256, tests ZIP CRC, inventories every member,
and compares observed dates with the quarter calendar. It does not claim that the
launcher observed a download that happened earlier.

## Leader authorization

Local integrity and research-source approval are distinct. The leader appends an
exact-hash processing authorization without editing YAML:

```powershell
.\RUN_ME.cmd authorize --quarter 2016Q1 --archive "D:\Backblaze\Raw\data_Q1_2016.zip" --approvals "D:\Backblaze\Control\source_approvals.yaml" --approved-by "Haady Abdul Jabar" --evidence-reference "review record DP-2016Q1" --source-url "https://f001.backblazeb2.com/file/Backblaze-Hard-Drive-Data/data_Q1_2016.zip" --evidence-level level1_local_integrity --approve-processing --confirm
```

If documented official-download evidence has been reviewed and the methodology
permits research use without a publisher checksum, use Level 2 and add
`--approve-research-use`. This records the limitation and never claims Level 3.
Level 3 additionally requires `--publisher-sha256` matching the archive.

## Cleaning

```powershell
.\RUN_ME.cmd clean --operator "Faheem" --quarter 2016Q1 --archive "D:\Backblaze\Raw\data_Q1_2016.zip" --output "D:\Backblaze\Cleaned\2016Q1\run-001" --evidence "D:\Backblaze\Evidence\2016Q1\run-001" --approvals "D:\Backblaze\Control\source_approvals.yaml"
```

For a handover add `--original-operator "Zahid"`. Use a new run directory; never
merge partial directories or overwrite historical reports.

## Analyze and revalidate

```powershell
.\RUN_ME.cmd analyze --quarter-output "D:\Backblaze\Cleaned\2016Q1\run-001" --analysis-output "D:\Backblaze\Analysis\2016Q1\run-001"
.\RUN_ME.cmd revalidate --quarter-output "D:\Backblaze\Cleaned\2016Q1\run-001" --analysis-output "D:\Backblaze\Analysis\2016Q1\run-001" --revalidation-output "D:\Backblaze\Revalidation\2016Q1\run-001" --source-approval-evidence "D:\Backblaze\Control\source_approvals.yaml"
.\RUN_ME.cmd status --root "D:\Backblaze\Revalidation\2016Q1\run-001"
```

Revalidation accepts integrity-compatible v0.1.4 outputs through a versioned adapter,
records the original pipeline version, and never rewrites historical Parquet or reports.

## Duplicate submissions and resources

```powershell
.\RUN_ME.cmd submissions --root "D:\Backblaze\Cleaned" --quarter 2016Q1
.\RUN_ME.cmd resource-check --quarter 2016Q1 --archive "D:\Backblaze\Raw\data_Q1_2016.zip" --report "D:\Backblaze\Evidence\2016Q1-resource.json"
```

The resource check is bounded and read-only. It is not proof that full ingestion fits
in 8 GB RAM. Central integration must admit at most one independently verified version
of each quarter and must reject conflicting archive identities.
