# Teammate quick start — DrivePriority AI 0.3.0

1. Verify the ZIP SHA-256 supplied by the project leader.
2. Extract the ZIP into a new folder.
3. Open PowerShell in the extracted package and run:

```powershell
py -3.11 -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
$env:DRIVEPRIORITY_PYTHON = "$PWD\.venv\Scripts\python.exe"
.\RUN_ME.cmd help
```

All five recognized operators may process any quarter. Select your real name for
traceability. Never share or overwrite another run directory.

Start with `RUN_ME.cmd inspect`. If the exact archive SHA-256 is not authorized,
send the generated summary to Haady Abdul Jabar. Only the leader uses
`RUN_ME.cmd authorize`; operators do not edit approval YAML.

After authorization, run `clean`, then `analyze`, then `revalidate`. A result of
`requires_review` is not a software failure: read its reasons and next actions.
Upload the immutable cleaned directory, analysis directory, revalidation directory,
and operator evidence together.

Existing v0.1.4 output should be analyzed and revalidated in place only when its
manifest inventory and hashes verify. The tools create new evidence directories and
never modify the old Parquet. A hash mismatch or unsupported contract remains blocked.

See `docs/C1_2_MASTER_GUIDE.md` for exact commands.
