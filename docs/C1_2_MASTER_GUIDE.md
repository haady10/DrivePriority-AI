# DrivePriority AI C1.2 master cleaning package

This code-only package supports Windows teammates without Git. It covers source inventory generation for every quarter from 2016 Q1 through 2026 Q2, but it never guesses teammate assignments, approvals, hashes, or missing-date decisions.

## Required inputs

1. The frozen C1.2 ZIP and its published SHA-256.
2. An existing approved Python 3.11 environment matching `requirements.txt`.
3. Locally downloaded Backblaze ZIP files named `data_Qn_YYYY.zip`.
4. A research-lead-completed copy of `configs/teammate_assignments.yaml`.
5. Optional documented approvals based on `configs/source_approvals_template.yaml`.

The official reference page is `https://www.backblaze.com/cloud-storage/resources/hard-drive-test-data`. Direct archive URLs use Backblaze's listed `data_Qn_YYYY.zip` links. The inventory tool does not download archives.

## Evidence and approval states

- `verified_local_bytes`: SHA-256, exact byte size, ZIP CRC, member selection, and date coverage were calculated locally.
- `unverified`: no independent publisher checksum or completed manual authenticity decision exists. Cleaning may run only if separately authorized, and output remains `requires_review`.
- `publisher_checksum_verified`: a published checksum was independently reviewed and documented.
- `manual_review_approved`: a named reviewer documented the official URL, archive integrity, date coverage, matching SHA-256, and evidence reference. This is explicitly distinct from a publisher-issued checksum.

An approval must bind the exact observed archive SHA-256. Any mismatch stops the launcher. Missing dates are never approved automatically; every approved gap must appear explicitly in the approval file and must actually be absent from the archive.

## Assignment procedure

The research lead edits a copy of `configs/teammate_assignments.yaml` outside the package. Each quarter may be assigned according to the project plan. Empty lists deliberately stop execution.

## Manual approval procedure

For every archive lacking a publisher checksum:

1. Confirm the quarter appears on the official Backblaze listing and review its direct download URL.
2. Verify the filename, exact local byte size, SHA-256, complete ZIP CRC, member counts, and complete archive-member accounting.
3. Review observed member dates against the complete quarter calendar.
4. Investigate every missing, duplicate, unexpected, or unrecognized date member.
5. Record any approved missing dates individually; never approve a date merely to make validation pass.
6. Record the reviewer name, UTC timestamp, exact archive SHA-256, evidence reference, processing decision, and authenticity status in a copy of `source_approvals_template.yaml`.
7. Use `authenticity_status: unverified` when cleaning is authorized for evidence generation but authenticity is not approved. Such output remains `requires_review`.
8. Use `manual_review_approved` only after all review controls are complete. The manifest retains this basis so it cannot be confused with `publisher_checksum_verified`.

## Launcher behavior

`RUN_C1_2.ps1` verifies the master ZIP hash, reads the named teammate's assignments, scans existing archives, writes immutable evidence/configuration files, and refuses unapproved quarters. With `-DryRunOnly`, it runs pipeline dry runs only. With `-ExecuteCleaning`, it runs authorized quarters and places completed output under exactly one of:

- `approved` for `passed` or `passed_with_warnings`;
- `requires_review` for `requires_review`;
- `failed` for failed validation or nonzero processing.

The launcher writes `source_inventory_report.json`, `execution_plan.json`, and `teammate_summary.json`. It never promotes `requires_review` output.

## PowerShell invocation

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\RUN_C1_2.ps1 `
    -PackagePath "C:\DrivePriority\DrivePriority_C1_2_Master.zip" `
    -ExpectedPackageSha256 "PUBLISHED_64_HEX_SHA256" `
    -Teammate "Faheem" `
    -ArchiveDirectory "C:\DrivePriority\Raw" `
    -EvidenceRoot "C:\DrivePriority\Evidence" `
    -OutputRoot "C:\DrivePriority\Cleaned" `
    -PythonPath "C:\DrivePriority\ApprovedRuntime\.venv\Scripts\python.exe" `
    -AssignmentsPath "C:\DrivePriority\Control\teammate_assignments.yaml" `
    -ApprovalsPath "C:\DrivePriority\Control\source_approvals.yaml" `
    -DryRunOnly
```

Replace `Faheem` with `Zahid`, `Sulaiman`, or `Shuhail` for the other teammates. Replace `-DryRunOnly` with `-ExecuteCleaning` only after the execution plan and approvals have been reviewed.
