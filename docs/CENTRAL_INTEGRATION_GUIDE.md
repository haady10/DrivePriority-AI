# DrivePriority AI central integration 1.0.1

Version 1.0.1 fixes Windows command dispatch; the admission and integration
contracts are unchanged and accept DrivePriority cleaning revalidation 1.1.0.

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
