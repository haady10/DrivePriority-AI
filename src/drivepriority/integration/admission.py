"""Brick 2 Step 2: fail-closed local quarter admission. Never writes into a dataset.

This checks a single cleaned quarter. It does not merge partitions, resolve identity
collisions, or establish official source authenticity by itself.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import re
from datetime import timedelta
from pathlib import Path, PurePosixPath
from typing import Any

from drivepriority.integration.central_contract import (
    load_contract, quarter_bounds, validate_field_contract,
)

HEX64 = re.compile(r"[0-9a-fA-F]{64}\Z")
GIT_COMMIT = re.compile(r"(?:[0-9a-fA-F]{40}|[0-9a-fA-F]{64})\Z")
PARQUET = re.compile(r"data/(\d{4}Q[1-4])/(.+\.parquet)\Z")
REQUIRED_REPORTS = ("schema_report.json", "quality_report.json", "validation_results.json")


def _load_json(root: Path, name: str, reasons: list[str]) -> dict[str, Any] | None:
    p = root / name
    if not p.is_file() or p.is_symlink():
        reasons.append(f"missing or unsafe report: {name}")
        return None
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
        if not isinstance(data, dict):
            raise ValueError("JSON root is not an object")
        return data
    except (OSError, UnicodeError, ValueError) as exc:
        reasons.append(f"unreadable report: {name} ({type(exc).__name__})")
        return None


def _safe_file(root: Path, relative: Any) -> Path | None:
    if not isinstance(relative, str) or not relative or "\\" in relative:
        return None
    p = PurePosixPath(relative)
    if p.is_absolute() or any(s in ("", ".", "..") for s in p.parts) or ":" in relative:
        return None
    candidate = root.joinpath(*p.parts)
    if not candidate.is_file() or candidate.is_symlink():
        return None
    try:
        candidate.resolve(strict=True).relative_to(root.resolve(strict=True))
    except (OSError, ValueError):
        return None
    return candidate


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for block in iter(lambda: f.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


REVALIDATION_AUTHENTICITY_LEVELS = {
    "level2_official_download_supported",
    "level3_publisher_bytes_verified",
}


def _revalidation_authenticity_level(revalidation: Any, source: Any) -> str | None:
    """Return the evidence level when a passed revalidation carries an exact-hash approval."""
    if not isinstance(revalidation, dict) or not isinstance(source, dict):
        return None
    revalidation_source = revalidation.get("source")
    evidence = revalidation_source.get("approval_evidence") if isinstance(revalidation_source, dict) else None
    source_sha = source.get("sha256")
    if (
        isinstance(evidence, dict)
        and evidence.get("research_use_source_approved") is True
        and evidence.get("evidence_level") in REVALIDATION_AUTHENTICITY_LEVELS
        and isinstance(source_sha, str)
        and str(evidence.get("archive_sha256", "")).lower() == source_sha.lower()
    ):
        return str(evidence["evidence_level"])
    return None


def _load_policy(path: str | Path | None) -> dict[str, Any]:
    """Load the optional central policy. Missing or invalid means the strict default."""
    candidate = Path(path) if path else Path(__file__).resolve().parents[3] / "configs" / "central_policy.json"
    try:
        value = json.loads(candidate.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}
    return value if isinstance(value, dict) else {}



def check_quarter(root: str | Path, *, expected_quarter: str | None = None,
                  contract_path: str | Path | None = None,
                  trusted_archive_sha256: str | None = None,
                  trusted_pipeline_commit: str | None = None,
                  trusted_runtime_source_sha256: str | None = None,
                  trusted_release_sha256: str | None = None,
                  revalidation_root: str | Path | None = None,
                  policy_path: str | Path | None = None) -> dict[str, Any]:
    """Return ADMIT / REVIEW / REJECT, with evidence. Never writes to root.

    ADMIT requires caller-provided trusted source archive hash AND pinned Git commit;
    a local manifest cannot authenticate itself. Status requires_review -> REVIEW.
    """
    root = Path(root).expanduser()
    reasons: list[str] = []
    checks: dict[str, Any] = {}
    result: dict[str, Any] = {"decision": "REJECT", "quarter": expected_quarter,
                              "checks": checks, "reasons": reasons,
                              "output_root": str(root)}
    if not root.is_dir() or root.is_symlink():
        reasons.append("cleaned-output root does not exist or is a symlink")
        return result
    try:
        contract = load_contract(contract_path)
    except (OSError, ValueError, KeyError, TypeError) as exc:
        reasons.append(f"contract unavailable: {type(exc).__name__}")
        return result
    manifest = _load_json(root, "source_manifest.json", reasons)
    schema = _load_json(root, "schema_report.json", reasons)
    quality = _load_json(root, "quality_report.json", reasons)
    validation = _load_json(root, "validation_results.json", reasons)
    if any(x is None for x in (manifest, schema, quality, validation)):
        return result
    assert manifest is not None and schema is not None and quality is not None and validation is not None

    quarter = manifest.get("assigned_quarter")
    result["quarter"] = quarter
    try:
        start, end = quarter_bounds(quarter, contract)
        if expected_quarter is not None and quarter != expected_quarter:
            reasons.append("quarter does not match requested quarter")
        if manifest.get("quarter_start") != start.isoformat() or manifest.get("quarter_end") != (end - timedelta(days=1)).isoformat():
            reasons.append("manifest calendar boundaries inconsistent")
        checks["quarter_bounds"] = True
    except (ValueError, TypeError, KeyError):
        reasons.append("invalid or out-of-scope quarter")
        return result

    revalidation_approved = False
    revalidation_authenticity_level: str | None = None
    if revalidation_root is not None:
        revalidation_path = Path(revalidation_root).expanduser().resolve()
        revalidation = _load_json(
            revalidation_path, "revalidation_report.json", reasons
        )
        if revalidation is not None:
            manifest_sha = _sha256(root / "source_manifest.json")
            if revalidation.get("quarter") != quarter:
                reasons.append("revalidation quarter mismatch")
            elif revalidation.get("source_manifest_sha256") != manifest_sha:
                reasons.append("revalidation source manifest mismatch")
            elif revalidation.get("status") not in ("passed", "passed_with_warnings"):
                reasons.append("revalidation has not resolved all blocking findings")
            elif revalidation.get("review_reasons"):
                reasons.append("revalidation still contains review reasons")
            elif (
                revalidation.get("identity", {}).get("remaining_unresolved_serials")
                != 0
            ):
                reasons.append("revalidation retains unresolved identity ambiguity")
            else:
                revalidation_approved = True
                revalidation_authenticity_level = _revalidation_authenticity_level(
                    revalidation, manifest.get("source")
                )
                checks["revalidation"] = {
                    "status": revalidation.get("status"),
                    "sha256": _sha256(
                        revalidation_path / "revalidation_report.json"
                    ),
                }

    status = validation.get("status")
    admission = validation.get("central_integration_admission")
    mv = manifest.get("validation")
    if not isinstance(admission, dict) or not isinstance(mv, dict):
        reasons.append("missing central admission or manifest validation")
    elif mv.get("status") != status or mv.get("central_integration_admission") != admission:
        reasons.append("manifest and validation report disagree")
    if manifest.get("status") != "complete":
        reasons.append("manifest processing not complete")
    if status not in ("passed", "passed_with_warnings", "requires_review", "failed"):
        reasons.append("unknown or missing validation status")
    elif status != "passed" and not revalidation_approved:
        reasons.append(f"validation status is {status}; explicit exception review or rejection required")
    if (
        not revalidation_approved
        and (
            not isinstance(admission, dict)
            or admission.get("eligible") is not True
            or admission.get("requires_explicit_review") is True
        )
    ):
        reasons.append("positive, non-review central admission absent")
    if not revalidation_approved and (
        validation.get("review_reasons") or validation.get("warning_reasons")
    ):
        reasons.append("unresolved validation review or warning reasons")
    if not isinstance(validation.get("checks"), list) or not validation["checks"]:
        reasons.append("validation checks missing")
    elif any(not isinstance(c, dict) or c.get("passed") is not True for c in validation["checks"]):
        reasons.append("one or more validation checks not passed")
    checks["validation_status"] = status

    fields = schema.get("canonical_fields")
    for issue in validate_field_contract(fields, quarter, contract):
        reasons.append(f"schema contract: {issue}")
    fprint = schema.get("canonical_schema_fingerprint")
    if not isinstance(fprint, str) or not HEX64.fullmatch(fprint):
        reasons.append("missing schema fingerprint")
    m_schema = manifest.get("schema_contract")
    if not isinstance(m_schema, dict) or m_schema.get("canonical_schema_fingerprint") != fprint:
        reasons.append("schema fingerprint disagreement")
    if not isinstance(m_schema, dict) or m_schema.get("canonical_columns") != fields:
        reasons.append("schema columns disagree")
    checks["schema_fields"] = len(fields) if isinstance(fields, list) else None

    source = manifest.get("source")
    software = manifest.get("software") or {}
    git = software.get("git")
    recovery = software.get("recovery_identity") or {}
    if not isinstance(source, dict) or not isinstance(source.get("sha256"), str) or not HEX64.fullmatch(source["sha256"]):
        reasons.append("missing source archive hash")
    elif not isinstance(trusted_archive_sha256, str) or source["sha256"].lower() != trusted_archive_sha256.lower():
        reasons.append("source archive hash not matched to separately trusted inventory")
    recorded_commit = git.get("commit") if isinstance(git, dict) else None
    if isinstance(recorded_commit, str) and GIT_COMMIT.fullmatch(recorded_commit):
        if (
            not isinstance(trusted_pipeline_commit, str)
            or recorded_commit.lower() != trusted_pipeline_commit.lower()
        ):
            reasons.append("Git commit not matched to independently pinned pipeline")
        else:
            checks["pipeline_identity"] = "git_commit"
    else:
        runtime_hash = recovery.get("runtime_source_sha256")
        if (
            not isinstance(runtime_hash, str)
            or not HEX64.fullmatch(runtime_hash)
            or not isinstance(trusted_runtime_source_sha256, str)
            or runtime_hash.lower() != trusted_runtime_source_sha256.lower()
        ):
            reasons.append("Gitless runtime source hash not independently pinned")
        elif not isinstance(trusted_release_sha256, str) or not HEX64.fullmatch(
            trusted_release_sha256
        ):
            reasons.append("Gitless release package hash not independently pinned")
        else:
            checks["pipeline_identity"] = "gitless_release_and_runtime_source_hash"
            checks["release_sha256"] = trusted_release_sha256.lower()
    auth = (source or {}).get("approved_inventory_evidence") if isinstance(source, dict) else None
    if isinstance(auth, dict) and auth.get("authenticity_verified") is True:
        checks["authenticity_basis"] = "source_manifest_authenticity_verified"
    else:
        policy = _load_policy(policy_path)
        if revalidation_authenticity_level is not None and policy.get(
            "revalidation_research_use_approval_satisfies_authenticity"
        ) is True:
            # Recorded coordinator decision: an exact-hash Level 2/3 research-use approval
            # bound into the passed revalidation counts as the authenticity basis.
            # This is NOT publisher-checksum verification and is recorded as such.
            checks["authenticity_basis"] = f"policy_accepted_revalidation_{revalidation_authenticity_level}"
            checks["authenticity_policy_decision"] = policy.get("decision", {})
        else:
            reasons.append("official source authenticity unverified")
    checks["source_hash_recorded"] = source.get("sha256") if isinstance(source, dict) else None

    outputs = manifest.get("outputs")
    if not isinstance(outputs, list) or not outputs:
        reasons.append("manifest output entries missing")
        return result
    seen: set[str] = set()
    total_rows = 0
    count_files = 0
    observed_fingerprint: set[str] = set()
    parquet_meta = None
    try:
        import pyarrow.parquet as pq
        parquet_meta = pq
    except ImportError:
        reasons.append("pyarrow unavailable; cannot verify physical Parquet metadata")
    for item in outputs:
        if not isinstance(item, dict):
            reasons.append("invalid manifest output entry")
            continue
        rel = item.get("path")
        if not isinstance(rel, str) or rel in seen:
            reasons.append("duplicate or invalid manifest output path")
            continue
        seen.add(rel)
        if not (rel in REQUIRED_REPORTS or rel == "exceptions.parquet" or PARQUET.fullmatch(rel)):
            reasons.append(f"unexpected output path: {rel}")
            continue
        file = _safe_file(root, rel)
        if file is None:
            reasons.append(f"unsafe or missing output file: {rel}")
            continue
        if not isinstance(item.get("bytes"), int) or item["bytes"] < 0 or file.stat().st_size != item["bytes"]:
            reasons.append(f"byte-size mismatch: {rel}")
        if not isinstance(item.get("sha256"), str) or not HEX64.fullmatch(item["sha256"]) or _sha256(file) != item["sha256"].lower():
            reasons.append(f"SHA-256 mismatch: {rel}")
        if rel in REQUIRED_REPORTS:
            continue
        if parquet_meta is None:
            continue
        try:
            pf = parquet_meta.ParquetFile(file)
            count = pf.metadata.num_rows
            recorded = item.get("rows")
            if not isinstance(recorded, int) or recorded < 0 or recorded != count:
                reasons.append(f"Parquet row count mismatch: {rel}")
            if rel.startswith("data/"):
                match = PARQUET.fullmatch(rel)
                if match is None or match.group(1) != quarter:
                    reasons.append(f"output file in wrong quarter: {rel}")
                count_files += 1
                total_rows += count
                actual_fields = [{"name": f.name, "arrow_type": str(f.type), "nullable": f.nullable}
                                 for f in pf.schema_arrow]
                expected_fields = [{"name": f["name"], "arrow_type": f["arrow_type"], "nullable": f["nullable"]}
                                   for f in fields] if isinstance(fields, list) else []
                if actual_fields != expected_fields:
                    reasons.append(f"physical schema mismatch: {rel}")
                observed_fingerprint.add(hashlib.sha256(pf.schema_arrow.serialize().to_pybytes()).hexdigest())
        except Exception as exc:
            reasons.append(f"Parquet readback error: {rel} ({type(exc).__name__})")
    for report in REQUIRED_REPORTS:
        if report not in seen:
            reasons.append(f"missing hashed report entry: {report}")
    if "exceptions.parquet" not in seen:
        reasons.append("exceptions.parquet not listed in manifest")
    if not count_files:
        reasons.append("no data Parquet files verified")
    if len(observed_fingerprint) != 1 or fprint not in observed_fingerprint:
        reasons.append("physical Arrow schema fingerprints inconsistent or missing")
    processing = manifest.get("processing")
    if not isinstance(processing, dict) or processing.get("output_rows") != total_rows or total_rows < 1:
        reasons.append("manifest processed row count does not match Parquet metadata")
    checks.update({"parquet_files": count_files, "parquet_rows": total_rows,
                   "checked_output_entries": len(seen), "observed_schema_fingerprints": sorted(observed_fingerprint)})
    if not reasons:
        result["decision"] = "ADMIT"
    elif status in ("passed", "passed_with_warnings", "requires_review") and not any(
        "mismatch" in reason or "unsafe" in reason or "unreadable" in reason or "invalid" in reason or "missing or unsafe" in reason for reason in reasons
    ):
        result["decision"] = "REVIEW"
    else:
        result["decision"] = "REJECT"
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description="Read-only DrivePriority central admission gate")
    parser.add_argument("--output-root", required=True)
    parser.add_argument("--quarter", required=True)
    parser.add_argument("--trusted-archive-sha256", required=True)
    identity = parser.add_mutually_exclusive_group(required=True)
    identity.add_argument("--trusted-pipeline-commit")
    identity.add_argument("--trusted-runtime-source-sha256")
    parser.add_argument("--trusted-release-sha256")
    parser.add_argument("--revalidation-root")
    parser.add_argument("--contract", help="Central contract JSON; defaults to checked-in config")
    args = parser.parse_args()
    result = check_quarter(args.output_root, expected_quarter=args.quarter,
                           contract_path=args.contract,
                           trusted_archive_sha256=args.trusted_archive_sha256,
                           trusted_pipeline_commit=args.trusted_pipeline_commit,
                           trusted_runtime_source_sha256=args.trusted_runtime_source_sha256,
                           trusted_release_sha256=args.trusted_release_sha256,
                           revalidation_root=args.revalidation_root)
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0 if result["decision"] == "ADMIT" else 2


if __name__ == "__main__":
    raise SystemExit(main())
