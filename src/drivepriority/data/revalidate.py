"""Immutable revalidation of cleaned output plus anomaly evidence."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Sequence

import pyarrow.parquet as pq
import yaml

from drivepriority.data.analyze import AnalysisError, _verified_input
from drivepriority.data.manifest import (
    output_inventory,
    read_json,
    sha256_file,
    utc_now,
    verify_inventory,
    write_json_atomic,
)


REVALIDATION_VERSION = "1.1.0"
LEGACY_ADAPTER_VERSION = "1.0.0"
SUPPORTED_PIPELINE_VERSIONS = {"0.1.4", "0.1.5", "0.2.0", "0.3.0"}


class RevalidationError(RuntimeError):
    """Raised when revalidation evidence is incomplete or inconsistent."""


def _verify_analysis(root: Path, expected_source_sha256: str) -> dict[str, Any]:
    manifest = read_json(root / "analysis_manifest.json")
    report = read_json(root / "anomaly_report.json")
    if manifest.get("source_manifest_sha256") != expected_source_sha256:
        raise RevalidationError("Analysis was produced for a different source manifest")
    errors = verify_inventory(
        root,
        manifest.get("outputs", []),
        excluded_names={"analysis_manifest.json"},
        required_paths={"anomaly_report.json", "collision_groups.parquet"},
    )
    if errors:
        raise RevalidationError("Analysis integrity failed: " + "; ".join(errors))
    return report


def revalidate_quarter(
    quarter_output: Path,
    analysis_output: Path,
    revalidation_output: Path,
    *,
    resolution_evidence: Path | None = None,
    authorization_evidence: Path | None = None,
    source_approval_evidence: Path | None = None,
) -> dict[str, Any]:
    quarter_output = quarter_output.resolve()
    analysis_output = analysis_output.resolve()
    revalidation_output = revalidation_output.resolve()
    if revalidation_output.exists() or revalidation_output.with_name(
        revalidation_output.name + ".staging"
    ).exists():
        raise RevalidationError("Revalidation output already exists; reports are immutable")
    manifest = _verified_input(quarter_output)
    input_pipeline_version = str(
        manifest.get("software", {}).get("pipeline_version", "unknown")
    )
    if input_pipeline_version not in SUPPORTED_PIPELINE_VERSIONS:
        raise RevalidationError(
            f"Unsupported cleaned-output pipeline version: {input_pipeline_version}"
        )
    source_manifest_sha = sha256_file(quarter_output / "source_manifest.json")
    analysis = _verify_analysis(analysis_output, source_manifest_sha)
    quarter = str(manifest.get("assigned_quarter"))
    if analysis.get("quarter") != quarter:
        raise RevalidationError("Analysis quarter does not match cleaned output")

    resolutions: dict[str, Any] = {}
    if resolution_evidence is not None:
        resolutions = read_json(resolution_evidence.resolve())
        if resolutions.get("quarter") != quarter:
            raise RevalidationError("Resolution evidence quarter mismatch")
        if resolutions.get("source_manifest_sha256") != source_manifest_sha:
            raise RevalidationError("Resolution evidence source identity mismatch")
    authorization: dict[str, Any] = {}
    if authorization_evidence is not None:
        authorization = read_json(authorization_evidence.resolve())
        if authorization.get("quarter") != quarter:
            raise RevalidationError("Authorization evidence quarter mismatch")
        if authorization.get("archive_sha256") != manifest.get("source", {}).get("sha256"):
            raise RevalidationError("Authorization evidence archive identity mismatch")
    source_approval: dict[str, Any] = {}
    if source_approval_evidence is not None:
        payload = yaml.safe_load(source_approval_evidence.resolve().read_text(encoding="utf-8"))
        records = payload.get("records") if isinstance(payload, dict) else None
        if not isinstance(records, list):
            raise RevalidationError("Source approval evidence must use version 2 records")
        archive_sha = manifest.get("source", {}).get("sha256")
        candidates = [
            record
            for record in records
            if isinstance(record, dict)
            and record.get("quarter") == quarter
            and str(record.get("archive_sha256", "")).lower()
            == str(archive_sha).lower()
        ]
        if not candidates:
            raise RevalidationError("No source approval matches this quarter/archive")
        source_approval = candidates[-1]
        if source_approval.get("research_use_source_approved") is not True:
            raise RevalidationError("Source approval has not approved research use")
        if source_approval.get("evidence_level") not in {
            "level2_official_download_supported",
            "level3_publisher_bytes_verified",
        }:
            raise RevalidationError("Source evidence level is insufficient for research use")

    data_files = sorted((quarter_output / "data").rglob("*.parquet"))
    row_count = 0
    schemas: set[str] = set()
    for path in data_files:
        parquet = pq.ParquetFile(path)
        row_count += parquet.metadata.num_rows
        schemas.add(str(parquet.schema_arrow))
    processing = manifest.get("processing", {})
    structural_errors: list[str] = []
    if row_count != processing.get("output_rows"):
        structural_errors.append("canonical Parquet row count differs from source manifest")
    if len(schemas) != 1:
        structural_errors.append("canonical Parquet schemas are inconsistent")

    unresolved = int(analysis.get("identity_analysis", {}).get("unresolved_serials", 0))
    approved_serials = resolutions.get("approved_serials", [])
    if not isinstance(approved_serials, list):
        raise RevalidationError("approved_serials must be a list")
    remaining_identity = max(0, unresolved - len(set(map(str, approved_serials))))
    original_validation = read_json(quarter_output / "validation_results.json")
    original_review_reasons = list(original_validation.get("review_reasons", []))
    source_review = [
        reason
        for reason in original_review_reasons
        if reason
        in {
            "approved_source_inventory_unavailable",
            "independent_archive_checksum_unavailable",
            "unexplained_calendar_coverage_gaps",
        }
    ]
    if source_approval:
        source_review = []
    authorization_required = processing.get("production_or_full_ingestion_authorized") is False
    authorization_approved = authorization.get("decision") == "authorized_post_run_review"
    review_reasons: list[str] = []
    if remaining_identity:
        review_reasons.append("unresolved_identity_ambiguity")
    review_reasons.extend(source_review)
    if authorization_required and not authorization_approved:
        review_reasons.append("full_ingestion_authorization_unresolved")

    if structural_errors:
        status = "failed"
    elif review_reasons:
        status = "requires_review"
    elif analysis.get("exception_groups") or analysis.get("identity_analysis", {}).get(
        "known_unknown_capacity_serials", 0
    ):
        status = "passed_with_warnings"
    else:
        status = "passed"
    report = {
        "revalidation_version": REVALIDATION_VERSION,
        "quarter": quarter,
        "generated_at": utc_now(),
        "status": status,
        "outcome_order": ["failed", "requires_review", "passed_with_warnings", "passed"],
        "source_manifest_sha256": source_manifest_sha,
        "analysis_manifest_sha256": sha256_file(analysis_output / "analysis_manifest.json"),
        "checks": {
            "cleaned_output_inventory": "verified",
            "analysis_inventory": "verified",
            "canonical_parquet_files": len(data_files),
            "canonical_rows": row_count,
            "schema_consistent": len(schemas) == 1,
            "failure_flag_reconciliation": original_validation.get("checks", []),
        },
        "identity": {
            "reported_unresolved_serials": unresolved,
            "approved_resolution_serials": len(set(map(str, approved_serials))),
            "remaining_unresolved_serials": remaining_identity,
            "automatic_comparison_treatments_do_not_rewrite_rows": True,
        },
        "source": {
            "archive_sha256": manifest.get("source", {}).get("sha256"),
            "review_reasons": source_review,
            "approval_evidence": source_approval or None,
        },
        "legacy_compatibility": {
            "input_pipeline_version": input_pipeline_version,
            "adapter_version": LEGACY_ADAPTER_VERSION,
            "historical_files_modified": False,
            "compatible": True,
        },
        "authorization": {
            "historically_required": authorization_required,
            "approved_post_run_review": authorization_approved,
            "evidence": authorization or None,
        },
        "structural_errors": structural_errors,
        "review_reasons": sorted(set(review_reasons)),
        "next_actions": (
            ["Resolve each listed review reason; do not overwrite this report"]
            if status == "requires_review"
            else ["Submit this immutable report to the central admission gate"]
        ),
    }
    staging = revalidation_output.with_name(revalidation_output.name + ".staging")
    staging.mkdir(parents=True)
    write_json_atomic(staging / "revalidation_report.json", report)
    write_json_atomic(
        staging / "revalidation_manifest.json",
        {
            "revalidation_version": REVALIDATION_VERSION,
            "quarter": quarter,
            "generated_at": report["generated_at"],
            "outputs": output_inventory(staging, excluded_names={"revalidation_manifest.json"}),
        },
    )
    staging.replace(revalidation_output)
    return report


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--quarter-output", type=Path, required=True)
    parser.add_argument("--analysis-output", type=Path, required=True)
    parser.add_argument("--revalidation-output", type=Path, required=True)
    parser.add_argument("--resolution-evidence", type=Path)
    parser.add_argument("--authorization-evidence", type=Path)
    parser.add_argument("--source-approval-evidence", type=Path)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    arguments = build_parser().parse_args(argv)
    try:
        result = revalidate_quarter(
            arguments.quarter_output,
            arguments.analysis_output,
            arguments.revalidation_output,
            resolution_evidence=arguments.resolution_evidence,
            authorization_evidence=arguments.authorization_evidence,
            source_approval_evidence=arguments.source_approval_evidence,
        )
    except (AnalysisError, RevalidationError, OSError, ValueError) as exc:
        print(f"ERROR: {exc}")
        return 2
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0 if result["status"] != "failed" else 2


if __name__ == "__main__":
    raise SystemExit(main())
