"""Windows-friendly cleaning, analysis, revalidation, and status commands."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any, Sequence
import uuid

from drivepriority import __version__
from drivepriority.data.analyze import analyze_quarter
from drivepriority.data.ingest import PipelineError, load_configuration, run_ingestion
from drivepriority.data.manifest import write_json_atomic
from drivepriority.data.revalidate import revalidate_quarter
from drivepriority.data.source_inventory import (
    _approval_for_scan,
    _write_yaml_atomic,
    OPERATORS,
    authorize_archive,
    build_quarter_configuration,
    load_approvals,
    official_archive_url,
    resource_check,
    scan_archive,
    validate_operator,
)
from drivepriority.data.manifest import read_json, sha256_file


WORKFLOW_VERSION = "1.1.0"


def _now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _clean(arguments: argparse.Namespace) -> int:
    operator = validate_operator(arguments.operator)
    evidence = arguments.evidence.resolve()
    if evidence.exists():
        raise PipelineError(f"Evidence directory already exists: {evidence}")
    evidence.mkdir(parents=True)
    configuration, _hash = load_configuration(arguments.config.resolve())
    approvals = load_approvals(arguments.approvals.resolve() if arguments.approvals else None)
    selected_approval = approvals.get(arguments.quarter)
    scan = scan_archive(
        arguments.quarter,
        arguments.archive.resolve(),
        configuration,
        source_url=(
            selected_approval.get("declared_source_url")
            if isinstance(selected_approval, dict)
            else None
        ),
    )
    approval = _approval_for_scan(arguments.quarter, scan, selected_approval)
    generated = build_quarter_configuration(configuration, scan, approval)
    config_path = evidence / "generated_ingestion.yaml"
    _write_yaml_atomic(config_path, generated)
    write_json_atomic(
        evidence / "source_evidence.json",
        {
            "workflow_version": WORKFLOW_VERSION,
            "operator": operator,
            "operator_selection_is_traceability_not_authentication": True,
            "original_operator": arguments.original_operator,
            "selected_quarter": arguments.quarter,
            "run_id": arguments.run_id or f"run-{uuid.uuid4().hex}",
            "pipeline_version": __version__,
            "processing_started_at": _now(),
            "requested_output": str(arguments.output.resolve()),
            "scan": scan,
            "approval": approval,
            "configuration_path": str(config_path),
        },
    )
    if not approval["processing_authorized"]:
        print(json.dumps({"status": "NOT_AUTHORIZED", "evidence": str(evidence)}, indent=2))
        return 3
    result = run_ingestion(
        config_path=config_path,
        quarter=arguments.quarter,
        archive=arguments.archive,
        output=arguments.output,
        source_url=official_archive_url(arguments.quarter),
        resume=arguments.resume,
        restart=arguments.restart,
    )
    manifest_path = arguments.output.resolve() / "source_manifest.json"
    manifest = read_json(manifest_path)
    result["operator_evidence"] = {
        "operator": operator,
        "original_operator": arguments.original_operator,
        "quarter": arguments.quarter,
        "archive_filename": arguments.archive.name,
        "archive_sha256": scan["archive"]["sha256"],
        "pipeline_version": __version__,
        "runtime_source_sha256": manifest.get("software", {})
        .get("recovery_identity", {})
        .get("runtime_source_sha256"),
        "processing_completed_at": _now(),
        "output": str(arguments.output.resolve()),
    }
    write_json_atomic(evidence / "cleaning_result.json", result)
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


def _inspect(arguments: argparse.Namespace) -> dict[str, Any]:
    validate_operator(arguments.operator)
    configuration, _ = load_configuration(arguments.config.resolve())
    scan = scan_archive(
        arguments.quarter,
        arguments.archive.resolve(),
        configuration,
        source_url=arguments.source_url,
        download_date=arguments.download_date,
        capture_method="operator_declared_manual_download",
        evidence_reference=arguments.evidence_reference,
    )
    result = {
        "workflow_version": WORKFLOW_VERSION,
        "operator": arguments.operator,
        "operator_selection_is_traceability_not_authentication": True,
        "scan": scan,
        "next_action": (
            "Leader must authorize this exact quarter/archive hash"
            if arguments.approvals is None
            else "Check approval status with the clean preflight"
        ),
    }
    if arguments.report:
        report = arguments.report.resolve()
        write_json_atomic(report, result)
        summary = report.with_suffix(".txt")
        summary.write_text(
            "\n".join(
                [
                    f"Quarter: {arguments.quarter}",
                    f"Operator: {arguments.operator}",
                    f"Archive: {scan['archive']['filename']}",
                    f"Bytes: {scan['archive']['bytes']}",
                    f"SHA-256: {scan['archive']['sha256']}",
                    f"ZIP CRC: {'PASS' if scan['archive']['zip_crc_verified'] else 'FAIL'}",
                    f"CSV members: {scan['archive']['data_csv_member_count']}",
                    f"Missing dates: {len(scan['calendar']['missing_dates'])}",
                    f"Authenticity: {scan['authenticity_status']}",
                    f"Next action: {result['next_action']}",
                ]
            )
            + "\n",
            encoding="utf-8",
        )
    return result


def _authorize(arguments: argparse.Namespace) -> dict[str, Any]:
    if not arguments.confirm:
        raise PipelineError("Leader authorization requires the explicit --confirm flag")
    return authorize_archive(
        quarter=arguments.quarter,
        archive=arguments.archive.resolve(),
        base_config_path=arguments.config.resolve(),
        approvals_path=arguments.approvals.resolve(),
        approved_by=arguments.approved_by,
        evidence_reference=arguments.evidence_reference,
        source_url=arguments.source_url,
        approve_processing=arguments.approve_processing,
        approve_research_use=arguments.approve_research_use,
        evidence_level=arguments.evidence_level,
        approved_missing_dates=arguments.approved_missing_date,
        publisher_sha256=arguments.publisher_sha256,
    )


def _resource(arguments: argparse.Namespace) -> dict[str, Any]:
    configuration, _ = load_configuration(arguments.config.resolve())
    result = resource_check(
        arguments.quarter,
        arguments.archive.resolve(),
        configuration,
        source_url=arguments.source_url,
    )
    if arguments.report:
        write_json_atomic(arguments.report.resolve(), result)
    return result


def _submissions(root: Path, quarter: str) -> dict[str, Any]:
    root = root.resolve()
    submissions: list[dict[str, Any]] = []
    for manifest_path in sorted(root.rglob("source_manifest.json")) if root.exists() else []:
        try:
            manifest = read_json(manifest_path)
        except (OSError, ValueError, json.JSONDecodeError):
            continue
        if manifest.get("assigned_quarter") != quarter:
            continue
        submissions.append(
            {
                "root": str(manifest_path.parent),
                "archive_sha256": manifest.get("source", {}).get("sha256"),
                "source_manifest_sha256": sha256_file(manifest_path),
                "pipeline_version": manifest.get("software", {}).get("pipeline_version"),
            }
        )
    archive_hashes = sorted(
        {str(item["archive_sha256"]) for item in submissions if item["archive_sha256"]}
    )
    return {
        "quarter": quarter,
        "submission_count": len(submissions),
        "submissions": submissions,
        "classification": (
            "none"
            if not submissions
            else "single"
            if len(submissions) == 1
            else "multiple_same_source"
            if len(archive_hashes) == 1
            else "conflicting_source_identity"
        ),
        "central_action": (
            "block_conflicting_quarter_versions"
            if len(archive_hashes) > 1
            else "independently_admit_at_most_one_version"
        ),
    }


def _status(root: Path) -> dict[str, Any]:
    root = root.resolve()
    result: dict[str, Any] = {"root": str(root)}
    for name in (
        "validation_results.json",
        "quality_report.json",
        "anomaly_report.json",
        "revalidation_report.json",
    ):
        matches = [root / name, root / "analysis" / name, root / "revalidation" / name]
        existing = next((path for path in matches if path.is_file()), None)
        if existing is not None:
            payload = json.loads(existing.read_text(encoding="utf-8"))
            result[name] = {
                "path": str(existing),
                "status": payload.get("status") or payload.get("original_validation_status"),
                "review_reasons": payload.get("review_reasons", []),
                "next_actions": payload.get("next_actions", []),
            }
    return result


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    inspect = commands.add_parser("inspect", help="Inspect one archive and collect source evidence")
    inspect.add_argument("--operator", required=True, choices=OPERATORS)
    inspect.add_argument("--quarter", required=True)
    inspect.add_argument("--archive", type=Path, required=True)
    inspect.add_argument("--source-url", required=True)
    inspect.add_argument("--download-date")
    inspect.add_argument("--evidence-reference")
    inspect.add_argument("--approvals", type=Path)
    inspect.add_argument("--report", type=Path)
    inspect.add_argument(
        "--config",
        type=Path,
        default=Path(__file__).resolve().parents[3] / "configs" / "ingestion.yaml",
    )

    authorize = commands.add_parser(
        "authorize", help="Leader-only append-only archive authorization"
    )
    authorize.add_argument("--quarter", required=True)
    authorize.add_argument("--archive", type=Path, required=True)
    authorize.add_argument("--approvals", type=Path, required=True)
    authorize.add_argument("--approved-by", required=True, choices=OPERATORS)
    authorize.add_argument("--evidence-reference", required=True)
    authorize.add_argument("--source-url", required=True)
    authorize.add_argument(
        "--evidence-level",
        choices=(
            "level1_local_integrity",
            "level2_official_download_supported",
            "level3_publisher_bytes_verified",
        ),
        default="level1_local_integrity",
    )
    authorize.add_argument("--publisher-sha256")
    authorize.add_argument("--approved-missing-date", action="append", default=[])
    authorize.add_argument("--approve-processing", action="store_true")
    authorize.add_argument("--approve-research-use", action="store_true")
    authorize.add_argument("--confirm", action="store_true")
    authorize.add_argument(
        "--config",
        type=Path,
        default=Path(__file__).resolve().parents[3] / "configs" / "ingestion.yaml",
    )

    clean = commands.add_parser("clean", help="Verify and clean one authorized archive")
    clean.add_argument("--operator", required=True, choices=OPERATORS)
    clean.add_argument("--original-operator", choices=OPERATORS)
    clean.add_argument("--run-id")
    clean.add_argument("--quarter", required=True)
    clean.add_argument("--archive", type=Path, required=True)
    clean.add_argument("--output", type=Path, required=True)
    clean.add_argument("--evidence", type=Path, required=True)
    clean.add_argument(
        "--config",
        type=Path,
        default=Path(__file__).resolve().parents[3] / "configs" / "ingestion.yaml",
    )
    clean.add_argument("--approvals", type=Path)
    action = clean.add_mutually_exclusive_group()
    action.add_argument("--resume", action="store_true")
    action.add_argument("--restart", action="store_true")

    analyze = commands.add_parser("analyze", help="Analyze an existing immutable quarter")
    analyze.add_argument("--quarter-output", type=Path, required=True)
    analyze.add_argument("--analysis-output", type=Path, required=True)
    analyze.add_argument("--memory-limit", default="1GB")

    revalidate = commands.add_parser("revalidate", help="Revalidate without re-cleaning")
    revalidate.add_argument("--quarter-output", type=Path, required=True)
    revalidate.add_argument("--analysis-output", type=Path, required=True)
    revalidate.add_argument("--revalidation-output", type=Path, required=True)
    revalidate.add_argument("--resolution-evidence", type=Path)
    revalidate.add_argument("--authorization-evidence", type=Path)
    revalidate.add_argument("--source-approval-evidence", type=Path)

    status = commands.add_parser("status", help="Display current reports and next actions")
    status.add_argument("--root", type=Path, required=True)
    submissions = commands.add_parser(
        "submissions", help="Detect duplicate or conflicting quarter submissions"
    )
    submissions.add_argument("--root", type=Path, required=True)
    submissions.add_argument("--quarter", required=True)
    resources = commands.add_parser(
        "resource-check", help="Run a bounded read-only archive resource check"
    )
    resources.add_argument("--quarter", required=True)
    resources.add_argument("--archive", type=Path, required=True)
    resources.add_argument("--source-url")
    resources.add_argument("--report", type=Path)
    resources.add_argument(
        "--config",
        type=Path,
        default=Path(__file__).resolve().parents[3] / "configs" / "ingestion.yaml",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    arguments = build_parser().parse_args(argv)
    try:
        if arguments.command == "inspect":
            result = _inspect(arguments)
        elif arguments.command == "authorize":
            result = _authorize(arguments)
        if arguments.command == "clean":
            return _clean(arguments)
        elif arguments.command == "analyze":
            result = analyze_quarter(
                arguments.quarter_output,
                arguments.analysis_output,
                memory_limit=arguments.memory_limit,
            )
        elif arguments.command == "revalidate":
            result = revalidate_quarter(
                arguments.quarter_output,
                arguments.analysis_output,
                arguments.revalidation_output,
                resolution_evidence=arguments.resolution_evidence,
                authorization_evidence=arguments.authorization_evidence,
                source_approval_evidence=arguments.source_approval_evidence,
            )
        elif arguments.command == "status":
            result = _status(arguments.root)
        elif arguments.command == "submissions":
            result = _submissions(arguments.root, arguments.quarter)
        elif arguments.command == "resource-check":
            result = _resource(arguments)
    except Exception as exc:
        print(f"ERROR: {exc}")
        return 2
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
