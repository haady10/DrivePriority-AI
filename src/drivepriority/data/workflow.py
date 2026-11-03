"""Windows-friendly cleaning, analysis, revalidation, and status commands."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Sequence

from drivepriority.data.analyze import analyze_quarter
from drivepriority.data.ingest import PipelineError, load_configuration, run_ingestion
from drivepriority.data.manifest import write_json_atomic
from drivepriority.data.revalidate import revalidate_quarter
from drivepriority.data.source_inventory import (
    _approval_for_scan,
    _write_yaml_atomic,
    build_quarter_configuration,
    load_approvals,
    official_archive_url,
    scan_archive,
)


WORKFLOW_VERSION = "1.0.0"


def _clean(arguments: argparse.Namespace) -> int:
    evidence = arguments.evidence.resolve()
    if evidence.exists():
        raise PipelineError(f"Evidence directory already exists: {evidence}")
    evidence.mkdir(parents=True)
    configuration, _hash = load_configuration(arguments.config.resolve())
    scan = scan_archive(arguments.quarter, arguments.archive.resolve(), configuration)
    approvals = load_approvals(arguments.approvals.resolve() if arguments.approvals else None)
    approval = _approval_for_scan(arguments.quarter, scan, approvals.get(arguments.quarter))
    generated = build_quarter_configuration(configuration, scan, approval)
    config_path = evidence / "generated_ingestion.yaml"
    _write_yaml_atomic(config_path, generated)
    write_json_atomic(
        evidence / "source_evidence.json",
        {
            "workflow_version": WORKFLOW_VERSION,
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
    write_json_atomic(evidence / "cleaning_result.json", result)
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


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
    clean = commands.add_parser("clean", help="Verify and clean one authorized archive")
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

    status = commands.add_parser("status", help="Display current reports and next actions")
    status.add_argument("--root", type=Path, required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    arguments = build_parser().parse_args(argv)
    try:
        if arguments.command == "clean":
            return _clean(arguments)
        if arguments.command == "analyze":
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
            )
        else:
            result = _status(arguments.root)
    except Exception as exc:
        print(f"ERROR: {exc}")
        return 2
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
