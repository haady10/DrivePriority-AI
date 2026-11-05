"""Durable incremental central-snapshot planning and atomic rebuild recovery."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
from typing import Any, Mapping, Sequence

from drivepriority.integration.central_validate import validate_snapshot
from drivepriority.integration.central_writer import CentralWriteError, integrate_quarters


INCREMENTAL_VERSION = "1.0.0"


class IncrementalError(RuntimeError):
    """Refuse an unsafe or inconsistent incremental update."""


def _hash(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _atomic_json(path: Path, payload: Mapping[str, Any]) -> None:
    temporary = path.with_name("." + path.name + ".tmp")
    data = (json.dumps(payload, indent=2, sort_keys=True) + "\n").encode("utf-8")
    with temporary.open("wb") as stream:
        stream.write(data)
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)


def _previous_sources(snapshot: Path) -> dict[str, dict[str, Any]]:
    verification = validate_snapshot(snapshot)
    if verification.get("status") != "PASS":
        raise IncrementalError(
            "Existing snapshot failed independent validation: "
            + "; ".join(verification.get("errors", []))
        )
    manifest = json.loads((snapshot / "central_manifest.json").read_text(encoding="utf-8"))
    sources: dict[str, dict[str, Any]] = {}
    for quarter, evidence in manifest["input_quarters"].items():
        source_manifest = Path(str(evidence.get("source_manifest", ""))).resolve()
        if not source_manifest.is_file():
            raise IncrementalError(
                f"Original admitted source is unavailable for schema-safe rebuild: {quarter}"
            )
        entry: dict[str, Any] = {
            "root": str(source_manifest.parent),
            "archive_sha256": evidence["archive_sha256"],
        }
        if evidence.get("revalidation_report"):
            # A quarter admitted through a bound revalidation must keep it when the snapshot is
            # rebuilt, otherwise the rebuild re-checks it without that evidence and blocks it.
            entry["revalidation_root"] = evidence["revalidation_report"]
        if evidence.get("pipeline_commit"):
            entry["pipeline_commit"] = evidence["pipeline_commit"]
        if evidence.get("runtime_source_sha256"):
            entry["runtime_source_sha256"] = evidence["runtime_source_sha256"]
            entry["release_sha256"] = evidence.get("release_sha256")
        sources[quarter] = entry
    return sources


def incremental_update(
    existing_snapshot: Path,
    new_sources: Mapping[str, Mapping[str, Any]],
    output: Path,
    *,
    contract_path: Path | None = None,
    batch_size: int = 8192,
    resume: bool = False,
    restart: bool = False,
    _interrupt_after_checkpoint: bool = False,
) -> dict[str, Any]:
    if resume and restart:
        raise IncrementalError("--resume and --restart are mutually exclusive")
    existing_snapshot = existing_snapshot.resolve()
    output = output.absolute()
    if output.exists():
        raise IncrementalError("Output already exists; central snapshots are immutable")
    previous = _previous_sources(existing_snapshot)
    overlap = sorted(set(previous) & set(new_sources))
    if overlap:
        raise IncrementalError("Duplicate quarter addition refused: " + ", ".join(overlap))
    combined = {**previous, **{str(k): dict(v) for k, v in new_sources.items()}}
    checkpoint = output.parent / ("." + output.name + ".incremental-checkpoint")
    identity_payload = {
        "version": INCREMENTAL_VERSION,
        "existing_manifest_sha256": _hash(existing_snapshot / "central_manifest.json"),
        "sources": combined,
        "batch_size": batch_size,
        "contract": str(contract_path.resolve()) if contract_path else None,
    }
    identity = hashlib.sha256(
        json.dumps(identity_payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    if restart and checkpoint.exists():
        shutil.rmtree(checkpoint)
    if checkpoint.exists():
        if not resume:
            raise IncrementalError("Incremental checkpoint exists; use --resume or --restart")
        state = json.loads((checkpoint / "state.json").read_text(encoding="utf-8"))
        if state.get("identity") != identity or state.get("identity_payload") != identity_payload:
            raise IncrementalError("Incremental checkpoint identity is inconsistent or corrupt")
    else:
        if resume:
            raise IncrementalError("No incremental checkpoint exists to resume")
        checkpoint.mkdir()
        _atomic_json(
            checkpoint / "state.json",
            {
                "version": INCREMENTAL_VERSION,
                "identity": identity,
                "identity_payload": identity_payload,
                "status": "planned",
            },
        )
    if _interrupt_after_checkpoint:
        raise IncrementalError("Controlled interruption after durable incremental checkpoint")
    result = integrate_quarters(
        combined,
        output,
        contract_path=contract_path,
        batch_size=batch_size,
    )
    validation = validate_snapshot(output)
    if validation.get("status") != "PASS":
        raise IncrementalError(
            "New snapshot failed independent validation: "
            + "; ".join(validation.get("errors", []))
        )
    shutil.rmtree(checkpoint)
    return {
        **result,
        "incremental_version": INCREMENTAL_VERSION,
        "previous_quarters": sorted(previous),
        "added_quarters": sorted(new_sources),
        "independent_validation": "PASS",
    }


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--existing", type=Path, required=True)
    parser.add_argument("--new-sources-json", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--contract", type=Path)
    parser.add_argument("--batch-size", type=int, default=8192)
    action = parser.add_mutually_exclusive_group()
    action.add_argument("--resume", action="store_true")
    action.add_argument("--restart", action="store_true")
    arguments = parser.parse_args(argv)
    try:
        sources = json.loads(arguments.new_sources_json.read_text(encoding="utf-8"))
        result = incremental_update(
            arguments.existing,
            sources,
            arguments.output,
            contract_path=arguments.contract,
            batch_size=arguments.batch_size,
            resume=arguments.resume,
            restart=arguments.restart,
        )
    except (IncrementalError, CentralWriteError, OSError, ValueError) as exc:
        print(f"ERROR: {exc}")
        return 2
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
