"""Read-only report on quarter ZIP packages: what the central gate will see.

Standalone (standard library only). Never extracts or changes a ZIP.

Usage:
  python inspect_packages.py <folder with quarter ZIPs> [--csv report.csv]
         [--release-registry central_release_trust_....json]

With --release-registry the tool also checks that the pipeline version in each
package is listed in that trust registry (the central gate blocks it otherwise).
Without it that check is skipped and every package gets a NOTE saying so.

Checks per ZIP (the same rules the central gate applies):
  * exactly one of each of the five evidence files
  * final revalidation status is passed or passed_with_warnings
  * research-use approval in the revalidation (Level 2/3, bound to the archive SHA-256)
    - the three quarters approved centrally (2016Q3, 2018Q3, 2019Q3) only warn about this
  * exceptions.parquet rule: required and listed only when processing.exception_rows > 0;
    with 0 exception rows the file must be absent and unlisted
  * pipeline version is in the release trust registry (only when --release-registry is given;
    packages that record a git commit are checked against the pinned commit by the gate instead)
  * authenticity_verified must not be true unless a publisher checksum really was verified (warning)

For every ZIP it prints: quarter, archive SHA-256 of the raw Backblaze file the
quarter was cleaned from, how many of each required evidence file the package
contains (the gate needs exactly 1 of each), the pipeline version, the final
revalidation status, and the research-use approval bound into the revalidation.
"""
from __future__ import annotations

import argparse
import csv
import json
import re
import sys
import zipfile
from pathlib import Path

REQUIRED = ("source_manifest.json", "analysis_manifest.json", "anomaly_report.json",
            "revalidation_manifest.json", "revalidation_report.json")
OK_LEVELS = {"level2_official_download_supported", "level3_publisher_bytes_verified"}
OK_STATUS = {"passed", "passed_with_warnings"}
CENTRALLY_APPROVED = {  # quarter -> archive SHA-256 (configs/central_policy.json, 1.1.4+)
    "2016Q3": "70d0531317b3a8215adacefc78250cc50a87fe867f5293dbc14f75f0a1927607",
    "2018Q3": "7ef11ee1a22c97780222d4c7e6e4031ba6d333f0071ccc9784989428f2d6bcec",
    "2019Q3": "53de1c8d32da5dfb0f6de3317b01af4b72630ff618d10a3781c322cc433ebd1d",
}


GIT_COMMIT = re.compile(r"[0-9a-f]{40}")


def _load_registry(path: str | None) -> dict | None:
    if not path:
        return None
    data = json.loads(Path(path).read_text(encoding="utf-8-sig"))
    releases = data.get("releases")
    if not isinstance(releases, dict):
        raise SystemExit(f"Registry {path} has no 'releases' object")
    return releases


def _members(z: zipfile.ZipFile, name: str) -> list[str]:
    return [i.filename for i in z.infolist()
            if not i.is_dir() and i.filename.rsplit("/", 1)[-1] == name
            and "__MACOSX" not in i.filename]


def _load(z: zipfile.ZipFile, member: str) -> dict:
    try:
        with z.open(member) as fh:
            data = json.load(fh)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _statuses(report: dict) -> str:
    found = []
    for key, value in report.items():
        k = key.lower()
        if isinstance(value, str) and ("status" in k or "outcome" in k) and len(value) < 40:
            found.append(f"{key}={value}")
    return "; ".join(found[:3]) or "-"


def inspect(path: Path, registry: dict | None = None) -> dict:
    row = {"zip": path.name, "mb": round(path.stat().st_size / 1e6, 1), "quarter": "-",
           "archive_sha256": "-", "pipeline_version": "-", "revalidation": "-",
           "approval_level": "-", "research_use_approved": "-", "exception_rows": "-",
           "runtime_hash": "-", "problems": [], "warnings": []}
    try:
        z = zipfile.ZipFile(path)
    except Exception as exc:
        row["problems"].append(f"cannot open ZIP: {exc}")
        return row
    with z:
        counts = {n: len(_members(z, n)) for n in REQUIRED}
        for n, c in counts.items():
            if c != 1:
                row["problems"].append(f"{n}: found {c}, need exactly 1")
        sm = _members(z, "source_manifest.json")
        rv = _members(z, "revalidation_report.json")
        manifest = _load(z, sm[0]) if sm else {}
        report = _load(z, rv[0]) if rv else {}
        source = manifest.get("source") if isinstance(manifest.get("source"), dict) else {}
        software = manifest.get("software") if isinstance(manifest.get("software"), dict) else {}
        row["quarter"] = manifest.get("assigned_quarter", "-")
        row["archive_sha256"] = source.get("sha256", "-")
        row["pipeline_version"] = software.get("pipeline_version", "-")
        row["revalidation"] = _statuses(report)
        git = software.get("git") if isinstance(software.get("git"), dict) else {}
        commit = git.get("commit")
        recovery = software.get("recovery_identity") if isinstance(software.get("recovery_identity"), dict) else {}
        row["runtime_hash"] = recovery.get("runtime_source_sha256", "-")
        if isinstance(commit, str) and GIT_COMMIT.fullmatch(commit.lower()):
            row["warnings"].append("pipeline recorded by git commit; the gate checks it against the pinned commit")
        elif sm:
            version = str(row["pipeline_version"])
            if registry is None:
                row["warnings"].append("pipeline version not checked (no --release-registry given)")
            elif version not in registry:
                row["problems"].append(f"pipeline version {version!r} is not in the release trust registry")
            else:
                pinned = registry[version].get("runtime_source_sha256") if isinstance(registry[version], dict) else None
                if pinned is not None:
                    allowed = {str(x).lower() for x in ([pinned] if isinstance(pinned, str) else pinned)}
                    if str(row["runtime_hash"]).lower() not in allowed:
                        row["problems"].append(
                            f"runtime source hash {row['runtime_hash']} is not the one pinned for {version}")
        rsrc = report.get("source") if isinstance(report.get("source"), dict) else {}
        ev = rsrc.get("approval_evidence") if isinstance(rsrc.get("approval_evidence"), dict) else {}
        row["approval_level"] = ev.get("evidence_level", "none")
        row["research_use_approved"] = ev.get("research_use_source_approved", "none")
        if rv:
            central = CENTRALLY_APPROVED.get(str(row["quarter"]), "").lower() == str(source.get("sha256", "")).lower() \
                and str(source.get("sha256", "")) != ""
            if ev.get("evidence_level") not in OK_LEVELS or ev.get("research_use_source_approved") is not True:
                if central:
                    row["warnings"].append("no approval record in the ZIP; covered by the central approval list")
                else:
                    row["problems"].append("no Level 2/3 research-use approval in revalidation")
            elif str(ev.get("archive_sha256", "")).lower() != str(source.get("sha256", "")).lower():
                row["problems"].append("approval is bound to a different archive SHA-256")
            status = report.get("status")
            if status not in OK_STATUS:
                row["problems"].append(f"final revalidation status is {status!r} (need passed or passed_with_warnings)")
            if report.get("review_reasons"):
                row["problems"].append("revalidation still lists review_reasons")
        if ev.get("authenticity_verified") is True or (isinstance(manifest.get("source"), dict) and
                (manifest["source"].get("approved_inventory_evidence") or {}).get("authenticity_verified") is True):
            row["warnings"].append("authenticity_verified is true: only valid if a publisher checksum was really verified")
        processing = manifest.get("processing") if isinstance(manifest.get("processing"), dict) else {}
        exc_rows = processing.get("exception_rows")
        row["exception_rows"] = exc_rows if exc_rows is not None else "-"
        listed = any(isinstance(o, dict) and o.get("path") == "exceptions.parquet"
                     for o in (manifest.get("outputs") or []))
        present = any(n.rsplit("/", 1)[-1] == "exceptions.parquet" for n in z.namelist())
        if isinstance(exc_rows, bool) or not isinstance(exc_rows, int) or exc_rows < 0:
            if sm:
                row["problems"].append("processing.exception_rows missing or invalid")
        elif exc_rows > 0 and not (listed and present):
            row["problems"].append(f"exception_rows={exc_rows} but exceptions.parquet listed={listed}, in ZIP={present}")
        elif exc_rows == 0 and (listed or present):
            row["problems"].append(f"exception_rows=0 but exceptions.parquet listed={listed}, in ZIP={present} (must be absent)")
    return row


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("folder")
    p.add_argument("--csv")
    p.add_argument("--release-registry", dest="registry")
    a = p.parse_args()
    folder = Path(a.folder)
    zips = sorted(folder.glob("*.zip"))
    if not zips:
        print(f"No ZIP files in {folder}")
        return 2
    registry = _load_registry(a.registry)
    rows = [inspect(z, registry) for z in zips]
    for r in rows:
        print(f"\n{r['zip']}  ({r['mb']} MB)")
        print(f"  quarter           : {r['quarter']}")
        print(f"  archive sha256    : {r['archive_sha256']}")
        print(f"  pipeline version  : {r['pipeline_version']}")
        print(f"  runtime hash      : {r['runtime_hash']}")
        print(f"  revalidation      : {r['revalidation']}")
        print(f"  approval level    : {r['approval_level']}   research-use approved: {r['research_use_approved']}")
        print(f"  exception rows    : {r['exception_rows']}")
        for w in r["warnings"]:
            print(f"  NOTE: {w}")
        print("  READY" if not r["problems"] else "  PROBLEMS: " + " | ".join(r["problems"]))
    if a.csv:
        with open(a.csv, "w", newline="", encoding="utf-8") as fh:
            w = csv.writer(fh)
            w.writerow(["zip", "mb", "quarter", "archive_sha256", "pipeline_version", "runtime_hash", "revalidation",
                        "approval_level", "research_use_approved", "exception_rows", "problems", "notes"])
            for r in rows:
                w.writerow([r["zip"], r["mb"], r["quarter"], r["archive_sha256"], r["pipeline_version"], r["runtime_hash"],
                            r["revalidation"], r["approval_level"], r["research_use_approved"],
                            r["exception_rows"], " | ".join(r["problems"]), " | ".join(r["warnings"])])
        print(f"\nCSV written: {a.csv}")
    ready = sum(1 for r in rows if not r["problems"])
    print(f"\n{ready} of {len(rows)} packages have no problems.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
