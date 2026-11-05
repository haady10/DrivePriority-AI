"""Read-only report on quarter ZIP packages: what the central gate will see.

Standalone (standard library only). Never extracts or changes a ZIP.

Usage:
  python inspect_packages.py <folder with quarter ZIPs> [--csv report.csv]

For every ZIP it prints: quarter, archive SHA-256 of the raw Backblaze file the
quarter was cleaned from, how many of each required evidence file the package
contains (the gate needs exactly 1 of each), the pipeline version, the final
revalidation status, and the research-use approval bound into the revalidation.
"""
from __future__ import annotations

import argparse
import csv
import json
import sys
import zipfile
from pathlib import Path

REQUIRED = ("source_manifest.json", "analysis_manifest.json", "anomaly_report.json",
            "revalidation_manifest.json", "revalidation_report.json")
OK_LEVELS = {"level2_official_download_supported", "level3_publisher_bytes_verified"}


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


def inspect(path: Path) -> dict:
    row = {"zip": path.name, "mb": round(path.stat().st_size / 1e6, 1), "quarter": "-",
           "archive_sha256": "-", "pipeline_version": "-", "revalidation": "-",
           "approval_level": "-", "research_use_approved": "-", "problems": []}
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
        rsrc = report.get("source") if isinstance(report.get("source"), dict) else {}
        ev = rsrc.get("approval_evidence") if isinstance(rsrc.get("approval_evidence"), dict) else {}
        row["approval_level"] = ev.get("evidence_level", "none")
        row["research_use_approved"] = ev.get("research_use_source_approved", "none")
        if rv:
            if ev.get("evidence_level") not in OK_LEVELS or ev.get("research_use_source_approved") is not True:
                row["problems"].append("no Level 2/3 research-use approval in revalidation")
            elif str(ev.get("archive_sha256", "")).lower() != str(source.get("sha256", "")).lower():
                row["problems"].append("approval is bound to a different archive SHA-256")
    return row


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("folder")
    p.add_argument("--csv")
    a = p.parse_args()
    folder = Path(a.folder)
    zips = sorted(folder.glob("*.zip"))
    if not zips:
        print(f"No ZIP files in {folder}")
        return 2
    rows = [inspect(z) for z in zips]
    for r in rows:
        print(f"\n{r['zip']}  ({r['mb']} MB)")
        print(f"  quarter           : {r['quarter']}")
        print(f"  archive sha256    : {r['archive_sha256']}")
        print(f"  pipeline version  : {r['pipeline_version']}")
        print(f"  revalidation      : {r['revalidation']}")
        print(f"  approval level    : {r['approval_level']}   research-use approved: {r['research_use_approved']}")
        print("  READY" if not r["problems"] else "  PROBLEMS: " + " | ".join(r["problems"]))
    if a.csv:
        with open(a.csv, "w", newline="", encoding="utf-8") as fh:
            w = csv.writer(fh)
            w.writerow(["zip", "mb", "quarter", "archive_sha256", "pipeline_version", "revalidation",
                        "approval_level", "research_use_approved", "problems"])
            for r in rows:
                w.writerow([r["zip"], r["mb"], r["quarter"], r["archive_sha256"], r["pipeline_version"],
                            r["revalidation"], r["approval_level"], r["research_use_approved"],
                            " | ".join(r["problems"])])
        print(f"\nCSV written: {a.csv}")
    ready = sum(1 for r in rows if not r["problems"])
    print(f"\n{ready} of {len(rows)} packages have no problems.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
