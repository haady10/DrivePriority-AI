"""Unattended watcher: finds new quarter ZIPs in the OneDrive team folder, checks them, integrates them.

What it does, every --poll-minutes (default 60):
  1. Finds ZIPs under --onedrive that are not yet handled (SHA-256 cache, skips files still syncing).
  2. Checks each one with tools/inspect_packages.py (the same rules as the central gate, plus the
     pipeline-version / runtime-hash check against --registry).
  3. READY ZIPs are copied (hash verified) into Input_Quarters. An older ZIP of the same quarter is
     MOVED to old_blocked_zips, never deleted.
  4. If anything new is READY and no run is active, runs the central operator once, then compacts the
     new snapshot (one Parquet file per quarter) for profiling and Phase 3.
  5. Writes Reports/auto_integrate.log and Reports/auto_integrate_status.json.

What it never does: edit a ZIP, manifest, report or hash; delete anything; bypass the gate; retry a
failing operator with changes. Anything else it cannot decide waits for a person (see the status file).

Only with --auto-ledger: a quarter between 2022Q1 and 2026Q2 whose ONLY problem is a missing
research-use approval gets an entry in configs/central_policy.json, bound to the archive SHA-256 read
from its own manifest, under the coordinator's written authorization of 2026-11-05 (manual Level 2
approval of the official Backblaze download, NOT a publisher-checksum verification).

Usage (Windows, from the repo root):
  .venv\\Scripts\\python tools\\auto_integrate.py --onedrive "<team folder>" --registry "<registry.json>" --auto-ledger
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import inspect_packages as ip  # noqa: E402

LEDGER_ONLY_PROBLEM = "no Level 2/3 research-use approval in revalidation"
LEDGER_SCOPE = re.compile(r"^(2022Q[1-4]|2023Q[1-4]|2024Q[1-4]|2025Q[1-4]|2026Q[12])$")
SNAPSHOT_NAME = re.compile(r"^Snapshot-(\d{4})$")


class Watcher:
    def __init__(self, root: Path, onedrive: Path, registry: Path, *, auto_ledger: bool = False,
                 dry_run: bool = False, settle_minutes: float = 5.0, python: str | None = None) -> None:
        self.root, self.onedrive, self.registry = root.resolve(), onedrive, registry
        self.auto_ledger, self.dry_run, self.settle = auto_ledger, dry_run, settle_minutes * 60
        self.python = python or sys.executable
        self.reports = self.root / "Reports"
        self.reports.mkdir(exist_ok=True)
        self.inputs = self.root / "Input_Quarters"
        self.inputs.mkdir(exist_ok=True)
        self.old = self.root.parent / "old_blocked_zips"
        self.state_path = self.reports / "auto_integrate_state.json"
        self.state = (json.loads(self.state_path.read_text(encoding="utf-8")) if self.state_path.exists()
                      else {"files": {}, "handled": {}, "operator_errors": 0, "runs": []})
        self.registry_data = json.loads(registry.read_text(encoding="utf-8-sig")).get("releases", {})

    # ---------- helpers ----------
    def log(self, message: str) -> None:
        line = f"[{datetime.now():%Y-%m-%d %H:%M:%S}] {message}"
        print(line, flush=True)
        with open(self.reports / "auto_integrate.log", "a", encoding="utf-8") as fh:
            fh.write(line + "\n")

    def save(self) -> None:
        if not self.dry_run:  # a dry run must never make a later real run skip a file
            tmp = self.state_path.with_suffix(".tmp")
            tmp.write_text(json.dumps(self.state, indent=2, sort_keys=True), encoding="utf-8")
            os.replace(tmp, self.state_path)
        status = {"updated": datetime.now().isoformat(timespec="seconds"), "handled": self.state["handled"],
                  "operator_errors": self.state["operator_errors"], "runs": self.state["runs"][-5:],
                  "integrated_quarters": sorted(self.integrated_quarters()), "dry_run": self.dry_run}
        (self.reports / "auto_integrate_status.json").write_text(json.dumps(status, indent=2), encoding="utf-8")

    @staticmethod
    def sha256(path: Path) -> str:
        digest = hashlib.sha256()
        with open(path, "rb") as fh:
            for chunk in iter(lambda: fh.read(8 * 1024 * 1024), b""):
                digest.update(chunk)
        return digest.hexdigest()

    def latest_snapshot(self) -> Path | None:
        base = self.root / "Integrated_Data"
        found = sorted((p for p in base.glob("Snapshot-*") if SNAPSHOT_NAME.match(p.name)), key=lambda p: p.name)
        return found[-1] if found else None

    def integrated_quarters(self) -> set[str]:
        snap = self.latest_snapshot()
        if not snap or not (snap / "central_manifest.json").is_file():
            return set()
        return set(json.loads((snap / "central_manifest.json").read_text(encoding="utf-8")).get("input_quarters", {}))

    # ---------- discovery ----------
    def candidates(self) -> list[Path]:
        found = []
        now = time.time()
        for path in sorted(self.onedrive.rglob("*.zip")):
            try:
                st = path.stat()
            except OSError:
                continue
            if now - st.st_mtime < self.settle:
                continue  # probably still syncing or being written
            found.append(path)
        return found

    def digest_cached(self, path: Path) -> str:
        st = path.stat()
        key = str(path)
        cached = self.state["files"].get(key)
        if cached and cached["size"] == st.st_size and cached["mtime"] == st.st_mtime:
            return cached["sha256"]
        digest = self.sha256(path)
        self.state["files"][key] = {"size": st.st_size, "mtime": st.st_mtime, "sha256": digest}
        return digest

    # ---------- ledger ----------
    def add_ledger(self, quarter: str, archive_sha: str) -> None:
        policy_path = self.root / "configs" / "central_policy.json"
        policy = json.loads(policy_path.read_text(encoding="utf-8"))
        ledger = policy.setdefault("central_research_use_approvals", {})
        if quarter in ledger and ledger[quarter].get("archive_sha256", "").lower() != archive_sha.lower():
            raise RuntimeError(f"Ledger already holds a different hash for {quarter}")
        backup = policy_path.with_name(f"central_policy.json.bak-{datetime.now():%Y%m%d%H%M%S}")
        shutil.copy2(policy_path, backup)
        ledger[quarter] = {
            "archive_sha256": archive_sha.lower(),
            "evidence_level": "level2_official_download_supported",
            "research_use_source_approved": True,
            "approved_by": "Haady Abdul Jabar V.A (project coordinator), as stated in the project session",
            "approved_on": "2026-11-05",
            "statement": "Manual approval of the official Backblaze download for research use. NOT a publisher-checksum verification.",
            "added_by": "tools/auto_integrate.py under the coordinator's written authorization of 2026-11-05 for 2022Q1-2026Q2",
        }
        tmp = policy_path.with_suffix(".tmp")
        tmp.write_text(json.dumps(policy, indent=2), encoding="utf-8")
        os.replace(tmp, policy_path)
        self.log(f"LEDGER entry added for {quarter} (archive {archive_sha[:12]}…); backup {backup.name}")

    # ---------- one candidate ----------
    def handle(self, path: Path) -> str | None:
        sha = self.digest_cached(path)
        if sha in self.state["handled"]:
            return None
        row = ip.inspect(path, self.registry_data)
        quarter = str(row["quarter"])
        record = {"file": str(path), "quarter": quarter, "time": datetime.now().isoformat(timespec="seconds"),
                  "problems": list(row["problems"]), "warnings": list(row["warnings"])}
        if quarter == "-":
            record["result"] = "not_a_quarter_package"
            self.state["handled"][sha] = record
            return None
        problems = list(row["problems"])
        if (self.auto_ledger and problems == [LEDGER_ONLY_PROBLEM] and LEDGER_SCOPE.match(quarter)
                and len(str(row["archive_sha256"])) == 64 and not self.dry_run):
            self.add_ledger(quarter, str(row["archive_sha256"]))
            problems = []
        if problems:
            record["result"] = "blocked_needs_person"
            self.log(f"BLOCKED {quarter} {path.name}: {' | '.join(problems)}")
        elif quarter in self.integrated_quarters():
            record["result"] = "quarter_already_integrated_needs_person"
            self.log(f"SKIPPED {quarter} {path.name}: quarter is already in the snapshot; a person must decide")
        elif self.dry_run:
            record["result"] = "ready_dry_run"
            self.log(f"READY (dry run, not copied) {quarter} {path.name}")
        else:
            self.stage(path, sha, quarter)
            record["result"] = "ready_copied_to_input"
            self.log(f"READY {quarter} {path.name} copied to Input_Quarters")
        self.state["handled"][sha] = record
        return record["result"]

    def stage(self, path: Path, sha: str, quarter: str) -> None:
        for existing in self.inputs.glob("*.zip"):
            if self.sha256(existing) == sha:
                return
            if str(ip.inspect(existing, self.registry_data)["quarter"]) == quarter:
                self.old.mkdir(exist_ok=True)
                target = self.old / f"{existing.stem}.replaced-{datetime.now():%Y%m%d%H%M%S}{existing.suffix}"
                shutil.move(str(existing), str(target))
                self.log(f"MOVED older {quarter} ZIP {existing.name} to {target}")
        dest = self.inputs / path.name
        if dest.exists():
            dest = self.inputs / f"{path.stem}_{sha[:8]}{path.suffix}"
        shutil.copy2(path, dest)
        if self.sha256(dest) != sha:
            dest.unlink()
            raise RuntimeError(f"Copy hash mismatch for {path.name}")

    # ---------- operator and compaction ----------
    @staticmethod
    def operator_running() -> bool:
        """True if any other integration operator process is running on this machine."""
        try:
            if os.name == "nt":
                cmd = ["powershell", "-NoProfile", "-Command",
                       "(Get-CimInstance Win32_Process -Filter \"name='python.exe'\" | "
                       "Where-Object { $_.CommandLine -like '*drivepriority.integration.operator*' } | "
                       "Measure-Object).Count"]
                out = subprocess.run(cmd, capture_output=True, text=True, timeout=120).stdout.strip()
                return int(out or "0") > 0
            out = subprocess.run(["pgrep", "-f", "drivepriority.integration.operator"],
                                 capture_output=True, text=True, timeout=30).stdout.strip()
            return bool(out)
        except Exception:
            return True  # cannot tell: do not start a second run

    def run_module(self, module: str, *args: str, log_name: str) -> int:
        env = dict(os.environ, PYTHONPATH=str(self.root / "src"), PYTHONDONTWRITEBYTECODE="1",
                   DRIVEPRIORITY_NO_PAUSE="1")
        out = self.reports / f"{log_name}_{datetime.now():%Y%m%d_%H%M%S}.log"
        with open(out, "w", encoding="utf-8") as fh:
            result = subprocess.run([self.python, "-B", "-m", module, *args], cwd=self.root, env=env,
                                    stdout=fh, stderr=subprocess.STDOUT)
        self.log(f"{module} exit {result.returncode}; log {out.name}")
        return result.returncode

    @staticmethod
    def dir_size(path: Path) -> int:
        total = 0
        for entry in os.scandir(path):
            if entry.is_dir(follow_symlinks=False):
                total += Watcher.dir_size(Path(entry.path))
            else:
                total += entry.stat(follow_symlinks=False).st_size
        return total

    def enough_disk(self) -> bool:
        """A new snapshot plus its compact copy need about 2.2x the latest snapshot, plus 10 GB."""
        snap = self.latest_snapshot()
        size = self.dir_size(snap) if snap else 0
        free = shutil.disk_usage(self.root).free
        need = int(size * 2.2) + 10 * 1024**3
        if free < need:
            self.log(f"WAIT: not enough disk for another snapshot (free {free / 1e9:.0f} GB, need about "
                     f"{need / 1e9:.0f} GB). A person must free space; nothing was deleted.")
            return False
        return True

    def integrate(self) -> None:
        if not self.enough_disk():
            return
        before = self.latest_snapshot()
        code = self.run_module("drivepriority.integration.operator", "--root", str(self.root),
                               "--release-registry", str(self.registry), log_name="auto_operator")
        run = {"time": datetime.now().isoformat(timespec="seconds"), "operator_exit": code}
        if code == 2:
            self.state["operator_errors"] += 1
            self.log("OPERATOR ERROR: not retrying with changes. See the newest auto_operator log.")
        else:
            self.state["operator_errors"] = 0
            after = self.latest_snapshot()
            run["snapshot"] = str(after) if after else None
            if after and after != before:
                compact = after.with_name(after.name + "_compact")
                if not compact.exists():
                    run["compact_exit"] = self.run_module(
                        "drivepriority.features.compact", "--snapshot", str(after), "--out", str(compact),
                        log_name="auto_compact")
        self.state["runs"].append(run)

    # ---------- main loop ----------
    def cycle(self) -> None:
        ready = 0
        for path in self.candidates():
            try:
                result = self.handle(path)
            except Exception as exc:  # one bad file must not stop the watcher
                self.log(f"ERROR handling {path.name}: {exc}")
                continue
            ready += result == "ready_copied_to_input"
        self.save()
        pending = any(r.get("result") == "ready_copied_to_input" and not r.get("integrated")
                      for r in self.state["handled"].values())
        if pending and not self.dry_run and self.state["operator_errors"] < 2 and self.operator_running():
            self.log("WAIT: an integration operator is already running; will check again next cycle")
            pending = False
        if pending and not self.dry_run and self.state["operator_errors"] < 2:
            self.integrate()
            if self.state["operator_errors"] == 0:
                for r in self.state["handled"].values():
                    if r.get("result") == "ready_copied_to_input":
                        r["integrated"] = True
        self.save()


def keep_awake() -> None:
    if os.name == "nt":
        import ctypes
        ctypes.windll.kernel32.SetThreadExecutionState(0x80000001)  # ES_CONTINUOUS | ES_SYSTEM_REQUIRED


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--root", type=Path, default=Path.cwd())
    p.add_argument("--onedrive", type=Path, required=True)
    p.add_argument("--registry", type=Path, required=True)
    p.add_argument("--auto-ledger", action="store_true")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--once", action="store_true")
    p.add_argument("--poll-minutes", type=float, default=60.0)
    p.add_argument("--settle-minutes", type=float, default=5.0)
    p.add_argument("--max-hours", type=float, default=20.0)
    p.add_argument("--stop-at-quarters", type=int, default=0,
                   help="Stop as soon as the latest snapshot holds this many quarters (0 = never)")
    a = p.parse_args(argv)
    keep_awake()
    watcher = Watcher(a.root, a.onedrive, a.registry, auto_ledger=a.auto_ledger, dry_run=a.dry_run,
                      settle_minutes=a.settle_minutes)
    lock = watcher.reports / "auto_integrate.lock"
    if lock.exists() and not a.dry_run:
        print(f"Lock file exists: {lock}. Another watcher may be running. If not, delete the file and rerun.")
        return 2
    if not a.dry_run:
        lock.write_text(datetime.now().isoformat(), encoding="utf-8")
    deadline = time.time() + a.max_hours * 3600
    try:
        watcher.log(f"START watcher (auto_ledger={a.auto_ledger}, dry_run={a.dry_run}, poll {a.poll_minutes} min)")
        while True:
            watcher.cycle()
            if a.stop_at_quarters and len(watcher.integrated_quarters()) >= a.stop_at_quarters:
                watcher.log(f"DONE: the latest snapshot holds {len(watcher.integrated_quarters())} quarters "
                            f"(target {a.stop_at_quarters}).")
                break
            if a.once or watcher.state["operator_errors"] >= 2 or time.time() > deadline:
                break
            time.sleep(a.poll_minutes * 60)
        if watcher.state["operator_errors"] >= 2:
            watcher.log("STOPPED: the operator failed twice. A person must read the newest auto_operator log.")
        else:
            watcher.log("STOP watcher (finished or time limit)")
    finally:
        if not a.dry_run:
            lock.unlink(missing_ok=True)
        if os.name == "nt":
            import ctypes
            ctypes.windll.kernel32.SetThreadExecutionState(0x80000000)
    return 0


if __name__ == "__main__":
    sys.exit(main())
