"""Read the frozen Phase 5 protocol (configs/phase5_protocol_v1.json).

Every number the replay uses comes from this file. The SHA-256 of the file is written into every
run_info.json, so a result can always be tied to the exact protocol that produced it.
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import date
from pathlib import Path

DEFAULT_CONFIG = Path(__file__).resolve().parents[3] / "configs" / "phase5_protocol_v1.json"
DEFAULT_AUTHORIZATION = DEFAULT_CONFIG.parent / "phase5_test_authorization.json"
DEFAULT_LOCK_DIR = Path.home() / ".drivepriority"

# SHA-256 of the frozen protocol JSON. The same value is printed in docs/phase5_protocol.md.
# A file with another hash is not the frozen protocol: results are marked and the test split is refused.
# The hash is taken after Windows line endings (CR LF) are turned into LF, so the same committed file gives
# the same hash on every machine. The 10% validation dry run of 11 Nov 2026 ran on a Windows checkout and
# recorded the hash of the CR LF copy of this same file (FROZEN_SHA256_CRLF_COPY).
FROZEN_SHA256 = {"phase5-v1": "22110661c20a838c709c9712249a36dfba6feef84291fd9316cbe66b8fa60af6"}
FROZEN_SHA256_CRLF_COPY = {"phase5-v1": "065900ccb5e01ab53754b5505555c423fa4484aac8c7e1deba0e56c661d4f2d2"}


@dataclass(frozen=True)
class QueueConfig:
    """Carry-forward queue sizes, all as multiples of the day's capacity K (protocol section 8)."""
    admit_mult: int
    cap_mult: int
    expire_days: int

    @property
    def tag(self) -> str:
        return f"q{self.admit_mult}-{self.cap_mult}-{self.expire_days}"


@dataclass(frozen=True)
class Protocol:
    """The parsed protocol. `raw` keeps the full JSON for the run record."""
    raw: dict
    sha256: str
    path: str

    @property
    def is_frozen(self) -> bool:
        """True when this is byte for byte the frozen protocol file."""
        return FROZEN_SHA256.get(self.raw.get("protocol_version")) == self.sha256

    # ---- convenience accessors (no defaults hidden in code: everything comes from the JSON)
    def split_days(self, split: str) -> tuple[date, date]:
        s = self.raw["splits"][split]
        return date.fromisoformat(s["first_day"]), date.fromisoformat(s["last_day"])

    @property
    def budget_primary(self) -> int:
        return int(self.raw["budget_per_10000"]["primary"])

    @property
    def budgets_sensitivity(self) -> list[int]:
        return [int(b) for b in self.raw["budget_per_10000"]["sensitivity"]]

    @property
    def tie_seeds(self) -> list[int]:
        return [int(s) for s in self.raw["tie"]["seeds"]]

    @property
    def tie_mode(self) -> str:
        return self.raw["tie"]["mode"]

    @property
    def cooldown_days(self) -> int:
        return int(self.raw["cooldown_days"])

    @property
    def queue_primary(self) -> QueueConfig:
        return QueueConfig(**self.raw["queue"]["primary"])

    @property
    def queues_sensitivity(self) -> list[QueueConfig]:
        return [QueueConfig(**q) for q in self.raw["queue"]["sensitivity"]]

    @property
    def smart(self) -> str:
        return self.raw["scores"]["smart"]

    @property
    def ml(self) -> str:
        return self.raw["scores"]["ml"]


def load_protocol(path: str | Path | None = None) -> Protocol:
    """Load and sanity-check the protocol JSON."""
    p = Path(path) if path else DEFAULT_CONFIG
    data = p.read_bytes().replace(b"\r\n", b"\n")       # same hash on Windows and on Mac/Linux checkouts
    raw = json.loads(data.decode("utf-8"))
    for key in ("splits", "budget_per_10000", "scores", "tie", "cooldown_days", "queue", "endpoint",
                "rq2", "bootstrap", "fleet_subsample"):
        if key not in raw:
            raise SystemExit(f"Protocol file {p} has no '{key}' section; stopping")
    if raw["tie"]["mode"] not in ("drive", "day"):
        raise SystemExit("tie.mode must be 'drive' or 'day'")
    e = raw["endpoint"]
    if not 1 <= e["lead_min"] <= e["lead_max"]:
        raise SystemExit("endpoint lead window is not valid")
    proto = Protocol(raw=raw, sha256=hashlib.sha256(data).hexdigest(), path=str(p))
    if path is None and not proto.is_frozen:
        raise SystemExit(f"{p} is not the frozen protocol (SHA-256 {proto.sha256}). A changed protocol file is a "
                         f"deviation: stop and tell the coordinator.")
    return proto


# ---------------------------------------------------------------------- test-split control (protocol section 16)

CONTROLLED_PATHS = ("configs", "src/drivepriority/simulation")     # must be exactly as committed for a test run


def committed_state(repo: Path | None = None, auth_file: Path | None = None) -> dict:
    """Is the authorization file committed, and are the protocol, the authorization and the simulation code
    exactly as in the commit? Asked from git. Without git, or outside a git checkout, the answer is no."""
    import subprocess
    repo = Path(repo) if repo else DEFAULT_CONFIG.parent.parent
    auth_file = Path(auth_file) if auth_file else DEFAULT_AUTHORIZATION

    def git(*args):
        return subprocess.run(["git", "-C", str(repo), *args], capture_output=True, text=True, timeout=60)
    try:
        head = git("rev-parse", "HEAD")
        if head.returncode != 0:
            return {"ok": False, "reason": "not a git checkout", "commit": None}
        tracked = git("ls-files", "--error-unmatch", str(auth_file.resolve().relative_to(repo.resolve())))
        if tracked.returncode != 0:
            return {"ok": False, "reason": "the authorization file is not committed", "commit": head.stdout.strip()}
        dirty = git("status", "--porcelain", "--", *CONTROLLED_PATHS)
        if dirty.returncode != 0 or dirty.stdout.strip():
            return {"ok": False, "reason": "configs/ or the simulation code differ from the commit: "
                    + " ".join(dirty.stdout.split()), "commit": head.stdout.strip()}
        return {"ok": True, "reason": "", "commit": head.stdout.strip()}
    except (OSError, ValueError, subprocess.SubprocessError) as e:
        return {"ok": False, "reason": f"git could not be asked ({type(e).__name__})", "commit": None}


def check_test_authorization(proto: Protocol, population: str) -> dict:
    """The test split needs a written authorization: configs/phase5_test_authorization.json, committed to the
    repository after the independent review. It names the frozen protocol hash and how many test replays each
    population may have. There is no command-line option for another file: the place is fixed.

    The run must also start from a clean checkout (protocol, authorization and simulation code exactly as
    committed), so the commit hash in the run record says which code and which authorization were used."""
    if not proto.is_frozen:
        raise SystemExit("The test split is refused: the protocol file is not the frozen one.")
    p = DEFAULT_AUTHORIZATION
    if not p.is_file():
        raise SystemExit(f"The test split is locked: no authorization file ({p}). It is written and committed by "
                         f"the coordinator after the independent review accepts the work.")
    auth = json.loads(p.read_text(encoding="utf-8"))
    if auth.get("protocol_sha256") != proto.sha256:
        raise SystemExit("The test authorization is for another protocol hash; the test split stays locked.")
    if not str(auth.get("review_document", "")).strip() or not str(auth.get("authorized_by", "")).strip():
        raise SystemExit("The test authorization must name the review document and who authorized the run.")
    if int((auth.get("test_replays_authorized") or {}).get(population, 0)) < 1:
        raise SystemExit(f"The test authorization does not allow a test replay for population '{population}'.")
    state = committed_state()
    if not state["ok"]:
        raise SystemExit(f"The test split is locked: {state['reason']}. Commit the authorization file and run from "
                         f"a clean checkout, so the run record names the exact commit.")
    return {"file": str(p), "sha256": hashlib.sha256(p.read_bytes().replace(b"\r\n", b"\n")).hexdigest(),
            "git_commit": state["commit"], **auth}


def claim_test_replay(proto: Protocol, population: str, auth: dict, out_dir: Path) -> Path:
    """Record one test replay in the lock register of this machine, or stop when the authorized number is used up.

    The register is in the home folder of the user (no option to move it), so a new output folder does not give
    a second run. Each entry is the audit record of one start: time, machine, user, commit, authorization hash.
    Limits, stated openly: the register cannot see other machines, and a person who edits this code or deletes
    the register can run again. The control is therefore: the coordinator runs the test replay, on one machine,
    from a clean commit, and commits the run record (docs/phase5_protocol.md section 16)."""
    import getpass
    import platform
    reg_dir = DEFAULT_LOCK_DIR
    reg_dir.mkdir(parents=True, exist_ok=True)
    reg = reg_dir / f"phase5_test_replays_{proto.sha256[:16]}.json"
    runs = json.loads(reg.read_text(encoding="utf-8")) if reg.is_file() else []
    used = sum(1 for r in runs if r["population"] == population)
    allowed = int(auth["test_replays_authorized"][population])
    if used >= allowed:
        raise SystemExit(f"The test split was already replayed {used} time(s) for '{population}' on this machine "
                         f"({reg}) and {allowed} are authorized. Another replay is a deviation: it must be logged in "
                         f"docs/phase5_protocol.md section 19 and authorized in the committed authorization file.")
    from datetime import datetime, timezone
    try:
        user = getpass.getuser()
    except Exception:
        user = "unknown"
    runs.append({"population": population, "started_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                 "output": str(out_dir), "authorization_sha256": auth["sha256"], "git_commit": auth.get("git_commit"),
                 "authorized_by": auth.get("authorized_by"), "machine": platform.node(), "user": user})
    reg.write_text(json.dumps(runs, indent=2), encoding="utf-8")
    return reg
