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
    data = p.read_bytes()
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
    return Protocol(raw=raw, sha256=hashlib.sha256(data).hexdigest(), path=str(p))
