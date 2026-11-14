"""Machine-independent keys for drives and for breaking ties.

Why not Python's hash() or DuckDB's hash(): both can change between versions or runs. Here the drive
key is the first 8 bytes of the MD5 of the drive id (fixed by the MD5 standard) and the mixing step is
SplitMix64 (a fixed public algorithm), so the same drive gets the same tie position on any machine.
"""
from __future__ import annotations

import hashlib

import numpy as np

_M1 = np.uint64(0xBF58476D1CE4E5B9)
_M2 = np.uint64(0x94D049BB133111EB)
_GOLDEN = np.uint64(0x9E3779B97F4A7C15)


def splitmix64(x: np.ndarray) -> np.ndarray:
    """One SplitMix64 mixing step on an array of uint64 (wraps around on overflow by design)."""
    with np.errstate(over="ignore"):
        z = (np.asarray(x, dtype=np.uint64) + _GOLDEN).astype(np.uint64)
        z = (z ^ (z >> np.uint64(30))) * _M1
        z = (z ^ (z >> np.uint64(27))) * _M2
        return z ^ (z >> np.uint64(31))


def drive_keys(drive_ids) -> np.ndarray:
    """uint64 key per drive id: first 8 bytes (big endian) of MD5(utf-8 id)."""
    out = np.empty(len(drive_ids), dtype=np.uint64)
    for i, d in enumerate(drive_ids):
        out[i] = int.from_bytes(hashlib.md5(str(d).encode("utf-8")).digest()[:8], "big")
    return out


def tie_keys_drive(keys: np.ndarray, seed: int) -> np.ndarray:
    """Frozen tie order (protocol section 6): one key per drive for a given seed, the same every day."""
    return splitmix64(np.asarray(keys, dtype=np.uint64) ^ splitmix64(np.array([seed], dtype=np.uint64))[0])


def tie_keys_day(drive_tie: np.ndarray, day: int) -> np.ndarray:
    """Sensitivity tie order: the per-drive key is mixed again with the day, so tied drives rotate."""
    return splitmix64(np.asarray(drive_tie, dtype=np.uint64) ^ splitmix64(np.array([day], dtype=np.uint64))[0])


def unit_float(u: np.ndarray) -> np.ndarray:
    """Map uint64 keys to floats in [0, 1) (53 random bits)."""
    return (np.asarray(u, dtype=np.uint64) >> np.uint64(11)).astype(np.float64) / float(1 << 53)


def in_subsample(keys: np.ndarray, rep: int, fraction: float) -> np.ndarray:
    """Whole-drive random subsample: True for about `fraction` of the drives, different for each rep."""
    u = splitmix64(np.asarray(keys, dtype=np.uint64) ^ splitmix64(np.array([1_000_003 + rep], dtype=np.uint64))[0])
    return unit_float(u) < fraction
