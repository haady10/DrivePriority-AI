"""Canonical schema and precision-safe conversion for Backblaze Drive Stats."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import date
from decimal import Decimal
import hashlib
import json
import re
from typing import Any, Iterable, Mapping, Sequence

import pyarrow as pa

from drivepriority.data import SCHEMA_VERSION


REQUIRED_SOURCE_FIELDS = (
    "date",
    "serial_number",
    "model",
    "capacity_bytes",
    "failure",
)
OPTIONAL_HARDWARE_FIELDS = ("manufacturer",)
PROVENANCE_FIELDS = ("source_quarter", "source_file", "schema_version")
SMART_PATTERN = re.compile(r"^smart_(?P<attribute>[a-z0-9]+)_(?P<form>raw|normalized)$")
UNSIGNED_INTEGER_PATTERN = re.compile(r"^[0-9]+$")
SIGNED_INTEGER_PATTERN = re.compile(r"^[+-]?[0-9]+$")
HEADER_SEPARATOR_PATTERN = re.compile(r"[^a-z0-9]+")

UINT64_MAX = (1 << 64) - 1
INT64_MAX = (1 << 63) - 1
SMART_DECIMAL_PRECISION = 38
SMART_DECIMAL_MAX = (10**SMART_DECIMAL_PRECISION) - 1
SMART_DECIMAL_MIN = -SMART_DECIMAL_MAX


class SchemaError(ValueError):
    """Raised when a source header cannot be mapped without ambiguity."""


@dataclass(frozen=True)
class HeaderAnalysis:
    """Reproducible mapping from an original member header to canonical names."""

    original_headers: tuple[str, ...]
    canonical_headers: tuple[str, ...]
    smart_columns: tuple[str, ...]
    unexpected_smart_columns: tuple[str, ...]
    unknown_columns: tuple[str, ...]
    missing_required_columns: tuple[str, ...]
    signature: str

    @property
    def index_by_canonical(self) -> dict[str, int]:
        return {name: index for index, name in enumerate(self.canonical_headers)}

    def to_report(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ConversionIssue:
    """One traceable source-data or conversion problem."""

    source_quarter: str
    source_file: str
    source_record_number: int | None
    source_line_number: int | None
    serial_number: str | None
    date_token: str | None
    column_name: str | None
    original_header: str | None
    raw_value: str | None
    exception_code: str
    severity: str
    row_disposition: str
    message: str
    raw_record_json: str | None
    raw_record_sha256: str | None
    first_seen_source_file: str | None = None
    first_seen_record_number: int | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def normalize_header(header: str) -> str:
    """Normalize a source header without guessing its semantic meaning."""

    normalized = header.lstrip("\ufeff").strip().lower()
    normalized = HEADER_SEPARATOR_PATTERN.sub("_", normalized).strip("_")
    return normalized


def smart_sort_key(name: str) -> tuple[int, int | str, int, str]:
    """Sort numeric SMART identifiers naturally and extensions deterministically."""

    match = SMART_PATTERN.fullmatch(name)
    if not match:
        return (2, name, 0, name)
    attribute = match.group("attribute")
    attribute_key: int | str = int(attribute) if attribute.isdigit() else attribute
    numeric_rank = 0 if attribute.isdigit() else 1
    form_rank = 0 if match.group("form") == "normalized" else 1
    return (numeric_rank, attribute_key, form_rank, name)


def analyze_headers(headers: Sequence[str]) -> HeaderAnalysis:
    """Inspect a source header and reject ambiguous canonical mappings."""

    if not headers:
        raise SchemaError("CSV member has no header columns")

    canonical = tuple(normalize_header(header) for header in headers)
    if any(not name for name in canonical):
        raise SchemaError("CSV member contains an empty header name")

    positions: dict[str, list[int]] = {}
    for index, name in enumerate(canonical):
        positions.setdefault(name, []).append(index)
    collisions = {name: indexes for name, indexes in positions.items() if len(indexes) > 1}
    if collisions:
        detail = ", ".join(f"{name} at positions {indexes}" for name, indexes in collisions.items())
        raise SchemaError(f"Header normalization collision: {detail}")

    smart = sorted((name for name in canonical if SMART_PATTERN.fullmatch(name)), key=smart_sort_key)
    unexpected_smart = sorted(
        name for name in canonical if name.startswith("smart_") and not SMART_PATTERN.fullmatch(name)
    )
    known = set(REQUIRED_SOURCE_FIELDS) | set(OPTIONAL_HARDWARE_FIELDS) | set(smart) | set(unexpected_smart)
    unknown = sorted(name for name in canonical if name not in known)
    missing = tuple(name for name in REQUIRED_SOURCE_FIELDS if name not in positions)
    signature_payload = json.dumps(list(headers), ensure_ascii=False, separators=(",", ":"))

    return HeaderAnalysis(
        original_headers=tuple(headers),
        canonical_headers=canonical,
        smart_columns=tuple(smart),
        unexpected_smart_columns=tuple(unexpected_smart),
        unknown_columns=tuple(unknown),
        missing_required_columns=missing,
        signature=hashlib.sha256(signature_payload.encode("utf-8")).hexdigest(),
    )


def union_smart_columns(analyses: Iterable[HeaderAnalysis]) -> tuple[tuple[str, ...], tuple[str, ...]]:
    smart: set[str] = set()
    unexpected: set[str] = set()
    for analysis in analyses:
        smart.update(analysis.smart_columns)
        unexpected.update(analysis.unexpected_smart_columns)
    return tuple(sorted(smart, key=smart_sort_key)), tuple(sorted(unexpected))


def canonical_arrow_schema(
    smart_columns: Sequence[str], unexpected_smart_columns: Sequence[str] = ()
) -> pa.Schema:
    """Return the stable Arrow/Parquet representation for one archive."""

    fields = [
        pa.field("date", pa.date32(), nullable=False),
        pa.field("serial_number", pa.string(), nullable=False),
        pa.field("model", pa.string()),
        pa.field("manufacturer", pa.string()),
        pa.field("capacity_bytes", pa.int64()),
        pa.field("failure", pa.uint8(), nullable=False),
    ]
    fields.extend(pa.field(name, pa.decimal128(SMART_DECIMAL_PRECISION, 0)) for name in smart_columns)
    fields.extend(pa.field(name, pa.string()) for name in unexpected_smart_columns)
    fields.extend(
        [
            pa.field("source_quarter", pa.string(), nullable=False),
            pa.field("source_file", pa.string(), nullable=False),
            pa.field("schema_version", pa.string(), nullable=False),
        ]
    )
    metadata = {
        b"drivepriority.schema_version": SCHEMA_VERSION.encode("ascii"),
        b"drivepriority.smart_semantics": b"manufacturer-specific; no universal interpretation applied",
    }
    return pa.schema(fields, metadata=metadata)


def exception_arrow_schema() -> pa.Schema:
    """Fixed schema for row-level provenance and exception evidence."""

    return pa.schema(
        [
            pa.field("source_quarter", pa.string(), nullable=False),
            pa.field("source_file", pa.string(), nullable=False),
            pa.field("source_record_number", pa.uint64()),
            pa.field("source_line_number", pa.uint64()),
            pa.field("serial_number", pa.string()),
            pa.field("date_token", pa.string()),
            pa.field("column_name", pa.string()),
            pa.field("original_header", pa.string()),
            pa.field("raw_value", pa.string()),
            pa.field("exception_code", pa.string(), nullable=False),
            pa.field("severity", pa.string(), nullable=False),
            pa.field("row_disposition", pa.string(), nullable=False),
            pa.field("message", pa.string(), nullable=False),
            pa.field("raw_record_json", pa.string()),
            pa.field("raw_record_sha256", pa.string()),
            pa.field("first_seen_source_file", pa.string()),
            pa.field("first_seen_record_number", pa.uint64()),
        ],
        metadata={b"drivepriority.schema_version": SCHEMA_VERSION.encode("ascii")},
    )


def parse_unsigned_integer(token: str, maximum: int = UINT64_MAX) -> int:
    """Parse exact base-10 unsigned integers without a floating-point intermediate."""

    candidate = token.strip()
    if not UNSIGNED_INTEGER_PATTERN.fullmatch(candidate):
        raise ValueError("value is not a nonnegative base-10 integer")
    value = int(candidate, 10)
    if value > maximum:
        raise OverflowError(f"value exceeds {maximum}")
    return value


def parse_smart_integer(token: str) -> Decimal:
    """Parse an exact integer into the canonical signed decimal128 range.

    Decimal128 precision 38 preserves integers from -(10**38-1) through
    +(10**38-1). Tokens outside that range, fractional values, and scientific
    notation are retained as exception evidence rather than rounded.
    """

    candidate = token.strip()
    if not SIGNED_INTEGER_PATTERN.fullmatch(candidate):
        raise ValueError("SMART value is not an exact base-10 integer token")
    value = int(candidate, 10)
    if not SMART_DECIMAL_MIN <= value <= SMART_DECIMAL_MAX:
        raise OverflowError(
            f"SMART value exceeds decimal128({SMART_DECIMAL_PRECISION}, 0) range "
            f"[{SMART_DECIMAL_MIN}, {SMART_DECIMAL_MAX}]"
        )
    return Decimal(value)


def parse_iso_date(token: str) -> date:
    """Parse only the Backblaze ISO calendar-date representation."""

    candidate = token.strip()
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", candidate):
        raise ValueError("date must use YYYY-MM-DD")
    return date.fromisoformat(candidate)


def raw_record_evidence(headers: Sequence[str], values: Sequence[str]) -> tuple[str, str]:
    """Serialize a source record losslessly enough for exception review."""

    evidence: Mapping[str, Any]
    if len(headers) == len(values):
        evidence = {header: value for header, value in zip(headers, values, strict=True)}
    else:
        evidence = {"headers": list(headers), "values": list(values)}
    serialized = json.dumps(evidence, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
    return serialized, hashlib.sha256(serialized.encode("utf-8")).hexdigest()
