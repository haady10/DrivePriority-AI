"""Brick 2 Step 3: strict, non-mutating quarterly Arrow schema unification.

No ingest, admission, Parquet writing or central dataset merge occurs here.
Callers MUST obtain ADMIT from the separate quarter admission gate first.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Mapping, Any

SMART = re.compile(r"smart_[0-9]+_(raw|normalized)\Z")
REQUIRED = {"date": "date32[day]", "serial_number": "string", "failure": "uint8"}
DECIMAL = "decimal128(38, 0)"


class SchemaUnionError(ValueError):
    """Refuse an unsafe or ambiguous schema."""


@dataclass(frozen=True)
class UnionPlan:
    fields: tuple[dict[str, Any], ...]
    present_columns_by_quarter: dict[str, tuple[str, ...]]
    absent_columns_by_quarter: dict[str, tuple[str, ...]]
    # These indicate fields present in CLEANED output; they cannot independently
    # prove absence vs blank in original CSV without retained raw-column evidence.
    def as_dict(self) -> dict[str, Any]:
        return {"fields": [dict(x) for x in self.fields],
                "present_columns_by_quarter": {k: list(v) for k, v in self.present_columns_by_quarter.items()},
                "absent_columns_by_quarter": {k: list(v) for k, v in self.absent_columns_by_quarter.items()},
                "source_column_presence_note": "cleaned-schema presence only; original CSV absence/blank needs separate source evidence"}


def _validate_fields(quarter: str, fields: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    if not isinstance(quarter, str) or not re.fullmatch(r"20\d{2}Q[1-4]", quarter):
        raise SchemaUnionError(f"invalid quarter identity: {quarter!r}")
    if not isinstance(fields, list) or not fields:
        raise SchemaUnionError(f"no schema fields: {quarter}")
    result = {}
    for f in fields:
        if not isinstance(f, dict) or not isinstance(f.get("name"), str) or not f["name"]:
            raise SchemaUnionError(f"malformed schema field: {quarter}")
        name = f["name"]
        if name in result:
            raise SchemaUnionError(f"duplicate schema field: {quarter} {name}")
        if not isinstance(f.get("nullable"), bool) or not isinstance(f.get("arrow_type"), str):
            raise SchemaUnionError(f"missing type/nullability: {quarter} {name}")
        if name in REQUIRED and (f["arrow_type"] != REQUIRED[name] or f["nullable"]):
            raise SchemaUnionError(f"incorrect required field: {quarter} {name}")
        if SMART.fullmatch(name):
            if f["arrow_type"] != DECIMAL or f.get("decimal_precision", 38) != 38 or f.get("decimal_scale", 0) != 0:
                raise SchemaUnionError(f"unsafe SMART numeric type: {quarter} {name}")
        result[name] = dict(f)
    for required in REQUIRED:
        if required not in result:
            raise SchemaUnionError(f"missing required field: {quarter} {required}")
    return result


def plan_union(schemas: Mapping[str, list[dict[str, Any]]]) -> UnionPlan:
    """Build deterministic union from already-admitted quarter schema descriptions.

    Fail on mismatched types/nullability or field descriptors; never cast/round.
    This plans nulls for absent CLEANED columns; source presence evidence is separate.
    """
    if not isinstance(schemas, Mapping) or not schemas:
        raise SchemaUnionError("at least one quarter schema is required")
    validated = {q: _validate_fields(q, fs) for q, fs in sorted(schemas.items())}
    merged: dict[str, dict[str, Any]] = {}
    for q, fs in validated.items():
        for name, f in fs.items():
            if name in merged and merged[name] != f:
                raise SchemaUnionError(f"incompatible schema for {name} at {q}")
            merged.setdefault(name, f)
    order = (*REQUIRED.keys(), *sorted(set(merged) - set(REQUIRED)))
    names = set(merged)
    return UnionPlan(
        fields=tuple(merged[n] for n in order),
        present_columns_by_quarter={q: tuple(n for n in order if n in fs) for q, fs in validated.items()},
        absent_columns_by_quarter={q: tuple(n for n in order if n not in fs) for q, fs in validated.items()},
    )


def arrow_descriptions(schema: Any) -> list[dict[str, Any]]:
    """Describe a PyArrow Schema, retaining exact Arrow types and nullability."""
    try:
        import pyarrow as pa
    except ImportError as exc:
        raise RuntimeError("pyarrow is required for Arrow schema handling") from exc
    if not isinstance(schema, pa.Schema):
        raise TypeError("expected pyarrow.Schema")
    result = []
    for field in schema:
        item = {"name": field.name, "arrow_type": str(field.type), "nullable": field.nullable}
        if pa.types.is_decimal(field.type):
            item["decimal_precision"] = field.type.precision
            item["decimal_scale"] = field.type.scale
        result.append(item)
    return result


def arrow_plan(schemas: Mapping[str, Any]) -> tuple[UnionPlan, Any]:
    """Validate real Arrow schemas and return plan + exact union Arrow schema."""
    try:
        import pyarrow as pa
    except ImportError as exc:
        raise RuntimeError("pyarrow is required for Arrow unification") from exc
    desc = {q: arrow_descriptions(s) for q, s in schemas.items()}
    plan = plan_union(desc)
    lookup: dict[str, Any] = {}
    for schema in schemas.values():
        for field in schema:
            existing = lookup.get(field.name)
            if existing is not None and not field.equals(existing, check_metadata=True):
                raise SchemaUnionError(f"Arrow field/metadata conflict: {field.name}")
            lookup[field.name] = field
    unified = pa.schema([lookup[f["name"]] for f in plan.fields])
    return plan, unified


def align_batch(batch: Any, quarter: str, plan: UnionPlan, unified_schema: Any) -> Any:
    """Align ONE bounded Arrow RecordBatch; absent columns become typed nulls.

    Does not change existing data or materialize a complete dataset in memory.
    """
    try:
        import pyarrow as pa
    except ImportError as exc:
        raise RuntimeError("pyarrow is required for Arrow alignment") from exc
    if not isinstance(batch, pa.RecordBatch) or not isinstance(unified_schema, pa.Schema):
        raise TypeError("expected RecordBatch and Schema")
    if quarter not in plan.present_columns_by_quarter:
        raise SchemaUnionError(f"quarter not in union plan: {quarter}")
    expected = plan.present_columns_by_quarter[quarter]
    if set(batch.schema.names) != set(expected) or len(batch.schema.names) != len(expected):
        raise SchemaUnionError(f"batch columns differ from declared schema: {quarter}")
    arrays = []
    for field in unified_schema:
        if field.name in batch.schema.names:
            index = batch.schema.get_field_index(field.name)
            current = batch.schema.field(index)
            if not current.equals(field, check_metadata=True):
                raise SchemaUnionError(f"unsafe field/type conversion: {quarter} {field.name}")
            arrays.append(batch.column(index))
        else:
            arrays.append(pa.nulls(batch.num_rows, type=field.type))
    return pa.RecordBatch.from_arrays(arrays, schema=unified_schema)
