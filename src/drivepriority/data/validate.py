"""Validation, quality accounting, and disk-backed duplicate classification."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal
import hashlib
import json
from pathlib import Path
from typing import Any, Mapping, Sequence

import duckdb
import pyarrow as pa

from drivepriority.data import SCHEMA_VERSION
from drivepriority.data.schema import (
    ConversionIssue,
    HeaderAnalysis,
    INT64_MAX,
    parse_iso_date,
    parse_smart_integer,
    parse_unsigned_integer,
    raw_record_evidence,
)


class ValidationError(RuntimeError):
    """Raised when an archive cannot be published as a validated dataset."""


@dataclass
class QualityStats:
    """Mergeable, JSON-serializable processing and quality counters."""

    input_rows: int = 0
    output_rows: int = 0
    quarantined_rows: int = 0
    malformed_rows: int = 0
    exact_duplicates: int = 0
    conflicting_duplicates: int = 0
    serial_identity_collisions: int = 0
    non_null_by_field: dict[str, int] = field(default_factory=dict)
    zero_by_field: dict[str, int] = field(default_factory=dict)
    smart_absent_by_field: dict[str, int] = field(default_factory=dict)
    smart_blank_by_field: dict[str, int] = field(default_factory=dict)
    smart_invalid_by_field: dict[str, int] = field(default_factory=dict)
    smart_zero_by_field: dict[str, int] = field(default_factory=dict)
    smart_negative_by_field: dict[str, int] = field(default_factory=dict)
    smart_negative_by_quarter: dict[str, dict[str, int]] = field(default_factory=dict)
    smart_negative_by_member: dict[str, dict[str, int]] = field(default_factory=dict)
    smart_negative_by_model: dict[str, dict[str, int]] = field(default_factory=dict)
    extension_state_counts: dict[str, dict[str, int]] = field(default_factory=dict)
    extension_states_by_quarter: dict[str, dict[str, dict[str, int]]] = field(default_factory=dict)
    extension_states_by_member: dict[str, dict[str, dict[str, int]]] = field(default_factory=dict)
    extension_states_by_model: dict[str, dict[str, dict[str, int]]] = field(default_factory=dict)
    issue_codes: dict[str, int] = field(default_factory=dict)
    issue_severity_counts: dict[str, int] = field(default_factory=dict)
    issues_by_field: dict[str, int] = field(default_factory=dict)
    model_rows: dict[str, int] = field(default_factory=dict)
    model_smart_non_null: dict[str, dict[str, int]] = field(default_factory=dict)
    model_columns_present: dict[str, set[str]] = field(default_factory=dict)
    daily_source_rows: dict[str, int] = field(default_factory=dict)
    daily_failure_flags: dict[str, int] = field(default_factory=dict)
    failure_flag_counts: dict[str, int] = field(default_factory=dict)
    retained_failure_flag_counts: dict[str, int] = field(default_factory=dict)
    quarantined_failure_flag_counts: dict[str, int] = field(default_factory=dict)
    daily_source_failure_flag_counts: dict[str, dict[str, int]] = field(default_factory=dict)
    daily_retained_failure_flag_counts: dict[str, dict[str, int]] = field(default_factory=dict)
    daily_quarantined_failure_flag_counts: dict[str, dict[str, int]] = field(default_factory=dict)
    date_min: str | None = None
    date_max: str | None = None

    def record_issue(self, issue: ConversionIssue) -> None:
        self.issue_codes[issue.exception_code] = self.issue_codes.get(issue.exception_code, 0) + 1
        self.issue_severity_counts[issue.severity] = (
            self.issue_severity_counts.get(issue.severity, 0) + 1
        )
        if issue.column_name:
            self.issues_by_field[issue.column_name] = self.issues_by_field.get(issue.column_name, 0) + 1
        if issue.exception_code == "exact_duplicate":
            self.exact_duplicates += 1
        elif issue.exception_code == "conflicting_duplicate":
            self.conflicting_duplicates += 1
        elif issue.exception_code == "serial_identity_collision":
            self.serial_identity_collisions += 1
        if issue.exception_code == "invalid_smart_value" and issue.column_name:
            self.smart_invalid_by_field[issue.column_name] = (
                self.smart_invalid_by_field.get(issue.column_name, 0) + 1
            )

    @staticmethod
    def _increment_nested(
        target: dict[str, dict[str, int]], outer: str, inner: str, amount: int = 1
    ) -> None:
        counts = target.setdefault(outer, {})
        counts[inner] = counts.get(inner, 0) + amount

    @staticmethod
    def _increment_state_dimension(
        target: dict[str, dict[str, dict[str, int]]],
        dimension: str,
        field_name: str,
        state: str,
    ) -> None:
        states = target.setdefault(dimension, {}).setdefault(field_name, {})
        states[state] = states.get(state, 0) + 1

    def record_smart_state(
        self,
        field_name: str,
        state: str,
        *,
        source_quarter: str | None = None,
        source_file: str | None = None,
        model: str | None = None,
    ) -> None:
        if state == "blank":
            self.smart_blank_by_field[field_name] = self.smart_blank_by_field.get(field_name, 0) + 1
        elif state == "zero":
            self.smart_zero_by_field[field_name] = self.smart_zero_by_field.get(field_name, 0) + 1
        elif state == "negative":
            self.smart_negative_by_field[field_name] = (
                self.smart_negative_by_field.get(field_name, 0) + 1
            )
            if source_quarter is not None:
                self._increment_nested(
                    self.smart_negative_by_quarter, source_quarter, field_name
                )
            if source_file is not None:
                self._increment_nested(self.smart_negative_by_member, source_file, field_name)
            if model is not None:
                self._increment_nested(self.smart_negative_by_model, model, field_name)

    def record_extension_state(
        self,
        field_name: str,
        state: str,
        *,
        source_quarter: str,
        source_file: str,
        model: str,
    ) -> None:
        states = self.extension_state_counts.setdefault(field_name, {})
        states[state] = states.get(state, 0) + 1
        self._increment_state_dimension(
            self.extension_states_by_quarter, source_quarter, field_name, state
        )
        self._increment_state_dimension(
            self.extension_states_by_member, source_file, field_name, state
        )
        self._increment_state_dimension(
            self.extension_states_by_model, model, field_name, state
        )

    def record_failure_flag(
        self,
        flag: str,
        observation_date: date | None,
        *,
        disposition: str,
    ) -> None:
        if disposition == "source":
            counts = self.failure_flag_counts
            daily = self.daily_source_failure_flag_counts
        elif disposition == "retained":
            counts = self.retained_failure_flag_counts
            daily = self.daily_retained_failure_flag_counts
        elif disposition == "quarantined":
            counts = self.quarantined_failure_flag_counts
            daily = self.daily_quarantined_failure_flag_counts
        else:
            raise ValueError(f"Unknown failure-flag disposition: {disposition}")
        counts[flag] = counts.get(flag, 0) + 1
        if observation_date is not None:
            self._increment_nested(daily, observation_date.isoformat(), flag)
        if disposition == "source" and flag == "1" and observation_date is not None:
            key = observation_date.isoformat()
            self.daily_failure_flags[key] = self.daily_failure_flags.get(key, 0) + 1

    def observe_output(self, row: Mapping[str, Any], member_smart_columns: Sequence[str]) -> None:
        self.output_rows += 1
        observed_date = row["date"].isoformat()
        self.date_min = observed_date if self.date_min is None else min(self.date_min, observed_date)
        self.date_max = observed_date if self.date_max is None else max(self.date_max, observed_date)

        for name, value in row.items():
            if value is not None and name not in {"source_quarter", "source_file", "schema_version"}:
                self.non_null_by_field[name] = self.non_null_by_field.get(name, 0) + 1
            if isinstance(value, (int, Decimal)) and not isinstance(value, bool) and value == 0:
                self.zero_by_field[name] = self.zero_by_field.get(name, 0) + 1

        model = row.get("model") or "<MISSING>"
        self.model_rows[model] = self.model_rows.get(model, 0) + 1
        self.model_columns_present.setdefault(model, set()).update(member_smart_columns)
        non_null = self.model_smart_non_null.setdefault(model, {})
        for name in member_smart_columns:
            if row.get(name) is not None:
                non_null[name] = non_null.get(name, 0) + 1

    @staticmethod
    def _merge_counts(target: dict[str, int], source: Mapping[str, int]) -> None:
        for key, value in source.items():
            target[key] = target.get(key, 0) + int(value)

    def merge(self, other: "QualityStats") -> None:
        for name in (
            "input_rows",
            "output_rows",
            "quarantined_rows",
            "malformed_rows",
            "exact_duplicates",
            "conflicting_duplicates",
            "serial_identity_collisions",
        ):
            setattr(self, name, getattr(self, name) + getattr(other, name))
        for target_name in (
            "non_null_by_field",
            "zero_by_field",
            "smart_absent_by_field",
            "smart_blank_by_field",
            "smart_invalid_by_field",
            "smart_zero_by_field",
            "smart_negative_by_field",
            "issue_codes",
            "issue_severity_counts",
            "issues_by_field",
            "model_rows",
            "daily_source_rows",
            "daily_failure_flags",
            "failure_flag_counts",
            "retained_failure_flag_counts",
            "quarantined_failure_flag_counts",
        ):
            self._merge_counts(getattr(self, target_name), getattr(other, target_name))
        for target_name in (
            "smart_negative_by_quarter",
            "smart_negative_by_member",
            "smart_negative_by_model",
            "daily_source_failure_flag_counts",
            "daily_retained_failure_flag_counts",
            "daily_quarantined_failure_flag_counts",
        ):
            target = getattr(self, target_name)
            for outer, counts in getattr(other, target_name).items():
                self._merge_counts(target.setdefault(outer, {}), counts)
        for target_name in (
            "extension_states_by_quarter",
            "extension_states_by_member",
            "extension_states_by_model",
        ):
            target = getattr(self, target_name)
            for dimension, fields in getattr(other, target_name).items():
                for field_name, states in fields.items():
                    self._merge_counts(
                        target.setdefault(dimension, {}).setdefault(field_name, {}),
                        states,
                    )
        for field_name, states in other.extension_state_counts.items():
            self._merge_counts(self.extension_state_counts.setdefault(field_name, {}), states)
        for model, counts in other.model_smart_non_null.items():
            self._merge_counts(self.model_smart_non_null.setdefault(model, {}), counts)
        for model, columns in other.model_columns_present.items():
            self.model_columns_present.setdefault(model, set()).update(columns)
        if other.date_min is not None:
            self.date_min = other.date_min if self.date_min is None else min(self.date_min, other.date_min)
        if other.date_max is not None:
            self.date_max = other.date_max if self.date_max is None else max(self.date_max, other.date_max)

    def to_dict(self) -> dict[str, Any]:
        return {
            "input_rows": self.input_rows,
            "output_rows": self.output_rows,
            "quarantined_rows": self.quarantined_rows,
            "malformed_rows": self.malformed_rows,
            "exact_duplicates": self.exact_duplicates,
            "conflicting_duplicates": self.conflicting_duplicates,
            "serial_identity_collisions": self.serial_identity_collisions,
            "non_null_by_field": dict(sorted(self.non_null_by_field.items())),
            "zero_by_field": dict(sorted(self.zero_by_field.items())),
            "smart_absent_by_field": dict(sorted(self.smart_absent_by_field.items())),
            "smart_blank_by_field": dict(sorted(self.smart_blank_by_field.items())),
            "smart_invalid_by_field": dict(sorted(self.smart_invalid_by_field.items())),
            "smart_zero_by_field": dict(sorted(self.smart_zero_by_field.items())),
            "smart_negative_by_field": dict(sorted(self.smart_negative_by_field.items())),
            "smart_negative_by_quarter": self._sorted_nested(self.smart_negative_by_quarter),
            "smart_negative_by_member": self._sorted_nested(self.smart_negative_by_member),
            "smart_negative_by_model": self._sorted_nested(self.smart_negative_by_model),
            "extension_state_counts": self._sorted_nested(self.extension_state_counts),
            "extension_states_by_quarter": self._sorted_state_dimensions(
                self.extension_states_by_quarter
            ),
            "extension_states_by_member": self._sorted_state_dimensions(
                self.extension_states_by_member
            ),
            "extension_states_by_model": self._sorted_state_dimensions(
                self.extension_states_by_model
            ),
            "issue_codes": dict(sorted(self.issue_codes.items())),
            "issue_severity_counts": dict(sorted(self.issue_severity_counts.items())),
            "issues_by_field": dict(sorted(self.issues_by_field.items())),
            "model_rows": dict(sorted(self.model_rows.items())),
            "model_smart_non_null": {
                model: dict(sorted(values.items()))
                for model, values in sorted(self.model_smart_non_null.items())
            },
            "model_columns_present": {
                model: sorted(columns) for model, columns in sorted(self.model_columns_present.items())
            },
            "daily_source_rows": dict(sorted(self.daily_source_rows.items())),
            "daily_failure_flags": dict(sorted(self.daily_failure_flags.items())),
            "failure_flag_counts": dict(sorted(self.failure_flag_counts.items())),
            "retained_failure_flag_counts": dict(
                sorted(self.retained_failure_flag_counts.items())
            ),
            "quarantined_failure_flag_counts": dict(
                sorted(self.quarantined_failure_flag_counts.items())
            ),
            "daily_source_failure_flag_counts": self._sorted_nested(
                self.daily_source_failure_flag_counts
            ),
            "daily_retained_failure_flag_counts": self._sorted_nested(
                self.daily_retained_failure_flag_counts
            ),
            "daily_quarantined_failure_flag_counts": self._sorted_nested(
                self.daily_quarantined_failure_flag_counts
            ),
            "date_min": self.date_min,
            "date_max": self.date_max,
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> "QualityStats":
        instance = cls(
            input_rows=int(payload.get("input_rows", 0)),
            output_rows=int(payload.get("output_rows", 0)),
            quarantined_rows=int(payload.get("quarantined_rows", 0)),
            malformed_rows=int(payload.get("malformed_rows", 0)),
            exact_duplicates=int(payload.get("exact_duplicates", 0)),
            conflicting_duplicates=int(payload.get("conflicting_duplicates", 0)),
            serial_identity_collisions=int(payload.get("serial_identity_collisions", 0)),
            non_null_by_field=dict(payload.get("non_null_by_field", {})),
            zero_by_field=dict(payload.get("zero_by_field", {})),
            smart_absent_by_field=dict(payload.get("smart_absent_by_field", {})),
            smart_blank_by_field=dict(payload.get("smart_blank_by_field", {})),
            smart_invalid_by_field=dict(payload.get("smart_invalid_by_field", {})),
            smart_zero_by_field=dict(payload.get("smart_zero_by_field", {})),
            smart_negative_by_field=dict(payload.get("smart_negative_by_field", {})),
            smart_negative_by_quarter={
                key: dict(values)
                for key, values in payload.get("smart_negative_by_quarter", {}).items()
            },
            smart_negative_by_member={
                key: dict(values)
                for key, values in payload.get("smart_negative_by_member", {}).items()
            },
            smart_negative_by_model={
                key: dict(values)
                for key, values in payload.get("smart_negative_by_model", {}).items()
            },
            extension_state_counts={
                key: dict(values)
                for key, values in payload.get("extension_state_counts", {}).items()
            },
            extension_states_by_quarter=cls._load_state_dimensions(
                payload.get("extension_states_by_quarter", {})
            ),
            extension_states_by_member=cls._load_state_dimensions(
                payload.get("extension_states_by_member", {})
            ),
            extension_states_by_model=cls._load_state_dimensions(
                payload.get("extension_states_by_model", {})
            ),
            issue_codes=dict(payload.get("issue_codes", {})),
            issue_severity_counts=dict(payload.get("issue_severity_counts", {})),
            issues_by_field=dict(payload.get("issues_by_field", {})),
            model_rows=dict(payload.get("model_rows", {})),
            model_smart_non_null={
                model: dict(values) for model, values in payload.get("model_smart_non_null", {}).items()
            },
            model_columns_present={
                model: set(values) for model, values in payload.get("model_columns_present", {}).items()
            },
            daily_source_rows=dict(payload.get("daily_source_rows", {})),
            daily_failure_flags=dict(payload.get("daily_failure_flags", {})),
            failure_flag_counts=dict(payload.get("failure_flag_counts", {})),
            retained_failure_flag_counts=dict(
                payload.get("retained_failure_flag_counts", {})
            ),
            quarantined_failure_flag_counts=dict(
                payload.get("quarantined_failure_flag_counts", {})
            ),
            daily_source_failure_flag_counts={
                key: dict(values)
                for key, values in payload.get("daily_source_failure_flag_counts", {}).items()
            },
            daily_retained_failure_flag_counts={
                key: dict(values)
                for key, values in payload.get("daily_retained_failure_flag_counts", {}).items()
            },
            daily_quarantined_failure_flag_counts={
                key: dict(values)
                for key, values in payload.get(
                    "daily_quarantined_failure_flag_counts", {}
                ).items()
            },
            date_min=payload.get("date_min"),
            date_max=payload.get("date_max"),
        )
        return instance

    @staticmethod
    def _sorted_nested(
        payload: Mapping[str, Mapping[str, int]]
    ) -> dict[str, dict[str, int]]:
        return {
            outer: dict(sorted(counts.items()))
            for outer, counts in sorted(payload.items())
        }

    @staticmethod
    def _sorted_state_dimensions(
        payload: Mapping[str, Mapping[str, Mapping[str, int]]]
    ) -> dict[str, dict[str, dict[str, int]]]:
        return {
            dimension: {
                field_name: dict(sorted(states.items()))
                for field_name, states in sorted(fields.items())
            }
            for dimension, fields in sorted(payload.items())
        }

    @staticmethod
    def _load_state_dimensions(
        payload: Mapping[str, Mapping[str, Mapping[str, int]]]
    ) -> dict[str, dict[str, dict[str, int]]]:
        return {
            dimension: {
                field_name: dict(states) for field_name, states in fields.items()
            }
            for dimension, fields in payload.items()
        }


def _is_missing(token: str | None, missing_tokens: set[str]) -> bool:
    return token is None or token.strip() in missing_tokens


def convert_record(
    values: Sequence[str],
    analysis: HeaderAnalysis,
    *,
    source_quarter: str,
    source_file: str,
    source_record_number: int,
    source_line_number: int,
    quarter_start: date,
    quarter_end: date,
    missing_tokens: set[str],
) -> tuple[dict[str, Any] | None, list[ConversionIssue], dict[str, str]]:
    """Convert one source row and retain exact evidence for every unsafe value."""

    if len(values) != len(analysis.original_headers):
        raise ValueError("record length must match the analyzed header")

    index = analysis.index_by_canonical
    raw_json, raw_sha = raw_record_evidence(analysis.original_headers, values)

    def token(name: str) -> str | None:
        position = index.get(name)
        return values[position] if position is not None else None

    def original_header(name: str) -> str | None:
        position = index.get(name)
        return analysis.original_headers[position] if position is not None else None

    serial_token = token("serial_number")
    date_token = token("date")
    issue_specs: list[dict[str, Any]] = []
    critical = False

    def add_issue(
        code: str,
        column: str | None,
        raw_value: str | None,
        message: str,
        *,
        severity: str,
        quarantines: bool = False,
    ) -> None:
        nonlocal critical
        critical = critical or quarantines
        issue_specs.append(
            {
                "column_name": column,
                "original_header": original_header(column) if column else None,
                "raw_value": raw_value,
                "exception_code": code,
                "severity": severity,
                "message": message,
            }
        )

    parsed_date: date | None = None
    if _is_missing(date_token, missing_tokens):
        add_issue("missing_date", "date", date_token, "Observation date is missing", severity="error", quarantines=True)
    else:
        try:
            parsed_date = parse_iso_date(date_token or "")
            if not quarter_start <= parsed_date <= quarter_end:
                add_issue(
                    "date_outside_assigned_quarter",
                    "date",
                    date_token,
                    f"Date is outside {source_quarter}",
                    severity="error",
                    quarantines=True,
                )
        except ValueError as exc:
            add_issue("invalid_date", "date", date_token, str(exc), severity="error", quarantines=True)

    serial: str | None = serial_token
    if serial_token is None or not serial_token.strip():
        serial = None
        add_issue(
            "missing_serial_number",
            "serial_number",
            serial_token,
            "Stable drive identifier is missing",
            severity="error",
            quarantines=True,
        )

    model_token = token("model")
    model = None if _is_missing(model_token, missing_tokens) else model_token
    if model is None:
        add_issue("missing_model", "model", model_token, "Hardware model is missing", severity="warning")

    manufacturer_token = token("manufacturer")
    manufacturer = None if _is_missing(manufacturer_token, missing_tokens) else manufacturer_token

    capacity_token = token("capacity_bytes")
    capacity: int | None = None
    if not _is_missing(capacity_token, missing_tokens):
        try:
            capacity = parse_unsigned_integer(capacity_token or "", INT64_MAX)
        except (ValueError, OverflowError) as exc:
            add_issue(
                "invalid_capacity_bytes",
                "capacity_bytes",
                capacity_token,
                str(exc),
                severity="warning",
            )

    failure_token = token("failure")
    failure: int | None = None
    if _is_missing(failure_token, missing_tokens):
        add_issue(
            "missing_failure_flag",
            "failure",
            failure_token,
            "Original failure flag is missing",
            severity="error",
            quarantines=True,
        )
    elif failure_token is not None and failure_token.strip() in {"0", "1"}:
        failure = int(failure_token.strip())
    else:
        add_issue(
            "invalid_failure_flag",
            "failure",
            failure_token,
            "Failure flag must be the literal integer 0 or 1",
            severity="error",
            quarantines=True,
        )

    converted_smart: dict[str, Decimal | None] = {}
    smart_states: dict[str, str] = {}
    for name in analysis.smart_columns:
        value_token = token(name)
        if _is_missing(value_token, missing_tokens):
            converted_smart[name] = None
            smart_states[name] = "blank"
            continue
        try:
            converted_smart[name] = parse_smart_integer(value_token or "")
            if converted_smart[name] == 0:
                smart_states[name] = "zero"
            elif converted_smart[name] < 0:
                smart_states[name] = "negative"
                add_issue(
                    "suspicious_negative_smart_value",
                    name,
                    value_token,
                    "Negative SMART value is preserved exactly but requires scientific review",
                    severity="warning",
                )
            else:
                smart_states[name] = "value"
        except (ValueError, OverflowError) as exc:
            converted_smart[name] = None
            smart_states[name] = "invalid"
            add_issue("invalid_smart_value", name, value_token, str(exc), severity="warning")

    string_extensions: dict[str, str | None] = {}
    for name in analysis.unexpected_smart_columns:
        value_token = token(name)
        if _is_missing(value_token, missing_tokens):
            string_extensions[name] = None
            smart_states[name] = "blank"
        else:
            string_extensions[name] = value_token
            smart_states[name] = "zero" if value_token and value_token.strip() == "0" else "value"

    disposition = "quarantined" if critical else "retained"
    issues = [
        ConversionIssue(
            source_quarter=source_quarter,
            source_file=source_file,
            source_record_number=source_record_number,
            source_line_number=source_line_number,
            serial_number=serial_token,
            date_token=date_token,
            row_disposition=disposition,
            raw_record_json=raw_json,
            raw_record_sha256=raw_sha,
            **spec,
        )
        for spec in issue_specs
    ]

    if critical:
        return None, issues, smart_states

    assert parsed_date is not None and serial is not None and failure is not None
    output: dict[str, Any] = {
        "date": parsed_date,
        "serial_number": serial,
        "model": model,
        "manufacturer": manufacturer,
        "capacity_bytes": capacity,
        "failure": failure,
        **converted_smart,
        **string_extensions,
        "source_quarter": source_quarter,
        "source_file": source_file,
        "schema_version": SCHEMA_VERSION,
    }
    return output, issues, smart_states


def canonical_fingerprint(row: Mapping[str, Any], fields: Sequence[str]) -> tuple[str, str]:
    payload = []
    for field_name in fields:
        value = row.get(field_name)
        if isinstance(value, date):
            payload.append(value.isoformat())
        elif isinstance(value, Decimal):
            payload.append(str(value))
        else:
            payload.append(value)
    serialized = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
    return serialized, hashlib.sha256(serialized.encode("utf-8")).hexdigest()


class DuplicateTracker:
    """Disk-backed duplicate detection with member-level transactional checkpoints."""

    def __init__(self, database_path: Path, *, memory_limit: str = "1GB", threads: int = 2) -> None:
        self.database_path = database_path
        self.connection = duckdb.connect(str(database_path))
        self.connection.execute(f"SET memory_limit = '{memory_limit}'")
        self.connection.execute(f"SET threads = {max(1, int(threads))}")
        self.connection.execute(
            """
            CREATE TABLE IF NOT EXISTS seen_records (
                observation_date DATE NOT NULL,
                serial_number VARCHAR NOT NULL,
                identity_signature VARCHAR NOT NULL,
                fingerprint VARCHAR NOT NULL,
                source_file VARCHAR NOT NULL,
                source_record_number UBIGINT NOT NULL,
                PRIMARY KEY (observation_date, serial_number)
            )
            """
        )
        self.connection.execute(
            """
            CREATE TABLE IF NOT EXISTS serial_identities (
                serial_number VARCHAR NOT NULL,
                identity_signature VARCHAR NOT NULL,
                first_observation_date DATE NOT NULL,
                last_observation_date DATE NOT NULL,
                first_source_file VARCHAR NOT NULL,
                first_source_record_number UBIGINT NOT NULL,
                PRIMARY KEY (serial_number, identity_signature)
            )
            """
        )
        self.connection.execute(
            """
            CREATE TABLE IF NOT EXISTS completed_members (
                member_index INTEGER PRIMARY KEY,
                stats_json VARCHAR NOT NULL,
                files_json VARCHAR NOT NULL
            )
            """
        )
        self._transaction_open = False

    def begin_member(self) -> None:
        if self._transaction_open:
            raise RuntimeError("duplicate tracker transaction is already open")
        self.connection.execute("BEGIN TRANSACTION")
        self._transaction_open = True

    def classify(
        self,
        entries: Sequence[tuple[Mapping[str, Any], int, int]],
        fingerprint_fields: Sequence[str],
    ) -> list[ConversionIssue]:
        if not entries:
            return []
        if not self._transaction_open:
            raise RuntimeError("begin_member must be called before duplicate classification")

        dates: list[date] = []
        serials: list[str] = []
        fingerprints: list[str] = []
        identity_signatures: list[str] = []
        source_files: list[str] = []
        record_numbers: list[int] = []
        payloads: list[str] = []
        quarters: list[str] = []
        for row, record_number, _line_number in entries:
            canonical_payload, canonical_hash = canonical_fingerprint(row, fingerprint_fields)
            _identity_payload, fallback_identity = canonical_fingerprint(
                row, ("model", "manufacturer", "capacity_bytes")
            )
            payload = str(row.get("_source_raw_record_json", canonical_payload))
            fingerprint = str(row.get("_source_raw_record_sha256", canonical_hash))
            identity_signature = str(row.get("_source_identity_signature", fallback_identity))
            dates.append(row["date"])
            serials.append(row["serial_number"])
            fingerprints.append(fingerprint)
            identity_signatures.append(identity_signature)
            source_files.append(row["source_file"])
            record_numbers.append(record_number)
            payloads.append(payload)
            quarters.append(row["source_quarter"])

        batch = pa.table(
            {
                "batch_ordinal": pa.array(range(len(entries)), type=pa.uint64()),
                "observation_date": pa.array(dates, type=pa.date32()),
                "serial_number": pa.array(serials, type=pa.string()),
                "fingerprint": pa.array(fingerprints, type=pa.string()),
                "identity_signature": pa.array(identity_signatures, type=pa.string()),
                "source_file": pa.array(source_files, type=pa.string()),
                "source_record_number": pa.array(record_numbers, type=pa.uint64()),
            }
        )
        self.connection.register("_drivepriority_batch", batch)
        try:
            duplicate_rows = self.connection.execute(
                """
                WITH ranked AS (
                    SELECT *,
                        row_number() OVER (
                            PARTITION BY observation_date, serial_number ORDER BY batch_ordinal
                        ) AS occurrence,
                        first_value(fingerprint) OVER (
                            PARTITION BY observation_date, serial_number ORDER BY batch_ordinal
                        ) AS batch_first_fingerprint,
                        first_value(identity_signature) OVER (
                            PARTITION BY observation_date, serial_number ORDER BY batch_ordinal
                        ) AS batch_first_identity,
                        first_value(source_file) OVER (
                            PARTITION BY observation_date, serial_number ORDER BY batch_ordinal
                        ) AS batch_first_file,
                        first_value(source_record_number) OVER (
                            PARTITION BY observation_date, serial_number ORDER BY batch_ordinal
                        ) AS batch_first_record
                    FROM _drivepriority_batch
                ), compared AS (
                    SELECT ranked.*,
                        COALESCE(
                            seen_records.fingerprint,
                            CASE WHEN occurrence > 1 THEN batch_first_fingerprint END
                        ) AS prior_fingerprint,
                        COALESCE(
                            seen_records.identity_signature,
                            CASE WHEN occurrence > 1 THEN batch_first_identity END
                        ) AS prior_identity,
                        COALESCE(
                            seen_records.source_file,
                            CASE WHEN occurrence > 1 THEN batch_first_file END
                        ) AS prior_file,
                        COALESCE(
                            seen_records.source_record_number,
                            CASE WHEN occurrence > 1 THEN batch_first_record END
                        ) AS prior_record
                    FROM ranked
                    LEFT JOIN seen_records USING (observation_date, serial_number)
                )
                SELECT batch_ordinal, prior_fingerprint, prior_identity, prior_file, prior_record
                FROM compared
                WHERE prior_fingerprint IS NOT NULL
                ORDER BY batch_ordinal
                """
            ).fetchall()

            identity_collision_rows = self.connection.execute(
                """
                WITH candidates AS (
                    SELECT batch.batch_ordinal,
                           history.first_observation_date AS first_seen_date,
                           history.last_observation_date AS last_seen_date,
                           history.first_source_file,
                           history.first_source_record_number
                    FROM _drivepriority_batch AS batch
                    JOIN serial_identities AS history
                      ON history.serial_number = batch.serial_number
                     AND history.identity_signature <> batch.identity_signature
                    UNION ALL
                    SELECT current.batch_ordinal,
                           prior.observation_date AS first_seen_date,
                           prior.observation_date AS last_seen_date,
                           prior.source_file AS first_source_file,
                           prior.source_record_number AS first_source_record_number
                    FROM _drivepriority_batch AS current
                    JOIN _drivepriority_batch AS prior
                      ON prior.serial_number = current.serial_number
                     AND prior.identity_signature <> current.identity_signature
                     AND prior.batch_ordinal < current.batch_ordinal
                ), ranked AS (
                    SELECT *, row_number() OVER (
                        PARTITION BY batch_ordinal
                        ORDER BY first_seen_date, first_source_file,
                                 first_source_record_number
                    ) AS first_rank
                    FROM candidates
                )
                SELECT batch_ordinal,
                       min(first_seen_date) AS first_seen_date,
                       max(last_seen_date) AS last_seen_date,
                       max(CASE WHEN first_rank = 1 THEN first_source_file END)
                           AS first_source_file,
                       max(CASE WHEN first_rank = 1 THEN first_source_record_number END)
                           AS first_source_record_number
                FROM ranked
                GROUP BY batch_ordinal
                ORDER BY batch_ordinal
                """
            ).fetchall()

            self.connection.execute(
                """
                INSERT INTO seen_records
                WITH ranked AS (
                    SELECT *, row_number() OVER (
                        PARTITION BY observation_date, serial_number ORDER BY batch_ordinal
                    ) AS occurrence
                    FROM _drivepriority_batch
                )
                SELECT observation_date, serial_number, identity_signature, fingerprint,
                       source_file, source_record_number
                FROM ranked
                WHERE occurrence = 1
                  AND NOT EXISTS (
                    SELECT 1 FROM seen_records
                    WHERE seen_records.observation_date = ranked.observation_date
                      AND seen_records.serial_number = ranked.serial_number
                  )
                """
            )

            self.connection.execute(
                """
                INSERT INTO serial_identities
                WITH ranked AS (
                    SELECT *,
                        row_number() OVER (
                            PARTITION BY serial_number, identity_signature
                            ORDER BY observation_date, batch_ordinal
                        ) AS first_rank,
                        min(observation_date) OVER (
                            PARTITION BY serial_number, identity_signature
                        ) AS first_observation_date,
                        max(observation_date) OVER (
                            PARTITION BY serial_number, identity_signature
                        ) AS last_observation_date
                    FROM _drivepriority_batch
                )
                SELECT serial_number, identity_signature,
                       first_observation_date, last_observation_date,
                       max(CASE WHEN first_rank = 1 THEN source_file END),
                       max(CASE WHEN first_rank = 1 THEN source_record_number END)
                FROM ranked
                GROUP BY serial_number, identity_signature,
                         first_observation_date, last_observation_date
                ON CONFLICT (serial_number, identity_signature) DO UPDATE SET
                    first_source_file = CASE
                        WHEN excluded.first_observation_date < serial_identities.first_observation_date
                        THEN excluded.first_source_file
                        ELSE serial_identities.first_source_file
                    END,
                    first_source_record_number = CASE
                        WHEN excluded.first_observation_date < serial_identities.first_observation_date
                        THEN excluded.first_source_record_number
                        ELSE serial_identities.first_source_record_number
                    END,
                    first_observation_date = least(
                        serial_identities.first_observation_date,
                        excluded.first_observation_date
                    ),
                    last_observation_date = greatest(
                        serial_identities.last_observation_date,
                        excluded.last_observation_date
                    )
                """
            )
        finally:
            self.connection.unregister("_drivepriority_batch")

        duplicate_by_ordinal = {
            int(ordinal): (prior_fingerprint, prior_identity, prior_file, int(prior_record))
            for ordinal, prior_fingerprint, prior_identity, prior_file, prior_record in duplicate_rows
        }
        collision_by_ordinal = {
            int(ordinal): (first_date, last_date, first_file, int(first_record))
            for ordinal, first_date, last_date, first_file, first_record in identity_collision_rows
        }
        issues: list[ConversionIssue] = []
        for ordinal in sorted(set(duplicate_by_ordinal) | set(collision_by_ordinal)):
            row, record_number, line_number = entries[ordinal]
            current_date = dates[ordinal]
            duplicate = duplicate_by_ordinal.get(ordinal)
            duplicate_identity_collision = bool(
                duplicate and identity_signatures[ordinal] != duplicate[1]
            )
            if duplicate and not duplicate_identity_collision:
                prior_fingerprint, _prior_identity, prior_file, prior_record = duplicate
                exact = fingerprints[ordinal] == prior_fingerprint
                issues.append(
                    ConversionIssue(
                        source_quarter=quarters[ordinal],
                        source_file=source_files[ordinal],
                        source_record_number=record_number,
                        source_line_number=line_number,
                        serial_number=serials[ordinal],
                        date_token=current_date.isoformat(),
                        column_name=None,
                        original_header=None,
                        raw_value=None,
                        exception_code=(
                            "exact_duplicate" if exact else "conflicting_duplicate"
                        ),
                        severity="warning" if exact else "error",
                        row_disposition="retained",
                        message=(
                            "Drive-date record exactly duplicates the first observed record"
                            if exact
                            else "Drive-date measurements conflict with the first observed record"
                        ),
                        raw_record_json=payloads[ordinal],
                        raw_record_sha256=fingerprints[ordinal],
                        first_seen_source_file=prior_file,
                        first_seen_record_number=prior_record,
                        first_seen_date=current_date.isoformat(),
                        last_seen_date=current_date.isoformat(),
                    )
                )

            collision = collision_by_ordinal.get(ordinal)
            if collision:
                first_date, prior_last_date, first_file, first_record = collision
                issues.append(
                    ConversionIssue(
                        source_quarter=quarters[ordinal],
                        source_file=source_files[ordinal],
                        source_record_number=record_number,
                        source_line_number=line_number,
                        serial_number=serials[ordinal],
                        date_token=current_date.isoformat(),
                        column_name=None,
                        original_header=None,
                        raw_value=None,
                        exception_code="serial_identity_collision",
                        severity="error",
                        row_disposition="retained",
                        message=(
                            "The same original serial occurs within the quarter with different "
                            "model, supplied manufacturer, or capacity evidence; all records are "
                            "preserved and final device identity must be resolved centrally"
                        ),
                        raw_record_json=payloads[ordinal],
                        raw_record_sha256=fingerprints[ordinal],
                        first_seen_source_file=first_file,
                        first_seen_record_number=first_record,
                        first_seen_date=min(first_date, current_date).isoformat(),
                        last_seen_date=max(prior_last_date, current_date).isoformat(),
                    )
                )
        return issues

    def complete_member(self, member_index: int, stats: QualityStats, files: Mapping[str, Any]) -> None:
        if not self._transaction_open:
            raise RuntimeError("no member transaction is open")
        self.connection.execute(
            "INSERT INTO completed_members VALUES (?, ?, ?)",
            [
                member_index,
                json.dumps(stats.to_dict(), sort_keys=True, separators=(",", ":")),
                json.dumps(files, sort_keys=True, separators=(",", ":")),
            ],
        )
        self.connection.execute("COMMIT")
        self._transaction_open = False

    def rollback_member(self) -> None:
        if self._transaction_open:
            self.connection.execute("ROLLBACK")
            self._transaction_open = False

    def completed_members(self) -> dict[int, dict[str, Any]]:
        rows = self.connection.execute(
            "SELECT member_index, stats_json, files_json FROM completed_members ORDER BY member_index"
        ).fetchall()
        return {
            int(index): {"stats": json.loads(stats_json), "files": json.loads(files_json)}
            for index, stats_json, files_json in rows
        }

    def close(self) -> None:
        self.rollback_member()
        self.connection.close()


def validate_accounting(stats: QualityStats) -> dict[str, Any]:
    expected = stats.output_rows + stats.quarantined_rows
    return {
        "name": "input_row_accounting",
        "passed": stats.input_rows == expected,
        "input_rows": stats.input_rows,
        "output_rows": stats.output_rows,
        "quarantined_rows": stats.quarantined_rows,
        "expected_input_rows": expected,
    }
