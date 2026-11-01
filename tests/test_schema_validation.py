from __future__ import annotations

from datetime import date
from decimal import Decimal

import pyarrow as pa
import pytest

from drivepriority.data.schema import (
    SMART_DECIMAL_MAX,
    SMART_DECIMAL_MIN,
    SchemaError,
    analyze_headers,
    canonical_arrow_schema,
    exception_arrow_schema,
    parse_smart_integer,
    union_smart_columns,
)
from drivepriority.data.validate import (
    DuplicateTracker,
    QualityStats,
    convert_record,
    validate_accounting,
)


BASE_HEADERS = [
    "date",
    "serial_number",
    "model",
    "manufacturer",
    "capacity_bytes",
    "failure",
]


def convert(headers: list[str], values: list[str]):
    return convert_record(
        values,
        analyze_headers(headers),
        source_quarter="2016Q1",
        source_file="2016-01-01.csv",
        source_record_number=1,
        source_line_number=2,
        quarter_start=date(2016, 1, 1),
        quarter_end=date(2016, 3, 31),
        missing_tokens={""},
    )


def test_header_analysis_discovers_all_smart_columns_and_extensions() -> None:
    analysis = analyze_headers(
        [*BASE_HEADERS, "SMART 198 Raw", "smart_5_normalized", "smart_vendor_blob", "extra"]
    )

    assert analysis.smart_columns == ("smart_5_normalized", "smart_198_raw")
    assert analysis.unexpected_smart_columns == ("smart_vendor_blob",)
    assert analysis.unknown_columns == ("extra",)
    assert analysis.missing_required_columns == ()
    assert len(analysis.signature) == 64


def test_header_normalization_collision_is_rejected() -> None:
    with pytest.raises(SchemaError, match="collision"):
        analyze_headers([*BASE_HEADERS, "SMART 5 RAW", "smart_5_raw"])


def test_schema_union_and_decimal_contract_are_deterministic() -> None:
    first = analyze_headers([*BASE_HEADERS, "smart_198_raw", "smart_5_raw"])
    second = analyze_headers([*BASE_HEADERS, "smart_9_normalized", "smart_vendor_blob"])
    smart, extensions = union_smart_columns([first, second])
    schema = canonical_arrow_schema(smart, extensions)

    assert smart == ("smart_5_raw", "smart_9_normalized", "smart_198_raw")
    assert extensions == ("smart_vendor_blob",)
    assert schema.field("smart_198_raw").type == pa.decimal128(38, 0)
    assert schema.field("manufacturer").type == pa.string()
    assert schema.names[-3:] == ["source_quarter", "source_file", "schema_version"]
    assert exception_arrow_schema().field("row_evidence_id").type == pa.string()


@pytest.mark.parametrize(
    ("token", "expected"),
    [
        ("0", Decimal(0)),
        ("-1", Decimal(-1)),
        ("+7", Decimal(7)),
        (str(SMART_DECIMAL_MAX), Decimal(SMART_DECIMAL_MAX)),
        (str(SMART_DECIMAL_MIN), Decimal(SMART_DECIMAL_MIN)),
    ],
)
def test_exact_smart_integer_boundaries(token: str, expected: Decimal) -> None:
    assert parse_smart_integer(token) == expected


@pytest.mark.parametrize("token", ["1.0", "1e6", "NaN", str(10**38), str(-(10**38))])
def test_unsafe_smart_tokens_are_rejected_without_rounding(token: str) -> None:
    with pytest.raises((ValueError, OverflowError)):
        parse_smart_integer(token)


def test_conversion_distinguishes_blank_zero_negative_and_invalid() -> None:
    headers = [
        *BASE_HEADERS,
        "smart_5_raw",
        "smart_9_raw",
        "smart_198_raw",
        "smart_200_raw",
    ]
    row, issues, states = convert(
        headers,
        ["2016-01-01", "SN-1", "Model A", "Vendor", "1000", "1", "", "0", "-4", "1.5"],
    )

    assert row is not None
    assert row["failure"] == 1
    assert row["manufacturer"] == "Vendor"
    assert row["smart_5_raw"] is None
    assert row["smart_9_raw"] == Decimal(0)
    assert row["smart_198_raw"] == Decimal(-4)
    assert row["smart_200_raw"] is None
    assert states == {
        "smart_5_raw": "blank",
        "smart_9_raw": "zero",
        "smart_198_raw": "negative",
        "smart_200_raw": "invalid",
    }
    assert [(issue.exception_code, issue.raw_value) for issue in issues] == [
        ("suspicious_negative_smart_value", "-4"),
        ("invalid_smart_value", "1.5")
    ]
    assert issues[0].raw_record_json is not None
    assert issues[0].raw_record_sha256 is not None


def test_invalid_capacity_is_nullable_and_traceable_without_losing_row() -> None:
    row, issues, _states = convert(
        [*BASE_HEADERS, "smart_5_raw"],
        ["2016-01-02", "SN-2", "Model B", "Vendor", str(2**63), "0", "3"],
    )

    assert row is not None
    assert row["capacity_bytes"] is None
    assert issues[0].exception_code == "invalid_capacity_bytes"
    assert issues[0].raw_value == str(2**63)
    assert issues[0].row_disposition == "retained"


@pytest.mark.parametrize(
    ("field_index", "replacement", "code"),
    [
        (0, "2016/01/02", "invalid_date"),
        (1, "", "missing_serial_number"),
        (5, "2", "invalid_failure_flag"),
    ],
)
def test_invalid_identity_date_or_failure_quarantines_complete_record(
    field_index: int, replacement: str, code: str
) -> None:
    values = ["2016-01-02", "SN-2", "Model B", "Vendor", "1000", "0"]
    values[field_index] = replacement
    row, issues, _states = convert(BASE_HEADERS, values)

    assert row is None
    assert code in {issue.exception_code for issue in issues}
    assert all(issue.row_disposition == "quarantined" for issue in issues)
    matching = next(issue for issue in issues if issue.exception_code == code)
    assert matching.raw_record_json is not None


def _duplicate_row(**changes):
    row = {
        "date": date(2016, 1, 1),
        "serial_number": "SN-1",
        "model": "Model A",
        "manufacturer": "Vendor",
        "capacity_bytes": 1000,
        "failure": 0,
        "smart_5_raw": Decimal(1),
        "source_quarter": "2016Q1",
        "source_file": "a.csv",
        "schema_version": "1.0.0",
    }
    row.update(changes)
    return row


def test_duplicate_tracker_separates_exact_conflict_and_identity_collision(tmp_path) -> None:
    tracker = DuplicateTracker(tmp_path / "state.duckdb")
    tracker.begin_member()
    first = _duplicate_row()
    exact = _duplicate_row(source_file="b.csv")
    conflict = _duplicate_row(source_file="c.csv", smart_5_raw=Decimal(2))
    collision = _duplicate_row(source_file="d.csv", model="Model B")
    fields = [
        "date",
        "serial_number",
        "model",
        "manufacturer",
        "capacity_bytes",
        "failure",
        "smart_5_raw",
    ]

    issues = tracker.classify(
        [(first, 1, 2), (exact, 1, 2), (conflict, 1, 2), (collision, 1, 2)], fields
    )
    stats = QualityStats(input_rows=4, output_rows=4)
    tracker.complete_member(0, stats, {"relative_paths": []})
    tracker.close()

    assert [issue.exception_code for issue in issues] == [
        "exact_duplicate",
        "conflicting_duplicate",
        "serial_identity_collision",
    ]
    assert issues[-1].first_seen_source_file == "a.csv"


def test_quality_stats_merge_preserves_source_state_categories() -> None:
    first = QualityStats(input_rows=2, output_rows=2)
    first.smart_absent_by_field["smart_5_raw"] = 2
    first.record_smart_state("smart_9_raw", "blank")
    second = QualityStats(input_rows=1, quarantined_rows=1)
    second.record_smart_state("smart_9_raw", "zero")

    first.merge(second)

    assert first.input_rows == 3
    assert first.smart_absent_by_field == {"smart_5_raw": 2}
    assert first.smart_blank_by_field == {"smart_9_raw": 1}
    assert first.smart_zero_by_field == {"smart_9_raw": 1}
    assert validate_accounting(first)["passed"]
