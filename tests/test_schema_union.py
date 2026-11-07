"""Step 3 metadata tests are dependency-free; Arrow behavior tested if pyarrow installed."""
import pytest
from drivepriority.integration.schema_union import (SchemaUnionError, plan_union, arrow_plan, align_batch)

BASE = [
    {"name": "date", "arrow_type": "date32[day]", "nullable": False},
    {"name": "serial_number", "arrow_type": "string", "nullable": False},
    {"name": "failure", "arrow_type": "uint8", "nullable": False},
]
SMART = {"name": "smart_5_raw", "arrow_type": "decimal128(38, 0)", "nullable": True, "decimal_precision": 38, "decimal_scale": 0}

def test_union_order_independent():
    left = plan_union({"2016Q1": BASE, "2024Q2": BASE + [SMART]})
    right = plan_union({"2024Q2": BASE + [SMART], "2016Q1": BASE})
    assert left == right
    assert list(left.absent_columns_by_quarter["2016Q1"]) == ["smart_5_raw"]
    assert not left.absent_columns_by_quarter["2024Q2"]

def test_preserves_negative_smart_descriptor():
    plan = plan_union({"2016Q1": BASE+[SMART]})
    assert plan.fields[-1]["arrow_type"] == "decimal128(38, 0)"

def test_missing_required_rejected():
    with pytest.raises(SchemaUnionError, match="missing required"):
        plan_union({"2016Q1": BASE[:2]})

def test_required_nullable_rejected():
    with pytest.raises(SchemaUnionError, match="incorrect required"):
        plan_union({"2016Q1": [dict(BASE[0], nullable=True)]+BASE[1:]})

def test_wrong_smart_type_rejected():
    with pytest.raises(SchemaUnionError, match="unsafe SMART"):
        plan_union({"2016Q1": BASE+[dict(SMART,arrow_type="double")]})

def test_wrong_smart_scale_rejected():
    with pytest.raises(SchemaUnionError, match="unsafe SMART"):
        plan_union({"2016Q1": BASE+[dict(SMART,decimal_scale=2)]})

def test_conflicting_columns_rejected():
    with pytest.raises(SchemaUnionError, match="incompatible schema"):
        plan_union({"2016Q1": BASE+[SMART], "2024Q2": BASE+[dict(SMART,nullable=False)]})

def test_duplicate_rejected():
    with pytest.raises(SchemaUnionError, match="duplicate"):
        plan_union({"2016Q1": BASE+[BASE[1]]})

def test_invalid_quarter_rejected():
    with pytest.raises(SchemaUnionError, match="invalid quarter"):
        plan_union({"2016-Q1": BASE})

def test_empty_rejected():
    with pytest.raises(SchemaUnionError, match="at least one"):
        plan_union({})

def test_unexpected_fields_preserved():
    other={"name":"model", "arrow_type":"string", "nullable":True}
    p=plan_union({"2016Q1":BASE+[other]})
    assert p.fields[-1]==other

def test_present_null_not_absent():
    p=plan_union({"2016Q1":BASE+[SMART],"2016Q2":BASE})
    assert "smart_5_raw" in p.present_columns_by_quarter["2016Q1"]
    assert "smart_5_raw" in p.absent_columns_by_quarter["2016Q2"]
    assert "source_column_presence_note" in p.as_dict()


def test_arrow_real_type_nulls_and_original_negative():
    pa=pytest.importorskip("pyarrow")
    from datetime import date
    from decimal import Decimal
    small=pa.schema([pa.field("date",pa.date32(),nullable=False),pa.field("serial_number",pa.string(),nullable=False),pa.field("failure",pa.uint8(),nullable=False)])
    full=small.append(pa.field("smart_5_raw",pa.decimal128(38,0),nullable=True))
    plan,schema=arrow_plan({"2016Q1":small,"2024Q2":full})
    batch=pa.RecordBatch.from_arrays([pa.array([date(2016,1,1)],type=pa.date32()),pa.array(["X"],type=pa.string()),pa.array([0],type=pa.uint8())],schema=small)
    out=align_batch(batch,"2016Q1",plan,schema)
    assert out.column(out.schema.get_field_index("smart_5_raw")).null_count==1
    row=pa.RecordBatch.from_arrays([pa.array([date(2024,4,1)],type=pa.date32()),pa.array(["Y"]),pa.array([1],type=pa.uint8()),pa.array([Decimal(-4)],type=pa.decimal128(38,0))],schema=full)
    preserved=align_batch(row,"2024Q2",plan,schema)
    assert preserved.column(3).to_pylist()==[Decimal(-4)]


def test_arrow_rejects_type_conversion():
    pa=pytest.importorskip("pyarrow")
    small=pa.schema([pa.field("date",pa.date32(),nullable=False),pa.field("serial_number",pa.string(),nullable=False),pa.field("failure",pa.uint8(),nullable=False)])
    plan,schema=arrow_plan({"2016Q1":small})
    bad=pa.RecordBatch.from_arrays([pa.array([],type=pa.date32()),pa.array([],type=pa.large_string()),pa.array([],type=pa.uint8())],names=["date","serial_number","failure"])
    with pytest.raises(SchemaUnionError):
        align_batch(bad,"2016Q1",plan,schema)


SIX_NEW = ["cluster_id", "datacenter", "is_legacy_format", "pod_id", "pod_slot_num", "vault_id"]


def test_2023_extension_string_columns_union_with_older_quarter():
    """Option A columns (nullable strings, zero padding kept) unify with a 2021-style quarter."""
    pa = pytest.importorskip("pyarrow")
    from datetime import date
    old = pa.schema([pa.field("date", pa.date32(), nullable=False), pa.field("serial_number", pa.string(), nullable=False),
                     pa.field("failure", pa.uint8(), nullable=False)])
    new = old
    for name in SIX_NEW:
        new = new.append(pa.field(name, pa.string(), nullable=True))
    plan, schema = arrow_plan({"2021Q4": old, "2023Q4": new})
    assert schema.names == old.names + SIX_NEW
    assert all(schema.field(n).type == pa.string() and schema.field(n).nullable for n in SIX_NEW)
    assert set(plan.absent_columns_by_quarter["2021Q4"]) == set(SIX_NEW)
    assert not plan.absent_columns_by_quarter["2023Q4"]
    b_old = pa.RecordBatch.from_arrays([pa.array([date(2021, 12, 31)], type=pa.date32()), pa.array(["A"]),
                                        pa.array([0], type=pa.uint8())], schema=old)
    out_old = align_batch(b_old, "2021Q4", plan, schema)
    assert all(out_old.column(schema.get_field_index(n)).null_count == 1 for n in SIX_NEW)
    vals = [pa.array([date(2023, 12, 31)], type=pa.date32()), pa.array(["B"]), pa.array([1], type=pa.uint8()),
            pa.array(["040"]), pa.array([None], type=pa.string()), pa.array(["False"]), pa.array(["07"]),
            pa.array(["0012"]), pa.array(["3001"])]
    out_new = align_batch(pa.RecordBatch.from_arrays(vals, schema=new), "2023Q4", plan, schema)
    assert out_new.column(schema.get_field_index("cluster_id")).to_pylist() == ["040"]
    assert out_new.column(schema.get_field_index("datacenter")).to_pylist() == [None]
    assert out_new.column(schema.get_field_index("pod_slot_num")).to_pylist() == ["0012"]
