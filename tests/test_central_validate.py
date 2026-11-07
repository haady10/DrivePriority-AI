"""Step 5: synthetic central snapshots only; never access production datasets."""
from datetime import date
from decimal import Decimal
import hashlib
import json

import pytest
from drivepriority.integration.central_validate import validate_snapshot, _bounds, _safe_path


def test_quarter_dates():
    assert _bounds('2016Q1') == (date(2016, 1, 1), date(2016, 4, 1))
    assert _bounds('2024Q4') == (date(2024, 10, 1), date(2025, 1, 1))


def test_bad_quarter():
    with pytest.raises(ValueError):
        _bounds('Q1_2016')


def test_traversal_rejected(tmp_path):
    with pytest.raises(ValueError):
        _safe_path(tmp_path, '../escaped.parquet')


def test_missing_snapshot_fails(tmp_path):
    result = validate_snapshot(tmp_path / 'missing')
    # Arrow may not exist in CI, but validation must fail closed in either case.
    assert result['status'] == 'FAIL'


def synthetic(tmp_path):
    pa = pytest.importorskip('pyarrow')
    pq = pytest.importorskip('pyarrow.parquet')
    root = tmp_path / 'central'
    part = root / '2016' / 'Q1'
    part.mkdir(parents=True)
    schema = pa.schema([
        pa.field('date', pa.date32(), nullable=False),
        pa.field('serial_number', pa.string(), nullable=False),
        pa.field('failure', pa.uint8(), nullable=False),
        pa.field('smart_5_raw', pa.decimal128(38, 0), nullable=True),
    ])
    table = pa.Table.from_arrays([
        pa.array([date(2016, 1, 1), date(2016, 1, 2)], type=pa.date32()),
        pa.array(['s1', 's2'], type=pa.string()),
        pa.array([0, 1], type=pa.uint8()),
        pa.array([Decimal(-12), None], type=pa.decimal128(38, 0)),
    ], schema=schema)
    file = part / 'part-00000000.parquet'
    pq.write_table(table, file)
    sha = hashlib.sha256(file.read_bytes()).hexdigest()
    plan_fields = [{'name': f.name, 'arrow_type': str(f.type), 'nullable': f.nullable} for f in schema]
    manifest = {'version': 'brick2-step4-v1',
        'files': [{'path': '2016/Q1/part-00000000.parquet', 'sha256': sha, 'bytes': file.stat().st_size, 'rows': 2}],
        'schema': {'fields': plan_fields,
            'present_columns_by_quarter': {'2016Q1': [f.name for f in schema]},
            'absent_columns_by_quarter': {'2016Q1': []}},
        'schema_arrow_sha256': hashlib.sha256(schema.serialize().to_pybytes()).hexdigest(),
        'total_rows': 2,
        'input_quarters': {'2016Q1': {'source_rows': 2, 'integrated_rows': 2, 'integrated_parquet_parts': 1,
            'failure_rows': 1, 'source_manifest_sha256': 'a'*64, 'archive_sha256': 'b'*64,
            'pipeline_commit': 'c'*40}}}
    (root / 'central_manifest.json').write_text(json.dumps(manifest))
    return root, file, manifest


def test_valid_synthetic_snapshot(tmp_path):
    root, _, _ = synthetic(tmp_path)
    result = validate_snapshot(root)
    assert result['status'] == 'PASS', result['errors']
    assert result['total_rows'] == 2 and result['failure_rows'] == 1
    assert result['verified_files'] == 1


def test_corrupt_file_fails(tmp_path):
    root, file, _ = synthetic(tmp_path)
    with file.open('ab') as f:
        f.write(b'corrupt')
    assert validate_snapshot(root)['status'] == 'FAIL'


def test_extra_parquet_fails(tmp_path):
    root, file, _ = synthetic(tmp_path)
    (file.parent / 'part-00000001.parquet').write_bytes(file.read_bytes())
    assert 'inventory mismatch' in validate_snapshot(root)['errors'][0]


def test_wrong_total_fails(tmp_path):
    root, _, manifest = synthetic(tmp_path)
    manifest['total_rows'] = 100
    (root / 'central_manifest.json').write_text(json.dumps(manifest))
    assert validate_snapshot(root)['status'] == 'FAIL'


def test_failure_count_mismatch_fails(tmp_path):
    root, _, manifest = synthetic(tmp_path)
    manifest['input_quarters']['2016Q1']['failure_rows'] = 0
    (root / 'central_manifest.json').write_text(json.dumps(manifest))
    assert validate_snapshot(root)['status'] == 'FAIL'


def test_manifest_missing_fails(tmp_path):
    root, _, _ = synthetic(tmp_path)
    (root / 'central_manifest.json').unlink()
    assert validate_snapshot(root)['status'] == 'FAIL'
