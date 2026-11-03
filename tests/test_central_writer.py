"""Synthetic-only writer tests; gate calls are mocked ONLY in these tests."""
from __future__ import annotations

import json
from datetime import date
from decimal import Decimal

import pytest

from drivepriority.integration import central_writer as cw

HASH = 'a' * 64
COMMIT = 'b' * 40


def test_rejects_empty_sources(tmp_path):
    with pytest.raises(cw.CentralWriteError, match='no quarters'):
        cw._validate_sources({}, tmp_path / 'output')


def test_rejects_bad_pins(tmp_path):
    with pytest.raises(cw.CentralWriteError, match='invalid trusted archive hash'):
        cw._validate_sources({'2016Q1': {'root': tmp_path, 'archive_sha256': 'bad', 'pipeline_commit': COMMIT}}, tmp_path / 'central')


def test_rejects_duplicate_source(tmp_path):
    values = {'root': str(tmp_path), 'archive_sha256': HASH, 'pipeline_commit': COMMIT}
    with pytest.raises(cw.CentralWriteError, match='duplicate quarter'):
        cw._validate_sources({'2016Q1': values, '2016Q2': values}, tmp_path.parent / 'out')


def test_rejects_nested_output(tmp_path):
    with pytest.raises(cw.CentralWriteError, match='separate'):
        cw._validate_sources({'2016Q1': {'root': str(tmp_path), 'archive_sha256': HASH, 'pipeline_commit': COMMIT}}, tmp_path / 'sub')


def test_rejects_invalid_quarter(tmp_path):
    with pytest.raises(cw.CentralWriteError, match='invalid quarter'):
        cw._validate_sources({'Q1_2016': {'root': str(tmp_path), 'archive_sha256': HASH, 'pipeline_commit': COMMIT}}, tmp_path.parent / 'out')


def test_rejects_batch_limit(tmp_path):
    with pytest.raises(cw.CentralWriteError, match='batch_size'):
        cw.integrate_quarters({}, tmp_path / 'out', batch_size=0)


def fixtures(tmp_path, pa, pq):
    root1 = tmp_path / 'q1'
    root2 = tmp_path / 'q2'
    root1.mkdir()
    root2.mkdir()
    core = pa.schema([pa.field('date', pa.date32(), nullable=False),
                      pa.field('serial_number', pa.string(), nullable=False),
                      pa.field('failure', pa.uint8(), nullable=False)])
    extended = core.append(pa.field('smart_5_raw', pa.decimal128(38, 0), nullable=True))
    for root, quarter, schema in [(root1, '2016Q1', core), (root2, '2016Q2', extended)]:
        folder = root / 'data' / quarter
        folder.mkdir(parents=True)
        values = [pa.array([date(2016, 1 if quarter.endswith('Q1') else 4, 1)], type=pa.date32()),
                  pa.array(['SERIAL'], type=pa.string()), pa.array([1], type=pa.uint8())]
        if quarter == '2016Q2':
            values.append(pa.array([Decimal(-7)], type=pa.decimal128(38, 0)))
        pq.write_table(pa.Table.from_arrays(values, schema=schema), folder / 'part.parquet')
        (root / 'source_manifest.json').write_text(json.dumps({'outputs': [{'path': f'data/{quarter}/part.parquet'}]}))
    anchors = {'archive_sha256': HASH, 'pipeline_commit': COMMIT}
    sources = {'2016Q1': {'root': str(root1), **anchors}, '2016Q2': {'root': str(root2), **anchors}}
    return sources


def test_synthetic_real_arrow_batches_and_manifest(tmp_path, monkeypatch):
    pa = pytest.importorskip('pyarrow')
    pq = pytest.importorskip('pyarrow.parquet')
    sources = fixtures(tmp_path, pa, pq)
    monkeypatch.setattr(cw, 'check_quarter', lambda *args, **kw: {'decision': 'ADMIT', 'checks': {'parquet_rows': 1}})
    output = tmp_path / 'central'
    result = cw.integrate_quarters(sources, output, batch_size=1)
    assert result['status'] == 'PASS' and result['rows'] == 2 and result['files'] == 2
    manifest = json.loads((output / 'central_manifest.json').read_text())
    assert manifest['input_quarters']['2016Q1']['failure_rows'] == 1
    assert len(manifest['files']) == 2
    table1 = pq.read_table(output / '2016' / 'Q1' / 'part-00000000.parquet')
    table2 = pq.read_table(output / '2016' / 'Q2' / 'part-00000000.parquet')
    assert table1['smart_5_raw'].to_pylist() == [None]
    assert table2['smart_5_raw'].to_pylist() == [Decimal(-7)]
    with pytest.raises(cw.CentralWriteError, match='already exists'):
        cw.integrate_quarters(sources, output, batch_size=1)


def test_review_quarter_fails_before_creating_output(tmp_path, monkeypatch):
    pytest.importorskip('pyarrow')
    root = tmp_path / 'q1'
    root.mkdir()
    src = {'2016Q1': {'root': str(root), 'archive_sha256': HASH, 'pipeline_commit': COMMIT}}
    monkeypatch.setattr(cw, 'check_quarter', lambda *args, **kw: {'decision': 'REVIEW', 'reasons': ['requires_review']})
    output = tmp_path / 'central'
    with pytest.raises(cw.CentralWriteError, match='not admitted'):
        cw.integrate_quarters(src, output)
    assert not output.exists()


def test_gate_error_fails_closed(tmp_path, monkeypatch):
    pytest.importorskip('pyarrow')
    root = tmp_path / 'q1'
    root.mkdir()
    src = {'2016Q1': {'root': str(root), 'archive_sha256': HASH, 'pipeline_commit': COMMIT}}
    monkeypatch.setattr(cw, 'check_quarter', lambda *args, **kw: {'decision': 'REJECT', 'reasons': ['hash mismatch']})
    with pytest.raises(cw.CentralWriteError, match='not admitted'):
        cw.integrate_quarters(src, tmp_path / 'central')
    assert not (tmp_path / 'central').exists()


def test_row_reconciliation_fails_and_no_output(tmp_path, monkeypatch):
    pa = pytest.importorskip('pyarrow')
    pq = pytest.importorskip('pyarrow.parquet')
    src = fixtures(tmp_path, pa, pq)
    monkeypatch.setattr(cw, 'check_quarter', lambda *args, **kw: {'decision': 'ADMIT', 'checks': {'parquet_rows': 999}})
    with pytest.raises(cw.CentralWriteError, match='reconciliation'):
        cw.integrate_quarters(src, tmp_path / 'central')
    assert not (tmp_path / 'central').exists()
    assert not list(tmp_path.glob('.central.staging-*'))
