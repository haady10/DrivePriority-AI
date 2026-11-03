"""Brick 2 Step 5: independent, read-only verification of a Step 4 central snapshot.

Checks manifest, exact file inventory, SHA-256, Parquet readback, unified Arrow
schema, dates, failure labels, row accounting and quarter provenance counts.
Does not authenticate original Backblaze bytes or prove CSV-token equality.
"""
from __future__ import annotations

import argparse
from contextlib import closing
from datetime import date
import hashlib
import json
from pathlib import Path, PurePosixPath
import re
from typing import Any

SHA256 = re.compile(r"[0-9a-fA-F]{64}\Z")
QUARTER = re.compile(r"20\d{2}Q[1-4]\Z")
PART = re.compile(r"(20\d{2})/(Q[1-4])/part-[0-9]{8}\.parquet\Z")


def _hash_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open('rb') as fp:
        for block in iter(lambda: fp.read(1024 * 1024), b''):
            h.update(block)
    return h.hexdigest()


def _safe_path(root: Path, relative: Any) -> Path:
    if not isinstance(relative, str) or not relative or '\\' in relative or ':' in relative:
        raise ValueError('unsafe relative file path')
    rel = PurePosixPath(relative)
    if rel.is_absolute() or any(x in ('.', '..', '') for x in rel.parts):
        raise ValueError('unsafe relative file path')
    candidate = root.joinpath(*rel.parts)
    if not candidate.is_file() or candidate.is_symlink():
        raise ValueError('missing or linked Parquet file')
    if not candidate.resolve(strict=True).is_relative_to(root.resolve(strict=True)):
        raise ValueError('file escapes snapshot root')
    return candidate


def _bounds(q: str) -> tuple[date, date]:
    if not isinstance(q, str) or not QUARTER.fullmatch(q):
        raise ValueError('invalid quarter')
    year = int(q[:4]); quarter = int(q[-1]); start_month = 1 + (quarter - 1) * 3
    start = date(year, start_month, 1)
    end = date(year + 1, 1, 1) if quarter == 4 else date(year, start_month + 3, 1)
    return start, end


def _positive_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value > 0


def validate_snapshot(root: str | Path, *, batch_size: int = 8192) -> dict[str, Any]:
    """Produce PASS/FAIL evidence without modifying the input dataset."""
    result: dict[str, Any] = {'status': 'FAIL', 'root': str(root), 'errors': [], 'verified_files': 0,
                              'total_rows': 0, 'failure_rows': 0, 'quarters': {},
                              'limitations': ['Does not authenticate official source archive bytes',
                                              'Does not independently prove original CSV-to-Parquet exact SMART token equality',
                                              'Source admission remains the responsibility of the Step 2 gate']}
    errors: list[str] = result['errors']
    if not _positive_int(batch_size) or batch_size > 65536:
        errors.append('invalid batch size'); return result
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError:
        errors.append('PyArrow unavailable'); return result
    try:
        root = Path(root).expanduser().resolve(strict=True)
        if not root.is_dir() or root.is_symlink():
            raise ValueError('unsafe or missing central directory')
        mp = root / 'central_manifest.json'
        if not mp.is_file() or mp.is_symlink():
            raise ValueError('missing or linked central manifest')
        manifest = json.loads(mp.read_text(encoding='utf-8'))
        if not isinstance(manifest, dict) or manifest.get('version') != 'brick2-step4-v1':
            raise ValueError('unsupported central manifest version')
        files = manifest.get('files')
        quarters = manifest.get('input_quarters')
        schema_info = manifest.get('schema')
        if not isinstance(files, list) or not files or not isinstance(quarters, dict) or not quarters:
            raise ValueError('missing files or quarter evidence')
        if not isinstance(schema_info, dict) or not isinstance(schema_info.get('fields'), list):
            raise ValueError('missing schema plan')
        if not isinstance(manifest.get('schema_arrow_sha256'), str) or not SHA256.fullmatch(manifest['schema_arrow_sha256']):
            raise ValueError('missing Arrow schema fingerprint')
        declared = {}
        for q, metadata in quarters.items():
            _bounds(q)
            if not isinstance(metadata, dict) or not all(_positive_int(metadata.get(k)) for k in ('source_rows', 'integrated_rows', 'integrated_parquet_parts')):
                raise ValueError('incomplete row and part accounting for quarter ' + str(q))
            for k, n in (('failure_rows', 0),):
                if not isinstance(metadata.get(k), int) or isinstance(metadata[k], bool) or metadata[k] < n:
                    raise ValueError('invalid failure count for ' + q)
            for key, pattern in [('source_manifest_sha256', SHA256), ('archive_sha256', SHA256)]:
                if not isinstance(metadata.get(key), str) or not pattern.fullmatch(metadata[key]):
                    raise ValueError('missing provenance hash for ' + q)
            commit = metadata.get('pipeline_commit')
            if not isinstance(commit, str) or not re.fullmatch(r'[0-9a-fA-F]{40}', commit):
                raise ValueError('missing pipeline commit for ' + q)
            if metadata['source_rows'] != metadata['integrated_rows']:
                raise ValueError('source and integrated row totals disagree for ' + q)
            declared[q] = {'rows': 0, 'failure_rows': 0, 'files': 0, 'paths': set()}
        quarter_presence = schema_info.get('present_columns_by_quarter')
        quarter_absence = schema_info.get('absent_columns_by_quarter')
        if not isinstance(quarter_presence, dict) or not isinstance(quarter_absence, dict) or set(quarter_presence) != set(declared) or set(quarter_absence) != set(declared):
            raise ValueError('unified schema quarter-presence plan mismatch')
        expected_paths: set[str] = set()
        for entry in files:
            if not isinstance(entry, dict):
                raise ValueError('invalid file manifest record')
            relative = entry.get('path'); match = PART.fullmatch(relative) if isinstance(relative, str) else None
            if match is None:
                raise ValueError('unexpected central partition file path')
            q = match[1] + match[2]
            if q not in declared:
                raise ValueError('file references undeclared quarter: ' + q)
            if relative in expected_paths:
                raise ValueError('duplicate manifest path: ' + relative)
            expected_paths.add(relative)
            if not _positive_int(entry.get('rows')) or not _positive_int(entry.get('bytes')):
                raise ValueError('missing file byte/row accounting: ' + relative)
            if not isinstance(entry.get('sha256'), str) or not SHA256.fullmatch(entry['sha256']):
                raise ValueError('missing file hash: ' + relative)
        # Compare exact physical file inventory; do not ignore unexpected Parquet parts.
        actual_paths = set()
        for p in root.rglob('*.parquet'):
            if p.is_symlink() or not p.is_file():
                raise ValueError('linked or unsafe Parquet path')
            actual_paths.add(p.relative_to(root).as_posix())
        if expected_paths != actual_paths:
            raise ValueError(f'Parquet inventory mismatch: missing={len(expected_paths - actual_paths)} unexpected={len(actual_paths - expected_paths)}')

        reference_schema = None
        for entry in files:
            relative = entry['path']; match = PART.fullmatch(relative)
            assert match is not None
            q = match[1] + match[2]
            file_path = _safe_path(root, relative)
            if file_path.stat().st_size != entry['bytes'] or _hash_file(file_path).lower() != entry['sha256'].lower():
                raise ValueError('Parquet size/hash mismatch: ' + relative)
            with closing(pq.ParquetFile(file_path)) as reader:
                schema = reader.schema_arrow
                if reference_schema is None:
                    reference_schema = schema
                    observed_sha = hashlib.sha256(schema.serialize().to_pybytes()).hexdigest()
                    if observed_sha.lower() != manifest['schema_arrow_sha256'].lower():
                        raise ValueError('unified Arrow schema fingerprint mismatch')
                    if schema.names != [item.get('name') for item in schema_info['fields']]:
                        raise ValueError('unified Arrow column order mismatch')
                    for field in schema:
                        description = next(i for i in schema_info['fields'] if i['name'] == field.name)
                        if str(field.type) != description.get('arrow_type') or field.nullable != description.get('nullable'):
                            raise ValueError('unified Arrow schema field mismatch: ' + field.name)
                        if re.fullmatch(r'smart_\d+_(raw|normalized)', field.name) and not field.type.equals(pa.decimal128(38, 0)):
                            raise ValueError('unsafe SMART numeric type: ' + field.name)
                elif not schema.equals(reference_schema, check_metadata=True):
                    raise ValueError('Parquet schema differs from unified Arrow schema: ' + relative)
                if reader.metadata.num_rows != entry['rows']:
                    raise ValueError('Parquet metadata row-count mismatch: ' + relative)
                start, end = _bounds(q)
                count = 0; failures = 0
                for batch in reader.iter_batches(batch_size=batch_size, columns=['date', 'failure', 'serial_number']):
                    count += batch.num_rows
                    for d, f, serial in zip(batch.column(0).to_pylist(), batch.column(1).to_pylist(), batch.column(2).to_pylist()):
                        if not isinstance(d, date) or d < start or d >= end or not isinstance(serial, str) or not serial:
                            raise ValueError('invalid quarter date or drive identity: ' + relative)
                        if type(f) is not int or f not in (0, 1):
                            raise ValueError('invalid failure flag: ' + relative)
                        failures += f
                if count != entry['rows']:
                    raise ValueError('Parquet readback row-count mismatch: ' + relative)
            declared[q]['rows'] += count
            declared[q]['failure_rows'] += failures
            declared[q]['files'] += 1
            declared[q]['paths'].add(relative)
            result['verified_files'] += 1
        for q, observed in declared.items():
            meta = quarters[q]
            if observed['rows'] != meta['integrated_rows'] or observed['files'] != meta['integrated_parquet_parts'] or observed['failure_rows'] != meta['failure_rows']:
                raise ValueError('quarter total reconciliation mismatch: ' + q)
            result['quarters'][q] = {'rows': observed['rows'], 'failure_rows': observed['failure_rows'], 'files': observed['files']}
        result['total_rows'] = sum(q['rows'] for q in result['quarters'].values())
        result['failure_rows'] = sum(q['failure_rows'] for q in result['quarters'].values())
        if not _positive_int(manifest.get('total_rows')) or result['total_rows'] != manifest['total_rows']:
            raise ValueError('central total row count mismatch')
        result['status'] = 'PASS'
        return result
    except Exception as exc:  # Fail closed on Parquet/Arrow decoding or filesystem errors.
        errors.append(f'{type(exc).__name__}: {exc}')
        return result


def main() -> int:
    parser = argparse.ArgumentParser(description='Read-only Step 5 central dataset verifier')
    parser.add_argument('--input', required=True, help='Step 4 central snapshot directory')
    parser.add_argument('--batch-size', type=int, default=8192)
    parser.add_argument('--report', default=None, help='Optional report JSON (must be outside input)')
    args = parser.parse_args()
    result = validate_snapshot(args.input, batch_size=args.batch_size)
    if args.report is not None:
        report = Path(args.report).expanduser().absolute()
        input_path = Path(args.input).expanduser().absolute()
        if report == input_path or input_path in report.parents:
            parser.error('report must be outside the immutable input dataset')
        if report.exists():
            parser.error('refusing to overwrite existing report')
        report.parent.mkdir(parents=True, exist_ok=True)
        report.write_text(json.dumps(result, indent=2, sort_keys=True), encoding='utf-8')
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0 if result['status'] == 'PASS' else 1


if __name__ == '__main__':
    raise SystemExit(main())
