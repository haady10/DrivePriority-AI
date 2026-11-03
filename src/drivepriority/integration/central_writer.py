"""Brick 2 Step 4: bounded-memory central Parquet writer.

Consumes exclusively ADMIT decisions from the frozen Step 2 gate.
Adds typed nulls for absent CLEANED columns using the frozen Step 3 union.
Does not change ingestion code, labels, source data or source reports.
"""
from __future__ import annotations

import argparse
from contextlib import closing
import hashlib
import json
import os
import re
import shutil
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

from drivepriority.integration.admission import check_quarter
from drivepriority.integration.schema_union import arrow_plan, align_batch

QUARTER = re.compile(r"20\d{2}Q[1-4]\Z")
SHA256 = re.compile(r"[a-fA-F0-9]{64}\Z")
COMMIT = re.compile(r"[a-fA-F0-9]{40}\Z")


class CentralWriteError(RuntimeError):
    """Refuse unsafe integration or unverified input."""


def _digest(path: Path) -> str:
    h = hashlib.sha256()
    with path.open('rb') as stream:
        for part in iter(lambda: stream.read(1 << 20), b''):
            h.update(part)
    return h.hexdigest()


def _real_dir(path: str | Path) -> Path:
    p = Path(path).expanduser().resolve(strict=True)
    if not p.is_dir() or p.is_symlink():
        raise CentralWriteError(f'unsafe input directory: {p}')
    return p


def _validate_sources(sources: Mapping[str, Mapping[str, Any]], output: Path) -> dict[str, dict[str, Any]]:
    if not isinstance(sources, Mapping) or not sources:
        raise CentralWriteError('no quarters supplied')
    trusted: dict[str, dict[str, Any]] = {}
    roots = []
    for q, entry in sorted(sources.items()):
        if not isinstance(q, str) or not QUARTER.fullmatch(q):
            raise CentralWriteError(f'invalid quarter: {q!r}')
        if not isinstance(entry, Mapping) or not all(k in entry for k in ('root', 'archive_sha256', 'pipeline_commit')):
            raise CentralWriteError(f'missing independent trust anchors: {q}')
        archive_hash, commit = entry['archive_sha256'], entry['pipeline_commit']
        if not isinstance(archive_hash, str) or not SHA256.fullmatch(archive_hash):
            raise CentralWriteError(f'invalid trusted archive hash: {q}')
        if not isinstance(commit, str) or not COMMIT.fullmatch(commit):
            raise CentralWriteError(f'invalid trusted pipeline commit: {q}')
        root = _real_dir(entry['root'])
        if root in roots:
            raise CentralWriteError('duplicate quarter input location')
        if root == output or root in output.parents or output in root.parents:
            raise CentralWriteError('input and output directories must be separate')
        roots.append(root)
        trusted[q] = {'root': root, 'archive_sha256': archive_hash.lower(), 'pipeline_commit': commit.lower()}
    return trusted


def integrate_quarters(sources: Mapping[str, Mapping[str, Any]], output_dir: str | Path, *,
                       contract_path: str | Path | None = None, batch_size: int = 8192) -> dict[str, Any]:
    """Write a NEW immutable central snapshot from admitted quarters only.

    Inputs are not mutated. A failed run cleans up its own staging directory.
    A new snapshot can later be built with additional quarters (Step 6 handles
    incremental recovery); this step intentionally never replaces existing output.
    """
    if isinstance(batch_size, bool) or not isinstance(batch_size, int) or not 1 <= batch_size <= 65536:
        raise CentralWriteError('batch_size must be between 1 and 65536')
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError as exc:
        raise CentralWriteError('pyarrow is required') from exc
    output = Path(output_dir).expanduser().absolute()
    if output.exists() or output.is_symlink():
        raise CentralWriteError('output already exists: refusing overwrite')
    parent = output.parent.resolve(strict=True)
    if not parent.is_dir() or parent.is_symlink():
        raise CentralWriteError('unsafe output parent')
    inputs = _validate_sources(sources, output)

    # Fail closed BEFORE writing a single output file.
    admissions = {}
    paths = {}
    schemas = {}
    for q, item in inputs.items():
        root = item['root']
        decision = check_quarter(root, expected_quarter=q, contract_path=contract_path,
                                 trusted_archive_sha256=item['archive_sha256'],
                                 trusted_pipeline_commit=item['pipeline_commit'])
        if decision.get('decision') != 'ADMIT':
            raise CentralWriteError(f'{q} not admitted: {decision.get("decision")}; {decision.get("reasons")}')
        manifest_path = root / 'source_manifest.json'
        manifest = json.loads(manifest_path.read_text(encoding='utf-8'))
        file_paths = []
        for record in manifest['outputs']:
            relative = record.get('path')
            if isinstance(relative, str) and relative.startswith(f'data/{q}/') and relative.endswith('.parquet'):
                file_paths.append(root / relative)
        if not file_paths:
            raise CentralWriteError(f'no data files in admitted quarter: {q}')
        with closing(pq.ParquetFile(file_paths[0])) as first_reader:
            first_schema = first_reader.schema_arrow
        for fp in file_paths[1:]:
            with closing(pq.ParquetFile(fp)) as schema_reader:
                if not schema_reader.schema_arrow.equals(first_schema, check_metadata=True):
                    raise CentralWriteError(f'inconsistent Arrow schema in {q}: {fp.name}')
        paths[q] = file_paths
        schemas[q] = first_schema
        admissions[q] = {'source_manifest_sha256': _digest(manifest_path),
                         'source_manifest': str(manifest_path),
                         'source_rows': decision['checks'].get('parquet_rows'),
                         'archive_sha256': item['archive_sha256'],
                         'pipeline_commit': item['pipeline_commit']}

    plan, union_schema = arrow_plan(schemas)
    staging = parent / ('.' + output.name + '.staging-' + uuid.uuid4().hex)
    staging.mkdir(mode=0o700)
    written = []
    try:
        for q, files in paths.items():
            partition = staging / q[:4] / q[4:]
            partition.mkdir(parents=True, exist_ok=False)
            source_rows = 0
            emitted_rows = 0
            failures = 0
            part_index = 0
            for file in files:
                with closing(pq.ParquetFile(file)) as parquet:
                    for batch in parquet.iter_batches(batch_size=batch_size):
                        source_rows += batch.num_rows
                        if batch.num_rows == 0:
                            continue
                        aligned = align_batch(batch, q, plan, union_schema)
                        if not aligned.schema.equals(union_schema, check_metadata=True):
                            raise CentralWriteError(f'output schema mismatch in {q}')
                        failures += sum(1 for f in aligned.column(union_schema.get_field_index('failure')).to_pylist() if f == 1)
                        dest = partition / f'part-{part_index:08d}.parquet'
                        pq.write_table(pa.Table.from_batches([aligned], schema=union_schema), dest,
                                       compression='zstd', version='2.6')
                        with closing(pq.ParquetFile(dest)) as readback:
                            if readback.metadata.num_rows != batch.num_rows or not readback.schema_arrow.equals(union_schema, check_metadata=True):
                                raise CentralWriteError(f'write readback mismatch: {q} part {part_index}')
                        written.append({'path': dest.relative_to(staging).as_posix(), 'sha256': _digest(dest),
                                        'bytes': dest.stat().st_size, 'rows': batch.num_rows})
                        emitted_rows += batch.num_rows
                        part_index += 1
            expected = admissions[q]['source_rows']
            if not isinstance(expected, int) or expected <= 0 or emitted_rows != expected or source_rows != expected:
                raise CentralWriteError(f'quarter row reconciliation failed: {q}; expected={expected}, read={source_rows}, written={emitted_rows}')
            if part_index == 0:
                raise CentralWriteError(f'no nonempty batches for {q}')
            admissions[q].update({'integrated_rows': emitted_rows, 'failure_rows': failures,
                                  'integrated_parquet_parts': part_index})
        manifest = {'version': 'brick2-step4-v1', 'created_utc': datetime.now(timezone.utc).isoformat(),
                    'status': 'integrated_synthetic_or_admitted_quarters',
                    'input_quarters': admissions, 'schema': plan.as_dict(),
                    'schema_arrow_sha256': hashlib.sha256(union_schema.serialize().to_pybytes()).hexdigest(),
                    'files': written, 'total_rows': sum(x['integrated_rows'] for x in admissions.values()),
                    'source_column_presence_limit': 'cleaned-schema presence only; original source absence/blank requires separate evidence',
                    'source_admission_note': 'Admission gate checked original inputs before central output was written'}
        (staging / 'central_manifest.json').write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding='utf-8')
        if output.exists():
            raise CentralWriteError('output appeared during run: refusing overwrite')
        staging.rename(output)
        return {'status': 'PASS', 'output': str(output), 'rows': manifest['total_rows'],
                'quarters': sorted(admissions), 'files': len(written)}
    except BaseException:
        if staging.exists():
            shutil.rmtree(staging)
        raise


def main() -> int:
    parser = argparse.ArgumentParser(description='Fail-closed central writer; accepted quarters only')
    parser.add_argument('--sources-json', required=True, help='JSON keyed by quarter with root/archive_sha256/pipeline_commit')
    parser.add_argument('--output', required=True, help='Must not exist; new central snapshot')
    parser.add_argument('--contract', default=None)
    parser.add_argument('--batch-size', type=int, default=8192)
    args = parser.parse_args()
    with open(args.sources_json, encoding='utf-8') as fp:
        sources = json.load(fp)
    result = integrate_quarters(sources, args.output, contract_path=args.contract, batch_size=args.batch_size)
    print(json.dumps(result, indent=2))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
