"""Step 2 fail-closed tests. Uses synthetic reports/files; no real archives or data are touched."""
import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from drivepriority.integration.admission import check_quarter, _safe_file
from drivepriority.integration.central_contract import load_contract

CONTRACT = load_contract()
HEX = 'a' * 64
COMMIT = 'b' * 40  # Git SHA-1 as in approved repository


class FakeSchema:
    def __init__(self, fields):
        self._fields = fields
    def __iter__(self):
        return iter(self._fields)
    def serialize(self):
        class B:
            def to_pybytes(self):
                return b'synthetic-schema'
        return B()

class FakeField:
    def __init__(self, field):
        self.name = field['name']
        self.type = field['arrow_type']
        self.nullable = field['nullable']

class FakeParquetFile:
    schema = None
    def __init__(self, path):
        self.schema_arrow = FakeParquetFile.schema
        self.metadata = type('Metadata', (), {'num_rows': 2})()


class AdmissionTests(unittest.TestCase):
    def setUp(self):
        self.t = tempfile.TemporaryDirectory()
        self.addCleanup(self.t.cleanup)
        self.root = Path(self.t.name)
        self.fields = CONTRACT['canonical_fields_2016Q1_reference']
        self.fp = hashlib.sha256(b'synthetic-schema').hexdigest()
        FakeParquetFile.schema = FakeSchema([FakeField(f) for f in self.fields])
        self.schema = {'canonical_fields': self.fields, 'canonical_schema_fingerprint': self.fp}
        self.quality = {'schema_version': '1.1.0'}
        self.validation = {'status': 'passed', 'central_integration_admission': {'eligible': True,
             'requires_explicit_review': False}, 'checks': [{'name': 'test', 'passed': True}],
             'review_reasons': [], 'warning_reasons': []}
        self.manifest = {
            'assigned_quarter': '2016Q1', 'quarter_start': '2016-01-01',
            'quarter_end': '2016-03-31', 'status': 'complete', 'validation': self.validation,
            'source': {'sha256': HEX, 'approved_inventory_evidence': {'authenticity_verified': True}},
            'software': {'git': {'commit': COMMIT}},
            'schema_contract': {'canonical_schema_fingerprint': self.fp,
                                'canonical_columns': self.fields},
            'processing': {'output_rows': 2}, 'outputs': []}
        self.parquet = self.root / 'data/2016Q1/a.parquet'
        self.parquet.parent.mkdir(parents=True)
        self.parquet.write_bytes(b'parquet placeholder')
        (self.root / 'exceptions.parquet').write_bytes(b'exceptions placeholder')
        self.save()

    def save(self):
        objects = {'schema_report.json': self.schema, 'quality_report.json': self.quality,
                   'validation_results.json': self.validation}
        for name, obj in objects.items():
            (self.root / name).write_text(json.dumps(obj), encoding='utf8')
        entries=[]
        for rel in ('data/2016Q1/a.parquet', 'exceptions.parquet', *objects):
            f=self.root / rel
            entry={'path': rel, 'bytes': f.stat().st_size, 'sha256': hashlib.sha256(f.read_bytes()).hexdigest()}
            if rel.endswith('.parquet'):
                entry['rows']=2
            entries.append(entry)
        self.manifest['outputs']=entries
        (self.root / 'source_manifest.json').write_text(json.dumps(self.manifest), encoding='utf8')

    def decide(self, **kw):
        with patch.dict('sys.modules', {'pyarrow': type('PyArrow', (), {'parquet': type('Parquet', (), {'ParquetFile': FakeParquetFile})})(),
                                        'pyarrow.parquet': type('Parquet', (), {'ParquetFile': FakeParquetFile})}):
            args = {'expected_quarter': '2016Q1', 'trusted_archive_sha256': HEX,
                    'trusted_pipeline_commit': COMMIT}
            args.update(kw)
            return check_quarter(self.root, **args)

    def test_positive_admission_when_all_evidence_present(self):
        self.assertEqual(self.decide()['decision'], 'ADMIT')

    def test_requires_review_blocked(self):
        self.validation['status']='requires_review'
        self.manifest['validation']=self.validation
        self.save()
        self.assertNotEqual(self.decide()['decision'], 'ADMIT')

    def test_warning_blocked(self):
        self.validation['status']='passed_with_warnings'
        self.save()
        self.assertNotEqual(self.decide()['decision'], 'ADMIT')

    def test_missing_positive_flag(self):
        self.validation['central_integration_admission']['eligible']=False
        self.save()
        self.assertNotEqual(self.decide()['decision'], 'ADMIT')

    def test_manifest_report_disagreement(self):
        self.manifest['validation']['status']='failed'
        # write manifest only, preserve independent report
        (self.root / 'source_manifest.json').write_text(json.dumps(self.manifest))
        self.assertNotEqual(self.decide()['decision'], 'ADMIT')

    def test_wrong_source_pin(self):
        self.assertNotEqual(self.decide(trusted_archive_sha256='c'*64)['decision'], 'ADMIT')

    def test_no_recorded_commit(self):
        self.manifest['software']['git']['commit']=None
        self.save()
        self.assertNotEqual(self.decide()['decision'], 'ADMIT')

    def test_gitless_release_identity_is_admitted_when_independently_pinned(self):
        runtime_hash = 'd' * 64
        self.manifest['software'] = {
            'git': {'commit': None},
            'recovery_identity': {'runtime_source_sha256': runtime_hash},
        }
        self.save()

        result = self.decide(
            trusted_pipeline_commit=None,
            trusted_runtime_source_sha256=runtime_hash,
            trusted_release_sha256='e' * 64,
        )

        self.assertEqual(result['decision'], 'ADMIT')
        self.assertEqual(
            result['checks']['pipeline_identity'],
            'gitless_release_and_runtime_source_hash',
        )

    def test_bound_revalidation_can_resolve_quality_review_but_not_source_auth(self):
        self.validation['status'] = 'requires_review'
        self.validation['central_integration_admission'] = {
            'eligible': False,
            'requires_explicit_review': True,
        }
        self.validation['review_reasons'] = ['serial_identity_collisions']
        self.manifest['validation'] = self.validation
        self.save()
        revalidation = self.root.parent / 'revalidation'
        revalidation.mkdir()
        report = {
            'quarter': '2016Q1',
            'source_manifest_sha256': hashlib.sha256(
                (self.root / 'source_manifest.json').read_bytes()
            ).hexdigest(),
            'status': 'passed_with_warnings',
            'review_reasons': [],
            'identity': {'remaining_unresolved_serials': 0},
        }
        (revalidation / 'revalidation_report.json').write_text(json.dumps(report))

        result = self.decide(revalidation_root=revalidation)

        self.assertEqual(result['decision'], 'ADMIT')

    def test_modified_output_hash(self):
        self.parquet.write_bytes(b'changed data')
        self.assertEqual(self.decide()['decision'], 'REJECT')

    def test_missing_output(self):
        self.parquet.unlink()
        self.assertNotEqual(self.decide()['decision'], 'ADMIT')

    def test_out_of_scope_quarter(self):
        self.manifest['assigned_quarter']='2026Q3'
        self.save()
        self.assertEqual(self.decide()['decision'], 'REJECT')

    def test_path_escape_rejected(self):
        self.assertIsNone(_safe_file(self.root, '../other.parquet'))
        self.assertIsNone(_safe_file(self.root, 'C:/x.parquet'))

    def test_wrong_schema(self):
        self.fields[0] = dict(self.fields[0], arrow_type='double')
        self.save()
        self.assertNotEqual(self.decide()['decision'], 'ADMIT')

    def test_missing_validation_report(self):
        (self.root / 'validation_results.json').unlink()
        self.assertEqual(self.decide()['decision'], 'REJECT')

if __name__ == '__main__':
    unittest.main()
