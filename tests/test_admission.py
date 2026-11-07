"""Step 2 fail-closed tests. Uses synthetic reports/files; no real archives or data are touched."""
import copy
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
        self.fields = copy.deepcopy(CONTRACT['canonical_fields_2016Q1_reference'])
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
        revalidation = self.root / 'revalidation'
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

    # ---- coordinator policy: revalidation Level 2/3 approval as authenticity basis ----
    def _revalidated_without_manifest_authenticity(self, evidence_level='level2_official_download_supported',
                                                   approved=True, sha=None):
        self.manifest['source'] = {'sha256': HEX}          # cleaned with authenticity unverified
        self.validation['status'] = 'requires_review'
        self.validation['central_integration_admission'] = {'eligible': False, 'requires_explicit_review': True}
        self.validation['review_reasons'] = ['serial_identity_collisions']
        self.manifest['validation'] = self.validation
        self.save()
        revalidation = self.root / 'revalidation'
        revalidation.mkdir()
        report = {
            'quarter': '2016Q1',
            'source_manifest_sha256': hashlib.sha256((self.root / 'source_manifest.json').read_bytes()).hexdigest(),
            'status': 'passed_with_warnings', 'review_reasons': [],
            'identity': {'remaining_unresolved_serials': 0},
            'source': {'approval_evidence': {'archive_sha256': sha or HEX, 'evidence_level': evidence_level,
                                             'research_use_source_approved': approved}},
        }
        (revalidation / 'revalidation_report.json').write_text(json.dumps(report))
        return revalidation

    def _policy(self, enabled):
        path = self.root / 'policy.json'
        path.write_text(json.dumps({'revalidation_research_use_approval_satisfies_authenticity': enabled,
                                    'decision': {'approved_by': 'test'}}))
        return path

    def test_policy_accepts_exact_hash_level2_revalidation_and_records_basis(self):
        revalidation = self._revalidated_without_manifest_authenticity()
        result = self.decide(revalidation_root=revalidation, policy_path=self._policy(True))
        self.assertEqual(result['decision'], 'ADMIT')
        self.assertEqual(result['checks']['authenticity_basis'],
                         'policy_accepted_revalidation_level2_official_download_supported')

    def test_without_policy_authenticity_stays_blocked(self):
        revalidation = self._revalidated_without_manifest_authenticity()
        result = self.decide(revalidation_root=revalidation, policy_path=self._policy(False))
        self.assertNotEqual(result['decision'], 'ADMIT')
        self.assertIn('official source authenticity unverified', result['reasons'])

    def test_missing_policy_file_is_strict(self):
        revalidation = self._revalidated_without_manifest_authenticity()
        result = self.decide(revalidation_root=revalidation, policy_path=self.root / 'nope.json')
        self.assertIn('official source authenticity unverified', result['reasons'])

    def test_policy_rejects_approval_bound_to_another_archive(self):
        revalidation = self._revalidated_without_manifest_authenticity(sha='c' * 64)
        result = self.decide(revalidation_root=revalidation, policy_path=self._policy(True))
        self.assertIn('official source authenticity unverified', result['reasons'])

    def test_policy_rejects_weak_evidence_level_or_unapproved(self):
        for kwargs in ({'evidence_level': 'unverified'}, {'approved': False}):
            with self.subTest(**kwargs):
                self.setUp()
                revalidation = self._revalidated_without_manifest_authenticity(**kwargs)
                result = self.decide(revalidation_root=revalidation, policy_path=self._policy(True))
                self.assertIn('official source authenticity unverified', result['reasons'])

    def test_policy_does_not_apply_without_revalidation(self):
        self.manifest['source'] = {'sha256': HEX}
        self.save()
        result = self.decide(policy_path=self._policy(True))
        self.assertIn('official source authenticity unverified', result['reasons'])

    def test_policy_does_not_hide_missing_exceptions_entry(self):
        revalidation = self._revalidated_without_manifest_authenticity()
        self.manifest['outputs'] = [o for o in self.manifest['outputs'] if o['path'] != 'exceptions.parquet']
        (self.root / 'source_manifest.json').write_text(json.dumps(self.manifest), encoding='utf8')
        report = json.loads((revalidation / 'revalidation_report.json').read_text())
        report['source_manifest_sha256'] = hashlib.sha256((self.root / 'source_manifest.json').read_bytes()).hexdigest()
        (revalidation / 'revalidation_report.json').write_text(json.dumps(report))
        result = self.decide(revalidation_root=revalidation, policy_path=self._policy(True))
        self.assertNotEqual(result['decision'], 'ADMIT')
        self.assertIn('exceptions.parquet not listed in manifest', result['reasons'])

    # ---- central ledger: coordinator approval held centrally, bound to the archive hash ----
    def _revalidation_without_in_package_approval(self):
        revalidation = self._revalidated_without_manifest_authenticity()
        report = json.loads((revalidation / 'revalidation_report.json').read_text())
        report.pop('source')
        (revalidation / 'revalidation_report.json').write_text(json.dumps(report))
        return revalidation

    def _ledger_policy(self, sha=HEX, quarter='2016Q1', level='level2_official_download_supported',
                       approved=True, by='Coordinator', on='2026-11-05'):
        path = self.root / 'ledger_policy.json'
        path.write_text(json.dumps({'central_research_use_approvals': {quarter: {
            'archive_sha256': sha, 'evidence_level': level, 'research_use_source_approved': approved,
            'approved_by': by, 'approved_on': on}}}))
        return path

    def test_central_ledger_accepts_exact_quarter_and_hash_and_records_basis(self):
        revalidation = self._revalidation_without_in_package_approval()
        result = self.decide(revalidation_root=revalidation, policy_path=self._ledger_policy())
        self.assertEqual(result['decision'], 'ADMIT')
        self.assertEqual(result['checks']['authenticity_basis'],
                         'central_ledger_approval_level2_official_download_supported')
        self.assertEqual(result['checks']['authenticity_policy_decision']['approved_by'], 'Coordinator')

    def test_central_ledger_rejects_other_archive_hash(self):
        revalidation = self._revalidation_without_in_package_approval()
        result = self.decide(revalidation_root=revalidation, policy_path=self._ledger_policy(sha='c' * 64))
        self.assertIn('official source authenticity unverified', result['reasons'])

    def test_central_ledger_rejects_other_quarter(self):
        revalidation = self._revalidation_without_in_package_approval()
        result = self.decide(revalidation_root=revalidation, policy_path=self._ledger_policy(quarter='2016Q2'))
        self.assertIn('official source authenticity unverified', result['reasons'])

    def test_central_ledger_rejects_weak_unapproved_or_unattributed_entries(self):
        for kwargs in ({'level': 'unverified'}, {'approved': False}, {'by': ''}, {'on': ''}):
            with self.subTest(**kwargs):
                self.setUp()
                revalidation = self._revalidation_without_in_package_approval()
                result = self.decide(revalidation_root=revalidation, policy_path=self._ledger_policy(**kwargs))
                self.assertIn('official source authenticity unverified', result['reasons'])

    def test_central_ledger_does_not_apply_without_revalidation(self):
        self.manifest['source'] = {'sha256': HEX}
        self.save()
        result = self.decide(policy_path=self._ledger_policy())
        self.assertIn('official source authenticity unverified', result['reasons'])

    def test_central_ledger_does_not_hide_missing_exceptions_entry(self):
        revalidation = self._revalidation_without_in_package_approval()
        self.manifest['outputs'] = [o for o in self.manifest['outputs'] if o['path'] != 'exceptions.parquet']
        (self.root / 'source_manifest.json').write_text(json.dumps(self.manifest), encoding='utf8')
        report = json.loads((revalidation / 'revalidation_report.json').read_text())
        report['source_manifest_sha256'] = hashlib.sha256((self.root / 'source_manifest.json').read_bytes()).hexdigest()
        (revalidation / 'revalidation_report.json').write_text(json.dumps(report))
        result = self.decide(revalidation_root=revalidation, policy_path=self._ledger_policy())
        self.assertNotEqual(result['decision'], 'ADMIT')
        self.assertIn('exceptions.parquet not listed in manifest', result['reasons'])

    # ---- exceptions.parquet rule mirrors the cleaning pipeline: required iff exception_rows > 0 ----
    def _unlist_exceptions(self, exception_rows, delete_file=True):
        self.manifest['processing']['exception_rows'] = exception_rows
        self.manifest['outputs'] = [o for o in self.manifest['outputs'] if o['path'] != 'exceptions.parquet']
        if delete_file:
            (self.root / 'exceptions.parquet').unlink()
        (self.root / 'source_manifest.json').write_text(json.dumps(self.manifest), encoding='utf8')

    def test_zero_exception_rows_without_file_is_valid(self):
        self.save()
        self._unlist_exceptions(0)
        result = self.decide()
        self.assertEqual(result['decision'], 'ADMIT')
        self.assertEqual(result['checks']['exceptions_rule'], 'zero_exception_rows_file_absent')

    def test_zero_exception_rows_with_listed_file_is_invalid(self):
        self.manifest['processing']['exception_rows'] = 0
        self.save()
        result = self.decide()
        self.assertNotEqual(result['decision'], 'ADMIT')
        self.assertIn('manifest inventories exceptions.parquet with zero exception rows', result['reasons'])

    def test_positive_exception_rows_require_listed_file(self):
        self.save()
        self._unlist_exceptions(5, delete_file=False)
        result = self.decide()
        self.assertNotEqual(result['decision'], 'ADMIT')
        self.assertIn('exceptions.parquet not listed in manifest', result['reasons'])

    def test_missing_or_invalid_exception_rows_keep_strict_rule(self):
        for value in (None, True, -1, '0', 1.5):
            with self.subTest(value=value):
                self.setUp()
                self.save()
                self._unlist_exceptions(value, delete_file=False)
                if value is None:
                    self.manifest['processing'].pop('exception_rows')
                    (self.root / 'source_manifest.json').write_text(json.dumps(self.manifest), encoding='utf8')
                result = self.decide()
                self.assertIn('exceptions.parquet not listed in manifest', result['reasons'])

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
