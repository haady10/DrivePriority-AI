import json
import unittest
from pathlib import Path
from drivepriority.integration.central_contract import load_contract,quarter_bounds,validate_field_contract,status_policy,union_field_descriptions

C=load_contract()
BASE=C['canonical_fields_2016Q1_reference']
class ContractTests(unittest.TestCase):
 def test_reference_schema(self): self.assertEqual(validate_field_contract(BASE,'2016Q1'),[])
 def test_last_quarter(self): self.assertEqual(quarter_bounds('2026Q2')[0].isoformat(),'2026-04-01')
 def test_out_of_scope(self):
  with self.assertRaises(ValueError): quarter_bounds('2026Q3')
 def test_bad_quarter(self):
  with self.assertRaises(ValueError): quarter_bounds('2016-q1')
 def test_missing_identity(self):
  fields=[f for f in BASE if f['name']!='serial_number']
  self.assertTrue(validate_field_contract(fields,'2016Q1'))
 def test_bad_smart_float(self):
  fields=[dict(f) for f in BASE]
  i=next(i for i,f in enumerate(fields) if f['name']=='smart_1_raw')
  fields[i]['arrow_type']='double'
  self.assertTrue(validate_field_contract(fields,'2016Q1'))
 def test_duplicate_field(self): self.assertTrue(validate_field_contract(BASE+[BASE[0]],'2016Q1'))
 def test_review_always_blocked(self): self.assertEqual(status_policy({'status':'requires_review','central_integration_admission':{'eligible':False}})[0],'BLOCKED')
 def test_contradiction_blocked(self): self.assertEqual(status_policy({'status':'passed','central_integration_admission':{'eligible':False}})[0],'BLOCKED')
 def test_missing_flag_blocked(self): self.assertEqual(status_policy({'status':'passed'})[0],'BLOCKED')
 def test_pass_candidate_not_admission(self): self.assertEqual(status_policy({'status':'passed','central_integration_admission':{'eligible':True}})[0],'CANDIDATE_ONLY')
 def test_warning_blocked(self): self.assertEqual(status_policy({'status':'passed_with_warnings','central_integration_admission':{'eligible':True}})[0],'BLOCKED')
 def test_union_preserves_missing_type_and_presence(self):
  fields=[f for f in BASE if f['name']!='smart_1_raw']
  out=union_field_descriptions({'2016Q1':fields,'2024Q2':BASE})
  self.assertNotIn('smart_1_raw',out['present_columns_by_quarter']['2016Q1'])
  self.assertEqual(next(x for x in out['fields'] if x['name']=='smart_1_raw')['arrow_type'],'decimal128(38, 0)')
 def test_union_rejects_type_conflict(self):
  fields=[dict(f) for f in BASE];fields[0]['arrow_type']='string'
  with self.assertRaises(ValueError): union_field_descriptions({'2016Q1':BASE,'2024Q2':fields})
if __name__=='__main__':unittest.main()
