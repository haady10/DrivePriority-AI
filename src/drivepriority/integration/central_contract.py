"""Brick 2 Step 1: metadata contract; NOT a real-quarter admission gate."""
import json
import re
from datetime import date, timedelta
from pathlib import Path

SMART = re.compile(r"^smart_[0-9]+_(?:raw|normalized)$")
STATUS = ('passed', 'passed_with_warnings', 'requires_review', 'failed')


def load_contract(path=None):
    path = Path(path) if path else Path(__file__).resolve().parents[3] / 'configs' / 'central_contract.json'
    return json.loads(path.read_text(encoding='utf-8'))


def quarter_bounds(quarter, contract=None):
    contract = contract or load_contract()
    m = re.fullmatch(r'(\d{4})Q([1-4])', quarter)
    if not m:
        raise ValueError('Quarter must look like 2016Q1')
    year, q = int(m[1]), int(m[2]); month=(q-1)*3+1
    start=date(year,month,1)
    end=date(year+1,1,1) if q==4 else date(year,month+3,1)
    # End is exclusive; upper study bound is June 30, 2026.
    upper=date.fromisoformat(contract['study_period']['end'])
    if start < date.fromisoformat(contract['study_period']['start']) or (end - timedelta(days=1)) > upper:
        raise ValueError('Quarter lies outside study scope')
    return start, end


def validate_field_contract(fields, quarter, contract=None):
    """Inspect schema descriptions, not actual Parquet contents."""
    contract = contract or load_contract()
    errors=[]
    try:
        quarter_bounds(quarter,contract)
    except ValueError as exc:
        errors.append(str(exc))
    if not isinstance(fields,list):
        return ['canonical_fields must be a list']
    names=[f.get('name') for f in fields if isinstance(f,dict)]
    if len(names)!=len(fields) or len(names)!=len(set(names)):
        errors.append('duplicate or invalid field descriptions')
    by_name={f['name']:f for f in fields if isinstance(f,dict) and isinstance(f.get('name'),str)}
    expected={'date':'date32[day]','serial_number':'string','failure':'uint8'}
    for name,typ in expected.items():
        if name not in by_name or by_name[name].get('arrow_type')!=typ or by_name[name].get('nullable') is not False:
            errors.append(f'required field {name} has incorrect type or nullability')
    for name,f in by_name.items():
        if SMART.fullmatch(name):
            if f.get('arrow_type')!='decimal128(38, 0)' or f.get('decimal_precision',38)!=38 or f.get('decimal_scale',0)!=0:
                errors.append(f'{name} is not exact decimal128(38, 0)')
    return errors


def status_policy(validation):
    """Only a preliminary policy classification; not proof of admission."""
    if not isinstance(validation,dict):
        return 'BLOCKED', ['missing validation report']
    status=validation.get('status')
    declared=validation.get('central_integration_admission',{})
    if status not in STATUS:
        return 'BLOCKED', ['missing or unknown validation status']
    if declared.get('eligible') is False:
        return 'BLOCKED', ['validation report explicitly denies central integration']
    if status in ('failed','requires_review'):
        return 'BLOCKED', [f'validation outcome: {status}']
    if status=='passed_with_warnings':
        return 'BLOCKED', ['warning adjudication required']
    if declared.get('eligible') is not True:
        return 'BLOCKED', ['positive admission flag missing']
    return 'CANDIDATE_ONLY', ['full file/provenance admission checks required in Step 2']


def union_field_descriptions(schemas):
    """Design typed-null schema union; never cast SMART to float."""
    merged={}; presence={}
    for quarter, fields in schemas.items():
        presence[quarter]=[]
        for f in fields:
            name=f['name']; typ=f['arrow_type']
            if name in merged and merged[name]['arrow_type'] != typ:
                raise ValueError(f'type conflict for {name}: {merged[name]["arrow_type"]} vs {typ}')
            if name not in merged:
                merged[name]=dict(f)
            presence[quarter].append(name)
    return {'fields':list(merged.values()), 'present_columns_by_quarter':presence,
            'policy':'missing columns become correctly typed nulls; source-presence metadata retained'}
