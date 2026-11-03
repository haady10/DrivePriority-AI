# DrivePriority AI — Brick 2, Step 1

This is a **separate draft central data contract**, not a patch to the frozen ingestion pipeline.

- `central_contract.json`: machine-readable contract, grounded in the Sulaiman 2016Q1 schema and validation evidence.
- `central_contract.py`: schema description checks, planned typed-null union, conservative admission status classification.
- `tests/test_contract.py`: synthetic tests.

Run from the package folder (Python 3.11; no installation needed):

```
python -m unittest discover -s tests -v
```

**Scope:** No real Parquet loaded, no archive ingested, no cleaned data admitted. `CANDIDATE_ONLY` is not `PASS`. Step 2 must independently verify actual input files, hashes, provenance, source-quality findings and a documented warning policy before accepting any quarter. Null from missing column versus blank source cell requires source-presence evidence; do not infer it from Parquet nulls alone. Metadata schema is anchored in observed 2016Q1, NOT an asserted universal 2016–2026 schema.

No Git commits, pushes, merges or changes to approved files.
