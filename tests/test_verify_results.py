"""The lightweight result verification passes on the archived results and fails when a number is changed."""
import io
import sys
import zipfile
from pathlib import Path

import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "tools"))
import verify_results  # noqa: E402

ARCH = ROOT / "published_results" / "phase5"
pytestmark = pytest.mark.skipif(not (ARCH / "SHA256SUMS.txt").exists(), reason="result archives not present")


def test_archived_results_verify():
    assert verify_results.main(str(ARCH)) == 0


def test_a_changed_event_result_is_detected(tmp_path):
    src = zipfile.ZipFile(ARCH / "phase5_test_full_results.zip")
    out = tmp_path / "changed.zip"
    with zipfile.ZipFile(out, "w") as z:
        for name in src.namelist():
            data = src.read(name)
            if name == "event_coverage.parquet":
                ev = pd.read_parquet(io.BytesIO(data))
                i = ev.index[ev.status == "eligible"][0]
                ev.loc[i, "E1_timely_share"] = 1.0 - ev.loc[i, "E1_timely_share"]
                buf = io.BytesIO()
                ev.to_parquet(buf, index=False)
                data = buf.getvalue()
            z.writestr(name, data)
    problems = verify_results.check_archive(zipfile.ZipFile(out))
    assert any(p.startswith("E1: rate") for p in problems)
