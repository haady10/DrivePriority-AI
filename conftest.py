"""Test configuration: put src/ on the import path, so `python -m pytest tests -q` works without PYTHONPATH."""
import sys
from pathlib import Path

SRC = Path(__file__).resolve().parent / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))
