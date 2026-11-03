"""Windows-safe dispatcher for the central integration command modules."""

from __future__ import annotations

import runpy
import sys
from typing import Sequence


MODULES = {
    "admit": "drivepriority.integration.admission",
    "integrate": "drivepriority.integration.central_writer",
    "incremental": "drivepriority.integration.incremental",
    "validate": "drivepriority.integration.central_validate",
}


def main(argv: Sequence[str] | None = None) -> int:
    arguments = list(sys.argv[1:] if argv is None else argv)
    if not arguments or arguments[0] in {"help", "--help", "-h"}:
        print("Usage:")
        print("  RUN_CENTRAL.cmd admit --output-root DIR --quarter YYYYQn [trust arguments]")
        print("  RUN_CENTRAL.cmd integrate --sources-json FILE --output DIR")
        print("  RUN_CENTRAL.cmd incremental --existing DIR --new-sources-json FILE --output DIR")
        print("  RUN_CENTRAL.cmd validate --root DIR --report FILE")
        return 0
    command = arguments.pop(0)
    module = MODULES.get(command)
    if module is None:
        print(f"ERROR: Unknown central command: {command}")
        return 2
    sys.argv = [module, *arguments]
    try:
        runpy.run_module(module, run_name="__main__")
    except SystemExit as exc:
        return int(exc.code or 0)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
