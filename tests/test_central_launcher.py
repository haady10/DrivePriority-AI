from __future__ import annotations

from drivepriority.integration import cli


def test_help_and_unknown_command(capsys) -> None:
    assert cli.main([]) == 0
    assert "RUN_CENTRAL.cmd admit" in capsys.readouterr().out
    assert cli.main(["unknown"]) == 2


def test_dispatch_removes_command_word(monkeypatch) -> None:
    observed = {}

    def fake_run_module(module, *, run_name):
        observed["module"] = module
        observed["run_name"] = run_name
        observed["argv"] = list(cli.sys.argv)
        raise SystemExit(0)

    monkeypatch.setattr(cli.runpy, "run_module", fake_run_module)
    assert cli.main(["admit", "--quarter", "2016Q1"]) == 0
    assert observed == {
        "module": "drivepriority.integration.admission",
        "run_name": "__main__",
        "argv": ["drivepriority.integration.admission", "--quarter", "2016Q1"],
    }
