"""mbse-rest's persistence cases (its tests/test_service.py), run unchanged with this plugin in place of `Memory`
(mbse-rest docs/CONTRACT.md, Persistence).

Those cases make `Persistence.Memory()` and look at what it holds through its `snapshots` and `entries`. They are
read from the sibling checkout and given, as their `Persistence`, mbse-rest's own module with `Memory` replaced by
`Recording`: this plugin over a mocked table, whose `snapshots` and `entries` are read back from that table, so that
each case's assertions are about what DynamoDB holds.
"""

from __future__ import annotations

import importlib.util
import json
import types
from pathlib import Path

import pytest

from mbse.Persistence.DynamoDB import Plugin
from mbse.Services.Rest import Persistence

from conftest import REGION

CASES = Path(__file__).resolve().parents[3] / "mbse-rest" / "python3" / "tests" / "test_service.py"
PERSISTENCE_CASES = ["test_a_transaction_that_writes_is_journaled_and_replayed",
                     "test_a_snapshot_every_so_many_transactions",
                     "test_a_refused_transaction_writes_and_journals_nothing"]


class Recording(Plugin.Plugin):
    """The plugin, with what `Persistence.Memory` shows of what it holds: every snapshot and every journal entry."""

    def __init__(self) -> None:
        super().__init__("mbse-rest-cases", region=REGION)

    @property
    def snapshots(self):
        return [record["data"] for record in self._records(Plugin.SNAPSHOT, 0)]

    @property
    def entries(self):
        return [record["data"] for record in self._records(Plugin.JOURNAL, 0)]


@pytest.fixture(scope="module")
def cases():
    spec = importlib.util.spec_from_file_location("mbse_rest_test_service", CASES)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_every_persistence_case_is_run(cases):
    """mbse-rest's persistence cases are those that make a `Memory`; a new one is run here once listed."""
    source = CASES.read_text()
    found = [name for name in dir(cases) if name.startswith("test_") and "Persistence.Memory()" in
             source.split(f"def {name}(", 1)[1].split("\ndef ", 1)[0]]
    assert sorted(found) == sorted(PERSISTENCE_CASES)


@pytest.mark.parametrize("name", PERSISTENCE_CASES)
def test_mbse_rest_case_with_this_plugin(name, cases, aws, monkeypatch):
    members = {member: getattr(Persistence, member) for member in Persistence.__all__}
    monkeypatch.setattr(cases, "Persistence", types.SimpleNamespace(**{**members, "Memory": Recording}))
    getattr(cases, name)()
    assert json.dumps(Recording().load())  # the table holds what the case left, as plain data


def test_the_demo_service_over_this_plugin(aws, monkeypatch):
    from fastapi.testclient import TestClient

    from mbse.Persistence.DynamoDB import Demo

    monkeypatch.setenv("MBSE_DYNAMODB_TABLE", "demo")
    first = TestClient(Demo.work_tracker())  # first start: the installs, then a snapshot of them in DynamoDB
    assert first.get("/schemas").status_code == 200
    snapshot, journal = Plugin.Plugin("demo", region=REGION).load()
    assert snapshot is not None and journal == []
    again = TestClient(Demo.work_tracker())  # a restart reads it back
    assert again.get("/schemas").json() == first.get("/schemas").json()
