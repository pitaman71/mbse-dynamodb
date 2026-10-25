"""mbse-rest's persistence cases (`mbse.Services.Rest.PersistenceCases`, mbse-rest docs/CONTRACT.md, Persistence), run
with this plugin, and mbse-rest's service selecting it by name, as `MBSE_REST_PLUGIN` does."""

from __future__ import annotations

import itertools

import pytest
from fastapi.testclient import TestClient

from mbse.Persistence.DynamoDB import Plugin
from mbse.Services.Rest import PersistenceCases, Service

from conftest import REGION

_tables = itertools.count()


@pytest.mark.parametrize("case", PersistenceCases.CASES, ids=lambda case: case.__name__)
def test_persistence_case_with_this_plugin(case, aws):
    """Each case is given a maker of fresh plugins, each over a table of its own in the mocked AWS."""
    case(lambda: Plugin.Plugin(f"cases-{next(_tables)}", region=REGION))


def test_mbse_rests_service_selects_this_plugin(aws, monkeypatch):
    """The service as the environment configures it, its plugin this one by name: a first start installs the
    WorkTracker and snapshots it into DynamoDB, and a restart reads it back. The plugin, made with no arguments, reads
    its own settings from the process's environment."""
    monkeypatch.setenv("MBSE_DYNAMODB_TABLE", "demo")
    environ = {"MBSE_REST_PLUGIN": "mbse.Persistence.DynamoDB.Plugin:Plugin"}
    first = TestClient(Service.from_environment(environ))
    assert first.get("/schemas").status_code == 200
    snapshot, journal = Plugin.Plugin("demo", region=REGION).load()
    assert snapshot is not None and journal == []
    again = TestClient(Service.from_environment(environ))
    assert again.get("/schemas").json() == first.get("/schemas").json()
