"""Against a real DynamoDB Local, where `MBSE_DYNAMODB_LOCAL` gives its endpoint URL (README, Running it); skipped
otherwise. Each run uses a store of its own in the table `mbse-test-local`."""

from __future__ import annotations

import os
import uuid

import pytest
from mbse.Schemas.Framework import Builders, Plain

from mbse.Persistence.DynamoDB import Plugin
from mbse.Services.Rest import Persistence

from test_plugin import add_tasks, work_tracker

ENDPOINT = os.environ.get("MBSE_DYNAMODB_LOCAL")
pytestmark = pytest.mark.skipif(not ENDPOINT, reason="MBSE_DYNAMODB_LOCAL gives no DynamoDB Local endpoint")


@pytest.fixture
def local(monkeypatch):
    for name, value in {"AWS_ACCESS_KEY_ID": "local", "AWS_SECRET_ACCESS_KEY": "local"}.items():
        monkeypatch.setenv(name, value)
    store = f"test-{uuid.uuid4()}"
    return lambda: Plugin.Plugin("mbse-test-local", store=store, region="us-east-1", endpoint_url=ENDPOINT)


def test_a_store_outlives_its_plugin(local):
    plugin = local()
    store = work_tracker()
    assert plugin.load() == (None, [])
    plugin.snapshot(Plain.ToPlain(store).Snapshot())
    small = add_tasks(store, ["Small"], first=100)
    large = add_tasks(store, [f"Task {n}: " + "a long description of the work " * 400 for n in range(40)], first=200)
    plugin.journal(small)
    plugin.journal(large)
    restarted = local()
    snapshot, journal = restarted.load()
    assert journal == [small, large]
    replayed = Builders.Store()
    assert Persistence.replay(replayed, restarted)
    assert Plain.ToPlain(replayed).Snapshot() == Plain.ToPlain(store).Snapshot()
    restarted.snapshot(Plain.ToPlain(replayed).Snapshot())
    assert local().load() == (Plain.ToPlain(store).Snapshot(), [])
