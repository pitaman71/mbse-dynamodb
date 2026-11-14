"""Against a real DynamoDB Local, where `MBSE_DYNAMODB_LOCAL` gives its endpoint URL (README, Running it); skipped
otherwise. Each run uses stores of its own in the table `mbse-test-local`."""

from __future__ import annotations

import os
import uuid

import pytest
from mbse.Schemas.Framework import Builders, Plain

from mbse.Persistence.DynamoDB import Plugin, Store
from mbse.Services.Rest import Persistence

from test_plugin import add_tasks, work_tracker
from test_store import above, append, crm, documents, overtaking, person, write

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


def kept(name, **options):
    """A store kept in DynamoDB Local, its schemas installed: one process's."""
    made = Store.Store("mbse-test-local", store=name, region="us-east-1", endpoint_url=ENDPOINT, **options)
    made.transaction(crm)
    return made


def test_a_store_kept_in_dynamodb_local(local):
    """The store kept in DynamoDB (CAP-289), against DynamoDB Local: documents as the store in memory gives them, from
    a store started again; two writers appending after one last, the second refused; a writer overtaken at its commit
    interpreting again; index items written out after their entry."""
    name = f"store-{uuid.uuid4()}"
    memory = Builders.Store()
    memory.transaction(crm)
    write(memory)
    write(kept(name, max_actions=8))
    assert documents(kept(name)) == documents(memory)

    first, second = above(kept(name)), above(kept(name))
    append(first, "n3", "one's")
    with pytest.raises(ValueError, match="breaks unique"):
        append(second, "n4", "two's")

    two = kept(name)
    overtaking(two, lambda: append(first, "n5", "first's"))
    with two.transaction():
        two.crm.Person(person(two, "ada"), lambda p: p.name("Ada K."))
    assert person(above(kept(name)), "ada").name.get() == "Ada K."
