"""Against a real DynamoDB Local, where `MBSE_DYNAMODB_LOCAL` gives its endpoint URL (README, Testing it); skipped
otherwise. Each run uses stores of its own in the table `mbse-test-local`."""

from __future__ import annotations

import os
import uuid

import pytest
from mbse.Schemas.Framework.Transactions import Frame

from mbse.Persistence.DynamoDB import Store
from test_store import (after, appended, crm, first_note, frames, lasts, memory, overtaking, people, people_in_memory,
                        person, pet, unbooked)

ENDPOINT = os.environ.get("MBSE_DYNAMODB_LOCAL")
pytestmark = pytest.mark.skipif(not ENDPOINT, reason="MBSE_DYNAMODB_LOCAL gives no DynamoDB Local endpoint")


@pytest.fixture
def local(monkeypatch):
    for name, value in {"AWS_ACCESS_KEY_ID": "local", "AWS_SECRET_ACCESS_KEY": "local"}.items():
        monkeypatch.setenv(name, value)


def kept(name, **options):
    """A store kept in DynamoDB Local: one process's, its schemas installed."""
    store = Store.Store("mbse-test-local", store=name, region="us-east-1", endpoint_url=ENDPOINT, **options)
    store.transaction(crm)
    return store


def test_a_store_kept_in_dynamodb_local(local):
    """The store kept in DynamoDB (CAP-289), against DynamoDB Local: a transaction over several exec calls, committed
    in one DynamoDB transaction; the data a store in memory holds after the same transactions, read by another process,
    a deletion in cascade among them; index items written out after their journal transaction; two writers appending
    after one last, the second refused; a writer overtaken at its commit interpreting again, one held open refused."""
    name = f"store-{uuid.uuid4()}"
    one, twin = kept(name, max_actions=8), memory()
    with one.transaction() as txn:
        frames(txn.exec(Frame(), person("bob", "Bob"), True))
        frames(txn.exec(Frame(), person("ada", "Ada", 36, knows=["bob"]), True))
    for spec in (person("bob", "Bob"), person("ada", "Ada", 36, knows=["bob"])):
        twin.transaction(spec)
    for spec in (pet("bob", "rex", "dog"), person("cy", "Cy"), unbooked("cy"), first_note("n1", "first")):
        one.transaction(spec)
        twin.transaction(spec)
    assert people(kept(name)) == people_in_memory(twin)
    two = kept(name)
    one.transaction(after("n1", "n2", "one's"))
    with pytest.raises(ValueError, match="breaks unique"):
        two.transaction(after("n1", "n3", "two's"))
    overtaking(two, lambda: one.transaction(appended("n4", "one's again")))
    two.transaction(appended("n5", "two's"))
    assert lasts(kept(name)) == ["two's"] and [t["id"] for t in two.journal()][-2:] == ["append/n4", "append/n5"]
    with pytest.raises(Store.Conflict):
        with two.transaction() as txn:
            frames(txn.exec(Frame(), appended("n6", "two's again"), True))
            one.transaction(appended("n7", "one's last"))
    assert [t["id"] for t in kept(name).journal()][-1] == kept(name).head() == "append/n7"
