"""Against a real DynamoDB Local, where `MBSE_DYNAMODB_LOCAL` gives its endpoint URL (README, Running it); skipped
otherwise. Each run uses stores of its own in the table `mbse-test-local`."""

from __future__ import annotations

import os
import uuid

import pytest
from mbse.Schemas.Framework import Builders

from mbse.Persistence.DynamoDB import Store
from test_store import above, append, crm, documents, overtaking, person, write

ENDPOINT = os.environ.get("MBSE_DYNAMODB_LOCAL")
pytestmark = pytest.mark.skipif(not ENDPOINT, reason="MBSE_DYNAMODB_LOCAL gives no DynamoDB Local endpoint")


@pytest.fixture
def local(monkeypatch):
    for name, value in {"AWS_ACCESS_KEY_ID": "local", "AWS_SECRET_ACCESS_KEY": "local"}.items():
        monkeypatch.setenv(name, value)


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
