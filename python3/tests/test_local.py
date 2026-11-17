"""Against a real DynamoDB Local, where `MBSE_DYNAMODB_LOCAL` gives its endpoint URL (README, Running it); skipped
otherwise. Each run uses stores of its own in the table `mbse-test-local`."""

from __future__ import annotations

import os
import uuid

import pytest
from mbse.Schemas.Framework import Builders

from mbse.Persistence.DynamoDB import Store
from test_store import above, append, crm, documents, lasts, overtaking, person, read, write

ENDPOINT = os.environ.get("MBSE_DYNAMODB_LOCAL")
pytestmark = pytest.mark.skipif(not ENDPOINT, reason="MBSE_DYNAMODB_LOCAL gives no DynamoDB Local endpoint")


@pytest.fixture
def local(monkeypatch):
    for name, value in {"AWS_ACCESS_KEY_ID": "local", "AWS_SECRET_ACCESS_KEY": "local"}.items():
        monkeypatch.setenv(name, value)


def kept(name, **options):
    """A store kept in DynamoDB Local: one process's."""
    return Store.Store("mbse-test-local", store=name, region="us-east-1", endpoint_url=ENDPOINT, **options)


def test_a_store_kept_in_dynamodb_local(local):
    """The store kept in DynamoDB (CAP-289), against DynamoDB Local: documents as the store in memory gives them, from
    a store started again; two writers appending after one last, the second refused; a writer overtaken at its commit
    interpreting again; index items written out after their entry."""
    name = f"store-{uuid.uuid4()}"
    memory = Builders.Store()
    memory.transaction(crm)
    write(memory)
    write(above(kept(name, max_actions=8)))
    assert documents(kept(name)) == documents(memory)

    first, second = above(kept(name)), above(kept(name))
    assert lasts(second) == ["second"]  # the last as the second sees it, before the first appends after it
    append(first, "n3", "one's")
    with pytest.raises(ValueError, match="breaks unique"):
        append(second, "n4", "two's")

    two = kept(name)
    app = above(two)
    overtaking(two, lambda: append(first, "n5", "first's"))
    with app.transaction():
        app.crm.Person(person(app, "ada"), lambda p: p.name("Ada K."))
    assert read(above(kept(name)), lambda s: person(s, "ada").name.get()) == "Ada K."
    assert [t["id"] for t in kept(name).journal()][-1] == kept(name).head()  # each a Changes.Transaction, by its id
