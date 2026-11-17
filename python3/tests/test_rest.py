"""mbse-rest's service over the store kept in DynamoDB (mbse-basis CAP-290), DynamoDB mocked by moto: the service's
store a `Builders.Store` over it, persisting by its journal alone, no snapshot taken or read; and mbse-rest's client, a
store over the service, stacked on as on any store below."""

from __future__ import annotations

import json

import pytest
from fastapi.testclient import TestClient
from mbse.Schemas.Framework import Builders, Changes, Plain

from mbse.Persistence.DynamoDB import Store as DynamoDB
from mbse.Services.Rest import Conformance, Service, Store

from conftest import REGION
from test_store import add, append, contacts, crm, lasts, person, read

TSK_1 = {"singleton": "work.Tasks", "next": {"entry": {"i": "tasks", "o": "directory", "properties": {"id": "TSK-001"},
                                                       "next": {"target": {}}}}}


def transport(api):
    """mbse-rest's client's transport to the service `api` serves, in this process: no network."""
    client = TestClient(api)

    def send(method, path, body):
        response = client.request(method, path, content=body, headers={"content-type": "application/json"})
        return response.status_code, response.content

    return send


def title(client, path):
    """The title the detail view gives the task at `path`."""
    store = client.app.state.service.store
    reference = lambda s: Plain.FromPlain(s)(s.References.OfObject.as_schema(), {"path": path})  # noqa: E731
    [line] = client.post("/views/detail", json=Conformance._map(store, lambda s: {"x": reference(s)})).text.splitlines()
    return next(a for a in json.loads(line)["assigns"] if "title" in a["properties"])["properties"]["title"]["str"]


def test_the_service_selects_the_store_kept_in_dynamodb(aws, monkeypatch):
    """The service as the environment configures it, its store below kept in DynamoDB by name: a first start, the table
    new, installs and seeds the WorkTracker, written to the journal; an edit is written to it; a start again, over the
    same table, is not seeded again, and reads the edit from the index, on demand. The store, made with no arguments,
    reads its own settings from the process's environment."""
    monkeypatch.setenv("MBSE_DYNAMODB_TABLE", "demo")
    monkeypatch.setenv("MBSE_DYNAMODB_REGION", REGION)
    environ = {"MBSE_REST_STORE": "mbse.Persistence.DynamoDB.Store:Store"}
    first = TestClient(Service.from_environment(environ))
    below = first.app.state.service.below
    assert isinstance(below, DynamoDB.Store) and [t["id"] for t in below.journal()] == ["work.seed/1"]
    store = first.app.state.service.store
    body = Conformance._edit(store, lambda s: Plain.FromPlain(s)(s.References.OfObject.as_schema(), {"path": TSK_1}),
                             {"title": "Kept in DynamoDB"})
    assert first.post("/views/detail", json=body).status_code == 200
    assert len(below.journal()) == 2
    again = TestClient(Service.from_environment(environ))
    assert again.get("/changes").json()["head"] is None  # not seeded again: the journal holds what it wrote
    assert title(again, TSK_1) == "Kept in DynamoDB"
    items = aws.scan(TableName="demo")["Items"]
    assert {item["key"]["S"][0] for item in items} == {"H", "L", "T", "O", "P"}  # the head, the journal and its
    # positions, the index and an entry's index items kept apart, the seed's being many: no snapshot


def test_two_services_over_one_store_kept_in_dynamodb(aws):
    """Two services, each a process of its own, over one store kept in DynamoDB, and a client of each, an app's store
    stacked over it: what one app writes reaches the other through the store below's journal, the other service
    polling it, and its change feed; an append overtaken by another's is refused, as the store below refuses it."""
    services = []
    for _ in range(2):
        kept = DynamoDB.Store("t", store="shared", region=REGION)
        services.append(Service.app([crm], kept))
    remotes = [Store.Remote(transport(api)) for api in services]
    here, there = (Builders.Store(remote) for remote in remotes)
    for app in (here, there):
        app.transaction(crm)
    add(here, "ada", "bob")
    seen = []
    Changes.observe(there, seen.append)
    assert sorted(contacts(there)) == ["ada", "bob"]  # loaded, on demand

    def ada(s):
        return read(s, lambda s: person(s, "ada").name.get())
    assert ada(there) == "ada"
    with here.transaction():
        here.crm.Person(person(here, "ada"), lambda p: p.name("Ada"))
    assert ada(there) == "ada"  # not yet read
    assert remotes[1].poll() == 1 and ada(there) == "Ada" and len(seen) == 1
    append(here, "n1", "first")
    remotes[1].poll()
    append(here, "n2", "here's")
    with pytest.raises(ValueError, match="breaks unique"):
        with there.transaction():  # after n1, as there saw it, before reading here's n2
            book = there.singleton("crm.TheBook")
            last = next(e.note for e in book.last)
            note = there.crm.Note(lambda n: n.book(lambda a: a.insert(lambda e: e.name("n3").book(book))).text("there's"))
            there.crm.Book(book, lambda b: b.last(lambda a: a.remove(lambda e: e.note(last)).insert(lambda e: e.note(note))))
    remotes[1].poll()
    assert lasts(there) == ["here's"]
    assert len(services[0].state.service.below.journal()) == 4  # ada and bob, Ada, n1, n2: n3 refused
