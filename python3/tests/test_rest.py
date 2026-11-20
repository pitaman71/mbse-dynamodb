"""mbse-rest's service serving a store kept in DynamoDB (mbse-basis CAP-290), DynamoDB mocked by moto: the service's
store is the store kept in DynamoDB itself, the lowest layer, whose transaction a client's transaction holds open
across its requests (`POST /transactions`, `/exec`, `/commit`, `DELETE`), persisting by its journal alone, no snapshot
taken or read; and mbse-rest's client, a cache layer over the service (`Store.Store`), in either follow mode."""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient
from mbse.Schemas.Framework import Plain
from mbse.Schemas.Framework.Transactions import Frame

from mbse.Persistence.DynamoDB import Store as DynamoDB
from mbse.Services.Rest import Service, Store

from conftest import REGION
from test_store import appended, crm, first_note, frames, lasts, people, person


def transport(api):
    """mbse-rest's client's transport to the service `api` serves, in this process: no network."""
    client = TestClient(api)

    def send(method, path, body):
        response = client.request(method, path, content=body, headers={"content-type": "application/json"})
        return response.status_code, response.content

    return send


def tasks(t):
    """A map of each task in the work tracker's directory, giving its title."""
    W = t.work
    return t.uid("tasks/1").mutations([lambda mu: mu.map(lambda m: m.symbols([lambda p: p.symbols(
        {"d": W.TaskDirectory, "x": W.Task}).spec([lambda _: _.d.tasks([_.x])])]))])


def retitled(title):
    """A transaction: the task TSK-001 retitled."""
    return lambda t: t.uid(f"retitled/{title}").mutations([lambda mu: mu.map(lambda m: m.symbols([lambda p: p.symbols(
        {"d": t.work.TaskDirectory, "x": t.work.Task}).spec([
            lambda _: _.d.tasks({"TSK-001": lambda e: e.task(_.x)})])])).then([lambda w: w.update(lambda u: u.apply([
                lambda p: p.symbols({"x": t.work.Task}).spec([lambda _: _.x.title(title)])]))])])


def titles(client):
    """Each task's id, read through the client, by its reference's path."""
    found = client.transaction(tasks)
    return sorted(Plain.ToPlain(client)(frame["x"].as_schema(), frame["x"])["path"]["next"]["entry"]["properties"]["id"]
                  for frame in found)


def test_the_service_serves_the_store_kept_in_dynamodb_by_name(aws, monkeypatch):
    """The service as the environment configures it, its store the store kept in DynamoDB, made by name with no
    arguments, reading its own settings from the process's environment: a first start, the table new, installs the
    WorkTracker's schemas, which the table does not hold, and seeds it, the seed journaled; an edit through a client
    is journaled; a start again, over the same table, is not seeded again, and its client reads the edit from the
    index."""
    monkeypatch.setenv("MBSE_DYNAMODB_TABLE", "demo")
    monkeypatch.setenv("MBSE_DYNAMODB_REGION", REGION)
    environ = {"MBSE_REST_STORE": "mbse.Persistence.DynamoDB.Store:Store"}
    first = Service.from_environment(environ)
    kept = first.state.service.store
    assert isinstance(kept, DynamoDB.Store) and [t["id"] for t in kept.journal()] == ["work.seed/1"]
    client = Store.Store(transport(first))
    assert titles(client) == ["TSK-001", "TSK-002"]
    client.transaction(retitled("Kept in DynamoDB"))
    assert [t["id"] for t in kept.journal()] == ["work.seed/1", "retitled/Kept in DynamoDB"]
    again = Service.from_environment(environ)
    assert [t["id"] for t in again.state.service.store.journal()] == ["work.seed/1", "retitled/Kept in DynamoDB"]
    later = Store.Store(transport(again))
    with later.transaction() as txn:
        (task,) = frames(txn.exec(Frame(), lambda t: t.uid("one/1").mutations([lambda mu: mu.map(lambda m: m.symbols([
            lambda p: p.symbols({"d": t.work.TaskDirectory, "x": t.work.Task}).spec([
                lambda _: _.d.tasks({"TSK-001": lambda e: e.task(_.x)}),
                lambda _: _.x.title("Kept in DynamoDB")])]))]), True))
    assert task.assigns["x"] == {"title": "Kept in DynamoDB"}
    keys = {item["key"]["S"][0] for item in first.state.service.store.client.scan(TableName="demo")["Items"]}
    assert keys == {"H", "L", "T", "X", "R", "E", "P"}  # the head, the journal and its positions, the index, and the
    # seed's index items, more than one DynamoDB transaction writes, kept beside its journal transaction: no snapshot


def test_a_client_transaction_spans_requests_in_one_transaction_of_the_table(aws):
    """A client's transaction, held open across its requests, is one transaction of the table's: each `exec` reads what
    those before it wrote, and none of it is in the table until the client commits; one aborted writes nothing."""
    kept = DynamoDB.Store("t", store="one", region=REGION)
    api = Service.app([crm], kept)
    client = Store.Store(transport(api))
    with client.transaction() as txn:
        frames(txn.exec(Frame(), person("ada", "Ada"), True))
        frames(txn.exec(Frame(), person("bob", "Bob", knows=["ada"]), True))
        assert kept.journal() == []
    assert [t["id"] for t in kept.journal()] == ["person/ada"]  # one transaction, under its first expression's uid
    assert people(kept) == [("Ada", None, [], []), ("Bob", None, ["Ada"], [])]
    with pytest.raises(ValueError, match="stop"):
        with client.transaction() as txn:
            frames(txn.exec(Frame(), person("cy", "Cy"), True))
            raise ValueError("stop")
    assert len(kept.journal()) == 1


def test_two_services_over_one_store_kept_in_dynamodb(aws):
    """Two services, each a process of its own, over one store kept in DynamoDB, and a client of each, a cache layer
    over it, in either follow mode: what one writes reaches the other through the table's journal, the other service
    catching up as its store's transaction begins, and through its change feed; a client's transaction overtaken at
    its commit is refused, to be run again (503); an append after a last another overtook breaks a unique clause."""
    services = [Service.app([crm], DynamoDB.Store("t", store="shared", region=REGION)) for _ in range(2)]
    here = Store.Store(transport(services[0]))
    there = Store.Store(transport(services[1]), follow=Store.POLLED)
    here.transaction(first_note("n1", "first"))
    there._below.poll()  # polled: what the other service committed, read from its change feed
    there.transaction(appended("n2", "there's"))
    here.transaction(appended("n3", "here's"))  # caught up: after there's
    kept = services[0].state.service.store
    assert lasts(kept) == ["here's"] and [t["id"] for t in kept.journal()] == ["note/n1", "append/n2", "append/n3"]
    with pytest.raises(Store.Unavailable, match="another store committed first"):
        with there.transaction() as txn:
            frames(txn.exec(Frame(), appended("n4", "there's again"), True))
            here.transaction(appended("n5", "here's again"))
    assert lasts(kept) == ["here's again"]
