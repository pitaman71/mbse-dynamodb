"""The plugin's own cases (CAP-013), over the WorkTracker (mbse-demo-apps) in a mocked DynamoDB."""

from __future__ import annotations

import json

import pytest
from botocore.exceptions import ClientError
from mbse.Schemas.Framework import Builders, Plain

from mbse.Demos.Apps.WorkTracker import Schemas, Seed
from mbse.Persistence.DynamoDB import Plugin
from mbse.Services.Rest import Persistence

from conftest import REGION


def work_tracker():
    store = Builders.Store()
    store.transaction(Schemas.install)
    store.transaction(Seed.seed)
    return store


def add_tasks(store, titles, first=100):
    """A transaction that writes: a task for each title, each in the task directory under its ID. Executed, and its
    expression given, as the service would journal it."""
    W = store.work
    symbols = {"d": W.TaskDirectory, **{f"x{i}": W.Task for i in range(len(titles))}}
    tx = store.Mutations.OfTransaction(lambda t: t.mutations([lambda mu: mu.upsert(lambda u: u.exists([lambda p: p
        .symbols(symbols)
        .spec([
            lambda _: _.d.tasks({f"TSK-{first + i}": (lambda i: lambda e: e.task(getattr(_, f"x{i}")))(i)
                                 for i in range(len(titles))}),
            *[(lambda i, title: lambda _: getattr(_, f"x{i}").title(title))(i, title) for i, title in enumerate(titles)],
        ])]))]))
    plain = Plain.ToPlain(store).Transaction(tx)
    store.transaction(Plain.FromPlain(store).Transaction(plain))
    return plain


def replayed(plugin):
    store = Builders.Store()
    assert Persistence.replay(store, plugin)
    return store


def test_an_empty_table_loads_nothing(plugin, aws):
    assert isinstance(plugin, Persistence.Plugin)
    assert plugin.load() == (None, [])
    assert aws.describe_table(TableName="mbse-test")["Table"]["TableStatus"] == "ACTIVE"
    assert not Persistence.replay(Builders.Store(), plugin)


def test_the_journal_in_commit_order(plugin):
    store = work_tracker()
    given = [add_tasks(store, [f"Task {n}"], first=100 + n) for n in range(12)]
    assert plugin.load() == (None, [])
    for plain in given:
        plugin.journal(plain)
    assert plugin.load() == (None, given)


def test_a_snapshot_then_only_the_journal_after_it(plugin, aws):
    store = work_tracker()
    before = add_tasks(store, ["Before"], first=100)
    plugin.journal(before)
    taken = Plain.ToPlain(store).Snapshot()
    plugin.snapshot(taken)
    after = add_tasks(store, ["After"], first=101)
    plugin.journal(after)
    snapshot, journal = plugin.load()
    assert snapshot == taken and journal == [after]

    restarted = Plugin.Plugin("mbse-test", region=REGION)  # a restart: numbering goes on from what the table holds
    assert restarted.load() == (snapshot, [after])
    later = add_tasks(store, ["Later"], first=102)
    restarted.journal(later)
    assert restarted.load() == (snapshot, [after, later])
    assert Plain.ToPlain(replayed(restarted)).Snapshot() == Plain.ToPlain(store).Snapshot()


def test_the_latest_snapshot_wins(plugin):
    store = work_tracker()
    plugin.snapshot(Plain.ToPlain(store).Snapshot())
    plugin.journal(add_tasks(store, ["One"], first=100))
    plugin.snapshot(Plain.ToPlain(store).Snapshot())
    assert plugin.load() == (Plain.ToPlain(store).Snapshot(), [])


def test_a_large_snapshot_in_parts(plugin, aws):
    store = work_tracker()
    titles = [f"Task {n}: " + "a long description of the work " * 400 for n in range(40)]
    plugin.journal(add_tasks(store, titles))
    snapshot = Plain.ToPlain(store).Snapshot()
    size = len(json.dumps(snapshot).encode())
    assert size > 400_000  # more than one item holds
    plugin.snapshot(snapshot)
    items = aws.query(TableName="mbse-test", KeyConditionExpression="#s = :s AND begins_with(#k, :p)",
                      ExpressionAttributeNames={"#s": "store", "#k": "key"},
                      ExpressionAttributeValues={":s": {"S": "store"}, ":p": {"S": "S#"}})["Items"]
    assert len(items) == -(-size // Plugin.PART_BYTES) > 1
    assert plugin.load() == (snapshot, [])
    assert Plain.ToPlain(replayed(plugin)).Snapshot() == snapshot


def test_a_large_transaction_in_parts(aws):
    plugin = Plugin.Plugin("mbse-test", region=REGION, part_bytes=1000)  # every record in many parts
    store = work_tracker()
    plugin.snapshot(Plain.ToPlain(store).Snapshot())
    given = [add_tasks(store, [f"Task {n}" for n in range(30)], first=100), add_tasks(store, ["Small"], first=200)]
    for plain in given:
        plugin.journal(plain)
    assert plugin.load()[1] == given
    assert Plain.ToPlain(replayed(plugin)).Snapshot() == Plain.ToPlain(store).Snapshot()


def failing_presence(plugin, monkeypatch):
    """Makes the next record's part 0, its presence, fail to write, as a lost connection would."""
    put = plugin.client.put_item

    def put_item(**request):
        if request["Item"]["key"]["S"].endswith("#000000"):
            monkeypatch.setattr(plugin.client, "put_item", put)
            raise ClientError({"Error": {"Code": "InternalServerError", "Message": "lost"}}, "PutItem")
        return put(**request)
    monkeypatch.setattr(plugin.client, "put_item", put_item)


def test_a_failed_write_leaves_nothing(aws, monkeypatch):
    plugin = Plugin.Plugin("mbse-test", region=REGION, part_bytes=500)
    store = work_tracker()
    first = add_tasks(store, ["First"], first=100)
    plugin.journal(first)
    failing_presence(plugin, monkeypatch)
    with pytest.raises(ClientError):
        plugin.journal(add_tasks(store, [f"Lost {n}" for n in range(20)], first=200))  # its later parts are written
    failing_presence(plugin, monkeypatch)
    with pytest.raises(ClientError):
        plugin.snapshot(Plain.ToPlain(store).Snapshot())
    assert plugin.load() == (None, [first])
    assert Plugin.Plugin("mbse-test", region=REGION).load() == (None, [first])

    shorter = add_tasks(store, ["Shorter"], first=300)  # written over the lost parts, fewer of them
    plugin.journal(shorter)
    assert plugin.load() == (None, [first, shorter])


def test_a_second_writer_is_refused(aws):
    one, two = (Plugin.Plugin("mbse-test", region=REGION) for _ in range(2))
    store = work_tracker()
    assert one.load() == two.load() == (None, [])
    first = add_tasks(store, ["One's"], first=100)
    one.journal(first)
    with pytest.raises(RuntimeError, match="another writer"):
        two.journal(add_tasks(store, ["Two's"], first=101))
    assert one.load() == (None, [first])


def test_stores_share_a_table(aws):
    a = Plugin.Plugin("mbse-test", store="a", region=REGION)
    b = Plugin.Plugin("mbse-test", store="b", region=REGION)
    store = work_tracker()
    plain = add_tasks(store, ["A's"], first=100)
    a.journal(plain)
    a.snapshot(Plain.ToPlain(store).Snapshot())
    assert b.load() == (None, [])
    assert a.load() == (Plain.ToPlain(store).Snapshot(), [])


def test_configured_by_the_environment(aws):
    plugin = Plugin.Plugin.from_environment({"MBSE_DYNAMODB_TABLE": "from-env", "MBSE_DYNAMODB_STORE": "demo"})
    assert (plugin.table, plugin.store) == ("from-env", "demo")
    assert "from-env" in aws.list_tables()["TableNames"]
    absent = Plugin.Plugin.from_environment({"MBSE_DYNAMODB_TABLE": "absent", "MBSE_DYNAMODB_CREATE": "0"})
    with pytest.raises(ClientError, match="ResourceNotFound"):
        absent.load()
    assert not plugin.create_table()  # there already
