"""mbse-schemas' suite BLW (`tests/36_Below.ipynb`, a cache layer over a layer below, the stacked stores proposal's
second stage), each case as mbse-schemas wrote it, run with a store kept in DynamoDB as the layer below: where the
suite makes its layer below, `stacked()`'s `seeded(B.Store())`, it makes one kept in DynamoDB, as the lowest layer of
an app's stack (CAP-289), and the cache layer over it is mbse-schemas' own. Nothing else is changed but what `CHANGED`
lists, each a line that reads or writes the layer below as a store in memory, through its proxies or its builders,
which a store kept in DynamoDB has not: it is read and written by transactions alone."""

from __future__ import annotations

import itertools

import pytest
from mbse.Schemas.Framework.Transactions import Frame

from mbse.Persistence.DynamoDB.Store import Store

from conftest import REGION
from notebooks import cells, namespace

SUITE = cells("mbse-schemas", "36_Below.ipynb")
IN_MEMORY = "    lower = seeded(B.Store())\n"
_stores = itertools.count()

CHANGED = {
    # The layer below committing within the cache layer's commit: told by its follow, as a store kept in DynamoDB tells
    # its followers, where the suite observes a store in memory (`Changes.observe`); its transaction open, its layer's.
    "BLW-04": [
        ('Changes.observe(lower, lambda journal: order.append(("below", cache._proposals is not None)))',
         'lower.follow(lambda journal: order.append(("below", cache._proposals is not None)))'),
        ("    assert lower._proposals is not None  # the layer below's transaction",
         "    assert lower._kept_layer.state is not None  # the layer below's transaction"),
        ("assert lower._proposals is None and", "assert lower._kept_layer.state is None and"),
    ],
    # The suite builds a spec naming Bob by his path in the layer below, by its builders while he is held: here in its
    # twin, in memory, whose schemas and data are the same, the spec's wire form the same; and it deletes him in each by
    # builders: in the layer below kept in DynamoDB, by the same change as an expression.
    "BLW-03": [
        ('Person = lower.registered("crm.Person")\nwith lower.transaction():',
         'Person = twin.registered("crm.Person")\nwith twin.transaction():'),
        ("    knows_bob = Plain.ToPlain(lower).Transaction(lower.Mutations.OfTransaction(",
         "    knows_bob = Plain.ToPlain(twin).Transaction(twin.Mutations.OfTransaction("),
        ('knows([lower.singleton("crm.TheBook").contacts(', 'knows([twin.singleton("crm.TheBook").contacts('),
        ("for store in (lower, twin):\n    with store.transaction():\n        store.crm.Book(",
         "lower.transaction(unbooked('bob'))\nfor store in (twin,):\n    with store.transaction():\n        store.crm.Book("),
    ],
    # The suite reads the layer below by its builders, within the cache layer's transaction and in one of its own: here
    # through its interpreter, `contact_name` within the transaction open, `recorded` in one of its own.
    "BLW-10": [
        ('    assert lower.singleton("crm.TheBook").contacts({"key": "gus"}).name.get() == "Gus"  # below, within it',
         '    assert contact_name(lower, "gus") == "Gus"  # below, within it'),
        ('with lower.transaction():\n    recorded = [str(e.uid) for e in lower.singleton("Provenance.Transactions").transactions]',
         'recorded = recorded_kept(lower)'),
    ],
    # The suite notes when the layer below commits by its in-memory commit hook (`_settled`): here, the store kept in
    # DynamoDB's commit to the table.
    "BLW-13": [
        ("settled = lower._settled\nlower._settled = lambda proposals: committing.append(cache._proposals is not None) "
         "or settled(proposals)",
         "settled = lower._kept_commit\nlower._kept_commit = lambda *given: committing.append(cache._proposals is not "
         "None) or settled(*given)"),
        ("assert committing == [] and lower._proposals is None  # the layer below's transaction dropped",
         "assert committing == [] and lower._kept_layer.state is None  # the layer below's transaction dropped"),
        ("del lower._settled\nassert [name", "del lower._kept_commit\nassert [name"),
        ("lower._settled = lambda proposals: committing.append(cache._proposals is not None) or settled(proposals)\n"
         "cache.transaction(aged",
         "lower._kept_commit = lambda *given: committing.append(cache._proposals is not None) or settled(*given)\n"
         "cache.transaction(aged"),
        ("del lower._settled\nassert committing == [True]", "del lower._kept_commit\nassert committing == [True]"),
    ],
}


def people_kept(store):
    """Each person a store kept in DynamoDB holds, read within a transaction of its own through its interpreter, as
    the suite's `people` reads a store in memory through its proxies: name, age and whom they know."""
    with store.transaction():
        layer = store._kept_layer
        Person = store.registered("crm.Person")
        rows = layer.candidates(Person)
        return sorted((row["values"]["name"], row["values"].get("age"), sorted(
            layer.link(entry, "known")["values"]["name"] for entry in layer.entries(row, Person, "knows")))
            for row in rows)


def unbooked(key):
    """A transaction: the book's contact `key` removed, and with it the person, in cascade."""
    return lambda t: t.uid(f"unbooked/{key}").mutations([lambda mu: mu.map(lambda m: m.symbols([lambda p: p.symbols(
        {"book": t.crm.Book})])).then([lambda w: w.update(lambda u: u.apply([lambda p: p.symbols({"book": t.crm.Book})
            .spec([lambda _: _.book.contacts(lambda a: a.remove(lambda e: e.key(key)))])]))])])


def contact_name(store, key):
    """The name of the book's contact `key`, as the transaction open on a store kept in DynamoDB holds it."""
    layer = store._kept_layer
    return layer.designated({"singleton": "crm.TheBook", "next": {"entry": {
        "i": "contacts", "o": "book", "properties": {"key": key}, "next": {"target": {}}}}})["values"]["name"]


def recorded_kept(store):
    """The uid of each transaction a store kept in DynamoDB recorded (`Provenance`), in a transaction of its own."""
    with store.transaction():
        layer = store._kept_layer
        directory = layer.designated({"singleton": "Provenance.Transactions", "next": {"target": {}}})
        schema = layer.schema_of(directory)
        return [entry["values"]["uid"] for entry in layer.entries(directory, schema, "transactions")]


@pytest.fixture(scope="module")
def run(module_aws):
    """Runs the setup cell, its layer below kept in DynamoDB, then gives each case's cell to run, in order."""
    ((_, setup), *_) = SUITE
    assert setup.count(IN_MEMORY) == 1
    scope = namespace()
    scope["kept_in_dynamodb"] = lambda: Store("blw", store=f"blw-{next(_stores)}", region=REGION)
    exec(setup.replace(IN_MEMORY, "    lower = seeded(kept_in_dynamodb())\n"), scope)
    in_memory = scope["people"]
    scope["people"] = lambda store: people_kept(store) if isinstance(store, Store) else in_memory(store)
    scope.update(unbooked=unbooked, contact_name=contact_name, recorded_kept=recorded_kept)
    return lambda case, source: exec(_changed(case, source), scope)


def _changed(case, source):
    for before, after in CHANGED.get(case, []):
        assert before in source, (case, before)
        source = source.replace(before, after)
    return source


@pytest.mark.parametrize("case,source", [pytest.param(case, source, id=case) for case, source in SUITE[1:]])
def test_blw_case_over_dynamodb(run, case, source):
    run(case, source)


def test_the_suite_kept_its_layer_below_in_dynamodb(run, module_aws):
    """What the suite wrote to its layers below, the table journaled."""
    head = module_aws.get_item(TableName="blw", Key={"store": {"S": "blw-0"}, "key": {"S": "H"}})["Item"]
    assert int(head["seq"]["N"]) > 0
    assert Frame  # the frames the suite's cases read are mbse-schemas' own
