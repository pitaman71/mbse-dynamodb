"""mbse-journals' suite JRN (`tests/03_Journals.ipynb`, the journal conformance every backend passes unchanged,
mbse-basis CAP-288), run unchanged with each journal kept in DynamoDB (CAP-289): every store the suite makes,
`Builders.Store()`, is made over a store kept in DynamoDB of its own, `Builders.Store(Store(...))`, as an app stacks
one.

A store above does not send its schema installs below, since a transaction that writes only schemas reports no changes
(`Changes.observe`): a store below is given its schemas apart, as an app gives its stores theirs. So that the suite runs
unchanged, the store made in its place installs into the store below it each install it is given, before itself.

Blocked by mbse-schemas 0.26.16: a path resolves to the wrong object where an owner owns two objects through two
relations by equal keys, each through an owner adjacency of the same name, since `Plain._owned_by` passes over the
path step's own adjacency (`i`). Every journal transaction owns its first binding (`Journals.Binds`) and its first
change (`Journals.Has`) so, each `{"index": 0}` through an adjacency named `transaction`, and a store above, over any
store below, one in memory as well, resolves the one's path to the other when its own transaction comes back to it
(CAP-285). While the defect stands (`test_a_path_resolves_through_its_owner_adjacency`), the cases from JRN-02 are
expected to fail; once mbse-schemas resolves the path, each runs as any case does.
"""

from __future__ import annotations

import itertools

import pytest
from mbse.Schemas.Framework import Builders, Plain, References
from mbse.Journals.Framework import Journals

from mbse.Persistence.DynamoDB.Store import Store

from conftest import REGION
from notebooks import cells, namespace

SUITE = cells("mbse-journals", "03_Journals.ipynb")
_stores = itertools.count()
InMemory = Builders.Store


def resolves_through_its_owner_adjacency() -> bool:
    """Whether the path of a journal transaction's first change resolves to it, not to its first binding, in a store
    in memory."""
    store = InMemory()
    store.transaction(Journals.install)
    journal = Journals.journal(store, "probe")
    changes = {"mutations": [{"upsert": {"exists": [{"symbols": {"s0": {"reference": {"path": {
        "singleton": "Journals.Directory", "next": {"target": {}}}}}}, "spec": [{"s0": [{"as_property": {
        "name": "name", "mutations": [{"clear": {}}]}}]}]}]}}]}
    (held,) = Journals.append(journal, changes).changes
    reference = Plain.FromPlain(store)(store.registered("References.OfObject"), {"path": Plain._path_of(held.change)})
    return References.resolve(store, reference) is held.change


RESOLVED = resolves_through_its_owner_adjacency()
BLOCKED = pytest.mark.xfail(not RESOLVED, strict=True, reason="mbse-schemas resolves a journal transaction's first "
                            "change's path to its first binding (Plain._owned_by passes over the step's adjacency)")


class Stacked(InMemory):
    """`Builders.Store()`, made over a store kept in DynamoDB, of its own."""

    def __init__(self, below=None):
        super().__init__(below if below is not None else Store("jrn", store=f"jrn-{next(_stores)}", region=REGION))

    def transaction(self, spec=None):
        if Builders._is_function(spec):  # an install: the store below's schemas first
            self._below.transaction(spec)
        return super().transaction(spec)


@pytest.fixture(scope="module")
def run(module_aws):
    """Runs the setup cell, each store it makes kept in DynamoDB, then gives each case's cell to run, in order."""
    ((_, setup), *_) = SUITE
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(Builders, "Store", Stacked)
        scope = namespace()
        exec(setup, scope)
        yield lambda source: exec(source, scope)


@pytest.mark.xfail(strict=True, reason="mbse-schemas 0.26.16's Plain._owned_by passes over the step's adjacency")
def test_a_path_resolves_through_its_owner_adjacency():
    assert RESOLVED


@pytest.mark.parametrize("source", [pytest.param(source, id=case, marks=() if case == "JRN-01" else BLOCKED)
                                    for case, source in SUITE[1:]])
def test_jrn_case_kept_in_dynamodb(run, source):
    run(source)


@BLOCKED
def test_every_store_the_suite_made_is_kept_in_dynamodb(run, module_aws):
    """Each of the suite's stores wrote its journal to the table."""
    made = next(_stores)
    assert made >= 6  # the model, the journals, the replica, again, here and there
    heads = [module_aws.get_item(TableName="jrn", Key={"store": {"S": f"jrn-{n}"}, "key": {"S": "H"}})["Item"]
             for n in range(made)]
    assert all(int(head["seq"]["N"]) > 0 for head in heads)
