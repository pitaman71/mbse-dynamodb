"""mbse-journals' suite JRN (`tests/03_Journals.ipynb`, the journal conformance every backend passes unchanged,
mbse-basis CAP-288), run unchanged with each journal kept in DynamoDB (CAP-289): every store the suite makes,
`Builders.Store()`, is made over a store kept in DynamoDB of its own, `Builders.Store(Store(...))`, as an app stacks
one.

A store above does not send its schema installs below, since a transaction that writes only schemas reports no changes
(`Changes.observe`): a store below is given its schemas apart, as an app gives its stores theirs. So that the suite runs
unchanged, the store made in its place installs into the store below it each install it is given, before itself.
"""

from __future__ import annotations

import itertools

import pytest
from mbse.Schemas.Framework import Builders

from mbse.Persistence.DynamoDB.Store import Store

from conftest import REGION
from notebooks import cells, namespace

SUITE = cells("mbse-journals", "03_Journals.ipynb")
_stores = itertools.count()
InMemory = Builders.Store


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


@pytest.mark.parametrize("source", [pytest.param(source, id=case) for case, source in SUITE[1:]])
def test_jrn_case_kept_in_dynamodb(run, source):
    run(source)


def test_every_store_the_suite_made_is_kept_in_dynamodb(run, module_aws):
    """Each of the suite's stores wrote its journal to the table."""
    made = next(_stores)
    assert made >= 6  # the model, the journals, the replica, again, here and there
    heads = [module_aws.get_item(TableName="jrn", Key={"store": {"S": f"jrn-{n}"}, "key": {"S": "H"}})["Item"]
             for n in range(made)]
    assert all(int(head["seq"]["N"]) > 0 for head in heads)
