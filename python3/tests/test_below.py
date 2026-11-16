"""mbse-schemas' suite BLW (`tests/36_Below.ipynb`, a store given a store below, mbse-basis CAP-283 to CAP-285), each case
as mbse-schemas wrote it, run with a store kept in DynamoDB at the bottom of its stacks: its setup's `below = B.Store()`
alone is changed, to a store over one kept in DynamoDB, as an app holds one (CAP-289), since the suite reads and writes
its store below as a `Builders.Store`, which a store kept in DynamoDB is not: it is a `Stores.Below` (CAP-306)."""

from __future__ import annotations

import itertools

import pytest

from mbse.Persistence.DynamoDB.Store import Store

from conftest import REGION
from notebooks import cells, namespace

SUITE = cells("mbse-schemas", "36_Below.ipynb")
IN_MEMORY = "below = B.Store()\n"
_stores = itertools.count()


@pytest.fixture(scope="module")
def run(module_aws):
    """Runs the setup cell, its store below one kept in DynamoDB, then gives each case's cell to run, in order."""
    ((_, setup), *_) = SUITE
    assert setup.count(IN_MEMORY) == 1
    scope = namespace()
    scope["kept_in_dynamodb"] = lambda: Store("blw", store=f"blw-{next(_stores)}", region=REGION)
    exec(setup.replace(IN_MEMORY, "below = B.Store(kept_in_dynamodb())\n"), scope)
    return lambda source: exec(source, scope)


@pytest.mark.parametrize("source", [source for case, source in SUITE[1:]], ids=[case for case, _ in SUITE[1:]])
def test_blw_case_over_dynamodb(run, source):
    run(source)


def test_the_suite_kept_its_store_in_dynamodb(run, module_aws):
    """What the suite wrote to its store below, the table journaled."""
    head = module_aws.get_item(TableName="blw", Key={"store": {"S": "blw-0"}, "key": {"S": "H"}})["Item"]
    assert int(head["seq"]["N"]) > 0
