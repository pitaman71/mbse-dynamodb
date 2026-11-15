"""mbse-schemas' suite BLW (`tests/36_Below.ipynb`, a store given a store below, mbse-basis CAP-283 to CAP-285), each case
as mbse-schemas wrote it, run with a store kept in DynamoDB as the store below in place of the store in memory: its
setup's `below = B.Store()` alone is changed (CAP-289)."""

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
    exec(setup.replace(IN_MEMORY, "below = kept_in_dynamodb()\n"), scope)
    return lambda source: exec(source, scope)


@pytest.mark.parametrize("source", [source for case, source in SUITE[1:]], ids=[case for case, _ in SUITE[1:]])
def test_blw_case_over_dynamodb(run, source):
    run(source)
