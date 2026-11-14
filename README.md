# mbse-dynamodb

Persistence in DynamoDB for any mbse-schemas store, so that an app's data outlives its process, and is shared by
every process that opens it, with no app-specific storage code.

## Why

An app holds one store, mbse-schemas' `Builders.Store`, which loads what it is asked for from the store below it and
writes each transaction through to it (mbse-basis [the stacked stores proposal](https://github.com/pitaman71/mbse-basis/blob/main/proposals/STACKED-STORES.md)). This package is the lowest of
those stores: a store kept in DynamoDB, whose journal, each committed transaction's changes in commit order, is the
record, and which answers a read of one object from an index kept with it, never from a snapshot
([CAP-289: journals kept in DynamoDB](https://github.com/pitaman71/mbse-basis/blob/main/docs/capabilities/README.md#CAP-289), refining [CAP-013: an in-memory store journaled to DynamoDB](https://github.com/pitaman71/mbse-basis/blob/main/docs/capabilities/CAP-013.md)). DynamoDB orders the
transactions of every process that writes it, and each process's followers are given them in that order. It stores
only mbse-schemas' plain form, so it knows nothing of any app: the same table holds any store.

It depends on [mbse-schemas](https://github.com/pitaman71/mbse-schemas), and for tests on
[mbse-journals](https://github.com/pitaman71/mbse-journals), [mbse-rest](https://github.com/pitaman71/mbse-rest) and
[mbse-demo-apps](https://github.com/pitaman71/mbse-demo-apps)' WorkTracker, each a checkout beside this repository
pinned in `siblings.json`.

## Using it

```python
from mbse.Persistence.DynamoDB import Store
from mbse.Schemas.Framework import Builders, Changes

kept = Store.Store("mbse-store", store="crm", region="us-east-1")   # on AWS, with boto3's credentials
kept = Store.Store("mbse-store", store="crm", endpoint_url="http://localhost:8000", region="us-east-1")  # DynamoDB Local
kept = Store.Store()                                                 # every setting from MBSE_DYNAMODB_*
kept.transaction(install)          # its schemas, as any store is given them: they are not written to the table
app = Builders.Store(kept)         # the app's store, which loads on demand and writes through
app.transaction(install)
Changes.observe(kept, print)       # each committed transaction's changes, in commit order
kept.poll()                        # what other processes committed since, given to the app's store and to followers
```

`kept.document(path)` gives one object's document, `kept.journal(after)` the journal from any entry. A transaction
another process overtook at its commit is interpreted again, caught up, and refused where what the other committed
refuses it; one written in a `with kept.transaction():` block raises `Store.Conflict` instead, to be run again. The
table is made if absent (`create=False`, or `MBSE_DYNAMODB_CREATE=0`, not to); several stores share one table, each
under its own `store` name. How the table holds the journal, the index and the head is `Store`'s module docstring.

`Plugin`, mbse-rest's persistence plugin, journaled with occasional snapshots, is superseded by `Store`, and kept
while mbse-rest's service persists through it ([CAP-290: a service's store stacked, with no snapshot](https://github.com/pitaman71/mbse-basis/blob/main/docs/capabilities/README.md#CAP-290)): mbse-rest's service selects it
by `MBSE_REST_PLUGIN=mbse.Persistence.DynamoDB.Plugin:Plugin`, and its table, store and snapshots are as
[CAP-013: an in-memory store journaled to DynamoDB](https://github.com/pitaman71/mbse-basis/blob/main/docs/capabilities/CAP-013.md#the-dynamodb-plugin-its-table) built them.

## Testing it

```sh
cd python3 && uv sync && uv run coverage run -m pytest && uv run coverage report
```

The tests need no AWS account: DynamoDB is mocked by moto. Besides the store's own cases, they run mbse-schemas'
suite BLW with a store kept in DynamoDB as the store below, and mbse-journals' suite JRN unchanged with each of its
journals kept in DynamoDB, both from the sibling checkouts; and the plugin's cases and mbse-rest's persistence cases.
With DynamoDB Local running, also run the tests against it:

```sh
docker run -d --rm --name dynamodb-local -p 8000:8000 amazon/dynamodb-local    # DynamoDB on localhost:8000
MBSE_DYNAMODB_LOCAL=http://localhost:8000 uv run pytest tests/test_local.py
```
