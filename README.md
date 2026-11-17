# mbse-dynamodb

Persistence in DynamoDB for any mbse-schemas store, so that an app's data outlives its process, and is shared by
every process that opens it, with no app-specific storage code.

## Why

An app holds one store, mbse-schemas' `Builders.Store`, which loads what it is asked for from the store below it and
writes each transaction through to it (mbse-basis [the stacked stores proposal](https://github.com/pitaman71/mbse-basis/blob/main/proposals/STACKED-STORES.md)). This package is the lowest of
those stores: a store kept in DynamoDB, whose journal, each committed transaction's changes in commit order, a
`Changes.Journal` as mbse-schemas' follow gives it, is the record, and which answers a read of one object from an index kept with it, never from a snapshot
([CAP-289: journals kept in DynamoDB](https://github.com/pitaman71/mbse-basis/blob/main/docs/capabilities/README.md#CAP-289), refining [CAP-013: an in-memory store journaled to DynamoDB](https://github.com/pitaman71/mbse-basis/blob/main/docs/capabilities/CAP-013.md)). DynamoDB orders the
transactions of every process that writes it, and each process's followers are given them in that order. It stores
only mbse-schemas' plain form, so it knows nothing of any app: the same table holds any store.

It depends on [mbse-schemas](https://github.com/pitaman71/mbse-schemas) and
[mbse-journals](https://github.com/pitaman71/mbse-journals), whose positions slice its journal, and for tests on
[mbse-rest](https://github.com/pitaman71/mbse-rest) and
[mbse-demo-apps](https://github.com/pitaman71/mbse-demo-apps)' WorkTracker, each a checkout beside this repository
pinned in `siblings.json`.

mbse-rest's service serves a store over one kept in DynamoDB, as the store below it
([CAP-290: a service's store stacked, with no snapshot](https://github.com/pitaman71/mbse-basis/blob/main/docs/capabilities/README.md#CAP-290)), selected by `MBSE_REST_STORE=mbse.Persistence.DynamoDB.Store:Store`, the store
made with no arguments, its settings from `MBSE_DYNAMODB_*`.

## Using it

```python
from mbse.Persistence.DynamoDB import Store
from mbse.Schemas.Framework import Builders

kept = Store.Store("mbse-store", store="crm", region="us-east-1")   # on AWS, with boto3's credentials
kept = Store.Store("mbse-store", store="crm", endpoint_url="http://localhost:8000", region="us-east-1")  # DynamoDB Local
kept = Store.Store()                                                 # every setting from MBSE_DYNAMODB_*
app = Builders.Store(kept)         # the app's store, which loads on demand and writes through
app.transaction(install)           # its schemas, run in the store kept in DynamoDB first: not written to the table
kept.follow(print)                 # each committed transaction, in commit order, a one-transaction journal
kept.poll()                        # what other processes committed since, given to the app's store and to followers
```

The store kept in DynamoDB is a store below, mbse-schemas' public `Stores.Below`
([CAP-306: a public store-below interface](https://github.com/pitaman71/mbse-basis/blob/main/docs/capabilities/README.md#CAP-306)), not a `Builders.Store`: an app reads and writes it
through a store over it. `kept.document(path)` gives one object's document, `kept.journal(after)` the journal, a
`Changes.Journal` as plain data, after any position, a transaction's `id`, and `kept.head()` the last position. A transaction another process overtook at its commit is interpreted again, caught up, and refused where what
the other committed refuses it; after `attempts` times running, it raises `Store.Conflict`. The table is made if absent
(`create=False`, or `MBSE_DYNAMODB_CREATE=0`, not to); several stores share one table, each under its own `store`
name. How the table holds the journal, the index and the head is `Store`'s module docstring. mbse-dynamodb 0.3.0 keeps
each transaction of the journal as a `Changes.Transaction`, where earlier versions kept a mutation transaction: a table
written before 0.3.0 is refused where its journal is read, though its index is read as before.

## Testing it

```sh
cd python3 && uv sync && uv run coverage run -m pytest && uv run coverage report
```

The tests need no AWS account: DynamoDB is mocked by moto. Besides the store's own cases, they run mbse-schemas'
suite BLW with a store kept in DynamoDB as the store below, from the sibling checkout, and mbse-rest's service over a
store kept in DynamoDB.
With DynamoDB Local running, also run the tests against it:

```sh
docker run -d --rm --name dynamodb-local -p 8000:8000 amazon/dynamodb-local    # DynamoDB on localhost:8000
MBSE_DYNAMODB_LOCAL=http://localhost:8000 uv run pytest tests/test_local.py
```
