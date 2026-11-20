# mbse-dynamodb

Persistence in DynamoDB for any mbse-schemas store, so that an app's data outlives its process, and is shared by
every process that opens it, with no app-specific storage code.

## Why

An app holds a stack of stores, each a layer over the one below, a transaction going down the stack and its frames
coming back up (mbse-basis [the stacked stores proposal](https://github.com/pitaman71/mbse-basis/blob/main/proposals/STACKED-STORES.md#a-cache-layer-and-the-layer-below)). This package is the lowest of
those layers: a store kept in DynamoDB, whose journal, each committed transaction's changes in commit order, a
`Changes.Journal` as mbse-schemas' follow gives it, is the record, and which answers a read of one object from an index
kept with it, never from a snapshot
([CAP-289: journals kept in DynamoDB](https://github.com/pitaman71/mbse-basis/blob/main/docs/capabilities/README.md#CAP-289), refining [CAP-013: an in-memory store journaled to DynamoDB](https://github.com/pitaman71/mbse-basis/blob/main/docs/capabilities/CAP-013.md)). It interprets
each transaction with an interpreter of its own, a class of mbse-schemas' `Transactions.Interpreter`, whose reads come
from the index and whose writes are held, within one transaction of the table's, until one DynamoDB transaction commits
all of it: DynamoDB orders the transactions of every process that writes it, and each process's followers are given
them in that order. It stores only mbse-schemas' plain form, so it knows nothing of any app: the same table holds any
store.

It depends on [mbse-schemas](https://github.com/pitaman71/mbse-schemas) and
[mbse-journals](https://github.com/pitaman71/mbse-journals), whose positions slice its journal, and for tests on
[mbse-rest](https://github.com/pitaman71/mbse-rest) and
[mbse-demo-apps](https://github.com/pitaman71/mbse-demo-apps)' WorkTracker, each a checkout beside this repository
pinned in `siblings.json`.

mbse-rest's service serves the store kept in DynamoDB itself, selected by
`MBSE_REST_STORE=mbse.Persistence.DynamoDB.Store:Store`, the store made with no arguments, its settings from
`MBSE_DYNAMODB_*`; a client's transaction is one transaction of the table's, held open across its requests
([CAP-290: a service's store stacked, with no snapshot](https://github.com/pitaman71/mbse-basis/blob/main/docs/capabilities/README.md#CAP-290)).

## Using it

```python
from mbse.Persistence.DynamoDB import Store
from mbse.Schemas.Framework import Builders
from mbse.Schemas.Framework.Transactions import Frame

kept = Store.Store("mbse-store", store="crm", region="us-east-1")   # on AWS, with boto3's credentials
kept = Store.Store("mbse-store", store="crm", endpoint_url="http://localhost:8000", region="us-east-1")  # DynamoDB Local
kept = Store.Store()                                                 # every setting from MBSE_DYNAMODB_*
kept.transaction(install)          # its schemas: run in the store's schemas alone, not written to the table
kept.transaction(spec)             # a transaction, executed whole, its frames given back; interpreted again if overtaken
with kept.transaction() as txn:    # a transaction held open: each exec reads what those before it wrote
    frames = list(txn.exec(Frame(), spec, True))
    more = list(txn.exec(frames[0], other, True))
                                   # ...then all of it in one DynamoDB transaction; Store.Conflict where overtaken
app = Builders.Store(kept)         # a cache layer over it, as an app holds one
kept.follow(print)                 # each committed transaction, in commit order, a one-transaction journal
kept.poll()                        # what other processes committed since, given to the followers
```

The store kept in DynamoDB is a `Builders.Store` holding the schemas alone, which each process gives it by its
installs, whose transaction is the table's, interpreted by its own interpreter (`Layer.Layer`): `txn.exec(frame,
remainder, allow_write)` as mbse-schemas' transaction gives it (`Transactions.Transaction`), its frames by their
references, each input frame's first frame carrying the journal of what it changed. At the scope's end, an owned object
made with no owner is refused, the objects a removal left with no owner deleted in cascade, the transaction recorded
(`Provenance`, where installed), unique clauses checked; then the journal transaction, its position, its index items
and the head go out in one conditional `TransactWriteItems`, on condition that the head is where the transaction
began. A transaction another process overtook is refused (`Store.Conflict`, a `RuntimeError`: nothing is written, and
it may be run again); one given whole, `kept.transaction(spec)`, is caught up and interpreted again, which what the
other committed may now refuse, up to `attempts` times. A transaction begins once the store has caught up, so that a
cache layer over it is given what others committed before each transaction of its own. `kept.journal(after)` gives the
journal after any position, a transaction's `id`, and `kept.head()` the last position. The table is made if absent
(`create=False`, or `MBSE_DYNAMODB_CREATE=0`, not to); several stores share one table, each under its own `store` name.
How the table holds the journal, the index and the head is `Store`'s module docstring; the layout of its keys is a
parameter (`Store.Layout`). mbse-dynamodb 0.4.0 keeps its index otherwise than 0.2 and 0.3 did: a table they wrote is
refused where the store is made.

## Decided by an agent, for review

Where the user's decisions were silent, the simplest choice consistent with them, each proposed until reviewed:

- **The index** is each object's row by its schema and an id given in the order objects are made (`X#<schema>#<oid>`),
  each owned object's path (`R#`), and each entry under every object it links (`E#<oid>#<eid>`), so that a symbol's
  candidates, an object by its reference and the entries linking an object are each one query for each schema. A
  symbol's candidates are the objects of its schema and of each schema refining it, as a store in memory's extent has
  them (one query for each), in the order they were made; an owned object is among them once it has an owner. An object's entries are read whole the first time one is read.
- **A conflict is the head moved**: a commit is conditioned on the head being where the transaction began, so that
  any commit made meanwhile refuses it, as mbse-dynamodb 0.3 did; the store catches up as each transaction begins, the
  follow mode of a cache layer over it in a process being to catch up always (mbse-rest's client keeps its own modes).
- **An install runs in the store's schemas alone, committed at once**, even within a transaction of the table's, which
  may then be dropped: the table holds no schemas. Data an install writes besides is not kept in the table.
- **What the store does not do**: give an owned object made with no owner one (mbse-schemas' store in memory generates
  its key: refused here); keep a parameter reference in an object's place, or an entry linking a schema object; keep a
  counted object that nothing links (no data the journal names); find a counted object an upsert made in an earlier
  transaction of the same uid (within one transaction, it does).
- **A transaction is recorded in the lowest layer** from what its first expression gives, its `uid`, `when`, `session`
  and `where`, else a uid of the store's, the time and `unknown`, as mbse-schemas' store in memory records one.
- **One transaction at a time** is open on a store; another, in the same process, is refused.

`Layer` and `Store` use mbse-schemas' public interface alone (CAP-306), which, since mbse-schemas 0.36.0, gives a layer
what every layer shares, one copy, each through its interpreter's hooks: `Transactions.Refused`, what `given` gives
for an object that has no reference; `Interpreter.check_uniques`, the unique clauses checked at commit;
`Transactions.single`, a relation holding one entry at most; `References.implied`, the schema a path implies;
`Changes.gather`, a transaction's journal from what it changed, net; `Provenance.record`, its record; and
`store.expressed`, an expression built from a callback given the store's schemas by name, so that `Changes.apply`
works within a callback given this store. `Layer` reads what a transaction changed, net, from its log, and gives it
`Changes.gather`; a counted object's values it leaves out, so that a counted object nothing links commits nothing. Its
transaction keeps the frames its `exec` gave to raise the first error of one (`Transaction.raised`), as mbse-schemas'
private `Transaction._raise` does: not shared yet.

## Testing it

```sh
cd python3 && uv sync && uv run coverage run -m pytest && uv run coverage report
```

The tests need no AWS account: DynamoDB is mocked by moto. Besides the store's own cases, they run mbse-schemas' suite
BLW with a store kept in DynamoDB as the layer below a cache layer, from the sibling checkout, and mbse-rest's service
serving a store kept in DynamoDB. With DynamoDB Local running, also run the tests against it:

```sh
docker run -d --rm --name dynamodb-local -p 8000:8000 amazon/dynamodb-local    # DynamoDB on localhost:8000
MBSE_DYNAMODB_LOCAL=http://localhost:8000 uv run pytest tests/test_local.py
```
