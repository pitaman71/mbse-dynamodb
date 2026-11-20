"""A store kept in DynamoDB: the lowest layer of a stack of stores, which interprets each transaction with an
interpreter of its own, coupled with it, reading and writing its index within the transaction
([CAP-289: journals kept in DynamoDB](mbse:mbse-basis/docs/capabilities/README.md#CAP-289), as
[the stacked stores proposal](mbse:mbse-basis/proposals/STACKED-STORES.md#a-cache-layer-and-the-layer-below), its third
stage, and [the journal storage proposal](mbse:mbse-basis/proposals/JOURNAL-STORAGE.md) state), written against
mbse-schemas' public interface.

`Store(table, store=name)` is kept in one partition of a DynamoDB table. It is a `Builders.Store` holding the schemas
alone, which each process gives it by its installs, and its transaction is one of the table's:

- `with store.transaction() as txn:` opens one: `txn.exec(frame, remainder, allow_write)` interprets the suffix of an
  expression over input frames, as any layer's transaction does (`Transactions.Transaction`), by the store's
  interpreter (`Layer`), whose reads come from the index within the transaction and whose writes it holds, read again
  by the reads after them. The scope's end commits it: an owned object made with no owner is refused, the objects a
  removal left with no owner deleted in cascade, the transaction recorded (`Provenance`, where installed), unique
  clauses checked (CAP-293); then what it changed, a `Changes.Transaction`, the journal's next transaction, its position,
  its index items and the head go out in one DynamoDB transaction (`TransactWriteItems`), on condition that the head is
  where the transaction began, so that a commit another store made meanwhile is detected: the transaction is refused
  (`Conflict`), to be run again by its writer, since its frames have been given. Where the scope raises, nothing is
  written. Several `exec` calls are one transaction.
- `store.transaction(expression)`, or a callback building one, executes it within a transaction of its own, over one
  empty frame, writing, its frames given back; where another store committed first, it is caught up and interpreted
  again, up to `attempts` times, then refused (`Conflict`): what the other committed may now refuse it.
- An install, an expression whose upsert or update binds a symbol of a meta-schema, run from its root, is run in the
  store's schemas alone, committed at once: the table holds no schemas.
- `follow(follower)` gives `follower` each transaction committed, in commit order, as a one-transaction
  `Changes.Journal` as plain data: this store's own as it commits them, and others', committed by other processes, as
  it catches up: when a transaction of it begins, and on `poll()`. A cache layer over it, `Builders.Store(store)`, is
  so given what others committed before each transaction of its own, and what its own committed within its commit.
- `journal(after)` gives the journal after any position, a transaction's `id`, sliced as mbse-journals slices one
  (`Journals.after`); `head()` the last position.

The table holds, under the store's partition key, as `Layout` names them (a mapping given as a parameter, as
[the journal storage proposal](mbse:mbse-basis/proposals/JOURNAL-STORAGE.md#decided) has it):

- `H`, the head: `seq`, the number of the journal's last transaction, and `indexed`, the last whose index items are
  written.
- `L#<n>#<part>`, the journal's transaction `n` from 1, a `Changes.Transaction` as plain data, UTF-8 JSON in parts: the
  record.
- `T#<id>`, by a hash of a transaction's `id`, its number: a position, from which a follower reads on.
- The index, from which the store answers a read of one object, written only with the journal transaction it follows
  from, never sent, read whole or moved between stores, so that it is no snapshot:
  - `X#<schema>#<oid>#<part>`, each object, by its schema and an `oid` of its own, given in the order objects are made:
    its path and its property values, plain;
  - `R#<path>`, by a hash of each owned object's path, its `oid` and schema;
  - `E#<oid>#<eid>`, each entry, under every object it links: its relation, its links, each an object by its schema and
    `oid`, and its property values; `eid`s are given in the order entries are added.
- `P#<n>#<part>`, the index items of the journal's transaction `n`, where they are more than one DynamoDB transaction
  writes beside it: written with it, then written out, by the writer or by whichever store reads next, before `indexed`
  reaches `n`.

A table written by mbse-dynamodb before 0.4.0, whose index is kept otherwise, is refused where the store is made.
"""

from __future__ import annotations

import contextlib
import json
import os
from collections.abc import Callable, Iterator, Mapping
from typing import Any

import boto3
from botocore.exceptions import ClientError
from mbse.Journals.Framework import Journals
from mbse.Schemas.Framework import Builders, Plain
from mbse.Schemas.Framework.Transactions import Frame

from .Layer import Layer, Transaction, digest, installs
from .Table import create_table, environment

__all__ = ["Store", "Layout", "Conflict", "PART_BYTES", "MAX_ACTIONS", "MAX_BYTES"]

PART_BYTES = 350_000  # a journal entry's part, or an object's, leaving room under DynamoDB's 400 KB item for its key
MAX_ACTIONS = 100  # DynamoDB's limit on the items one TransactWriteItems writes
MAX_BYTES = 3_500_000  # below DynamoDB's 4 MB limit on one TransactWriteItems, leaving room for keys
WIDTH, PART_WIDTH = 20, 6  # digits in an entry's number and a part's, so that sort keys sort as numbers
END = "~"  # sorts after every character a key holds after its prefix


class Conflict(RuntimeError):
    """Another store committed to the table first: a transaction given whole, each time it was interpreted again,
    `attempts` times running; a transaction held open, whose frames have been given, at once. Nothing it wrote is kept,
    and it may be run again."""


class _Conflict(Exception):
    """The head moved: another store committed first."""


class Layout:
    """The keys a store's items are kept under, within its partition: how the store lays itself out in the table, a
    mapping given as a parameter (`Store(layout=...)`), so that a layout changes without the store rewritten."""

    head = "H"
    journal = "L"
    position = "T"
    pending = "P"
    objects = "X"
    paths = "R"
    links = "E"

    def log(self, log: str, number: int, part: int) -> str:
        return f"{log}#{number:0{WIDTH}d}#{part:0{PART_WIDTH}d}"

    def transaction(self, position: str) -> str:
        return f"{self.position}#{digest(position)}"

    def extent(self, schema: str) -> str:
        """The prefix of the items of every object of `schema`."""
        return f"{self.objects}#{schema}#"

    def row(self, schema: str, oid: str) -> str:
        """The prefix of an object's items, its parts."""
        return f"{self.objects}#{schema}#{oid}#"

    def part(self, prefix: str, part: int) -> str:
        return f"{prefix}{part:0{PART_WIDTH}d}"

    def path(self, hashed: str) -> str:
        return f"{self.paths}#{hashed}"

    def entries(self, oid: str) -> str:
        """The prefix of the items of every entry linking the object `oid`."""
        return f"{self.links}#{oid}#"

    def entry(self, oid: str, eid: str) -> str:
        return f"{self.links}#{oid}#{eid}"

    def singleton(self, name: str) -> str:
        """A singleton's `oid`, which it has without being made."""
        return f"s{digest(name)[:24]}"


def _dumps(plain: Any) -> str:
    """Plain data as mbse-schemas wrote it, keys in its order (mbse-basis
    [PRI-024: the plain form, carried as written](mbse:mbse-basis/docs/PRINCIPLES.md#PRI-024))."""
    return json.dumps(plain, separators=(",", ":"), ensure_ascii=False, allow_nan=False)


def _size(actions: list[dict[str, Any]]) -> int:
    """About how many bytes a DynamoDB transaction's actions write."""
    total = 0
    for action in actions:
        ((_, body),) = action.items()
        for attribute in (body.get("Item") or body["Key"]).values():
            ((_, value),) = attribute.items()
            total += len(value) if isinstance(value, bytes) else len(str(value).encode())
    return total


class _Index:
    """The table's reads a store's interpreter makes: an object's items, or an entry's, by the prefix of their keys,
    and one item by its key, each strongly consistent."""

    def __init__(self, store: Store) -> None:
        self.store = store

    @property
    def layout(self) -> Layout:
        return self.store.layout

    @property
    def part_bytes(self) -> int:
        return self.store.part_bytes

    def items(self, prefix: str) -> Iterator[dict[str, Any]]:
        return self.store._kept_query(prefix, prefix + END)

    def item(self, key: str) -> dict[str, Any] | None:
        return self.store.client.get_item(TableName=self.store.table, Key=self.store._kept_key(key),
                                          ConsistentRead=True).get("Item")


class Store(Builders.Store):
    """A store kept in one partition, `store`, of the DynamoDB table `table`: a `Builders.Store` holding the schemas
    alone, whose transaction is the table's, interpreted by its own interpreter (`Layer`).

    Settings are as `Table.environment` reads them from `MBSE_DYNAMODB_*`, an argument overriding each: `region` and
    `endpoint_url` go to boto3 (`http://localhost:8000` for DynamoDB Local), `client`, where given, is used instead;
    where `create`, the table is made if absent. `attempts` bounds how many times a transaction given whole is
    interpreted again after another store committed first. `max_actions` and `max_bytes` bound what one DynamoDB
    transaction writes; `part_bytes` a journal entry's part, or an object's. `layout` names the keys."""

    def __init__(self, table: str | None = None, *, store: str | None = None, region: str | None = None,
                 endpoint_url: str | None = None, client: Any = None, create: bool | None = None,
                 environ: Mapping[str, str] | None = None, attempts: int = 8, max_actions: int = MAX_ACTIONS,
                 max_bytes: int = MAX_BYTES, part_bytes: int = PART_BYTES, layout: Layout | None = None) -> None:
        super().__init__()
        settings = environment(os.environ if environ is None else environ)
        self.table = table if table is not None else settings["table"]
        self.store_name = store if store is not None else settings["store"]
        region = region if region is not None else settings["region"]
        endpoint_url = endpoint_url if endpoint_url is not None else settings["endpoint_url"]
        self.client = client if client is not None else boto3.client(
            "dynamodb", region_name=region, endpoint_url=endpoint_url)
        if (create if create is not None else settings["create"]):
            create_table(self.client, self.table)
        self.attempts, self.max_actions, self.max_bytes, self.part_bytes = attempts, max_actions, max_bytes, part_bytes
        self.layout = layout or Layout()
        try:
            self.client.put_item(TableName=self.table, Item={**self._kept_key(self.layout.head), "seq": {"N": "0"},
                                                             "indexed": {"N": "0"}},
                                 ConditionExpression="attribute_not_exists(#k)", ExpressionAttributeNames={"#k": "key"})
        except ClientError as error:
            if error.response["Error"]["Code"] != "ConditionalCheckFailedException":
                raise
        if next(self._kept_query("O#", "O#" + END), None) is not None:  # mbse-dynamodb 0.2 and 0.3 kept documents
            raise ValueError(f"{self.table}/{self.store_name}: its index was written by mbse-dynamodb before 0.4.0, "
                             "which kept each object's document: this store cannot read it; keep the store anew")
        self._kept_seq = self._kept_head()[0]  # the last transaction its followers have been given
        self._kept_followers: dict[object, Callable[[Any], None]] = {}
        self._kept_layer = Layer(self, _Index(self))

    # --- Its transaction ---

    def transaction(self, spec: Any = None) -> Any:
        """With no `spec`, the scope of a transaction of the table's, giving it (`Layer.Transaction`): committed where
        the scope ends, refused (`Conflict`) where another committed first, nothing written where it raises. With a
        transaction's expression, or a callback building one, it executed within a transaction of its own over one
        empty frame, writing, its frames given back, interpreted again where another store committed first; an
        install run in the store's schemas alone."""
        if spec is None:
            return self._kept_scope(Conflict)
        expression = self.expressed(spec)
        if installs(self, Plain.ToPlain(self).Transaction(expression)):
            return Builders.Store.transaction(self, expression)
        for _ in range(self.attempts):
            try:
                with self._kept_scope(_Conflict) as txn:
                    frames = [item for item in txn.exec(Frame(), expression, True) if isinstance(item, Frame)]
                return frames
            except _Conflict:
                continue
        raise Conflict(f"{self.table}/{self.store_name}: other stores committed first {self.attempts} times running")

    @contextlib.contextmanager
    def _kept_scope(self, conflict: type[Exception]) -> Iterator[Transaction]:
        """A transaction of the table's: begun once this store has caught up, so that it commits as the journal's
        next; committed at the scope's end, or, where another committed first, `conflict` raised."""
        layer = self._kept_layer
        if layer.state is not None:
            raise ValueError(f"{self.table}/{self.store_name}: a transaction is open on this store: one at a time")
        self._kept_catch_up()
        txn = layer.begin(self._kept_seq + 1)
        journal = None
        try:
            yield txn
            txn.raised()
            layer.settle()
            layer.record()
            layer.uniques()
            changed = layer.changes(layer.held.log, layer.uid())
            journal = None if changed is None else Plain.ToPlain(self)(self.registered("Changes.Transaction"), changed)
            if journal is not None:
                try:
                    self._kept_commit(journal, layer.writes())
                except _Conflict as error:
                    raise conflict(f"{self.table}/{self.store_name}: another store committed first; nothing is written, "
                                   "and the transaction may be run again") from error
        finally:
            layer.end()
        if journal is not None:
            for follower in list(self._kept_followers.values()):
                follower([journal])

    # --- Its follow, and the journal ---

    def follow(self, follower: Callable[[Any], None]) -> Callable[[], None]:
        """Gives `follower` each transaction from now, in commit order, as a one-transaction journal (`Changes.Journal`,
        as plain data); gives back a function that ends it."""
        token = object()  # this following, though the same follower follow twice
        self._kept_followers[token] = follower

        def end() -> None:
            self._kept_followers.pop(token, None)

        return end

    def poll(self) -> int:
        """Gives the followers each transaction another store committed since this one last caught up, in commit order;
        how many. Nothing, while a transaction of this store's is open: it catches up as the next begins."""
        return 0 if self._kept_layer.state is not None else self._kept_catch_up()

    def journal(self, after: str | None = None) -> Journals.Journal:
        """The journal of the transactions committed, in commit order, a `Changes.Journal` as plain data: from the
        first, or after the transaction of id `after`, a position, as mbse-journals' `Journals.after` slices it; a
        position the table holds no transaction of is refused (`LookupError`)."""
        last = self._kept_head()[0]
        first = 1 if after is None else self._kept_number(after)
        return Journals.after([self._kept_entry(number) for number in range(first, last + 1)], after)

    def head(self) -> str | None:
        """The position of the last transaction committed, its id; None where none is."""
        last = self._kept_head()[0]
        return None if last == 0 else self._kept_entry(last)["id"]

    def _kept_catch_up(self) -> int:
        """Gives the followers each transaction committed since they were last given one."""
        last, count = self._kept_head()[0], 0
        while self._kept_seq < last:
            journal = [self._kept_entry(self._kept_seq + 1)]
            self._kept_seq += 1
            count += 1
            for follower in list(self._kept_followers.values()):
                follower(journal)
        return count

    def _kept_entry(self, number: int) -> Any:
        """The journal's transaction `number`, a `Changes.Transaction` as plain data; refused where the table holds it
        as mbse-dynamodb wrote it before 0.3.0, a mutation transaction."""
        transaction = self._kept_record(self.layout.journal, number)
        if not (isinstance(transaction, dict) and isinstance(transaction.get("id"), str)):
            raise ValueError(f"{self.table}/{self.store_name}: the journal's transaction {number} is no "
                             "Changes.Transaction, as mbse-dynamodb before 0.3.0 wrote it: this store cannot read it")
        return transaction

    def _kept_number(self, position: str) -> int:
        """The number of the journal's transaction of id `position`; refused (`LookupError`) where it holds none."""
        item = _Index(self).item(self.layout.transaction(position))
        if item is None:
            raise LookupError(f"{self.table}/{self.store_name}: the journal holds no transaction {json.dumps(position)}")
        return int(item["seq"]["N"])

    # --- The table ---

    def _kept_key(self, key: str) -> dict[str, Any]:
        return {"store": {"S": self.store_name}, "key": {"S": key}}

    def _kept_head(self) -> tuple[int, int]:
        """The head, `seq` and `indexed`, the index items of every entry written out first."""
        while True:
            item = _Index(self).item(self.layout.head)
            assert item is not None  # made with the store
            seq, indexed = int(item["seq"]["N"]), int(item["indexed"]["N"])
            if indexed == seq:
                return seq, indexed
            self._kept_write_out(indexed + 1)

    def _kept_query(self, low: str, high: str) -> Iterator[dict[str, Any]]:
        """Each item of the store whose key is from `low` to `high`, in key order, strongly consistent."""
        start = None
        while True:
            page = self.client.query(
                TableName=self.table, ConsistentRead=True,
                KeyConditionExpression="#s = :s AND #k BETWEEN :low AND :high",
                ExpressionAttributeNames={"#s": "store", "#k": "key"},
                ExpressionAttributeValues={":s": {"S": self.store_name}, ":low": {"S": low}, ":high": {"S": high}},
                **({"ExclusiveStartKey": start} if start else {}))
            yield from page.get("Items", [])
            start = page.get("LastEvaluatedKey")
            if not start:
                return

    def _kept_record(self, log: str, number: int) -> Any:
        """Entry `number` of the journal or of the index items written out after it, its parts joined."""
        first = self.layout.log(log, number, 0)
        return json.loads(b"".join(bytes(item["data"]["B"]) for item in self._kept_query(first, first[:-PART_WIDTH] + END)))

    def _kept_parts(self, log: str, number: int, plain: Any) -> list[dict[str, Any]]:
        encoded = _dumps(plain).encode()
        chunks = [encoded[i:i + self.part_bytes] for i in range(0, len(encoded), self.part_bytes)]
        return [{**self._kept_key(self.layout.log(log, number, part)), "data": {"B": chunk}}
                for part, chunk in enumerate(chunks)]

    def _kept_actions(self, writes: dict[str, str | None]) -> list[dict[str, Any]]:
        return [{"Put": {"TableName": self.table, "Item": {**self._kept_key(key), "data": {"S": data}}}}
                if data is not None else {"Delete": {"TableName": self.table, "Key": self._kept_key(key)}}
                for key, data in writes.items()]

    def _kept_advance(self, number: int, indexed: bool) -> dict[str, Any]:
        """The head moved to `number`, on condition that it is where the transaction began."""
        names, values = {"#seq": "seq", "#ix": "indexed"}, {":n": {"N": str(number - 1)}, ":n1": {"N": str(number)}}
        return {"Update": {"TableName": self.table, "Key": self._kept_key(self.layout.head),
                           "UpdateExpression": "SET #seq = :n1, #ix = :n1" if indexed else "SET #seq = :n1",
                           "ConditionExpression": "#seq = :n AND #ix = :n",
                           "ExpressionAttributeNames": names, "ExpressionAttributeValues": values}}

    def _kept_commit(self, transaction: Any, writes: dict[str, str | None]) -> None:
        """Commits a journal transaction, the journal's next, its position, and its index items, within one DynamoDB
        transaction where they fit, else its index items kept apart, written out after."""
        number = self._kept_seq + 1
        log = [{"Put": {"TableName": self.table, "Item": item}}
               for item in self._kept_parts(self.layout.journal, number, transaction)]
        log.append({"Put": {"TableName": self.table, "Item": {
            **self._kept_key(self.layout.transaction(transaction["id"])), "seq": {"N": str(number)}}}})
        together = [self._kept_advance(number, True), *log, *self._kept_actions(writes)]
        if len(together) <= self.max_actions and _size(together) <= self.max_bytes:
            self._kept_transact(together)
        else:
            pending = [{"Put": {"TableName": self.table, "Item": item}} for item in self._kept_parts(
                self.layout.pending, number, [[key, data] for key, data in writes.items()])]
            self._kept_transact([self._kept_advance(number, False), *log, *pending])
            self._kept_seq = number  # committed: its index items are written out by this store, or by the next to read
            with contextlib.suppress(ClientError):
                self._kept_write_out(number)
        self._kept_seq = number

    def _kept_transact(self, actions: list[dict[str, Any]]) -> None:
        if len(actions) > self.max_actions or _size(actions) > self.max_bytes:
            raise ValueError(f"{self.table}/{self.store_name}: a transaction's changes are more than one DynamoDB "
                             f"transaction writes ({len(actions)} items, {_size(actions)} bytes)")
        try:
            self.client.transact_write_items(TransactItems=actions)
        except ClientError as error:
            code = error.response["Error"]["Code"]
            reasons = [reason.get("Code") for reason in error.response.get("CancellationReasons", [])]
            if code == "TransactionCanceledException" and (reasons[:1] in (["ConditionalCheckFailed"],
                                                                            ["TransactionConflict"]) or not reasons):
                raise _Conflict() from error
            raise

    def _kept_write_out(self, number: int) -> None:
        """Writes the index items of the journal's transaction `number`, kept apart beside it, then moves `indexed` to
        it: by the store that committed it, or by whichever reads first, where that one did not finish. Writing them
        again changes nothing."""
        writes = self._kept_record(self.layout.pending, number)
        for i in range(0, len(writes), 25):
            requests = [{"PutRequest": {"Item": {**self._kept_key(key), "data": {"S": data}}}} if data is not None
                        else {"DeleteRequest": {"Key": self._kept_key(key)}} for key, data in writes[i:i + 25]]
            while requests:
                left = self.client.batch_write_item(RequestItems={self.table: requests}).get("UnprocessedItems", {})
                requests = left.get(self.table, [])
        try:
            self.client.update_item(TableName=self.table, Key=self._kept_key(self.layout.head),
                                    UpdateExpression="SET #ix = :n", ConditionExpression="#ix = :before",
                                    ExpressionAttributeNames={"#ix": "indexed"},
                                    ExpressionAttributeValues={":n": {"N": str(number)},
                                                               ":before": {"N": str(number - 1)}})
        except ClientError as error:
            if error.response["Error"]["Code"] != "ConditionalCheckFailedException":  # written out by another
                raise
