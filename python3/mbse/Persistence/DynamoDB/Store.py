"""A store kept in DynamoDB: the lowest of stacked stores, below a lazy `Builders.Store`
([CAP-289: journals kept in DynamoDB](mbse:mbse-basis/docs/capabilities/README.md#CAP-289), as
[the stacked stores proposal](mbse:mbse-basis/proposals/STACKED-STORES.md) and
[the journal storage proposal](mbse:mbse-basis/proposals/JOURNAL-STORAGE.md) state).

`Store(table, store=name)` is an mbse-schemas store (`Builders.Store`) over one partition of a DynamoDB table. It is
given its schemas as any store is, `store.transaction(install)`, and a store above it is made over it,
`Builders.Store(Store(...))`, as over a store in memory:

- `document(path)` gives the object at `path` as a one-transaction document, each object it links by its path, None
  where it holds none ([CAP-128: graphs sent as one-transaction documents](mbse:mbse-basis/docs/capabilities/CAP-128.md)),
  read from the index, one object's items.
- `transaction(...)` interprets a mutation-transaction stream with mbse-schemas' own interpreter, so that it refuses
  what any store refuses ([CAP-293: an entry inserted against a unique clause refuses its transaction](mbse:mbse-basis/docs/capabilities/README.md#CAP-293)),
  and commits it within one DynamoDB transaction (`TransactWriteItems`), which orders it: its journal entry, the index
  items it changes, and the head, the count of the journal's entries, on condition that no other has committed since
  this store last caught up. Where another has, the store catches up and interprets the transaction again: what the
  other committed may now refuse it.
- `Changes.observe(store, observer)` gives `observer` each committed transaction's changes, in commit order: this
  store's own as it commits them, and others', committed by other processes, as it catches up, before each of its
  transactions and on `poll()`, read from the table in order.

The table holds, under the store's partition key:

- `H`, the head: `seq`, the number of the last journal entry, and `indexed`, the last whose index items are written.
- `L#<n>#<part>`, journal entry `n` from 1: the transaction's changes as mbse-schemas writes them, the journal language
  ([CAP-271: the journal language](mbse:mbse-basis/docs/capabilities/README.md#CAP-271)), UTF-8 JSON in parts. It is the record.
- `O#<object>#V` and `O#<object>#E#<entry>`, the index, by a hash of each object's path: its schema and property values,
  and each entry that links it, under each object it links. It is how the database answers a read of one object: it
  is written only with the journal entry it follows from, and never sent, read whole or moved between stores, so that
  it is no snapshot.
- `P#<n>#<part>`, the index items of entry `n`, where they are too many for one DynamoDB transaction beside it: written
  with the entry, and then written out, by the writer or by whichever store reads next, before `indexed` reaches `n`.

Nothing here is a snapshot: a store that starts loads what it is asked for from the index, and a follower that starts
reads the journal from any entry (`journal`).
"""

from __future__ import annotations

import contextlib
import hashlib
import json
import os
from collections.abc import Iterator, Mapping
from typing import Any

import boto3
from botocore.exceptions import ClientError
from mbse.Schemas.Framework import Builders, Changes, Plain, Proxies

from .Table import create_table, environment

__all__ = ["Store", "Conflict", "PART_BYTES", "MAX_ACTIONS", "MAX_BYTES"]

PART_BYTES = 350_000  # a journal entry's part, leaving room under DynamoDB's 400 KB item for its key
MAX_ACTIONS = 100  # DynamoDB's limit on the items one TransactWriteItems writes
MAX_BYTES = 3_500_000  # below DynamoDB's 4 MB limit on one TransactWriteItems, leaving room for keys
HEAD = "H"
WIDTH, PART_WIDTH = 20, 6  # digits in an entry's number and a part's, so that sort keys sort as numbers
END = "~"  # sorts after every digit and '#'


class Conflict(RuntimeError):
    """Another store committed to the table while a transaction written in a `with store.transaction():` block was
    open: the store has caught up, and the block is to be run again. A transaction given as an expression is run again
    by the store itself."""


class _Conflict(Exception):
    """The head moved: another store committed first."""


def _canonical(plain: Any) -> str:
    return json.dumps(plain, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)


def _dumps(plain: Any) -> str:
    """Plain data as mbse-schemas wrote it, keys in its order (mbse-basis
    [PRI-024: the plain form, carried as written](mbse:mbse-basis/docs/PRINCIPLES.md#PRI-024))."""
    return json.dumps(plain, separators=(",", ":"), ensure_ascii=False, allow_nan=False)


def _hash(plain: Any) -> str:
    return hashlib.sha256(_canonical(plain).encode()).hexdigest()[:32]


def _log_key(log: str, number: int, part: int) -> str:
    return f"{log}#{number:0{WIDTH}d}#{part:0{PART_WIDTH}d}"


def _object_key(path: Any) -> str:
    return f"O#{_hash(path)}#"


class _Index:
    """The index, as the store below the DynamoDB store's own proxies: it gives an object's document. It has no
    `records`: the DynamoDB store, the lowest, records each transaction (`Builders.Store.records`)."""

    def __init__(self, store: Store) -> None:
        self.store = store

    def document(self, path: Any) -> Any:
        return self.store.document(path)


class Store(Builders.Store):
    """An mbse-schemas store kept in one partition, `store`, of the DynamoDB table `table`.

    Settings are as `Table.environment` reads them from `MBSE_DYNAMODB_*`, an argument overriding each: `region` and
    `endpoint_url` go to boto3 (`http://localhost:8000` for DynamoDB Local), `client`, where given, is used instead;
    where `create`, the table is made if absent. `attempts` bounds how many times a transaction given as an expression
    is interpreted again after another store committed first. `max_actions` and `max_bytes` bound what one DynamoDB
    transaction writes; `part_bytes` a journal entry's part."""

    def __init__(self, table: str | None = None, *, store: str | None = None, region: str | None = None,
                 endpoint_url: str | None = None, client: Any = None, create: bool | None = None,
                 environ: Mapping[str, str] | None = None, attempts: int = 8, max_actions: int = MAX_ACTIONS,
                 max_bytes: int = MAX_BYTES, part_bytes: int = PART_BYTES) -> None:
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
        self._below = _Index(self)  # its own objects load from the index, on demand (CAP-284)
        self._settling: Any = None  # the proposals of the transaction being committed (`Builders.Store._sending`
        # is another: how deep in sending one below, which this store, the lowest, never does)
        try:
            self.client.put_item(TableName=self.table, Item={**self._item_key(HEAD), "seq": {"N": "0"}, "indexed": {"N": "0"}},
                                 ConditionExpression="attribute_not_exists(#k)", ExpressionAttributeNames={"#k": "key"})
        except ClientError as error:
            if error.response["Error"]["Code"] != "ConditionalCheckFailedException":
                raise
        self._seq = self._head()[0]  # the last entry its objects and its followers have been given

    # --- Reads ---

    def document(self, path: Any) -> Any:
        """The document of the object at `path`, from the index, as of one entry of the journal; None where the store
        holds none. Read again where another store committed while it was read."""
        while True:
            seq = self._head()[0]
            items = list(self._query(_object_key(path), _object_key(path) + END))
            if self._head()[0] == seq:
                return self._render(path, items)

    def journal(self, after: int = 0) -> Iterator[Any]:
        """The changes of each transaction committed, in commit order, as plain data: from the first, or those after
        entry `after`."""
        last = self._head()[0]
        for number in range(after + 1, last + 1):
            yield self._record("L", number)

    def poll(self) -> int:
        """Gives this store's objects, and its followers, each transaction another store committed since it last
        caught up, in commit order; how many."""
        return self._catch_up()

    # --- Transactions ---

    def transaction(self, spec: Any = None) -> Any:
        """A transaction (`Builders.Store.transaction`), interpreted once the store has caught up, and committed within
        one DynamoDB transaction. Where another store commits first, one given as an expression is interpreted again,
        caught up, up to `attempts` times; one written in a `with` block raises `Conflict`."""
        if self._loading or self._applying or self._proposals is not None:  # within one of its own: no other
            return super().transaction(spec)
        if spec is None:
            return self._block()
        for _ in range(self.attempts):
            self._catch_up()
            try:
                return super().transaction(spec)
            except _Conflict:
                continue
        raise Conflict(f"{self.table}/{self.store_name}: other stores committed first {self.attempts} times running")

    @contextlib.contextmanager
    def _block(self) -> Iterator[None]:
        self._catch_up()
        try:
            with super().transaction():
                yield
        except _Conflict:
            self._catch_up()
            raise Conflict(f"{self.table}/{self.store_name}: another store committed first; caught up, run it again") from None

    def _settled(self, proposals: Any) -> None:
        self._settling = proposals
        try:
            super()._settled(proposals)
        finally:
            self._settling = None

    def _send(self, plain: Any, expression: Any) -> None:
        """Commits a transaction's changes, and the index items they change, within one DynamoDB transaction, or, where
        they are too many, its entry and its index items first and those items after (CAP-289). The lowest store, it
        records the transaction itself (`Provenance`, its record among the changes), and sends nothing below: its
        expression is not needed."""
        writes = self._writes(self._settling, self._seq + 1)
        self._commit(plain, writes)

    def _catch_up(self) -> int:
        """Applies to the objects present, and gives the followers, each entry committed since the last given."""
        last, count = self._head()[0], 0
        while self._seq < last:
            plain = self._record("L", self._seq + 1)
            self._seq += 1
            count += 1
            self._deliver(plain)
        return count

    def _deliver(self, plain: Any) -> None:
        """Another store's transaction: applied to the objects present, as a store above applies the store below's
        (CAP-285), its followers given the whole of it, as committed."""
        observers, self._observers = self._observers, []
        try:
            self._apply(plain)
        finally:
            self._observers = observers
        if observers:
            effect = Plain.FromPlain(self).Transaction(plain)
            for observer in list(observers):
                observer(effect)

    # --- The index: what a transaction changes ---

    def _object_path(self, x: Any, committed: bool = False) -> Any:
        """The path of an object a transaction names, as the store held it or as the transaction leaves it; a counted
        object, which has none, by its schema and values, which are its identity."""
        if isinstance(x, Proxies._Unresolved):
            return x.path
        if Proxies._counted(x):
            schema = object.__getattribute__(x, "_schema")
            return {"counted": schema.name, "values": [[name, plain] for name in schema.properties
                                                       if (plain := Changes._value(self, x, name)) is not None]}
        if committed:
            with Changes._as_committed(self):
                return Plain._path_of(x)
        return Plain._path_of(x)

    def _plain_entry(self, entry: Any, committed: bool) -> dict[str, Any]:
        relation = entry._relation
        return {"relation": relation.name,
                "links": {link: self._object_path(x, committed) for link, x in entry._links.items()},
                "properties": {name: Plain.ToPlain(self)(relation.entry.properties[name].schema, value)
                               for name, value in entry._properties.items() if value is not None}}

    def _writes(self, proposals: Any, number: int) -> dict[str, str | None]:
        """The index items a transaction changes, by key: each one's data, or None where it goes."""
        writes: dict[str, str | None] = {}
        base = proposals.base
        made = {id(x): x for x in proposals.created.values() if isinstance(x, Proxies._ObjectData)}
        changed: dict[str, tuple[dict[str, Any], int]] = {}  # each entry changed, by its hash: as plain, how many held
        removed: list[Any] = []
        order = 0
        for holder, tables in proposals.entries_.values():
            before = base.entries(holder)
            for relation, table in tables.items():
                old = before.get(relation, {})
                gone = [entry for key, entry in old.items() if key not in table]
                new = [entry for key, entry in table.items() if key not in old]
                removed += gone
                for entry, committed in [*((e, True) for e in gone), *((e, False) for e in new)]:
                    plain = self._plain_entry(entry, committed)
                    if entry._relation.open:  # equal entries kept apart: how many the table holds now
                        value = entry._value()
                        count = sum(1 for e in table.values() if e._value() == value)
                    else:
                        count = 0 if committed else 1
                    key = _hash([plain["relation"], plain["links"], plain["properties"]])
                    order += 1
                    if key not in changed or not committed:
                        changed[key] = ({**plain, "order": f"{number:0{WIDTH}d}.{order:0{PART_WIDTH}d}"}, count)
        for key, (plain, count) in changed.items():
            for path in plain["links"].values():
                if "counted" in path:  # kept under the objects it links that have paths
                    continue
                writes[_object_key(path) + "E#" + key] = _dumps({**plain, "count": count}) if count else None
        deleted = [self._object_path(x, True) for entry in removed for x in entry._links.values()
                   if isinstance(x, Proxies._ObjectData) and id(x) not in made
                   and Proxies._owner_adjacencies(object.__getattribute__(x, "_schema")) and not Proxies._owner_entries(x)]
        holders = {id(holder): holder for holder, _ in proposals.values_.values()
                   if isinstance(holder, Proxies._ObjectData)}
        holders.update(made)
        for holder in holders.values():
            schema = object.__getattribute__(holder, "_schema")
            if Proxies._counted(holder) or (Proxies._owner_adjacencies(schema) and not Proxies._owner_entries(holder)):
                continue  # counted, held by its value in each entry that links it; deleted; or made and not kept
            path = self._object_path(holder)
            values = [[name, plain] for name in schema.properties
                      if (plain := Changes._value(self, holder, name)) is not None]
            writes[_object_key(path) + "V"] = _dumps({
                "path": path, "schema": Plain._schema_path(self._path(schema)), "values": values})
        for path in [*deleted, *Proxies._gone_paths(proposals, self)]:
            self._erase(path, writes)
        return writes

    def _erase(self, path: Any, writes: dict[str, str | None]) -> None:
        """The index items of a deleted object go, with each entry that links it, under every object it links, and
        each object it owns, in cascade, as its deletion removes them in every store."""
        prefix = _object_key(path)
        own = _canonical(path)
        for item in self._query(prefix, prefix + END):
            key = item["key"]["S"]
            writes[key] = None
            if key.endswith("#V"):
                continue
            entry = json.loads(item["data"]["S"])
            relation = self.registered(entry["relation"])
            for link, linked in entry["links"].items():
                if "counted" in linked:
                    continue
                writes[_object_key(linked) + key[len(prefix):]] = None
                if _canonical(linked) != own and Proxies._owns_through(relation, link, self):
                    self._erase(linked, writes)

    # --- The index: what a read gives ---

    def _render(self, path: Any, items: list[dict[str, Any]]) -> Any:
        """An object's document from its index items, as `Proxies.OfStore.document` writes one: its schema, its
        property values, then each named adjacency's entries."""
        head = next((json.loads(item["data"]["S"]) for item in items if item["key"]["S"].endswith("#V")), None)
        singleton, steps = Plain._steps(path, ())
        if head is not None:
            schema_path = head["schema"]
            schema = Plain._by_path(self, schema_path, ())
            if schema is None:
                raise LookupError(f"{self.table}/{self.store_name}: the object at {_canonical(path)} is of a schema the "
                                  f"store does not hold, at {_canonical(schema_path)}: install its schemas")
            values = head["values"]
        elif not steps and singleton in self._singletons:
            schema = object.__getattribute__(self._singletons[singleton], "_schema")
            schema_path, values = Plain._schema_path(self._path(schema)), []
        else:
            return None
        entries = sorted((json.loads(item["data"]["S"]) for item in items if "#E#" in item["key"]["S"]),
                         key=lambda entry: entry["order"])
        own = _canonical(path)
        members: list[Any] = [{"as_property": {"name": name, "mutations": [{"set": {"value": plain}}]}}
                              for name, plain in values]
        written: set[int] = set()
        for name, adjacency in schema.adjacencies.items():
            relation, me = adjacency.relation, adjacency.me
            rows, held = [], []
            for i, entry in enumerate(entries):
                if entry["relation"] != relation.name or _canonical(entry["links"].get(me)) != own or i in written:
                    continue
                held.append(i)
                row: dict[str, Any] = {link: self._linked(entry["links"][link], own)
                                       for link in relation.entry.links if link != me}
                row.update({prop: entry["properties"][prop] for prop in relation.entry.properties
                            if prop in entry["properties"]})
                rows += [row] * entry["count"]
            written.update(held)
            if rows:
                members.append({"as_adjacency": {"name": name, "mutations": [{"insert": {"entries": [
                    Plain._Document._spec_entry(row, adjacency) for row in Plain._sorted(adjacency, rows, rows)]}}]}})
        return {"mutations": [{"upsert": {"exists": [{
            "symbols": {"0": {"schema": [{"$ref": schema_path}]}},
            "spec": [{"0": members}] if members else []}]}}]}

    @staticmethod
    def _linked(linked: Any, own: str) -> Any:
        """What an entry links, as a document refers to it: the object itself, else by its path; a counted object is
        refused, as `Proxies.OfStore.document` refuses it."""
        if "counted" in linked:
            raise ValueError(f"a document refers to a counted {linked['counted']!r} it does not hold: a counted object "
                             "has no identity beyond the document that writes it")
        return {"symbol": "0"} if _canonical(linked) == own else {"path": linked}

    # --- The table ---

    def _item_key(self, key: str) -> dict[str, Any]:
        return {"store": {"S": self.store_name}, "key": {"S": key}}

    def _head(self) -> tuple[int, int]:
        """The head, `seq` and `indexed`, the index items of every entry written out first."""
        while True:
            item = self.client.get_item(TableName=self.table, Key=self._item_key(HEAD), ConsistentRead=True)["Item"]
            seq, indexed = int(item["seq"]["N"]), int(item["indexed"]["N"])
            if indexed == seq:
                return seq, indexed
            self._write_out(indexed + 1)

    def _query(self, low: str, high: str) -> Iterator[dict[str, Any]]:
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

    def _record(self, log: str, number: int) -> Any:
        """Entry `number` of the journal (`L`) or of the index items written out after it (`P`), its parts joined."""
        parts = self._query(_log_key(log, number, 0), _log_key(log, number, 0)[:-PART_WIDTH] + END)
        return json.loads(b"".join(bytes(item["data"]["B"]) for item in parts))

    def _parts(self, log: str, number: int, plain: Any) -> list[dict[str, Any]]:
        encoded = _dumps(plain).encode()
        chunks = [encoded[i:i + self.part_bytes] for i in range(0, len(encoded), self.part_bytes)] or [b""]
        return [{**self._item_key(_log_key(log, number, part)), "data": {"B": chunk}} for part, chunk in enumerate(chunks)]

    def _actions(self, writes: dict[str, str | None]) -> list[dict[str, Any]]:
        return [{"Put": {"TableName": self.table, "Item": {**self._item_key(key), "data": {"S": data}}}} if data is not None
                else {"Delete": {"TableName": self.table, "Key": self._item_key(key)}} for key, data in writes.items()]

    def _advance(self, number: int, indexed: bool) -> dict[str, Any]:
        """The head moved to `number`, on condition that it is where this store last saw it."""
        names, values = {"#seq": "seq", "#ix": "indexed"}, {":n": {"N": str(number - 1)}, ":n1": {"N": str(number)}}
        return {"Update": {"TableName": self.table, "Key": self._item_key(HEAD),
                           "UpdateExpression": "SET #seq = :n1, #ix = :n1" if indexed else "SET #seq = :n1",
                           "ConditionExpression": "#seq = :n AND #ix = :n",
                           "ExpressionAttributeNames": names, "ExpressionAttributeValues": values}}

    def _commit(self, plain: Any, writes: dict[str, str | None]) -> None:
        number = self._seq + 1
        log = [{"Put": {"TableName": self.table, "Item": item}} for item in self._parts("L", number, plain)]
        index = self._actions(writes)
        together = [self._advance(number, True), *log, *index]
        if len(together) <= self.max_actions and _size(together) <= self.max_bytes:
            self._transact(together)
        else:
            pending = [{"Put": {"TableName": self.table, "Item": item}}
                       for item in self._parts("P", number, [[key, data] for key, data in writes.items()])]
            self._transact([self._advance(number, False), *log, *pending])
            self._seq = number  # committed: its index items are written out by this store, or by the next to read
            with contextlib.suppress(ClientError):
                self._write_out(number)
        self._seq = number

    def _transact(self, actions: list[dict[str, Any]]) -> None:
        if len(actions) > self.max_actions or _size(actions) > self.max_bytes:
            raise ValueError(f"{self.table}/{self.store_name}: a transaction's changes are more than one DynamoDB transaction "
                             f"writes ({len(actions)} items, {_size(actions)} bytes)")
        try:
            self.client.transact_write_items(TransactItems=actions)
        except ClientError as error:
            code = error.response["Error"]["Code"]
            reasons = [reason.get("Code") for reason in error.response.get("CancellationReasons", [])]
            if code == "TransactionCanceledException" and (reasons[:1] in (["ConditionalCheckFailed"],
                                                                            ["TransactionConflict"]) or not reasons):
                raise _Conflict() from error
            raise

    def _write_out(self, number: int) -> None:
        """Writes the index items of entry `number`, kept apart beside it, then moves `indexed` to it: by the store that
        committed it, or by whichever reads first, where that one did not finish. Writing them again changes nothing."""
        writes = self._record("P", number)
        for i in range(0, len(writes), 25):
            requests = [{"PutRequest": {"Item": {**self._item_key(key), "data": {"S": data}}}} if data is not None
                        else {"DeleteRequest": {"Key": self._item_key(key)}} for key, data in writes[i:i + 25]]
            while requests:
                left = self.client.batch_write_item(RequestItems={self.table: requests}).get("UnprocessedItems", {})
                requests = left.get(self.table, [])
        try:
            self.client.update_item(TableName=self.table, Key=self._item_key(HEAD), UpdateExpression="SET #ix = :n",
                                    ConditionExpression="#ix = :before", ExpressionAttributeNames={"#ix": "indexed"},
                                    ExpressionAttributeValues={":n": {"N": str(number)},
                                                               ":before": {"N": str(number - 1)}})
        except ClientError as error:
            if error.response["Error"]["Code"] != "ConditionalCheckFailedException":  # written out by another
                raise


def _size(actions: list[dict[str, Any]]) -> int:
    """About how many bytes a DynamoDB transaction's actions write."""
    total = 0
    for action in actions:
        ((_, body),) = action.items()
        for attribute in (body.get("Item") or body["Key"]).values():
            ((_, value),) = attribute.items()
            total += len(value) if isinstance(value, bytes) else len(str(value).encode())
    return total
