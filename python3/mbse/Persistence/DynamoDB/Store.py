"""A store kept in DynamoDB: the lowest of stacked stores, below a lazy `Builders.Store`
([CAP-289: journals kept in DynamoDB](mbse:mbse-basis/docs/capabilities/README.md#CAP-289), as
[the stacked stores proposal](mbse:mbse-basis/proposals/STACKED-STORES.md) and
[the journal storage proposal](mbse:mbse-basis/proposals/JOURNAL-STORAGE.md) state), written against mbse-schemas'
public interface alone ([CAP-306: a public store-below interface](mbse:mbse-basis/docs/capabilities/README.md#CAP-306)).

`Store(table, store=name)` is a store below (mbse-schemas' `Stores.Below`) kept in one partition of a DynamoDB table.
An app's store is made over it, `Builders.Store(Store(...))`, and gives it its schemas as it installs them:

- `document(path)` gives the object at `path` as a one-transaction document, each object it links by its path, None
  where it holds none ([CAP-128: graphs sent as one-transaction documents](mbse:mbse-basis/docs/capabilities/CAP-128.md)),
  read from the index, one object's items, as of one entry of the journal.
- `commit(plain)` interprets a transaction's changes with mbse-schemas' own interpreter, a `Builders.Store` of its own
  over the index, so that it refuses what any store refuses
  ([CAP-293: an entry inserted against a unique clause refuses its transaction](mbse:mbse-basis/docs/capabilities/README.md#CAP-293));
  that store records it (`Provenance`), since the index records nothing, and sends its changes to the index, which
  commits them within one DynamoDB transaction (`TransactWriteItems`) that orders it: its journal entry, the index
  items it changes, and the head, the count of the journal's entries, on condition that no other has committed since
  this store last caught up. Where another has, the store catches up and interprets the transaction again, up to
  `attempts` times: what the other committed may now refuse it. It gives back the journal entry, its record among it.
- `install(plain)` runs an install in its interpreter: the schemas are no part of the table, so that each process gives
  its store its installs, as a store above does.
- `follow(follower)` gives `follower` each journal entry in commit order: this store's own as it commits them, and
  others', committed by other processes, as it catches up, before each commit and on `poll()`, read from the table.
- `records` is true: it records each transaction, the lowest store.

The table holds, under the store's partition key:

- `H`, the head: `seq`, the number of the last journal entry, and `indexed`, the last whose index items are written.
- `L#<n>#<part>`, journal entry `n` from 1: the transaction's changes as mbse-schemas writes them, the journal language
  ([CAP-271: the journal language](mbse:mbse-basis/docs/capabilities/README.md#CAP-271)), UTF-8 JSON in parts. It is the record.
- `O#<object>#D#<part>`, the index, by a hash of each object's path: its document as the interpreter gives it
  (`document(path)`), and the paths it links, in parts; and `O#<object>#B#<other>`, each object whose document links
  it, so that a deletion reaches what links what it deleted. It is how the database answers a read of one object: it
  is written only with the journal entry it follows from, and never sent, read whole or moved between stores, so that
  it is no snapshot.
- `P#<n>#<part>`, the index items of entry `n`, where they are too many for one DynamoDB transaction beside it: written
  with the entry, and then written out, by the writer or by whichever store reads next, before `indexed` reaches `n`.

The index items a transaction changes are computed from the interpreter's documents after it, within its transaction:
of each object its changes name, and each it made, which it holds; an object they delete, named by its owner's key,
goes with what it owns, as the documents before it say; and each other document that linked one of those loses those
entries. Nothing here is a snapshot: a store that starts loads what it is asked for from the index, and a follower
that starts reads the journal from any entry (`journal`).
"""

from __future__ import annotations

import contextlib
import copy
import hashlib
import json
import os
from collections.abc import Callable, Iterator, Mapping
from typing import Any

import boto3
from botocore.exceptions import ClientError
from mbse.Schemas.Framework import Builders, Paths, Plain

from .Table import create_table, environment

__all__ = ["Store", "Conflict", "PART_BYTES", "MAX_ACTIONS", "MAX_BYTES"]

PART_BYTES = 350_000  # a journal entry's part, or a document's, leaving room under DynamoDB's 400 KB item for its key
MAX_ACTIONS = 100  # DynamoDB's limit on the items one TransactWriteItems writes
MAX_BYTES = 3_500_000  # below DynamoDB's 4 MB limit on one TransactWriteItems, leaving room for keys
HEAD = "H"
WIDTH, PART_WIDTH = 20, 6  # digits in an entry's number and a part's, so that sort keys sort as numbers
END = "~"  # sorts after every digit and '#'


class Conflict(RuntimeError):
    """Other stores committed to the table first, each time this store interpreted a transaction again, `attempts`
    times running."""


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


# --- Plain data: paths and documents, as mbse-schemas writes them ---


def _steps(path: Any) -> tuple[str, list[str]]:
    """A path's singleton, and each entry step it goes through to its object, canonical."""
    steps, at = [], path.get("next", {})
    while "entry" in at:
        step = dict(at["entry"])
        at = step.pop("next", {})
        steps.append(_canonical(step))
    return path.get("singleton"), steps


def _under(path: Any, owner: Any) -> bool:
    """Whether `path` is `owner`'s or that of an object `owner` owns, through any number of owning entries."""
    (singleton, steps), (at, owners) = _steps(path), _steps(owner)
    return singleton == at and steps[:len(owners)] == owners


def _references(plain: Any) -> Iterator[Any]:
    """The path of each object a transaction's changes name by its reference."""
    if isinstance(plain, dict):
        reference = plain.get("reference")
        if len(plain) == 1 and isinstance(reference, dict) and "path" in reference:
            yield reference["path"]
        for value in plain.values():
            yield from _references(value)
    elif isinstance(plain, list):
        for value in plain:
            yield from _references(value)


def _links(plain: Any) -> Iterator[Any]:
    """The path of each object a document, or an entry of one, links."""
    if isinstance(plain, dict):
        ref = plain.get("ref")
        if len(plain) == 1 and isinstance(ref, dict) and "path" in ref:
            yield ref["path"]
        for value in plain.values():
            yield from _links(value)
    elif isinstance(plain, list):
        for value in plain:
            yield from _links(value)


def _members(document: Any) -> list[Any]:
    """A document's members: its properties' and its adjacencies' (`as_property`, `as_adjacency`)."""
    (pattern,) = document["mutations"][0]["upsert"]["exists"]
    return [member for spec in pattern.get("spec", []) for members in spec.values() for member in members]


def _rows(document: Any, adjacency: str) -> list[Any]:
    """The entries a document inserts in its adjacency `adjacency`, each as its mutations."""
    return [entry for member in _members(document) if member.get("as_adjacency", {}).get("name") == adjacency
            for mutation in member["as_adjacency"]["mutations"] for entry in mutation.get("insert", {}).get("entries", [])]


def _properties(entry: Any) -> dict[str, str]:
    """An entry's properties, each its value, canonical."""
    return {part["as_property"]["name"]: _canonical(part["as_property"]["mutations"]) for part in entry["mutations"]
            if "as_property" in part}


def _without(document: Any, gone: Callable[[Any], bool]) -> Any:
    """A document without each entry that links an object `gone` says was deleted, as its store removes them."""
    document = copy.deepcopy(document)
    (pattern,) = document["mutations"][0]["upsert"]["exists"]
    for spec in pattern["spec"]:  # a document that links an object has a spec, and inserts alone
        for symbol, members in spec.items():
            for adjacency in [member["as_adjacency"] for member in members if "as_adjacency" in member]:
                for mutation in adjacency["mutations"]:
                    mutation["insert"]["entries"] = [entry for entry in mutation["insert"]["entries"]
                                                     if not any(gone(path) for path in _links(entry))]
                adjacency["mutations"] = [m for m in adjacency["mutations"] if m["insert"]["entries"]]
            spec[symbol] = [m for m in members if m.get("as_adjacency", {"mutations": [1]})["mutations"]]
    pattern["spec"] = [spec for spec in pattern["spec"] if any(spec.values())]
    return document


class _Index:
    """The index, as the store below the store's interpreter (`Stores.Below`): it gives an object's document from the
    table, and commits what the interpreter commits, its journal entry and its index items, within one DynamoDB
    transaction. It records nothing, so that the interpreter, the lowest store, records each transaction
    (`Provenance`); it holds no schemas, so that an install is the interpreter's alone."""

    records = False

    def __init__(self, store: Store) -> None:
        self.store = store
        self.followers: list[Callable[[Any], None]] = []  # the interpreter's, which follows it while it is held

    def document(self, path: Any) -> Any:
        return self.store.document(path)

    def commit(self, plain: Any) -> Any:
        self.store._commit(plain, self.store._writes(plain))
        return plain

    def install(self, plain: Any) -> None:
        pass

    def follow(self, follower: Callable[[Any], None]) -> Callable[[], None]:
        self.followers.append(follower)
        return lambda: None  # the interpreter follows it for as long as the store holds either


class _Old:
    """An object's index items as the table holds them before the transaction: its document, the paths it links, and
    its items' keys."""

    def __init__(self, path: Any, record: dict[str, Any] | None, keys: list[str]) -> None:
        self.path, self.keys = path, keys
        self.record = record or {}
        self.document = self.record.get("document")
        self.links = {_canonical(p): p for p in self.record.get("links", [])}


class Store:
    """A store below (`Stores.Below`) kept in one partition, `store`, of the DynamoDB table `table`.

    Settings are as `Table.environment` reads them from `MBSE_DYNAMODB_*`, an argument overriding each: `region` and
    `endpoint_url` go to boto3 (`http://localhost:8000` for DynamoDB Local), `client`, where given, is used instead;
    where `create`, the table is made if absent. `attempts` bounds how many times a transaction is interpreted again
    after another store committed first. `max_actions` and `max_bytes` bound what one DynamoDB transaction writes;
    `part_bytes` a journal entry's part, or a document's."""

    records = True
    """It records each transaction it commits (`Provenance`), the lowest store: a store above it records none."""

    def __init__(self, table: str | None = None, *, store: str | None = None, region: str | None = None,
                 endpoint_url: str | None = None, client: Any = None, create: bool | None = None,
                 environ: Mapping[str, str] | None = None, attempts: int = 8, max_actions: int = MAX_ACTIONS,
                 max_bytes: int = MAX_BYTES, part_bytes: int = PART_BYTES) -> None:
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
        try:
            self.client.put_item(TableName=self.table, Item={**self._item_key(HEAD), "seq": {"N": "0"}, "indexed": {"N": "0"}},
                                 ConditionExpression="attribute_not_exists(#k)", ExpressionAttributeNames={"#k": "key"})
        except ClientError as error:
            if error.response["Error"]["Code"] != "ConditionalCheckFailedException":
                raise
        self._seq = self._head()[0]  # the last entry its interpreter and its followers have been given
        self._followers: dict[object, Callable[[Any], None]] = {}
        self._index = _Index(self)
        self._interpreter = Builders.Store(self._index)  # interprets each transaction, loading from the index

    # --- Stores.Below ---

    def document(self, path: Any) -> Any:
        """The document of the object at `path`, from the index, as of one entry of the journal; None where the store
        holds none. Read again where another store committed while it was read. A document the store refuses, as any
        store refuses one that links a counted object, is refused, raising as it does."""
        while True:
            seq = self._head()[0]
            old = self._old(path)
            if self._head()[0] == seq:
                break
        if old.record.get("refused") is not None:
            raise ValueError(old.record["refused"])
        return old.document if old.document is not None else self._empty(path)

    def _empty(self, path: Any) -> Any:
        """A singleton's document where the table holds nothing of it, as a store gives one it made and never wrote:
        its schema alone, as the store's interpreter holds it; None for any other object, or a singleton it has no
        schema of."""
        singleton, steps = _steps(path)
        if steps:
            return None
        try:
            schema = self._interpreter.singleton(singleton).as_schema()
        except LookupError:
            return None
        at = Paths.of(schema)
        return {"mutations": [{"upsert": {"exists": [{"symbols": {"0": {"schema": [
            {"$ref": Plain.ToPlain(None)(at.as_schema(), at)}]}}}]}}]}

    def commit(self, plain: Any) -> Any:
        """Commits a transaction's changes, refused as the interpreter refuses them, interpreted again where another
        store committed first; gives back its journal entry, its record among it, None where it changed nothing. Its
        followers are given it once committed."""
        for _ in range(self.attempts):
            self._catch_up()
            try:
                committed = self._interpreter.commit(plain)
            except _Conflict:
                continue
            if committed is not None:
                for follower in list(self._followers.values()):
                    follower(committed)
            return committed
        raise Conflict(f"{self.table}/{self.store_name}: other stores committed first {self.attempts} times running")

    def install(self, plain: Any) -> None:
        """Runs an install in the interpreter: its schemas, which the table does not hold."""
        self._interpreter.install(plain)

    def follow(self, follower: Callable[[Any], None]) -> Callable[[], None]:
        """Gives `follower` each journal entry from now, in commit order; gives back a function that ends it."""
        token = object()  # this following, though the same follower follow twice
        self._followers[token] = follower

        def end() -> None:
            self._followers.pop(token, None)

        return end

    # --- The journal ---

    def journal(self, after: int = 0) -> Iterator[Any]:
        """The changes of each transaction committed, in commit order, as plain data: from the first, or those after
        entry `after`."""
        last = self._head()[0]
        for number in range(after + 1, last + 1):
            yield self._record("L", number)

    def poll(self) -> int:
        """Gives the interpreter, and the followers, each transaction another store committed since it last caught up,
        in commit order; how many."""
        return self._catch_up()

    def _catch_up(self) -> int:
        """Gives the interpreter, applied to the objects it holds, and the followers each entry committed since the last
        given."""
        last, count = self._head()[0], 0
        while self._seq < last:
            plain = self._record("L", self._seq + 1)
            self._seq += 1
            count += 1
            for follower in [*self._index.followers, *self._followers.values()]:
                follower(plain)
        return count

    # --- The index: what a transaction changes ---

    def _old(self, path: Any) -> _Old:
        """The object's index items, as the table holds them."""
        prefix = _object_key(path) + "D#"
        items = list(self._query(prefix, prefix + END))
        data = "".join(item["data"]["S"] for item in items)
        return _Old(path, json.loads(data) if data else None, [item["key"]["S"] for item in items])

    def _linkers(self, path: Any) -> list[Any]:
        """The paths of the objects whose documents link the object at `path`."""
        prefix = _object_key(path) + "B#"
        return [json.loads(item["data"]["S"]) for item in self._query(prefix, prefix + END)]

    def _written(self, path: Any) -> Any:
        """The object's document as the interpreter gives it, within the transaction it commits: a record of it, and of
        the paths it links; one the interpreter refuses, as it refuses one that links a counted object, kept refused."""
        try:
            document = self._interpreter.document(path)
        except ValueError as error:
            return {"path": path, "document": None, "refused": str(error), "links": []}
        return None if document is None else {
            "path": path, "document": document, "links": list({_canonical(p): p for p in _links(document)}.values())}

    def _deleted(self, plain: Any, old: Callable[[Any], _Old]) -> dict[str, Any]:
        """The objects a transaction's changes delete, by path: each whose owning entry they remove, named by its owner's
        key, as the owner's document had it, and what each owns, in cascade, as its document had it."""
        (pattern,) = plain["mutations"][0]["upsert"]["exists"]
        symbols = pattern.get("symbols", {})
        deleted: dict[str, Any] = {}
        found: dict[str, Any] = {}
        for spec in pattern.get("spec", []):
            for symbol, members in spec.items():
                owner = symbols.get(symbol, {}).get("reference", {}).get("path")
                for member in members:
                    adjacency = member.get("as_adjacency")
                    if owner is None or adjacency is None:
                        continue
                    for mutation in adjacency["mutations"]:
                        for entry in mutation.get("remove", {}).get("entries", []):
                            if any("as_link" in part for part in entry["mutations"]):
                                continue  # an entry removed: its objects are named, and kept
                            keys = _properties(entry)
                            for row in _rows(old(owner).document, adjacency["name"]):
                                if all(_properties(row).get(k) == v for k, v in keys.items()):
                                    found.update({_canonical(p): p for p in _links(row)})
        while found:
            key, path = found.popitem()
            deleted[key] = path
            found.update({k: p for k, p in old(path).links.items() if _under(p, path) and k not in deleted})  # owned
        return deleted

    def _writes(self, plain: Any) -> dict[str, str | None]:
        """The index items a transaction's changes change, by key: each one's data, or None where it goes."""
        olds: dict[str, _Old] = {}

        def old(path: Any) -> _Old:
            key = _canonical(path)
            if key not in olds:
                olds[key] = self._old(path)
            return olds[key]

        deleted = self._deleted(plain, old)

        def gone(path: Any) -> bool:
            return any(_under(path, d) for d in deleted.values())

        records: dict[str, tuple[Any, Any]] = {}  # each object's path, and its record after, None where it goes
        todo = [p for p in _references(plain)]
        while todo:  # what the changes name, and what they made, which the interpreter holds
            path = todo.pop()
            key = _canonical(path)
            if key in records or gone(path):
                continue
            records[key] = (path, self._written(path))
            after = {_canonical(p) for p in (records[key][1] or {}).get("links", [])}
            todo += [p for p in (records[key][1] or {}).get("links", []) if _canonical(p) not in old(path).links]
            todo += [p for k, p in old(path).links.items() if k not in after]
        for path in deleted.values():
            records[_canonical(path)] = (path, None)
        for path in list(deleted.values()):  # what linked what they deleted: its entries linking them go
            for linker in self._linkers(path):
                key = _canonical(linker)
                if key in records or gone(linker):
                    continue
                document = _without(old(linker).document, gone)  # one that links any has a document
                records[key] = (linker, {"path": linker, "document": document,
                                         "links": list({_canonical(p): p for p in _links(document)}.values())})
        writes: dict[str, str | None] = {}
        for key, (path, record) in records.items():
            before = old(path)
            if record == (before.record or None):
                continue
            prefix = _object_key(path)
            for item in before.keys:
                writes[item] = None
            if record is not None:
                data = json.dumps(record, separators=(",", ":"), ensure_ascii=True, allow_nan=False)
                for part, i in enumerate(range(0, len(data), self.part_bytes)):
                    writes[f"{prefix}D#{part:0{PART_WIDTH}d}"] = data[i:i + self.part_bytes]
            after = {_canonical(p): p for p in (record or {}).get("links", [])}
            for link, linked in before.links.items():
                if link not in after:
                    writes[_object_key(linked) + "B#" + _hash(path)] = None
            for link, linked in after.items():
                if link not in before.links:
                    writes[_object_key(linked) + "B#" + _hash(path)] = _dumps(path)
            if record is None:  # gone: what linked it is rewritten above, or goes too
                for item in self._query(prefix + "B#", prefix + "B#" + END):
                    writes[item["key"]["S"]] = None
        return writes

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
