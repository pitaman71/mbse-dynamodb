"""The interpreter of a store kept in DynamoDB: the lowest layer of a stack, which interprets each transaction with an
interpreter of its own, coupled with it, reading and writing its index within the transaction
([the stacked stores proposal](mbse:mbse-basis/proposals/STACKED-STORES.md#a-cache-layer-and-the-layer-below), its
third stage, as the user decided it: "the database layer's class, coupled with it, drives its own reads within its
database's transaction ... and its writes").

`Layer` is a `Transactions.Interpreter`, mbse-schemas' base class, which holds what every layer interprets alike: the
walk of the terms, symbols, frames, errors, unique clauses at commit and the stream. It gives the base class its hooks
from the index, each read made within the transaction and each write held in it, read again by the reads after it,
until the store commits the transaction whole (`Store`):

- a symbol's candidates by schema (`candidates`), from the items of the objects of the schema and of each schema refining
  it (`X#<schema>#`), in the order they were made;
- an object by its object reference (`designated`), from the index of paths (`R#<path>`), a singleton by its name;
- the entries that link an object (`entries`, `linking`), from its entry items (`E#<object>#`), in the order they were
  added.

An object is a row, a dictionary, the same one each time the transaction reads it, which is what the base class
compares by identity: `{"oid", "schema", "path", "values", "created", "gone"}`, its property values plain, as a spec
gives them. An entry is likewise `{"eid", "relation", "links", "values", "persisted", "gone"}`, each link an object by
its `oid`. Schema objects are the store's own, read as mbse-schemas' interpreter of a store in memory reads them
(`Transactions.InMemory`): the index holds no schemas, each process giving its store its installs.

What a transaction changed is read, net, from its log of writes (`changes`), and gathered as a `Changes.Transaction`
(mbse-schemas' journal language) by mbse-schemas' `Changes.gather`, as every layer's is: the journal transaction the
store commits, and the journal an input frame's first frame carries, a savepoint's. The transaction is recorded by
mbse-schemas' `Provenance.record`, as every layer's is; a callback is built by the store's `expressed`, as any layer's
is, given the store's schemas by name, so that `Changes.apply` works within it. It uses mbse-schemas' public interface
alone.
"""

from __future__ import annotations

import contextlib
import hashlib
import itertools
import json
import uuid
from collections.abc import Iterable, Iterator, Mapping
from types import SimpleNamespace
from typing import Any

from mbse.Schemas.Framework import Builders, Changes, Plain, Provenance, Schemas, Transactions
from mbse.Schemas.Framework.Transactions import Frame, Refused, Remainder

__all__ = ["Layer", "Transaction", "canonical", "digest", "installs"]

PlainData = Any


def canonical(plain: PlainData) -> str:
    """Plain data as one text, its keys sorted: what tells two paths, or two values, apart."""
    return json.dumps(plain, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)


def digest(plain: PlainData) -> str:
    return hashlib.sha256(canonical(plain).encode()).hexdigest()[:32]


def _dumps(plain: PlainData) -> str:
    return json.dumps(plain, separators=(",", ":"), ensure_ascii=True, allow_nan=False)


def _owners(schema: Any) -> list[Any]:
    """A schema's adjacencies marked `owner()`, named or anonymous: an object of it is owned."""
    return [a for a in (*schema.adjacencies.values(), *schema.anonymous) if a.owner]


def _counted(schema: Any) -> bool:
    """Whether an object of `schema` is counted: neither a singleton nor owned, so that it has no reference."""
    return schema.singleton is None and not _owners(schema)


def _owns(schema: Any, relation: Any, link: str) -> bool:
    """Whether an object of `schema` linked by `link` of an entry of `relation` is owned by it."""
    return any(a.relation is relation and a.me == link for a in _owners(schema))


def _deeper(nested: PlainData, step: PlainData) -> PlainData:
    """A path's `next`, its innermost `{"target": {}}` given a step more."""
    if "target" in nested:
        return step
    return {"entry": {**nested["entry"], "next": _deeper(nested["entry"]["next"], step)}}


def _steps(path: PlainData) -> list[PlainData]:
    """A path's steps, each an entry's `{"i", "o", "properties", "next"}`, from its singleton."""
    found, at = [], path["next"]
    while "entry" in at:
        found.append(at["entry"])
        at = at["entry"]["next"]
    return found


def _meta(store: Any, plain: PlainData) -> bool:
    """Whether a symbol, as an expression's wire form writes it, is of a meta-schema: its objects are schema objects."""
    for reference in plain.get("schema") or []:
        held = store.held(reference["path"])
        if held is not None and store.framework(held) and store.name_of(held).startswith("Schemas."):
            return True
    return False


def installs(store: Any, plain: PlainData) -> bool:
    """Whether an expression, as its wire form writes it, is an install: an upsert or an update of it binds a symbol of
    a meta-schema, as a cache layer finds one."""
    if isinstance(plain, Mapping):
        for verb, patterns in (("upsert", "exists"), ("update", "apply")):
            if verb in plain and any(_meta(store, symbol) for pattern in plain[verb].get(patterns, [])
                                     for symbol in pattern.get("symbols", {}).values()):
                return True
        return any(installs(store, value) for value in plain.values())
    return isinstance(plain, list) and any(installs(store, value) for value in plain)


class Transaction(Transactions.Transaction):
    """The transaction open on a store kept in DynamoDB, which `with store.transaction() as txn:` gives: its `exec`, as
    mbse-schemas' transaction gives it, each frame it gave kept, so that the scope raises the first error annotated on
    one at its end (`raised`)."""

    def __init__(self, layer: Layer, life: Any) -> None:
        super().__init__(layer, life)
        self.life_, self.frames = life, []

    def exec(self, frame: Any, remainder: Any, allow_write: bool) -> Iterator[Any]:
        if self.life_.ended:
            raise ValueError("a transaction's exec runs within it: this one has ended (store.transaction)")
        return self._kept(super().exec(frame, remainder, allow_write))

    def _kept(self, stream: Iterator[Any]) -> Iterator[Any]:
        for item in stream:
            if isinstance(item, Frame):
                self.frames.append(item)
            yield item

    def raised(self) -> None:
        """The first error annotated on a frame its `exec` gave, raised, its `frames` every frame it gave."""
        failed = next((frame for frame in self.frames if frame.errors), None)
        if failed is not None:
            error = failed.errors[0].error or RuntimeError(failed.errors[0].message)
            error.frames = self.frames  # type: ignore[attr-defined]
            raise error


class _State:
    """A transaction's state: the rows and entries it read or made, as it holds them; what the index held of each row
    before it; its log of writes, each with how it is undone, last; and what it is known by."""

    def __init__(self, seq: int) -> None:
        self.seq = seq  # the number of the journal's transaction it commits as
        self.serial = itertools.count()
        self.rows: dict[str, dict[str, Any]] = {}
        self.before: dict[str, dict[str, Any]] = {}  # by oid: what the index held of a row, and its items' keys
        self.schemas: dict[str, str] = {}  # each oid's schema, as an entry names it before its row is read
        self.entries: dict[str, dict[str, Any]] = {}
        self.lists: dict[str, list[str]] = {}  # by oid: the eids of the entries linking it, once read, in order
        self.extents: set[str] = set()  # the schemas whose objects it read
        self.paths: dict[str, str] = {}  # each path an object was given within it, canonical: the object's oid
        self.vacated: set[str] = set()  # each path no object has since it was written within it
        self.orphans: set[str] = set()  # each object whose owning entry it removed
        self.missed: set[str] = set()  # each relation a removal removed nothing of (CAP-293)
        self.upserted: dict[Any, dict[str, Any]] = {}
        self.log: list[tuple[Any, ...]] = []
        self.first: Any = None  # the first expression it was given: its uid's, its record's
        self.uid: str | None = None
        self.life = SimpleNamespace(ended=False)

    def logged(self, kind: str, *record: Any) -> None:
        """A write, its kind and what it wrote, and how it is undone, last."""
        self.log.append((kind, *record))

    def undo(self, mark: int) -> None:
        while len(self.log) > mark:
            self.log.pop()[-1]()

    def oid(self) -> str:
        """A new object's oid, or an entry's eid: in the order they are made, those of later transactions after."""
        return f"{self.seq:012d}.{next(self.serial):06d}"


class Layer(Transactions.Interpreter):
    """The interpreter of a store kept in DynamoDB, `store`, coupled with it: its reads from the index, `index`, made
    within the transaction open, `state`, and its writes, held in it until the store commits it."""

    def __init__(self, store: Any, index: Any) -> None:
        super().__init__(store)
        self.index = index  # the table's reads (`items(prefix)`, `item(key)`), its key layout and its part size
        self.state: _State | None = None
        self.schemas = Transactions.InMemory(store)  # the store's schema objects, read as a store in memory reads them

    # --- The transaction ---

    def begin(self, seq: int) -> Transaction:
        """A transaction, to commit as the journal's transaction `seq`."""
        self.state = _State(seq)
        return Transaction(self, self.state.life)

    def end(self) -> None:
        if self.state is not None:
            self.state.life.ended = True
        self.state = None

    @property  # type: ignore[override]
    def life(self) -> Any:
        return None if self.state is None else self.state.life

    @property
    def held(self) -> _State:
        if self.state is None:
            raise ValueError("a store kept in DynamoDB is read and written within a transaction (store.transaction)")
        return self.state

    def exec(self, txn: Transactions.Transaction, frame: Any, remainder: Any, allow_write: bool) -> Iterator[Any]:
        """`txn.exec`: a callback is built as an expression is, outside the transaction; an install, an expression whose
        upsert or update binds a symbol of a meta-schema, executed from its root, is run in the store's schemas alone,
        which the table does not hold, committed at once; the first expression given is the transaction's own, its
        uid and its record's."""
        store = self.store
        given = remainder if isinstance(remainder, Remainder) else store.expressed(remainder)
        expression = given.transaction if isinstance(given, Remainder) else given
        plain = Plain.ToPlain(store).Transaction(expression)
        if (not isinstance(given, Remainder) or len(given.at) <= 3) and installs(store, plain):
            inputs = [frame] if isinstance(frame, Mapping) else list(frame)
            with Builders.Store.transaction(store) as local:
                items = list(local.exec(inputs, expression, allow_write))
            return iter(items)
        state = self.held
        if state.first is None:
            state.first = expression
            state.uid = plain.get("uid")
        return super().exec(txn, frame, given, allow_write)

    def uid(self) -> str:
        state = self.held
        if state.uid is None:
            state.uid = str(uuid.uuid4())
        return state.uid

    @contextlib.contextmanager
    def savepoint(self, journal: Any = None) -> Iterator[list[Any]]:
        state = self.held
        mark = len(state.log)
        changed: list[Any] = []
        try:
            yield changed
        except BaseException:
            state.undo(mark)
            raise
        if journal is not None:
            changed.append(self.changes(state.log[mark:], journal()))

    # --- Rows and entries, read from the index ---

    def _schema_object(self, schema: Any) -> bool:
        return self.store.framework(schema) and self.store.name_of(schema).startswith("Schemas.")

    def _load(self, schema: str, oid: str, items: list[dict[str, Any]]) -> dict[str, Any]:
        """A row read from its items, kept as the transaction's; the one it holds already, where it holds one. A
        singleton the index holds nothing of is held, holding nothing."""
        state = self.held
        if oid in state.rows:
            return state.rows[oid]
        data = json.loads("".join(item["data"]["S"] for item in items)) if items else {"path": None, "values": {}}
        singleton = self.store.registered(schema).singleton
        path = {"singleton": singleton, "next": {"target": {}}} if singleton is not None else data["path"]
        row = state.rows[oid] = {"oid": oid, "schema": schema, "path": path, "values": data["values"],
                                 "created": False, "gone": False}
        state.schemas[oid] = schema
        state.before[oid] = {"path": path, "values": dict(data["values"]), "keys": [i["key"]["S"] for i in items]}
        return row

    def _row(self, oid: str) -> dict[str, Any]:
        """The row of `oid`, read from the index where the transaction holds none."""
        state = self.held
        if oid in state.rows:
            return state.rows[oid]
        schema = state.schemas[oid]
        items = list(self.index.items(self.index.layout.row(schema, oid)))
        if not items and self.store.registered(schema).singleton is None:
            raise LookupError(f"the index holds no {schema!r} of oid {oid}: it is not as its journal left it")
        return self._load(schema, oid, items)

    def _singleton(self, schema: Any) -> dict[str, Any]:
        oid = self.index.layout.singleton(schema.singleton)
        self.held.schemas.setdefault(oid, self.store.name_of(schema))
        return self._row(oid)

    def _entries(self, row: dict[str, Any]) -> list[dict[str, Any]]:
        """Every entry linking `row`, as the transaction holds them, in the order they were added."""
        state = self.held
        oid = row["oid"]
        if oid not in state.lists:
            state.lists[oid] = []
            for item in self.index.items(self.index.layout.entries(oid)):
                eid = item["key"]["S"].rsplit("#", 1)[1]
                if eid not in state.entries:
                    data = json.loads(item["data"]["S"])
                    for schema, linked in data["links"].values():
                        state.schemas.setdefault(linked, schema)
                    state.entries[eid] = {"eid": eid, "relation": data["relation"], "values": data["values"],
                                          "links": {link: linked for link, (_, linked) in data["links"].items()},
                                          "persisted": True, "gone": False}
                state.lists[oid].append(eid)
        return [state.entries[eid] for eid in state.lists[oid] if not state.entries[eid]["gone"]]

    def _owning(self, row: dict[str, Any]) -> list[tuple[dict[str, Any], str]]:
        """The entries that own `row`, each with the link that links its owner."""
        found = []
        for adjacency in _owners(self.store.registered(row["schema"])):
            for entry in self._entries(row):
                if entry["relation"] == adjacency.relation.name and entry["links"].get(adjacency.me) == row["oid"]:
                    found.append((entry, next(link for link in entry["links"] if link != adjacency.me)))
        return found

    def _owned(self, row: dict[str, Any]) -> list[tuple[dict[str, Any], str, dict[str, Any]]]:
        """What `row` owns: each entry owning an object through it, the link that links the object, and the object."""
        found = []
        for entry in self._entries(row):
            relation = self.store.registered(entry["relation"])
            for link, oid in entry["links"].items():
                other = self._row(oid)
                if oid != row["oid"] and _owns(self.store.registered(other["schema"]), relation, link):
                    found.append((entry, link, other))
        return found

    def _present(self, row: dict[str, Any]) -> bool:
        """Whether a row is held: not deleted, nor left with no owner by a removal of the transaction's."""
        return not row["gone"] and (row["oid"] not in self.held.orphans or bool(self._owning(row)))

    # --- What a layer gives: its reads ---

    def candidates(self, schema: Any) -> list[Any]:
        if self._schema_object(schema):
            return self.schemas.candidates(schema)
        if schema.singleton is not None:
            return [self._singleton(schema)]
        state = self.held
        names = self._refining(schema)
        for name in names:
            if name not in state.extents:
                state.extents.add(name)
                grouped: dict[str, list[dict[str, Any]]] = {}
                for item in self.index.items(self.index.layout.extent(name)):
                    grouped.setdefault(item["key"]["S"].split("#")[-2], []).append(item)
                for oid, items in grouped.items():
                    self._load(name, oid, items)
        # an owned object is among them once placed, reached from its singleton
        return [row for _, row in sorted(state.rows.items()) if row["schema"] in names and self._present(row)
                and (_counted(self.store.registered(row["schema"])) or row["path"] is not None)]

    def _refining(self, schema: Any) -> set[str]:
        """The names of `schema` and of each object schema the store holds that refines it, directly or not: an object
        of a refinement is one of `schema`'s, as a store in memory's extent has it."""
        found = {self.store.name_of(schema)}
        for name in self.store.names():
            held = self.store.registered(name)
            if isinstance(held, Schemas.OfObject.Data) and Schemas.is_a(held, schema):
                found.add(name)
        return found

    def named(self, schema: Any, name: str | None) -> list[Any]:
        return self.schemas.named(schema, name)

    def designated(self, path: Any) -> Any:
        if path["singleton"] == ".":
            return self.store.held(path)  # a schema object, the store's own
        state = self.held
        key = canonical(path)
        if key in state.paths:
            return state.rows[state.paths[key]]
        if key in state.vacated:
            return None
        if not _steps(path):
            schemas = [schema for schema in map(self.store.registered, self.store.names())
                       if getattr(schema, "singleton", None) == path["singleton"]]
            return self._singleton(schemas[0]) if schemas else None
        found = self.index.item(self.index.layout.path(digest(path)))
        if found is None:
            return None
        data = json.loads(found["data"]["S"])
        state.schemas.setdefault(data["oid"], data["schema"])
        row = self._row(data["oid"])
        return row if self._present(row) and row["path"] is not None and canonical(row["path"]) == key else None

    def schema_of(self, target: Any) -> Any:
        if isinstance(target, dict):
            return self.store.registered(target["schema"])
        return self.schemas.schema_of(target)

    def key(self, target: Any) -> Any:
        return ("row", target["oid"]) if isinstance(target, dict) else self.schemas.key(target)

    def value(self, holder: Any, schema: Any, name: str) -> tuple[bool, Any]:
        if not isinstance(holder, dict):
            return self.schemas.value(holder, schema, name)
        values = holder["values"]
        return (True, values[name]) if name in values else (False, None)

    def entries(self, target: Any, schema: Any, name: str) -> list[Any]:
        if not isinstance(target, dict):
            return self.schemas.entries(target, schema, name)
        adjacency = schema.adjacencies[name]
        return [entry for entry in self._entries(target) if entry["relation"] == adjacency.relation.name
                and entry["links"].get(adjacency.me) == target["oid"]]

    def link(self, entry: Any, name: str) -> Any:
        if not isinstance(entry, dict):
            return self.schemas.link(entry, name)
        oid = entry["links"].get(name)
        return None if oid is None else self._row(oid)

    def linking(self, target: Any, relation: Any) -> list[Any]:
        return [entry for entry in self._entries(target) if entry["relation"] == relation.name]

    def part(self, entry: Any, name: str) -> Any:
        value = entry["values"].get(name)
        return None if value is None else canonical(value)

    def shown(self, entry: Any, names: list[str]) -> str:
        def one(name: str) -> str:
            oid = entry["links"].get(name)
            return f"<{self._row(oid)['schema']}>" if oid is not None else repr(entry["values"].get(name))
        return "{" + ", ".join(f"{name}: {one(name)}" for name in names) + "}"

    def given(self, target: Any, names: Iterable[str]) -> tuple[Any, dict[str, Any]]:
        if not isinstance(target, dict):
            return target, {}  # a schema object, as itself
        schema = self.store.registered(target["schema"])
        if _counted(schema):
            return Refused(f"a counted {target['schema']!r} has no reference: nothing names it (make it a singleton's "
                           "or an owner's)", target, self.life), {}
        if not self._present(target) or target["path"] is None:
            return Refused(f"a {target['schema']!r} the transaction deleted, or left with no owner, has no reference: "
                           "nothing designates it"), {}
        reference = Plain.FromPlain(self.store)(self.store.registered("References.OfObject"), {"path": target["path"]})
        return reference, {name: Plain.FromPlain(self.store)(schema.properties[name].schema, target["values"][name])
                           for name in names if name in target["values"]}

    # --- What a layer gives: its writes, held in the transaction ---

    def _own(self, target: Any) -> dict[str, Any]:
        """`target`, where it is an object the table keeps: a store kept in DynamoDB writes no schema object, nor links
        one, nor a parameter reference in an object's place."""
        if not isinstance(target, dict):
            raise ValueError("a store kept in DynamoDB keeps its own objects alone: a schema object is written by an "
                             "install, which each process gives it, and no entry links one, nor a parameter reference "
                             "in an object's place (CAP-312)")
        return target

    def assign(self, target: Any, schema: Any, name: str, verb: Any) -> None:
        row = self._own(target)
        values = row["values"]
        had, old = name in values, values.get(name)
        if "set" in verb:
            values[name] = verb["set"]["value"]
        else:
            values.pop(name, None)
        self.held.logged("assign", row, name, had, old,
                         lambda: values.__setitem__(name, old) if had else values.pop(name, None))

    def insert(self, target: Any, schema: Any, name: str, links: dict[str, Any], values: dict[str, Any]) -> None:
        state = self.held
        row = self._own(target)
        adjacency = schema.adjacencies[name]
        relation = adjacency.relation
        if Transactions.single(relation, adjacency.me) and relation.name not in state.missed:
            held = self.entries(row, schema, name)
            if held:  # one entry at most: the one held is replaced, but where a removal removed nothing (CAP-293)
                self.remove(row, schema, name, list(range(len(held))))
        linked = {adjacency.me: row, **{link: self._own(other) for link, other in links.items()}}
        entry = {"eid": state.oid(), "relation": relation.name, "links": {link: r["oid"] for link, r in linked.items()},
                 "values": dict(values), "persisted": False, "gone": False}
        objects = list({r["oid"]: r for r in linked.values()}.values())
        for other in objects:
            self._entries(other)
        state.entries[entry["eid"]] = entry
        for other in objects:
            state.lists[other["oid"]].append(entry["eid"])

        def undo() -> None:
            entry["gone"] = True
            for other in objects:
                state.lists[other["oid"]].remove(entry["eid"])
        state.logged("insert", entry, row, undo)
        for link, owned in linked.items():  # an object owned through it is placed: its owner's path, a step on
            if _owns(self.store.registered(owned["schema"]), relation, link):
                self._placed(owned, entry, link)

    def _placed(self, owned: dict[str, Any], entry: dict[str, Any], link: str) -> None:
        """Places an object owned through `entry`'s link `link` a step on from its owner's path; where its owner has no
        path yet, it has none, until its owner is placed."""
        relation = self.store.registered(entry["relation"])
        owner_link = next(other for other in entry["links"] if other != link)
        owner = self._row(entry["links"][owner_link])
        if owner["path"] is None:
            self._place(owned, None)
            return
        i = next((name for name, a in self.store.registered(owner["schema"]).adjacencies.items()
                  if a.relation is relation and a.me == owner_link), "")
        o = next(a.name for a in _owners(self.store.registered(owned["schema"])) if a.relation is relation
                 and a.me == link)
        step = {"entry": {"i": i, "o": o, "properties": dict(entry["values"]), "next": {"target": {}}}}
        self._place(owned, {**owner["path"], "next": _deeper(owner["path"]["next"], step)})

    def _place(self, row: dict[str, Any], path: PlainData) -> None:
        """Gives a row its path, None where it has none, and each object it owns, in cascade, its path under it."""
        state = self.held
        old = row["path"]
        if canonical(old) == canonical(path):
            return
        if old is not None:
            state.paths.pop(canonical(old), None)
            state.vacated.add(canonical(old))
        if path is not None:
            state.paths[canonical(path)] = row["oid"]
            state.vacated.discard(canonical(path))
        row["path"] = path

        def undo() -> None:
            row["path"] = old
            if path is not None:
                state.paths.pop(canonical(path), None)
                state.vacated.add(canonical(path))
            if old is not None:
                state.paths[canonical(old)] = row["oid"]
                state.vacated.discard(canonical(old))
        state.logged("path", row, old, undo)
        for entry, link, owned in self._owned(row):
            self._placed(owned, entry, link)

    def remove(self, target: Any, schema: Any, name: str, positions: list[int]) -> None:
        state = self.held
        row = self._own(target)
        relation = schema.adjacencies[name].relation
        if not positions:  # a removal that removed nothing: what a writer whose view was stale sends (CAP-293)
            if relation.name not in state.missed:
                state.missed.add(relation.name)
                state.logged("missed", lambda: state.missed.discard(relation.name))
            return
        held = self.entries(row, schema, name)
        for index in positions:
            self._drop(held[index], row)

    def _drop(self, entry: dict[str, Any], holder: dict[str, Any] | None) -> None:
        """An entry removed, written from `holder`, None where it went with what it linked: each object it owned is
        left with no owner, its path and its own's gone, until it is given another."""
        state = self.held
        entry["gone"] = True
        state.logged("remove", entry, holder, lambda: entry.__setitem__("gone", False))
        relation = self.store.registered(entry["relation"])
        for link, oid in entry["links"].items():
            owned = self._row(oid)
            if _owns(self.store.registered(owned["schema"]), relation, link):
                if oid not in state.orphans:
                    state.orphans.add(oid)
                    state.logged("orphan", lambda oid=oid: state.orphans.discard(oid))
                if not self._owning(owned):
                    self._place(owned, None)

    def create(self, schema: Any, values: dict[str, Any], within: tuple[Any, str] | None = None) -> Any:
        if self._schema_object(schema):
            self._own(schema)
        state = self.held
        row = {"oid": state.oid(), "schema": self.store.name_of(schema), "path": None,
               "values": {name: verb["set"]["value"] for name, verb in values.items()}, "created": True, "gone": False}
        state.rows[row["oid"]] = row
        state.schemas[row["oid"]] = row["schema"]
        state.lists[row["oid"]] = []

        def undo() -> None:
            row["gone"] = True
            del state.rows[row["oid"]]
        state.logged("create", row, undo)
        return row

    def upserted(self, index: tuple[Any, ...], made: Any = None) -> Any:
        state = self.held
        if made is not None:
            state.upserted[index] = made
            state.logged("upserted", lambda: state.upserted.pop(index, None))
            return made
        return state.upserted.get(index)

    # --- At commit ---

    def settle(self) -> None:
        """Before the transaction commits: refuses an owned object it made with no owner, which a store kept in DynamoDB
        gives none, but one it gave one and removed it from since; deletes each object a removal left with no owner, and, in cascade, what it owns, every entry
        linking either removed (object identity 48 in
        [CAP-121: owned objects deleted in cascade](mbse:mbse-basis/docs/capabilities/CAP-121.md#decided))."""
        state = self.held
        for row in [row for row in state.rows.values() if row["created"] and row["oid"] not in state.orphans]:
            owners = _owners(self.store.registered(row["schema"]))
            if owners and not self._owning(row):
                names = ", ".join(repr(a.name or a.relation.name) for a in owners)
                raise ValueError(f"a {row['schema']!r} needs an owner through {names}: a store kept in DynamoDB gives it "
                                 "none, so give it one in the same write")
        done: set[str] = set()
        while todo := sorted(state.orphans - done):
            for oid in todo:
                done.add(oid)
                row = self._row(oid)
                if row["gone"] or self._owning(row):
                    continue
                row["gone"] = True
                state.logged("delete", row, lambda row=row: row.__setitem__("gone", False))
                for entry in self._entries(row):
                    self._drop(entry, None)

    def record(self) -> None:
        """Records the transaction, within it, by mbse-schemas' `Provenance.record`, as every layer's is recorded, the
        lowest layer of a stack recording each transaction once
        ([CAP-134: transactions recorded with provenance](mbse:mbse-basis/docs/capabilities/CAP-134.md)), where the
        store has `Provenance` installed: from what the transaction's first expression gives, each object it made, and
        each it wrote, made before it."""
        state = self.held
        created = [row for row in state.rows.values() if row["created"] and not row["gone"]]
        written = {r["oid"]: r for kind, r, *_ in state.log if kind == "assign"}
        written.update({record[2]["oid"]: record[2] for record in state.log
                        if record[0] in ("insert", "remove") and record[2] is not None})
        Provenance.record(self, state.first, created,
                          [r for r in written.values() if not r["created"] and not r["gone"]])

    def uniques(self) -> None:
        """The unique clauses of each relation an entry the transaction inserted is of, checked against what the
        transaction leaves (CAP-293), by the base class (`check_uniques`)."""
        by_relation: dict[str, list[dict[str, Any]]] = {}
        for kind, entry, *_ in self.held.log:
            if kind == "insert" and not entry["gone"]:
                by_relation.setdefault(entry["relation"], []).append(entry)
        for name, inserted in by_relation.items():
            relation = self.store.registered(name)
            if relation.uniques:
                self.check_uniques(relation, inserted)

    # --- What a transaction changed ---

    def changes(self, log: list[tuple[Any, ...]], uid: str) -> Any:
        """What a stretch of the transaction's log changed, net, as a `Changes.Transaction`, its id `uid`, gathered by
        mbse-schemas' `Changes.gather`, as every layer's is; None where it changed nothing: the objects it made, those
        held before it that it deleted, the entries it removed and inserted, and the properties it changed, each object
        held before by its path as it was."""
        made = [r for kind, r, *_ in log if kind == "create" and not r["gone"]]
        made_oids = {r["oid"] for kind, r, *_ in log if kind == "create"}
        deleted = list({r["oid"]: r for kind, r, *_ in log if kind == "delete" and r["oid"] not in made_oids}.values())
        paths: dict[str, PlainData] = {}
        for kind, *record in log:
            if kind == "path":
                paths.setdefault(record[0]["oid"], record[1])
        new = {e["eid"] for kind, e, *_ in log if kind == "insert"}
        inserted = [e for kind, e, *_ in log if kind == "insert" and not e["gone"]]
        removed = list({e["eid"]: e for kind, e, *_ in log if kind == "remove" and e["gone"] and e["eid"] not in new}
                       .values())
        properties: dict[tuple[str, str], tuple[dict[str, Any], tuple[bool, Any]]] = {}
        for kind, *record in log:
            if kind == "assign":
                row, name, had, old = record[:4]
                properties.setdefault((row["oid"], name), (row, (False, None) if row["oid"] in made_oids else (had, old)))
        changed = [(row, name) for (_, name), (row, before) in properties.items()  # a counted object's: its value
                   if not row["gone"] and not _counted(self.store.registered(row["schema"]))  # names it, unchanged
                   and (name in row["values"], row["values"].get(name)) != before]
        relation = self.store.registered
        return Changes.gather(self, uid, made, deleted, [(relation(e["relation"]), e) for e in removed],
                              [(relation(e["relation"]), e) for e in inserted], changed,
                              lambda row: paths.get(row["oid"], row["path"]))

    # --- What the index is given ---

    def writes(self) -> dict[str, str | None]:
        """The index items the transaction changes, by key: each one's data, or None where it goes."""
        state = self.held
        layout = self.index.layout
        writes: dict[str, str | None] = {}
        for oid, row in state.rows.items():
            before = state.before.get(oid)
            now = None if row["gone"] else (row["path"], row["values"])
            if before is not None and now == (before["path"], before["values"]):
                continue
            if before is not None:
                writes.update({key: None for key in before["keys"]})
                if _steps(before["path"] or {"next": {}}):
                    writes[layout.path(digest(before["path"]))] = None
            if now is None:
                continue
            data = _dumps({"path": row["path"], "values": row["values"]})
            for part, start in enumerate(range(0, len(data), self.index.part_bytes)):
                writes[layout.part(layout.row(row["schema"], oid), part)] = data[start:start + self.index.part_bytes]
            if _steps(row["path"] or {"next": {}}):
                writes[layout.path(digest(row["path"]))] = _dumps({"oid": oid, "schema": row["schema"]})
        for eid, entry in state.entries.items():
            objects = set(entry["links"].values())
            if entry["persisted"] and entry["gone"]:
                writes.update({layout.entry(oid, eid): None for oid in objects})
            elif not entry["persisted"] and not entry["gone"]:
                data = _dumps({"relation": entry["relation"], "values": entry["values"],
                               "links": {link: [state.schemas[oid], oid] for link, oid in entry["links"].items()}})
                writes.update({layout.entry(oid, eid): data for oid in objects})
        return writes
