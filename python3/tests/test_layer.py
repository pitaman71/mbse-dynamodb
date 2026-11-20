"""The store kept in DynamoDB's interpreter, `Layer`, as a layer's class of mbse-schemas' base class (suite LAY's
kinds of case): what it gives the base class and what it does not touch; schema objects read as the store in memory
reads them; its savepoints; and what a transaction's writes leave, as the index and the journal keep it."""

from __future__ import annotations

import inspect
import re

import pytest
from mbse.Schemas.Framework import Builders, Changes, Paths, Plain, Provenance, Transactions
from mbse.Schemas.Framework.Transactions import Frame

from mbse.Persistence.DynamoDB import Layer, Store as DynamoDB

from conftest import REGION
from test_store import (BOOK, contact, first_note, frames, held, keys, lasts, memory, note, people, people_in_memory,
                        person, process, reference, update)


def private(cls):
    """The names a class keeps private: its own, not Python's, nor the abstract base class machinery's."""
    return {name for name in vars(cls) if name.startswith("_") and not name.startswith("__") and name != "_abc_impl"}


def test_the_layer_is_an_interpreter_of_its_own_and_overrides_no_private_name(aws):
    """`Layer` is a `Transactions.Interpreter`, giving each hook the base class asks for, and the store's transaction a
    `Transactions.Transaction`; neither, nor the store, a `Builders.Store`, defines a name the class it extends keeps
    private (mbse-basis CAP-306), nor names a private name of mbse-schemas' modules: each uses mbse-schemas' public
    interface alone."""
    kept = process()
    layer = kept._kept_layer
    assert isinstance(layer, Transactions.Interpreter) and not Transactions.Interpreter.__abstractmethods__ - set(
        vars(Layer.Layer))
    taken = set().union(*(private(cls) for cls in Transactions.Interpreter.__mro__))
    assert not private(Layer.Layer) & taken
    assert not private(Layer.Transaction) & set().union(*(private(cls) for cls in Transactions.Transaction.__mro__))
    store_taken = set().union(*(private(cls) for cls in Builders.Store.__mro__), vars(Builders.Store()))
    added = private(DynamoDB.Store) | (set(vars(kept)) - set(vars(Builders.Store())))
    assert added and not added & store_taken
    modules = "Builders|Changes|Paths|Plain|Provenance|Proxies|References|Schemas|Transactions"
    for module in (Layer, DynamoDB):
        source = inspect.getsource(module)
        assert not re.findall(rf"\b(?:{modules})\._\w+", source) and "_check_uniques" not in source
    with kept.transaction() as txn:
        assert isinstance(txn, Transactions.Transaction) and layer.state is not None and layer.life is txn.life_
    assert layer.state is None and layer.life is None
    layer.end()  # ended twice: nothing more


def test_outside_its_transaction_it_reads_nothing(aws):
    """Its interpreter reads and writes within a transaction alone; a transaction's exec is refused once it has ended;
    a store holds one transaction open at a time; a callback is given the store's schemas by name, and no private
    name."""
    kept = process()
    with pytest.raises(ValueError, match="within a transaction"):
        kept._kept_layer.candidates(kept.registered("crm.Person"))
    with kept.transaction() as txn:
        with pytest.raises(ValueError, match="one at a time"):
            kept.transaction(person("ada", "Ada"))
    with pytest.raises(ValueError, match="this one has ended"):
        txn.exec(Frame(), person("ada", "Ada"), True)
    with pytest.raises(AttributeError, match="_hidden"):
        kept.transaction(lambda t: t._hidden)


def test_schema_objects_are_read_as_a_store_in_memory_reads_them(aws):
    """Schema objects are the store's own, which the table does not hold: a symbol of a meta-schema binds them, a
    reference to one designates it, read as the store in memory's interpreter reads them; written by an install alone,
    never within a transaction of the table's, nor linked by an entry."""
    kept = process()
    every = lambda t: t.uid("every/1").mutations([lambda mu: mu.map(lambda m: m.symbols([  # noqa: E731
        lambda p: p.symbols({"s": t.Schemas.OfObject})]))])
    found = kept.transaction(every)
    assert "crm.Person" in [frame["s"].name for frame in found]
    path = Paths.of(kept.registered("crm.Person"))
    schema = reference(kept, Plain.ToPlain(kept)(path.as_schema(), path))
    by_reference = lambda t: t.uid("by-reference/1").mutations([lambda mu: mu.map(lambda m: m.symbols([  # noqa: E731
        lambda p: p.symbols({"s": schema})]))])
    (frame,) = kept.transaction(by_reference)
    assert frame["s"] is kept.registered("crm.Person")
    layer = kept._kept_layer
    Person, OfObject = kept.registered("crm.Person"), kept.registered("Schemas.OfObject")
    with kept.transaction():
        assert layer.schema_of(Person) is OfObject and layer.key(Person) == layer.schemas.key(Person)
        (named,) = layer.entries(Person, OfObject, "adjacencies")[:1]
        assert layer.link(named, "adjacency") is not None and layer.value(Person, OfObject, "name") == (True, "crm.Person")
        with pytest.raises(ValueError, match="written by an install"):
            layer.assign(Person, OfObject, "name", {"set": {"value": "crm.Other"}})
        with pytest.raises(ValueError, match="written by an install"):
            layer.create(OfObject, {})


def test_a_savepoint_undoes_what_it_wrote(aws):
    """A savepoint that raises undoes each write it made, the transaction's state as it was before it: objects made,
    properties set, entries inserted and removed, paths given and taken."""
    kept = process()
    kept.transaction(person("ada", "Ada", 36))
    layer = kept._kept_layer
    Person, Book = kept.registered("crm.Person"), kept.registered("crm.Book")
    with kept.transaction():
        ada, book = layer.designated(contact("ada")), layer.designated(BOOK)
        (holds,) = layer.entries(book, Book, "contacts")
        with pytest.raises(RuntimeError, match="undone"):
            with layer.savepoint():
                bob = layer.create(Person, {"name": {"set": {"value": "Bob"}}})
                layer.insert(book, Book, "contacts", {"contact": bob}, {"key": "bob"})
                layer.assign(ada, Person, "age", {"clear": {}})
                layer.assign(ada, Person, "name", {"set": {"value": "Ada L."}})
                layer.remove(book, Book, "contacts", [0])
                layer.remove(book, Book, "contacts", [])
                layer.upserted(("k",), bob)
                raise RuntimeError("undone")
        assert layer.entries(book, Book, "contacts") == [holds] and ada["values"] == {"name": "Ada", "age": 36}
        assert layer.designated(contact("bob")) is None and layer.designated(contact("ada")) is ada
        assert layer.upserted(("k",)) is None and not layer.held.missed and layer.held.log == []


def test_a_removal_and_its_owner_given_again_in_one_transaction(aws):
    """Within one transaction: an object's owning entry removed, given again, and removed again, deletes it at the
    commit, its path gone at once; one given a new owning entry before its old one is removed keeps its new path; an
    entry of a relation an object holds one of at most replaced; a removal of what is not held removes nothing, twice;
    a property set and set back changes nothing."""
    kept = process()
    for spec in (person("ada", "Ada"), person("bob", "Bob"), first_note("n1", "first"), first_note("n2", "second")):
        kept.transaction(spec)
    assert lasts(kept) == ["second"]  # the last replaced: one at most
    churn = lambda t: t.uid("churn/1").mutations([lambda mu: mu.map(lambda m: m.symbols([lambda p: p.symbols(  # noqa
        {"x": reference(t._store, contact("ada")), "y": reference(t._store, contact("bob")), "b": t.crm.Book})])).then([
            lambda w: w.update(lambda u: u.apply([lambda p: p.symbols({"x": reference(t._store, contact("ada")),
                                                                      "y": reference(t._store, contact("bob")),
                                                                      "b": t.crm.Book}).spec([
                lambda _: _.b.contacts(lambda a: a.remove(lambda e: e.key("ada"))),
                lambda _: _.b.contacts({"ada": lambda e: e.contact(_.x)}),
                lambda _: _.b.contacts(lambda a: a.remove(lambda e: e.key("ada"))),
                lambda _: _.b.contacts({"robert": lambda e: e.contact(_.y)}),
                lambda _: _.b.contacts(lambda a: a.remove(lambda e: e.key("bob"))),
                lambda _: _.y.name("Robert"), lambda _: _.y.name("Bob")])]))])])
    with kept.transaction() as txn:
        (frame,) = frames(txn.exec(Frame(), churn, True))
        with pytest.raises(LookupError, match="has no reference"):
            frame["x"]  # deleted, or left with no owner: refused
        assert kept._kept_layer.designated(contact("ada")) is None
    twin = memory()
    for spec in (person("bob", "Bob"),):
        twin.transaction(spec)
    assert people(kept) == people_in_memory(twin)
    assert held(kept, lambda layer: layer.designated(contact("robert"))["values"]) == {"name": "Bob"}
    (transaction,) = kept.journal()[-1:]
    assert not [c for c in transaction["changes"] if "property" in c]  # Bob's name set and set back: no change
    with kept.transaction() as txn:  # made, and its owning entry removed: deleted, nothing committed
        frames(txn.exec(Frame(), person("zed", "Zed"), True))
        frames(txn.exec(Frame(), lambda t: t.uid("unzed/1").mutations([lambda mu: mu.map(lambda m: m.symbols([
            lambda p: p.symbols({"b": t.crm.Book})])).then([lambda w: w.update(lambda u: u.apply([lambda p: p.symbols(
                {"b": t.crm.Book}).spec([lambda _: _.b.contacts(lambda a: a.remove(lambda e: e.key("zed")))])]))])]),
            True))
    assert people(kept) == people_in_memory(twin)
    stale = lambda t: t.uid("stale/1").mutations([lambda mu: mu.map(lambda m: m.symbols([lambda p: p.symbols(  # noqa
        {"b": t.crm.Book, "l": reference(t._store, note("n1"))})])).then([lambda w: w.update(lambda u: u.apply([
            lambda p: p.symbols({"b": t.crm.Book, "l": reference(t._store, note("n1"))}).spec([
                lambda _: _.b.last(lambda a: a.remove(lambda e: e.note(_.l))),
                lambda _: _.b.last(lambda a: a.remove(lambda e: e.note(_.l)))])]))])])
    length = len(kept.journal())
    kept.transaction(stale)  # n1 is not the last: nothing removed, twice, nothing committed
    assert len(kept.journal()) == length and lasts(kept) == ["second"]


def zoo(txn):
    """zoo.Keeper, a singleton with a name, which keeps animals by name, cats and dogs, each owned through one relation
    by an adjacency of the same name: a path through it does not imply which."""
    S = txn.Schemas
    return txn.uid("zoo/1").mutations([lambda mu: mu.upsert(lambda u: u.exists([lambda p: p
        .symbols({"Keeps": S.OfRelation, "Keeper": S.OfObject, "Cat": S.OfObject, "Dog": S.OfObject})
        .spec([
            lambda _: _.Keeps.name("zoo.Keeps").links([lambda t: t.name("keeper"), lambda t: t.name("animal")])
            .properties({"name": S.OfStr}).uniques([["keeper", "name"], ["animal"]]),
            lambda _: _.Keeper.name("zoo.Keeper").ref(True).singleton("zoo.TheKeeper").properties({"name": S.OfStr})
            .adjacencies({"animals": lambda t: t.relation([_.Keeps]).me("keeper")}),
            lambda _: _.Cat.name("zoo.Cat").ref(True).properties({"lives": S.OfInt}).adjacencies([
                lambda t: t.name("keeper").relation([_.Keeps]).me("animal").owner(True)]),
            lambda _: _.Dog.name("zoo.Dog").ref(True).adjacencies([
                lambda t: t.name("keeper").relation([_.Keeps]).me("animal").owner(True)]),
        ])]))])


def test_what_its_journal_names_where_a_path_implies_no_schema(aws):
    """An object owned through a relation that owns objects of several schemas is named in the journal by its
    reference and its schema, which its path does not imply; a singleton's own property is kept in the index; a
    follower in memory applies the journal and holds the same."""
    kept = DynamoDB.Store("t", store="zoo", region=REGION)
    kept.transaction(zoo)
    cat = {"singleton": "zoo.TheKeeper", "next": {"entry": {"i": "animals", "o": "keeper", "properties": {"name": "tom"},
                                                            "next": {"target": {}}}}}
    kept.transaction(lambda t: t.uid("tom/1").mutations([lambda mu: mu.upsert(lambda u: u.exists([lambda p: p.symbols(
        {"k": t.zoo.Keeper, "c": t.zoo.Cat}).spec([lambda _: _.k.name("Zed").animals({"tom": lambda e: e.animal(_.c)}),
                                                    lambda _: _.c.lives(9)])]))]))
    kept.transaction(lambda t: t.uid("tom/2").mutations([lambda mu: mu.map(lambda m: m.symbols([lambda p: p.symbols(
        {"k": t.zoo.Keeper, "c": t.zoo.Cat}).spec([lambda _: _.k.animals([_.c])])])).then([lambda w: w.update(
            lambda u: u.apply([lambda p: p.symbols({"c": t.zoo.Cat}).spec([lambda _: _.c.lives(8)])]))])]))
    kept.transaction(update({"singleton": "zoo.TheKeeper", "next": {"target": {}}}, lambda _: _.x.name("Zoe"), "zoe/1"))
    (_, lived, renamed) = kept.journal()
    assert "schema" in lived["symbols"]["o0"] and "schema" not in renamed["symbols"]["o0"]
    replica = Builders.Store()
    replica.transaction(zoo)
    for transaction in kept.journal():
        replica.transaction(lambda txn, one=transaction: Changes.apply(txn, Plain.FromPlain(replica)(
            replica.registered("Changes.Journal"), [one])))
    with replica.transaction():
        keeper = replica.singleton("zoo.TheKeeper")
        assert (keeper.name.get(), [replica.member(e.animal, "lives") for e in keeper.animals]) == ("Zoe", [8])
    assert held(kept, lambda layer: layer.designated(cat)["values"]) == {"lives": 8}
    assert any(key.startswith("X#zoo.Keeper#") for key in keys(aws, "zoo"))


def test_a_journal_applied_within_a_callback(aws):
    """A callback given to the store is given its schemas by name as any store's is (`store.expressed`), so that
    `Changes.apply` writes another store's journal within it: a store kept in DynamoDB made to hold what another holds,
    transaction by transaction."""
    kept, replica = process("s"), process("r")
    for spec in (person("ada", "Ada", 36), person("bob", "Bob"), update(contact("ada"), lambda _: _.x.age(37), "a/1")):
        kept.transaction(spec)
    schema = replica.registered("Changes.Journal")
    for transaction in kept.journal():
        replica.transaction(lambda t, one=transaction: Changes.apply(t, Plain.FromPlain(replica)(schema, [one])))
    assert people(replica) == people(kept) and len(replica.journal()) == len(kept.journal()) == 3


def test_provenance_found_again(aws):
    """A transaction of a uid recorded already finds its record, and an object it wrote, recorded modified by it,
    stays so; a session of an identity recorded already is made, its identity found."""
    kept = process()
    kept.transaction(Provenance.install)
    kept.transaction(lambda t: Provenance.extend(t, [t.crm.Person]))
    kept.transaction(person("ada", "Ada"))
    for uid, sid, age in (("w1", "s1", 1), ("w1", "s1", 2), ("w2", "s2", 3)):
        kept.transaction(lambda t, uid=uid, sid=sid, age=age: update(contact("ada"), lambda _: _.x.age(age), uid)(t)
                         .session(lambda s: s.id(sid).who("ann")))

    def read(layer):
        directory = layer.designated({"singleton": "Provenance.Sessions", "next": {"target": {}}})
        sessions = layer.entries(directory, layer.schema_of(directory), "sessions")
        identities = layer.designated({"singleton": "Provenance.Identities", "next": {"target": {}}})
        transactions = layer.designated({"singleton": "Provenance.Transactions", "next": {"target": {}}})
        return ([e["values"]["id"] for e in sessions],
                [e["values"]["name"] for e in layer.entries(identities, layer.schema_of(identities), "identities")],
                [e["values"]["uid"] for e in layer.entries(transactions, layer.schema_of(transactions), "transactions")])
    assert held(kept, read) == (["unknown", "s1", "s2"], ["unknown", "ann"], [
        "person/ada", "w1", "w2"])


def test_an_index_not_as_its_journal_left_it_is_refused(aws):
    """An entry linking an object the index holds no row of, as a store written otherwise could leave it, is refused
    where it is read."""
    kept = process()
    kept.transaction(person("ada", "Ada"))
    (row,) = [key for key in keys(aws) if key.startswith("X#crm.Person#")]
    aws.delete_item(TableName="t", Key={"store": {"S": "s"}, "key": {"S": row}})
    with pytest.raises(LookupError, match="not as its journal left it"):
        held(kept, lambda layer: [layer.link(e, "contact") for e in layer.entries(
            layer.designated(BOOK), kept.registered("crm.Book"), "contacts")])
