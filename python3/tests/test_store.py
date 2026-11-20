"""The store kept in DynamoDB's own cases (CAP-289), in a mocked DynamoDB: its transaction, held open over several
`exec` calls and committed in one DynamoDB transaction; how DynamoDB orders its transactions, among several stores over
one partition, each standing for a process of its own, a commit made meanwhile refused or interpreted again; what its
followers are given; and what it keeps in the table, which is no snapshot."""

from __future__ import annotations

import json

import pytest
from botocore.exceptions import ClientError
from mbse.Schemas.Framework import Builders, Changes, Plain, Provenance
from mbse.Schemas.Framework.Transactions import Frame

from mbse.Persistence.DynamoDB import Store as DynamoDB

from conftest import REGION


def crm(txn):
    """crm.Person, owned by the singleton book by key, with a name, an age and a home; persons know persons, a set, and
    introduce persons to persons (an open relation, which keeps equal entries), and keep pets, owned by name; persons are
    tagged with counted tags; the book logs notes, owned by name, and keeps its one last note."""
    S = txn.Schemas
    return txn.uid("crm/1").mutations([lambda mu: mu.upsert(lambda u: u.exists([lambda p: p
        .symbols({"Home": S.OfObject, "Holds": S.OfRelation, "Knows": S.OfRelation, "Introduces": S.OfRelation,
                  "Tag": S.OfObject, "Tagged": S.OfRelation, "Note": S.OfObject, "Logs": S.OfRelation,
                  "Last": S.OfRelation, "Book": S.OfObject, "Person": S.OfObject, "Pet": S.OfObject,
                  "Keeps": S.OfRelation})
        .spec([
            lambda _: _.Home.name("crm.Home").properties({"city": S.OfStr}),
            lambda _: _.Holds.name("crm.Holds").links([lambda t: t.name("book"), lambda t: t.name("contact")])
            .properties({"key": S.OfStr}).uniques([["book", "key"], ["contact"]]),
            lambda _: _.Knows.name("crm.Knows").links([lambda t: t.name("knower"), lambda t: t.name("known")])
            .uniques([]),  # a set: one clause, naming nothing
            lambda _: _.Introduces.name("crm.Introduces").links([lambda t: t.name("host"), lambda t: t.name("guest")]),
            lambda _: _.Tagged.name("crm.Tagged").links([lambda t: t.name("tagged"), lambda t: t.name("tag")]),
            lambda _: _.Tag.name("crm.Tag").ref(True).properties({"label": S.OfStr}).adjacencies([
                lambda t: t.name("on").relation([_.Tagged]).me("tag")]),
            lambda _: _.Logs.name("crm.Logs").links([lambda t: t.name("book"), lambda t: t.name("note")])
            .properties({"name": S.OfStr}).uniques([["book", "name"], ["note"]]),
            lambda _: _.Last.name("crm.Last").links([lambda t: t.name("book"), lambda t: t.name("note")])
            .uniques([["book"], ["note"]]),
            lambda _: _.Note.name("crm.Note").ref(True).properties({"text": S.OfStr}).adjacencies([
                lambda t: t.name("book").relation([_.Logs]).me("note").owner(True),
                lambda t: t.name("last_of").relation([_.Last]).me("note")]),
            lambda _: _.Keeps.name("crm.Keeps").links([lambda t: t.name("keeper"), lambda t: t.name("pet")])
            .properties({"name": S.OfStr}).uniques([["keeper", "name"], ["pet"]]),
            lambda _: _.Pet.name("crm.Pet").ref(True).properties({"kind": S.OfStr}).adjacencies([
                lambda t: t.name("keeper").relation([_.Keeps]).me("pet").owner(True)]),
            lambda _: _.Book.name("crm.Book").ref(True).singleton("crm.TheBook").adjacencies({
                "contacts": lambda t: t.relation([_.Holds]).me("book"),
                "notes": lambda t: t.relation([_.Logs]).me("book"),
                "last": lambda t: t.relation([_.Last]).me("book")}),
            lambda _: _.Person.name("crm.Person").ref(True).properties({"name": S.OfStr, "age": S.OfInt, "home": _.Home})
            .adjacencies([
                lambda t: t.name("book").relation([_.Holds]).me("contact").owner(True),
                lambda t: t.name("knows").relation([_.Knows]).me("knower"),
                lambda t: t.name("known_by").relation([_.Knows]).me("known"),
                lambda t: t.name("hosts").relation([_.Introduces]).me("host"),
                lambda t: t.name("tags").relation([_.Tagged]).me("tagged"),
                lambda t: t.name("pets").relation([_.Keeps]).me("keeper")]),
        ])]))])


BOOK = {"singleton": "crm.TheBook", "next": {"target": {}}}


def contact(key, *pets):
    """The path of the book's contact `key`, or of a pet it keeps, by name."""
    step = {"target": {}}
    for name in reversed(pets):
        step = {"entry": {"i": "pets", "o": "keeper", "properties": {"name": name}, "next": step}}
    return {"singleton": "crm.TheBook", "next": {"entry": {"i": "contacts", "o": "book", "properties": {"key": key},
                                                           "next": step}}}


def note(name):
    return {"singleton": "crm.TheBook", "next": {"entry": {"i": "notes", "o": "book", "properties": {"name": name},
                                                           "next": {"target": {}}}}}


def process(name="s", **options):
    """A store kept in DynamoDB: one process's, its schemas installed, as each process gives its store them."""
    kept = DynamoDB.Store("t", store=name, region=REGION, **options)
    kept.transaction(crm)
    return kept


def reference(store, path):
    return Plain.FromPlain(store)(store.registered("References.OfObject"), {"path": path})


def paths(store, frames):
    """Each frame's bindings, by symbol, as their paths, and what it assigns."""
    return [({symbol: Plain.ToPlain(store)(target.as_schema(), target)["path"] for symbol, target in sorted(frame.items())},
             frame.assigns) for frame in frames]


def frames(items):
    return [item for item in items if isinstance(item, Frame)]


def person(key, name, age=None, knows=()):
    """A transaction making the contact `key`, named `name`, of `age`, knowing the contacts `knows`."""
    def build(t):
        symbols = {"p": t.crm.Person, "book": t.crm.Book, **{k: t.crm.Person for k in knows}}
        spec = [lambda _: _.book.contacts({key: lambda e: e.contact(_.p), **{
                    k: (lambda e, k=k: e.contact(getattr(_, k))) for k in knows}}),
                lambda _: _.p.name(name) if age is None else _.p.name(name).age(age)]
        spec += [lambda _, k=k: _.p.knows([getattr(_, k)]) for k in knows]
        return t.uid(f"person/{key}").mutations([lambda mu: mu.upsert(lambda u: u.exists([
            lambda p: p.symbols(symbols).spec(spec)]))])
    return build


def everyone(t):
    """A map of every person, giving their names."""
    return t.uid("everyone/1").mutations([lambda mu: mu.map(lambda m: m.symbols([lambda p: p.symbols(
        {"x": t.crm.Person})]))])


def named(name):
    """A map of the person named `name`."""
    return lambda t: t.uid(f"named/{name}").mutations([lambda mu: mu.map(lambda m: m.symbols([lambda p: p.symbols(
        {"x": t.crm.Person}).spec([lambda _: _.x.name(name)])]))])


def aged(name, age):
    """A map of the person named `name`, then an update writing their age (None: clearing it)."""
    return lambda t: t.uid(f"aged/{name}/{age}").mutations([lambda mu: mu.map(lambda m: m.symbols([lambda p: p.symbols(
        {"x": t.crm.Person}).spec([lambda _: _.x.name(name)])])).then([lambda w: w.update(lambda u: u.apply([
            lambda p: p.symbols({"x": t.crm.Person}).spec([lambda _: _.x.age(age)])]))])])


def update(path, spec, uid="update/1"):
    """A transaction: the object at `path`, by its reference, updated by `spec` over the frame (`_.x`)."""
    def build(t):
        return t.uid(uid).mutations([lambda mu: mu.map(lambda m: m.symbols([lambda p: p.symbols(
            {"x": reference(t._store, path)})])).then([lambda w: w.update(lambda u: u.apply([lambda p: p.symbols(
                {"x": reference(t._store, path)}).spec([spec])]))])])
    return build


def unbooked(key):
    """A transaction: the book's contact `key` removed, and with it the person and what they own, in cascade."""
    return lambda t: t.uid(f"unbooked/{key}").mutations([lambda mu: mu.map(lambda m: m.symbols([lambda p: p.symbols(
        {"b": t.crm.Book})])).then([lambda w: w.update(lambda u: u.apply([lambda p: p.symbols({"b": t.crm.Book})
            .spec([lambda _: _.b.contacts(lambda a: a.remove(lambda e: e.key(key)))])]))])])


def pet(key, name, kind):
    """A transaction: the contact `key` keeps a pet `name` of `kind`."""
    return lambda t: t.uid(f"pet/{key}/{name}").mutations([lambda mu: mu.map(lambda m: m.symbols([lambda p: p.symbols(
        {"x": reference(t._store, contact(key))})])).then([lambda w: w.upsert(lambda u: u.exists([lambda p: p.symbols(
            {"x": reference(t._store, contact(key)), "q": t.crm.Pet}).spec([
                lambda _: _.x.pets({name: lambda e: e.pet(_.q)}), lambda _: _.q.kind(kind)])]))])])


def first_note(name, text):
    """A transaction: the book's first note, logged and last."""
    return lambda t: t.uid(f"note/{name}").mutations([lambda mu: mu.upsert(lambda u: u.exists([lambda p: p.symbols(
        {"b": t.crm.Book, "n": t.crm.Note}).spec([
            lambda _: _.b.notes({name: lambda e: e.note(_.n)}), lambda _: _.n.text(text),
            lambda _: _.b.last([_.n])])]))])


def appended(name, text):
    """A transaction: a note appended to the book's log, after its last as the transaction finds it."""
    return lambda t: t.uid(f"append/{name}").mutations([lambda mu: mu.map(lambda m: m.symbols([lambda p: p.symbols(
        {"b": t.crm.Book, "l": t.crm.Note}).spec([lambda _: _.b.last([_.l])])])).then([lambda w: w.upsert(
            lambda u: u.exists([lambda p: p.symbols({"b": t.crm.Book, "l": t.crm.Note, "n": t.crm.Note}).spec([
                lambda _: _.b.notes({name: lambda e: e.note(_.n)}), lambda _: _.n.text(text),
                lambda _: _.b.last(lambda a: a.remove(lambda e: e.note(_.l)).insert(lambda e: e.note(_.n)))])]))])])


def after(last, name, text):
    """A transaction: a note appended after the note `last`, by its reference, the last as its writer saw it."""
    return lambda t: t.uid(f"after/{name}").mutations([lambda mu: mu.map(lambda m: m.symbols([lambda p: p.symbols(
        {"b": t.crm.Book, "l": reference(t._store, note(last))})])).then([lambda w: w.upsert(lambda u: u.exists([
            lambda p: p.symbols({"b": t.crm.Book, "l": reference(t._store, note(last)), "n": t.crm.Note}).spec([
                lambda _: _.b.notes({name: lambda e: e.note(_.n)}), lambda _: _.n.text(text),
                lambda _: _.b.last(lambda a: a.remove(lambda e: e.note(_.l)).insert(lambda e: e.note(_.n)))])]))])])


def held(kept, read):
    """What `read`, given the store's interpreter, reads within a transaction of the store's own."""
    with kept.transaction():
        return read(kept._kept_layer)


def people(kept):
    """Each person the store holds: name, age, whom they know, and their pets, by name and kind."""
    def read(layer):
        Person = kept.registered("crm.Person")
        return sorted((row["values"].get("name"), row["values"].get("age"),
                       sorted(layer.link(e, "known")["values"]["name"] for e in layer.entries(row, Person, "knows")),
                       sorted((e["values"]["name"], layer.link(e, "pet")["values"].get("kind"))
                              for e in layer.entries(row, Person, "pets")))
                      for row in layer.candidates(Person))
    return held(kept, read)


def lasts(kept):
    """The text of the book's last note."""
    def read(layer):
        book = layer.designated(BOOK)
        return [layer.link(e, "note")["values"]["text"] for e in layer.entries(book, layer.schema_of(book), "last")]
    return held(kept, read)


def keys(aws, name="s"):
    items = aws.query(TableName="t", KeyConditionExpression="#s = :s", ExpressionAttributeNames={"#s": "store"},
                      ExpressionAttributeValues={":s": {"S": name}})["Items"]
    return sorted(item["key"]["S"] for item in items)


def overtaking(kept, other):
    """Makes `other` commit once, just before `kept`'s next DynamoDB transaction: `kept` is overtaken at its commit."""
    transact = kept.client.transact_write_items

    def first_another(**request):
        kept.client.transact_write_items = transact
        other()
        return transact(**request)
    kept.client.transact_write_items = first_another


def memory():
    """A store in memory holding the same schemas: what a store kept in DynamoDB is held to."""
    store = Builders.Store()
    store.transaction(crm)
    return store


def people_in_memory(store):
    with store.transaction():
        return sorted((str(p.name), store.member(p, "age"), sorted(str(e.known.name) for e in p.knows),
                       sorted((str(e.name), store.member(e.pet, "kind")) for e in p.pets))
                      for p in store.extent("crm.Person"))


# --- Its transaction ---


def test_a_transaction_spans_several_exec_calls(aws):
    """One transaction of the table's, held open over several `exec` calls: each reads what those before it wrote,
    within it, the table holding none of it until the scope ends; then all of it commits as one journal transaction,
    under the uid of the first expression given."""
    kept = process()
    kept.transaction(person("bob", "Bob"))
    with kept.transaction() as txn:
        (made,) = frames(txn.exec(Frame(), person("ada", "Ada", 36, knows=["bob"]), True))
        assert made.journal is not None and len(kept.journal()) == 1  # nothing in the table yet
        (found,) = frames(txn.exec(Frame(), named("Ada"), False))  # read-only: it reads what the first wrote
        assert paths(kept, [found]) == [({"x": contact("ada")}, {"x": {"name": "Ada"}})]
        (older,) = frames(txn.exec({"x": found["x"]}, aged("Ada", 37), True))
        assert older.assigns == {"x": {"name": "Ada", "age": 37}} and older.scope == "0"
    journal = kept.journal()
    assert [t["id"] for t in journal] == ["person/bob", "person/ada"]
    assert people(kept) == [("Ada", 37, ["Bob"], []), ("Bob", None, [], [])]
    twin = memory()
    for spec in (person("bob", "Bob"), person("ada", "Ada", 36, knows=["bob"]), aged("Ada", 37)):
        twin.transaction(spec)
    assert people(kept)[:1] == [(*people_in_memory(twin)[0][:3], [])]


def test_its_frames_and_data_are_a_store_in_memorys(aws):
    """The same transactions, in a store kept in DynamoDB and in one in memory, give the same frames, by their
    references, and leave the same data: an upsert making owned objects, run again finding them; a map through
    entries and properties; an update setting and clearing; a reduce; a partition's branches joined."""
    kept, twin = process(), memory()
    pairs = lambda t: t.uid("pairs/1").mutations([lambda mu: mu.map(lambda m: m.symbols([lambda p: p.symbols(  # noqa
        {"x": t.crm.Person, "y": t.crm.Person})])).then([lambda w: w.reduce(lambda r: r.symbols([
            lambda c: c.symbols(["y"])]))])])
    split = lambda t: t.uid("split/1").mutations([lambda mu: mu.map(lambda m: m.symbols([lambda p: p.symbols(  # noqa
        {"x": t.crm.Person})])).then([lambda w: w.partition(lambda q: q.branches([
            lambda e: e.branch(lambda b: b.then([lambda w2: w2.map(lambda m2: m2.symbols([lambda p: p.symbols(
                {"x": t.crm.Person}).spec([lambda _: _.x.name("Ada")])]))])),
            lambda e: e.branch(lambda b: b.then([lambda w2: w2.update(lambda u: u.apply([lambda p: p.symbols(
                {"x": t.crm.Person}).spec([lambda _: _.x.age(7)])]))]))]))])])
    knowing = lambda t: t.uid("knowing/1").mutations([lambda mu: mu.map(lambda m: m.symbols([lambda p: p.symbols(  # noqa
        {"x": t.crm.Person, "y": t.crm.Person}).spec([lambda _: _.x.knows([_.y]), lambda _: _.y.name("Bob")])]))])
    homed = update(contact("ada"), lambda _: _.x.home(lambda h: h.city("Oslo")), "homed/1")
    for spec in (person("bob", "Bob"), person("ada", "Ada", 36, knows=["bob"]), person("ada", "Ada", 36, knows=["bob"]),
                 everyone, knowing, aged("Ada", 40), aged("Ada", None), pairs, split, pet("bob", "rex", "dog"),
                 pet("bob", "rex", "dog"), homed):
        mine, theirs = paths(kept, kept.transaction(spec)), paths(twin, twin.transaction(spec))
        assert sorted(map(repr, mine)) == sorted(map(repr, theirs)), spec
    assert people(kept) == people_in_memory(twin) == [("Ada", 7, ["Bob"], []), ("Bob", 7, [], [("rex", "dog")])]
    (home,) = kept.transaction(update(contact("ada"), lambda _: _.x.name("Ada"), "home/1"))
    assert home.assigns == {"x": {"name": "Ada"}}
    with kept.transaction() as txn:
        (given,) = frames(txn.exec(Frame(), lambda t: t.uid("home/2").mutations([lambda mu: mu.map(lambda m: m.symbols([
            lambda p: p.symbols({"x": reference(t._store, contact("ada"))}).spec([
                lambda _: _.x.home(lambda h: h.city("Oslo"))])]))]), True))
    assert Plain.ToPlain(kept)(kept.registered("crm.Home"), given.assigns["x"]["home"]) == {"city": "Oslo"}


def test_read_only_it_halts_and_the_same_transaction_completes_it(aws):
    """Read-only, a frame halts where a term would write, given with its remainder, which the same transaction
    executes again, writing: the loopback, as a layer with no layer below gives it; a partition's join kept through
    it; an error annotated on its frame, the scope then raising it and writing nothing."""
    kept = process()
    kept.transaction(person("ada", "Ada", 36))
    with kept.transaction() as txn:
        (halted,) = frames(txn.exec(Frame(), aged("Ada", 60), False))
        assert halted.remainder.subterm == "$.mutations[0].map.then[0].update"
        (looped,) = frames(txn.exec(halted, halted.remainder, True))
    assert looped.assigns == {"x": {"name": "Ada", "age": 60}} and people(kept) == [("Ada", 60, [], [])]
    split = lambda t: t.uid("split/2").mutations([lambda mu: mu.map(lambda m: m.symbols([lambda p: p.symbols(  # noqa
        {"x": t.crm.Person}).spec([lambda _: _.x.name("Ada")])])).then([lambda w: w.partition(lambda q: q.branches([
            lambda e: e.branch(lambda b: b.then([lambda w2: w2.update(lambda u: u.apply([lambda p: p.symbols(
                {"x": t.crm.Person}).spec([lambda _: _.x.age(3)])]))]))]).then([lambda w3: w3.map(lambda m3: m3.symbols([
                    lambda p: p.symbols({"bk": t.crm.Book})]))]))])])
    with kept.transaction() as txn:
        (joined,) = frames(txn.exec(Frame(), split, False))
    assert joined.remainder is None and sorted(joined) == ["bk", "x"] and people(kept) == [("Ada", 3, [], [])]
    nobody = reference(kept, contact("zed"))
    older = aged("Ada", 50)
    with pytest.raises(LookupError, match="no object of the store has its reference's path") as raised:
        with kept.transaction() as txn:
            items = list(txn.exec([{"x": joined["x"]}, {"x": nobody}], lambda t: t.uid("older/1").mutations([
                lambda mu: mu.update(lambda u: u.apply([lambda p: p.symbols({"x": t.crm.Person}).spec([
                    lambda _: _.x.age(50)])]))]), True))
            assert [bool(frame.errors) for frame in frames(items)] == [False, True]
    assert len(raised.value.frames) == 2 and people(kept) == [("Ada", 3, [], [])]  # nothing written
    assert older


def test_unique_clauses_refuse_a_transaction_whole(aws):
    """A write that leaves two entries agreeing on a clause's key is refused, whole, with the message a store in memory
    gives; removing the one held first is accepted, the object moved to its new key, and what it owns with it."""
    kept, twin = process(), memory()
    for store in (kept, twin):
        store.transaction(person("ada", "Ada"))
        store.transaction(person("bob", "Bob"))
        store.transaction(pet("bob", "rex", "dog"))
    def rekeyed(*specs):
        """Bob, by his name, with the book; an update of the book's contacts; then Bob dropped from the frame."""
        return lambda t: t.uid(f"rekeyed/{len(specs)}").mutations([lambda mu: mu.map(lambda m: m.symbols([
            lambda p: p.symbols({"x": t.crm.Person, "b": t.crm.Book}).spec([lambda _: _.x.name("Bob")])])).then([
                lambda w: w.update(lambda u: u.apply([lambda p: p.symbols({"x": t.crm.Person, "b": t.crm.Book}).spec(
                    list(specs))])).then([lambda w2: w2.reduce(lambda r: r.symbols([lambda c: c.symbols(["x"])]))])])])
    twice = rekeyed(lambda _: _.b.contacts({"ada": lambda e: e.contact(_.x)}))
    moved = rekeyed(lambda _: _.b.contacts(lambda a: a.remove(lambda e: e.contact(_.x))),
                    lambda _: _.b.contacts({"robert": lambda e: e.contact(_.x)}))
    messages = []
    for store in (kept, twin):
        with pytest.raises(ValueError, match="breaks unique") as refused:
            store.transaction(twice)
        messages.append(str(refused.value))
    assert messages[0] == messages[1]
    for store in (kept, twin):
        store.transaction(moved)
    found = held(kept, lambda layer: (layer.designated(contact("robert")), layer.designated(contact("bob")),
                                      layer.designated(contact("robert", "rex")), layer.designated(contact("bob", "rex"))))
    assert found[0]["values"]["name"] == "Bob" and found[1] is None
    assert found[2]["values"]["kind"] == "dog" and found[3] is None  # what it owns, moved with it
    assert people(kept) == people_in_memory(twin)
    again = process()  # another process reads it from the table
    assert held(again, lambda layer: layer.designated(contact("robert", "rex"))["values"]) == {"kind": "dog"}


def test_a_deletion_reaches_what_its_store_never_read(aws):
    """Removing an owning entry deletes what it owned, at the commit, and what that owns, in cascade, every entry
    linking either removed, from the index too, in another process that never read them; the journal names the
    removal alone, by the owner's key, as a store in memory's does."""
    one, twin = process(), memory()
    for store in (one, twin):
        store.transaction(person("bob", "Bob"))
        store.transaction(person("ada", "Ada", knows=["bob"]))
        store.transaction(pet("bob", "rex", "dog"))
    seen, theirs = [], []
    other = process()  # nothing read
    one.follow(seen.append)
    twin.follow(theirs.append)
    for store in (other, twin):
        store.transaction(unbooked("bob"))
    one.poll()
    assert seen[-1][0]["changes"] == theirs[-1][0]["changes"] == [{"entry": {"remove": {
        "holder": {"symbol": "o0"}, "adjacency": "contacts", "properties": {"key": "bob"}}}}]
    assert people(one) == people_in_memory(twin) == [("Ada", None, [], [])]
    assert held(one, lambda layer: (layer.designated(contact("bob")), layer.designated(contact("bob", "rex")))) == (
        None, None)
    kinds = {key.split("#")[0] + "#" + key.split("#")[1] for key in keys(aws) if key[0] in "XR"}
    assert kinds == {"X#crm.Person", "R#" + DynamoDB.digest(contact("ada"))}  # Ada alone, and her path
    assert len([key for key in keys(aws) if key.startswith("E#")]) == 2  # Ada's contact entry, under her and the book


def test_an_object_made_with_no_owner_is_refused(aws):
    kept = process()
    stray = lambda t: t.uid("stray/1").mutations([lambda mu: mu.upsert(lambda u: u.exists([lambda p: p.symbols(  # noqa
        {"p": t.crm.Person}).spec([lambda _: _.p.name("Nobody")])]))])
    with pytest.raises(ValueError, match="needs an owner through 'book'"):
        kept.transaction(stray)
    assert kept.journal() == []


def badges(txn):
    """crm.Badge, a crm.Tag with a level besides: a schema refining another."""
    S = txn.Schemas
    return txn.uid("badges/1").mutations([lambda mu: mu.upsert(lambda u: u.exists([lambda p: p.symbols(
        {"Badge": S.OfObject}).spec([lambda _: _.Badge.name("crm.Badge").ref(True).refines([txn.crm.Tag]).properties(
            {"level": S.OfInt})])]))])


def tag_ada(kind, label, uid):
    """Ada tagged with a counted tag of `kind`, labelled `label`."""
    return lambda t: t.uid(uid).mutations([lambda mu: mu.map(lambda m: m.symbols([lambda p: p.symbols(
        {"x": reference(t._store, contact("ada"))})])).then([lambda w: w.upsert(lambda u: u.exists([lambda p: p.symbols(
            {"g": getattr(t.crm, kind), "x": reference(t._store, contact("ada"))}).spec([
                lambda _: _.g.label(label).on(lambda a: a.insert(lambda e: e.tagged(_.x)))])]))])])


def test_a_symbol_binds_objects_of_the_schemas_refining_its_own(aws):
    """A symbol's candidates are the objects of its schema and of each schema refining it, as a store in memory's extent
    has them: a map over crm.Tag binds a crm.Badge too, a spec on what crm.Tag declares finding it; one over crm.Badge
    binds the badge alone."""
    kept, twin = process(), memory()
    for store in (kept, twin):
        for spec in (badges, person("ada", "Ada"), tag_ada("Tag", "vip", "tag/1"), tag_ada("Badge", "gold", "tag/2")):
            store.transaction(spec)

    def mapped(kind, label=None):
        spec = [] if label is None else [lambda _: _.g.label(label)]
        return lambda t: t.uid("mapped/1").mutations([lambda mu: mu.map(lambda m: m.symbols([lambda p: p.symbols(
            {"g": getattr(t.crm, kind)}).spec(spec)]))])

    for kind, label, count in (("Tag", None, 2), ("Tag", "gold", 1), ("Badge", None, 1), ("Badge", "vip", 0)):
        assert len(kept.transaction(mapped(kind, label))) == len(twin.transaction(mapped(kind, label))) == count
    Tag, Badge = kept.registered("crm.Tag"), kept.registered("crm.Badge")
    assert held(kept, lambda layer: [(row["schema"], row["values"]["label"]) for row in layer.candidates(Tag)]) == [
        ("crm.Tag", "vip"), ("crm.Badge", "gold")]
    assert held(kept, lambda layer: [row["values"]["label"] for row in layer.candidates(Badge)]) == ["gold"]


def test_a_counted_object_is_kept_and_refused_by_reference(aws):
    """A counted object has no reference: kept, and linked by the objects that have one; a frame binding it is refused,
    as a store in memory refuses it, but resumed within the transaction that gave it; the journal names it by its
    value. A transaction making it again, of the same uid, finds it again; another's makes another."""
    kept = process()
    kept.transaction(person("ada", "Ada"))
    tag = lambda t: t.uid("tag/1").mutations([lambda mu: mu.upsert(lambda u: u.exists([lambda p: p.symbols(  # noqa
        {"g": t.crm.Tag}).spec([lambda _: _.g.label("vip")])]))])
    tagged = lambda t: t.uid("tagged/1").mutations([lambda mu: mu.map(lambda m: m.symbols([lambda p: p.symbols(  # noqa
        {"x": reference(t._store, contact("ada"))})])).then([lambda w: w.update(lambda u: u.apply([lambda p: p.symbols(
            {"g": t.crm.Tag, "x": reference(t._store, contact("ada"))}).spec([
                lambda _: _.g.on(lambda a: a.insert(lambda e: e.tagged(_.x)))])]))])])
    given = []
    kept.follow(given.append)
    with kept.transaction() as txn:
        (made,) = frames(txn.exec(Frame(), tag, True))
        with pytest.raises(LookupError, match="a counted 'crm.Tag' has no reference"):
            made["g"]
        (again,) = frames(txn.exec(Frame(), tag, True))  # the same uid: found again
        (linked,) = frames(txn.exec(made, tagged, True))  # resumed within the transaction that gave it
        assert not linked.errors
    other = lambda t: t.uid("tag/2").mutations([lambda mu: mu.upsert(lambda u: u.exists([  # noqa: E731
        lambda p: p.symbols({"g": t.crm.Tag}).spec([lambda _: _.g.label("vip")])]))])
    kept.transaction(other)  # made, linked by nothing: no data a journal names, so nothing is committed
    with kept.transaction() as txn:
        (made_again,) = frames(txn.exec(Frame(), other, True))  # another transaction's: another, linked
        frames(txn.exec(made_again, tagged, True))
    Tag = kept.registered("crm.Tag")
    assert held(kept, lambda layer: [row["values"] for row in layer.candidates(Tag)]) == [{"label": "vip"}] * 2
    assert len(kept.journal()) == 3
    ((transaction,),) = given[:1]
    (link,) = [change["entry"]["insert"]["links"]["tag"] for change in transaction["changes"] if "entry" in change]
    assert link["counted"]["properties"] == {"label": "vip"}
    with pytest.raises(LookupError, match="symbol 'g' of the frame: a counted 'crm.Tag' has no reference"):
        with kept.transaction() as txn:
            list(txn.exec(made, tagged, True))  # given to another transaction: refused


# --- How DynamoDB orders transactions ---


def test_a_transaction_overtaken_at_its_commit_is_caught_up_and_interpreted_again(aws):
    """Where another commits between a transaction's beginning and its commit, DynamoDB refuses its commit on the
    head's condition: a transaction given whole is caught up and interpreted again, which what the other committed
    may change, and both are kept, in the order DynamoDB gave; its followers are given both, in that order."""
    one, two = process(), process()
    one.transaction(first_note("n1", "first"))
    seen = []
    two.follow(seen.append)
    overtaking(two, lambda: one.transaction(appended("n2", "one's")))
    two.transaction(appended("n3", "two's"))  # after n2, as it finds it once caught up
    assert [t["id"] for t in two.journal()] == ["note/n1", "append/n2", "append/n3"]
    assert [journal[0]["id"] for journal in seen] == ["note/n1", "append/n2", "append/n3"] and lasts(two) == ["two's"]


def test_a_transaction_held_open_overtaken_is_refused(aws):
    """A transaction held open over `exec` calls, whose frames have been given, cannot be interpreted again: overtaken
    at its commit, it is refused, nothing it wrote kept, to be run again by its writer."""
    one, two = process(), process()
    one.transaction(first_note("n1", "first"))
    with pytest.raises(DynamoDB.Conflict, match="another store committed first"):
        with two.transaction() as txn:
            list(txn.exec(Frame(), appended("n3", "two's"), True))
            one.transaction(appended("n2", "one's"))
    assert [t["id"] for t in two.journal()] == ["note/n1", "append/n2"] and lasts(two) == ["one's"]


def test_overtaken_again_and_again_it_gives_up(aws):
    one, two = process(), process(attempts=1)
    overtaking(two, lambda: one.transaction(person("bob", "Bob")))
    with pytest.raises(DynamoDB.Conflict, match="1 times running"):
        two.transaction(person("cy", "Cy"))


def test_two_writers_appending_after_one_last(aws):
    """Two processes, each appending after the last it saw, by its reference: the second's removal of that last removes
    nothing, and its insertion breaks the clause that keeps one last, so that it is refused, as mbse-schemas refuses it
    in every store (CAP-293)."""
    one, two = process(), process()
    one.transaction(first_note("n1", "first"))
    one.transaction(after("n1", "n2", "one's"))
    with pytest.raises(ValueError, match="breaks unique"):
        two.transaction(after("n1", "n3", "two's"))
    assert len(one.journal()) == 2 and lasts(two) == ["one's"]


# --- Its follow, and the journal ---


def test_the_table_holds_a_journal_and_an_index_and_no_snapshot(aws):
    """The journal, transactions 1 to n in commit order, each a `Changes.Transaction`, and each one's position by its
    id; the index, by object, path and entry; the head. Nothing else: an install writes nothing, a transaction that
    changes nothing commits nothing. Its followers are given each as a one-transaction journal, and it is read after
    any position, a transaction's id, as mbse-journals slices a journal."""
    kept = process()
    process()  # another process, its install the same
    assert keys(aws) == ["H"] and kept.head() is None
    given = []
    kept.follow(given.append)
    end = kept.follow(given.append)
    end()
    end()
    for spec in (person("bob", "Bob"), person("ada", "Ada", knows=["bob"]), pet("ada", "rex", "dog"), aged("Ada", 3)):
        kept.transaction(spec)
    assert {key[0] for key in keys(aws)} == {"H", "L", "T", "X", "R", "E"}
    journal = kept.journal()
    assert [[t] for t in journal] == given and len(journal) == 4 and kept.head() == "aged/Ada/3"
    assert all(set(t) == {"id", "symbols", "changes"} for t in journal)
    assert kept.journal(after=journal[1]["id"]) == journal[2:] and kept.journal(after=journal[-1]["id"]) == []
    with pytest.raises(LookupError, match="no transaction"):
        kept.journal(after="nothing")
    kept.transaction(named("Ada"))
    kept.transaction(aged("Ada", 3))  # it changes nothing: nothing committed
    assert len(kept.journal()) == 4


def test_a_journal_a_follower_applies(aws):
    """What its followers are given is a journal, which mbse-schemas applies to a store as any journal: a replica in
    memory, given the journal from the first, applied at once, holds what the store kept in DynamoDB holds."""
    kept, twin = process(), memory()
    for spec in (person("bob", "Bob"), person("ada", "Ada", 36, knows=["bob"]), pet("bob", "rex", "dog"),
                 aged("Ada", None), update(contact("ada"), lambda _: _.x.home(lambda h: h.city("Oslo")), "home/1"),
                 unbooked("bob"), first_note("n1", "first"), appended("n2", "second")):
        kept.transaction(spec)
        twin.transaction(spec)
    replica = memory()
    journal = [Plain.FromPlain(replica)(replica.registered("Changes.Journal"), [transaction])
               for transaction in kept.journal()]
    for transaction in journal:  # each as a follower is given it
        replica.transaction(lambda txn, one=transaction: Changes.apply(txn, one))
    assert people_in_memory(replica) == people_in_memory(twin) == [("Ada", None, [], [])]
    with replica.transaction():
        assert [str(e.note.text) for e in replica.singleton("crm.TheBook").last] == ["second"]


def test_others_commits_reach_followers_and_a_cache_layer_in_commit_order(aws):
    """Two processes over one partition: what one commits, the other's followers are given whole, in commit order, when
    it catches up, on `poll()` or as a transaction of its begins; a cache layer over it is changed where it holds the
    objects; while a transaction of its is open, a poll gives nothing."""
    one, two = process(), process()
    cache = Builders.Store(two)
    cache.transaction(crm)
    cache.transaction(person("ada", "Ada"))
    given = []
    two.follow(given.append)
    one.transaction(aged("Ada", 5))
    one.transaction(person("bob", "Bob"))
    with two.transaction():
        assert two.poll() == 0  # it caught up as the transaction began
    assert [journal[0]["id"] for journal in given] == ["aged/Ada/5", "person/bob"] and two.poll() == 0
    with cache.transaction():
        assert sorted((str(p.name), cache.member(p, "age")) for p in cache.extent("crm.Person")) == [
            ("Ada", 5), ("Bob", None)]  # Bob made under the book, which it holds
    one.transaction(aged("Ada", 6))
    assert two.poll() == 1
    with cache.transaction():
        assert sorted(str(p.name) for p in cache.extent("crm.Person") if cache.member(p, "age") == 6) == ["Ada"]


# --- Provenance ---


def test_a_transaction_is_recorded_once_in_the_lowest_layer(aws):
    """mbse-schemas' Provenance: the store kept in DynamoDB, the lowest layer, records each transaction that makes or
    writes an object, by the uid its expression gives, with its `when`, `where` and session, found again by its id;
    each object of a schema with a history linked to the transaction that made it, and to the latest that wrote it; a
    cache layer over it records none, its record reaching it as the store below's writes do."""
    kept = process()
    cache = Builders.Store(kept)
    cache.transaction(crm)
    cache.transaction(Provenance.install)
    cache.transaction(lambda t: Provenance.extend(t, [t.crm.Person]))
    stamped = lambda uid, key: lambda t: person(key, key.title())(t).uid(uid).when("2026-11-20T10:00:00Z").session(  # noqa
        lambda s: s.id("s1").who("ann").locale("en-GB")).where(lambda w: w.latitude(1.5).longitude(2.5))
    kept.transaction(stamped("w1", "ada"))
    cache.transaction(stamped("w2", "bob"))
    kept.transaction(update(contact("ada"), lambda _: _.x.age(3), "w3"))
    kept.transaction(update(contact("ada"), lambda _: _.x.age(4), "w4"))

    def read(layer):
        directory = layer.designated({"singleton": "Provenance.Transactions", "next": {"target": {}}})
        records = layer.entries(directory, layer.schema_of(directory), "transactions")
        ada = layer.designated(contact("ada"))
        made = [e["links"]["transaction"] for e in layer.linking(ada, kept.registered("Provenance.CreatedBy"))]
        modified = [e["links"]["transaction"] for e in layer.linking(ada, kept.registered("Provenance.ModifiedBy"))]
        by_oid = {e["links"]["transaction"]: e["values"]["uid"] for e in records}
        first = layer.link(records[0], "transaction")
        sessions = layer.entries(first, layer.schema_of(first), "session")
        return ([e["values"]["uid"] for e in records], [by_oid[o] for o in made], [by_oid[o] for o in modified],
                first["values"], layer.link(sessions[0], "session")["values"])
    uids, made, modified, record, session = held(kept, read)
    assert uids == ["w1", "w2", "w3", "w4"] and made == ["w1"] and modified == ["w4"]
    assert record == {"when": "2026-11-20T10:00:00Z", "where": {"latitude": 1.5, "longitude": 2.5}}
    assert session == {"started": "2026-11-20T10:00:00Z", "locale": "en-GB"}
    with cache.transaction():
        assert [str(e.uid) for e in cache.singleton("Provenance.Transactions").transactions] == uids  # recorded below,
        # held above as the store below's writes are
    with pytest.raises(ValueError, match="session 's1' is another identity's"):
        kept.transaction(lambda t: person("cy", "Cy")(t).uid("w5").session(lambda s: s.id("s1").who("bea")))
    with pytest.raises(ValueError, match="a transaction's session has no id"):
        kept.transaction(lambda t: person("cy", "Cy")(t).uid("w6").session(lambda s: s.who("bea")))
    with kept.transaction() as txn:  # an imperative scope's record: its uid the store's, its session unknown
        frames(txn.exec(Frame(), lambda t: t.mutations([lambda mu: mu.upsert(lambda u: u.exists([lambda p: p.symbols(
            {"b": t.crm.Book, "p": t.crm.Person}).spec([lambda _: _.b.contacts({"dee": lambda e: e.contact(_.p)})])]))]),
            True))
    assert len(held(kept, read)[0]) == 5
