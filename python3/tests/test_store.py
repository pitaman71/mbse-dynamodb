"""The store kept in DynamoDB's own cases (CAP-289), in a mocked DynamoDB: what it answers, as the store in memory does;
how DynamoDB orders its transactions, among several stores over one partition, each standing for a process of its
own; what its followers are given; and what it keeps in the table, which is no snapshot."""

from __future__ import annotations

import json

import pytest
from botocore.exceptions import ClientError
from mbse.Schemas.Framework import JSON, Builders, References, Stores

from mbse.Persistence.DynamoDB import Store as DynamoDB

from conftest import REGION


def crm(txn):
    """crm.Person, owned by the singleton book by key, with a name and a home; persons know persons, and introduce
    persons to persons (an open relation, which keeps equal entries), and own pets by name; persons are tagged with
    counted tags; the book keeps a log of notes, owned by name, and its one last note."""
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
            .uniques([["knower", "known"]]),
            lambda _: _.Introduces.name("crm.Introduces").links([lambda t: t.name("host"), lambda t: t.name("guest")]),
            lambda _: _.Tagged.name("crm.Tagged").links([lambda t: t.name("tagged"), lambda t: t.name("tag")]),
            lambda _: _.Tag.name("crm.Tag").ref(True).properties({"label": S.OfStr}),
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
            lambda _: _.Person.name("crm.Person").ref(True).properties({"name": S.OfStr, "home": _.Home}).adjacencies([
                lambda t: t.name("book").relation([_.Holds]).me("contact").owner(True),
                lambda t: t.name("knows").relation([_.Knows]).me("knower"),
                lambda t: t.name("known_by").relation([_.Knows]).me("known"),
                lambda t: t.name("hosts").relation([_.Introduces]).me("host"),
                lambda t: t.name("tags").relation([_.Tagged]).me("tagged"),
                lambda t: t.name("pets").relation([_.Keeps]).me("keeper")]),
        ])]))])


BOOK = {"singleton": "crm.TheBook", "next": {"target": {}}}


def contact(key):
    return {"singleton": "crm.TheBook", "next": {"entry": {"i": "contacts", "o": "book", "properties": {"key": key},
                                                           "next": {"target": {}}}}}


def process(name="s", **options):
    """A store kept in DynamoDB: one process's."""
    return DynamoDB.Store("t", store=name, region=REGION, **options)


def above(below):
    """A store over it, as an app holds one, its schemas installed, which it runs in the store below first."""
    made = Builders.Store(below)
    made.transaction(crm)
    return made


def store(name="s", **options):
    """An app's store over one process's store kept in DynamoDB."""
    return above(process(name, **options))


def add(s, *keys):
    """Persons added to the book of `s`, by key, each named by it."""
    book = s.singleton("crm.TheBook")
    with s.transaction():
        return [s.crm.Person(lambda p, k=k: p.book(lambda a: a.insert(lambda e: e.key(k).book(book))).name(k))
                for k in keys]


def person(s, key):
    return next(e.contact for e in s.singleton("crm.TheBook").contacts if e.key.get() == key)


def append(s, name, text):
    """A note appended to the book's log, after its last as `s` sees it: the last moved to it in one transaction."""
    book = s.singleton("crm.TheBook")
    last = next((e.note for e in book.last), None)
    with s.transaction():
        note = s.crm.Note(lambda n: n.book(lambda a: a.insert(lambda e: e.name(name).book(book))).text(text))
        s.crm.Book(book, lambda b: b.last(lambda a: (a.remove(lambda e: e.note(last)) if last is not None else a)
                                          .insert(lambda e: e.note(note))))
    return note


def write(s):
    """The same transactions, written into any store: persons who know and introduce one another, a home, a removal."""
    ada, bob, cy, dee = add(s, "ada", "bob", "cy", "dee")
    with s.transaction():
        s.crm.Person(ada, lambda p: p.home(lambda h: h.city("Paris")).knows(lambda a: a.insert(lambda e: e.known(bob))
                                                                              .insert(lambda e: e.known(cy))))
        s.crm.Person(bob, lambda p: p.hosts(lambda a: a.insert(lambda e: e.guest(cy)).insert(lambda e: e.guest(cy))))
    with s.transaction():
        s.crm.Person(ada, lambda p: p.home(lambda h: h.city("Rome")).name("Ada"))
        s.crm.Person(dee, lambda p: p.knows(lambda a: a.insert(lambda e: e.known(ada))))
    with s.transaction():
        s.crm.Person(bob, lambda p: p.hosts(lambda a: a.remove(lambda e: e.guest(cy))))
    with s.transaction():  # cy goes, and every entry that links cy
        s.crm.Book(s.singleton("crm.TheBook"), lambda b: b.contacts(lambda a: a.remove(lambda e: e.key("cy"))))
    append(s, "n1", "first")
    append(s, "n2", "second")


def documents(s):
    return [JSON.dumps(s.document(path)) for path in [BOOK, *(contact(k) for k in ("ada", "bob", "cy", "dee", "zed"))]]


def keys(aws, name="s"):
    items = aws.query(TableName="t", KeyConditionExpression="#s = :s", ExpressionAttributeNames={"#s": "store"},
                      ExpressionAttributeValues={":s": {"S": name}})["Items"]
    return sorted(item["key"]["S"] for item in items)


def test_documents_as_the_store_in_memory_gives_them(aws):
    """Each object's document, read from the index one object at a time, is the one the store in memory gives after
    the same transactions: properties, value objects, entries in each named adjacency, equal entries of an open
    relation each kept, a removed entry gone, a deleted object gone with each entry linking it. A process that reads
    it needs no schemas: the index holds documents."""
    memory, kept = Builders.Store(), process()
    memory.transaction(crm)
    write(memory)
    write(above(kept))
    assert isinstance(kept, Stores.Below) and not isinstance(kept, Builders.Store)
    assert documents(kept) == documents(memory)
    assert kept.document(contact("cy")) is None and kept.document({"singleton": "crm.Nobody", "next": {"target": {}}}) is None
    assert documents(process()) == documents(memory)  # another process, given no installs


def test_a_store_above_loads_one_object_at_a_time_and_writes_through(aws):
    write(store())
    kept = DynamoDB.Store("t", store="s", region=REGION, environ={"MBSE_DYNAMODB_CREATE": "0"})
    read, document = [], kept.document
    kept.document = lambda path: (read.append(path), document(path))[1]
    app = above(kept)
    ada = person(app, "ada")
    assert (ada.name.get(), ada.home.city.get(), len(read)) == ("Ada", "Rome", 2)  # the book, then ada
    with app.transaction():
        app.crm.Person(ada, lambda p: p.name("Ada L."))
    fresh = store()
    assert person(fresh, "ada").name.get() == "Ada L."
    assert sorted(e.key.get() for e in fresh.singleton("crm.TheBook").contacts) == ["ada", "bob", "dee"]


def test_the_table_holds_a_journal_and_an_index_and_no_snapshot(aws):
    """The journal, entries 1 to n in commit order, each the changes as mbse-schemas writes them; the index, by object;
    the head. Nothing else: a schema install writes nothing, and nothing is ever a snapshot of the store."""
    kept = process()
    app = above(kept)
    assert keys(aws) == ["H"]
    effects = []
    kept.follow(effects.append)
    end = kept.follow(effects.append)
    end()
    end()
    write(app)
    found = keys(aws)
    assert {key[0] for key in found} == {"H", "L", "O"}
    journal = list(kept.journal())
    assert journal == effects and len(journal) == 7
    assert list(kept.journal(after=5)) == effects[5:]
    same = {"mutations": [{"upsert": {"exists": [{"symbols": {"x": {"reference": {"path": contact("ada")}}}, "spec": [
        {"x": [{"as_property": {"name": "name", "mutations": [{"set": {"value": "Ada"}}]}}]}]}]}}]}
    assert kept.commit(same) is None and len(list(kept.journal())) == 7  # it changed nothing: nothing committed


def test_others_commits_reach_followers_in_commit_order(aws):
    """Two processes over one partition: what one commits, the other's followers are given whole, as committed, in
    commit order, when it catches up; a store above it is changed where its objects are present."""
    one, two = process(), process()
    first = above(one)
    add(first, "ada", "bob")
    app = above(two)
    two.poll()
    ada = person(app, "ada")
    given = []
    two.follow(given.append)
    with first.transaction():
        first.crm.Person(person(first, "ada"), lambda p: p.name("Ada"))
    with first.transaction():
        first.crm.Person(person(first, "bob"), lambda p: p.name("Bob"))  # bob is present in neither store over two
    assert ada.name.get() == "ada"
    assert two.poll() == 2 and two.poll() == 0
    assert given == list(one.journal(after=1))
    assert ada.name.get() == "Ada" and person(app, "bob").name.get() == "Bob"


def test_two_writers_appending_after_one_last(aws):
    """Two processes, each appending after the last it saw: the second's removal of that last removes nothing, and its
    insertion breaks the clause that keeps one last, so that it is refused, as mbse-schemas refuses it in every store
    (CAP-293); its store above then holds what the first wrote."""
    one, two = process(), process()
    first, second = above(one), above(two)
    append(first, "n1", "first")
    assert [e.note.text.get() for e in second.singleton("crm.TheBook").last] == ["first"]
    append(first, "n2", "one's")
    with pytest.raises(ValueError, match="breaks unique"):
        append(second, "n3", "two's")
    assert [e.note.text.get() for e in second.singleton("crm.TheBook").last] == ["one's"]
    assert len(list(one.journal())) == 2  # n1, n2: two's refused, never committed


def overtaking(s, other):
    """Makes `other` commit once, just before `s`'s next DynamoDB transaction: `s` is overtaken at its commit."""
    transact = s.client.transact_write_items

    def first_another(**request):
        s.client.transact_write_items = transact
        other()
        return transact(**request)
    s.client.transact_write_items = first_another


def test_a_writer_overtaken_at_its_commit_interprets_again(aws):
    """Where another commits between a store's catching up and its commit, DynamoDB refuses its commit on the head's
    condition; it catches up and interprets the transaction again, and both are kept, in the order DynamoDB gave. A
    transaction written in a `with` block above is given as plain data, so that it too is interpreted again."""
    one, two = process(), process()
    first = above(one)
    add(first, "ada")
    second = above(two)
    overtaking(two, lambda: add(first, "bob"))
    add(second, "cy")  # a block
    assert [e.key.get() for e in second.singleton("crm.TheBook").contacts] == ["ada", "cy", "bob"]  # its own first
    assert [len(list(one.journal())), len(list(two.journal())), two._seq] == [3, 3, 3]
    assert process().document(contact("bob")) is not None
    assert [e.key.get() for e in store().singleton("crm.TheBook").contacts] == ["ada", "bob", "cy"]


def test_a_writer_overtaken_again_and_again_gives_up(aws):
    one, two = process(), process(attempts=1)
    first = above(one)
    overtaking(two, lambda: add(first, "bob"))
    with pytest.raises(DynamoDB.Conflict, match="1 times running"):
        add(above(two), "cy")


def test_index_items_too_many_for_one_transaction_written_out_after(aws):
    """A transaction whose index items are more than one DynamoDB transaction writes beside its entry: its entry and
    its index items, kept apart, are committed first, then written out; a store that reads meanwhile writes them out
    first, as one does where the writer stopped before it did."""
    memory, kept = Builders.Store(), process(max_actions=6)
    memory.transaction(crm)
    app = above(kept)
    write(memory)
    write(app)
    assert documents(kept) == documents(memory)
    assert any(key.startswith("P#") for key in keys(aws))
    batch = kept.client.batch_write_item

    def stopped(**request):  # the writer stops before it writes them out
        kept.client.batch_write_item = batch
        raise ClientError({"Error": {"Code": "InternalServerError", "Message": "stopped"}}, "BatchWriteItem")
    kept.client.batch_write_item = stopped
    add(app, "eve", "fay")
    head = aws.get_item(TableName="t", Key={"store": {"S": "s"}, "key": {"S": "H"}})["Item"]
    assert int(head["indexed"]["N"]) == int(head["seq"]["N"]) - 1
    reader = process()
    assert JSON.loads(JSON.dumps(reader.document(contact("fay"))))["mutations"][0]["upsert"]["exists"][0]["spec"][0][
        "0"][0] == {"as_property": {"name": "name", "mutations": [{"set": {"value": "fay"}}]}}
    head = aws.get_item(TableName="t", Key={"store": {"S": "s"}, "key": {"S": "H"}})["Item"]
    assert head["indexed"] == head["seq"]


def test_index_items_written_out_by_two_at_once(aws):
    """Two stores writing out the same index items: the second's moving `indexed` finds it moved already."""
    kept = process(max_actions=4)
    batch = kept.client.batch_write_item

    def stopped(**request):
        kept.client.batch_write_item = batch
        raise ClientError({"Error": {"Code": "InternalServerError", "Message": "stopped"}}, "BatchWriteItem")
    kept.client.batch_write_item = stopped
    add(above(kept), "ada")
    number = kept._seq
    reader = process()
    reader._write_out(number)  # as a reader that read the head before another wrote them out
    reader._write_out(number)
    assert reader.document(contact("ada")) is not None


def test_a_journal_entry_and_a_document_in_parts(aws):
    kept = process(part_bytes=400)
    add(above(kept), *(f"person-{n}" for n in range(5)))
    assert len([key for key in keys(aws) if key.startswith("L#")]) > 5
    assert len([key for key in keys(aws) if key.startswith(DynamoDB._object_key(BOOK) + "D#")]) > 5
    assert list(kept.journal()) == list(process().journal())
    assert len(list(kept.journal())) == 1
    assert JSON.dumps(kept.document(contact("person-3"))) == JSON.dumps(above(process()).document(contact("person-3")))


def test_more_than_one_dynamodb_transaction_writes_is_refused(aws):
    kept = process(max_bytes=500)
    with pytest.raises(ValueError, match="more than one DynamoDB transaction writes"):
        add(above(kept), "ada")
    assert list(kept.journal()) == [] and keys(aws) == ["H"]


def test_a_counted_object_linked_is_kept_and_its_document_refused(aws):
    """A counted object has no path: a document of an object that links one is refused, as the store in memory refuses
    it, and its deletion leaves nothing of it."""
    memory, kept = Builders.Store(), process()
    memory.transaction(crm)
    app = above(kept)
    for s in (memory, app):
        (ada,) = add(s, "ada")
        with s.transaction():
            s.crm.Person(ada, lambda p: p.tags(lambda a: a.insert(lambda e: e.tag(s.crm.Tag(lambda t: t.label("vip"))))))
    with pytest.raises(ValueError) as in_memory:
        memory.document(contact("ada"))
    with pytest.raises(ValueError) as in_dynamodb:
        kept.document(contact("ada"))
    assert str(in_dynamodb.value) == str(in_memory.value)
    with app.transaction():  # a tagged person deleted: its entries go, the tag held by none
        app.crm.Book(app.singleton("crm.TheBook"), lambda b: b.contacts(lambda a: a.remove(lambda e: e.key("ada"))))
    book = DynamoDB._object_key(BOOK)
    assert [key for key in keys(aws) if key.startswith("O#")] == [book + "D#000000"]  # the book's alone, no entry left


def test_a_failure_of_dynamodb_is_raised(aws):
    kept = process()
    app = above(kept)
    transact = kept.client.transact_write_items

    def failing(**request):
        kept.client.transact_write_items = transact
        raise ClientError({"Error": {"Code": "ValidationException", "Message": "too large"}}, "TransactWriteItems")
    kept.client.transact_write_items = failing
    with pytest.raises(ClientError, match="too large"):
        add(app, "ada")
    put = kept.client.put_item

    def refused(**request):
        raise ClientError({"Error": {"Code": "AccessDeniedException", "Message": "denied"}}, "PutItem")
    kept.client.put_item = refused
    with pytest.raises(ClientError, match="denied"):
        DynamoDB.Store("t", store="other", client=kept.client)
    kept.client.put_item = put
    update = kept.client.update_item

    def lost(**request):
        raise ClientError({"Error": {"Code": "InternalServerError", "Message": "lost"}}, "UpdateItem")
    other = process("other", max_actions=4)
    other.client.update_item = lost
    add(above(other), "ada")  # committed; its index items written out, `indexed` not moved
    other.client.update_item = update
    assert process("other").document(contact("ada")) is not None


def test_configured_by_the_environment(aws):
    kept = DynamoDB.Store(environ={"MBSE_DYNAMODB_TABLE": "from-env", "MBSE_DYNAMODB_STORE": "demo",
                                   "MBSE_DYNAMODB_REGION": REGION})
    assert (kept.table, kept.store_name) == ("from-env", "demo")
    with pytest.raises(ClientError, match="ResourceNotFound"):
        DynamoDB.Store("absent", region=REGION, create=False)


def test_a_deletion_reaches_what_its_store_never_loaded(aws):
    """A store that deletes an object it never loaded removes, from the index, the object, what it owns, in cascade,
    and every entry linking either, from every document that held one, loaded or not, as the store in memory does."""
    memory, one = Builders.Store(), process()
    memory.transaction(crm)
    for s in (memory, above(one)):
        ada, bob = add(s, "ada", "bob")
        with s.transaction():
            s.crm.Person(ada, lambda p: p.knows(lambda a: a.insert(lambda e: e.known(bob))))
            s.crm.Pet(lambda p: p.keeper(lambda a: a.insert(lambda e: e.name("rex").keeper(bob))).kind("dog"))
    app = store()  # another process: nothing loaded
    for s in (memory, app):
        with s.transaction():
            s.crm.Book(s.singleton("crm.TheBook"), lambda b: b.contacts(lambda a: a.remove(lambda e: e.key("bob"))))
    assert documents(one) == documents(memory)
    rex = {"singleton": "crm.TheBook", "next": {"entry": {"i": "contacts", "o": "book", "properties": {"key": "bob"},
           "next": {"entry": {"i": "pets", "o": "keeper", "properties": {"name": "rex"}, "next": {"target": {}}}}}}}
    assert one.document(rex) is None and memory.document(rex) is None
    assert len([key for key in keys(aws) if key.startswith("O#")]) == 4  # the book's and ada's documents, and each
    # linking the other


def test_a_read_overtaken_by_a_commit_reads_again(aws):
    """Another store's commit between reading an object's items and reading the head again: the object is read again,
    so that a document is of one entry of the journal."""
    one, two = process(), process()
    first = above(one)
    add(first, "ada")
    query = two.client.query

    def overtaken(**request):
        two.client.query = query
        found = query(**request)
        with first.transaction():
            first.crm.Person(person(first, "ada"), lambda p: p.name("Ada"))
        return found
    two.client.query = overtaken
    assert JSON.dumps(two.document(contact("ada"))) == JSON.dumps(one.document(contact("ada")))
    assert '"Ada"' in JSON.dumps(two.document(contact("ada")))


def test_read_in_pages(aws):
    """DynamoDB gives a query's items in pages: each is read."""
    kept = process(part_bytes=50)
    add(above(kept), "ada", "bob")
    query = kept.client.query
    kept.client.query = lambda **request: query(**request, Limit=1)
    assert documents(kept) == documents(process()) and len(list(kept.journal())) == 1


def test_index_items_left_unprocessed_are_written_again(aws):
    kept = process(max_actions=4)
    batch = kept.client.batch_write_item

    def some_left(**request):
        (requests,) = request["RequestItems"].values()
        done = batch(RequestItems={kept.table: requests[:1]})
        return {**done, "UnprocessedItems": {kept.table: requests[1:]} if requests[1:] else {}}
    kept.client.batch_write_item = some_left
    add(above(kept), "ada", "bob")
    assert documents(kept) == documents(process())


def test_the_table_made_meanwhile_by_another(aws):
    from mbse.Persistence.DynamoDB import Table

    describe, made = aws.describe_table, []

    def absent_then_made(**request):
        if not made:
            made.append(True)
            Table.create_table(aws, request["TableName"])  # another process makes it meanwhile
            raise ClientError({"Error": {"Code": "ResourceNotFoundException", "Message": "absent"}}, "DescribeTable")
        return describe(**request)
    aws.describe_table = absent_then_made
    assert Table.create_table(aws, "raced")
    aws.describe_table = lambda **request: (_ for _ in ()).throw(
        ClientError({"Error": {"Code": "AccessDeniedException", "Message": "denied"}}, "DescribeTable"))
    with pytest.raises(ClientError, match="denied"):
        Table.create_table(aws, "raced")
    aws.describe_table = describe
    aws.create_table = lambda **request: (_ for _ in ()).throw(
        ClientError({"Error": {"Code": "LimitExceededException", "Message": "too many"}}, "CreateTable"))
    with pytest.raises(ClientError, match="too many"):
        Table.create_table(aws, "another")


def provenanced(s):
    """`s`, Provenance installed, and crm.Person recorded as modified by each transaction."""
    from mbse.Schemas.Framework import Provenance

    s.transaction(Provenance.install)
    s.transaction(lambda t: Provenance.extend(t, [t.crm.Person]))
    return s


def recorded(s):
    return [str(e.uid) for e in s.singleton("Provenance.Transactions").transactions]


def test_a_transaction_is_recorded_once_in_the_store_kept_in_dynamodb(aws):
    """mbse-schemas' Provenance: the store kept in DynamoDB, the lowest, records each transaction, the `uid` its
    expression gives where it gives one; a store above records none, its record reaching it as the store below's
    writes do, and another process reading it from the table."""
    kept = process()
    app = provenanced(above(kept))
    assert kept.records is True
    (ada,) = add(app, "ada")  # a block: no expression, no uid given
    app.transaction(app.Mutations.OfTransaction(lambda t: t.uid("w1").mutations([lambda mu: mu.upsert(lambda u: u.exists(
        [lambda p: p.symbols({"p": References.of(app, ada)}).spec([lambda _: _.p.name("Ada")])]))])))
    (first, second) = recorded(app)
    assert second == "w1" and len(list(kept.journal())) == 2
    assert all("Provenance" in JSON.dumps(entry) for entry in kept.journal())
    assert recorded(provenanced(store())) == [first, "w1"]
    with app.transaction():  # ada deleted: the records that linked her, which no store loaded, link her no longer
        app.crm.Book(app.singleton("crm.TheBook"), lambda b: b.contacts(lambda a: a.remove(lambda e: e.key("ada"))))
    linked = [json.loads(item["data"]["S"])["links"] for item in aws.scan(TableName="t")["Items"]
              if "#D#" in item["key"]["S"]]
    assert linked and all(path != contact("ada") for links in linked for path in links)
    assert process().document(contact("ada")) is None and len(recorded(provenanced(store()))) == 3
