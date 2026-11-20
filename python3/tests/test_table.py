"""What the store kept in DynamoDB asks of the table, and what it does where DynamoDB refuses or fails it, in a mocked
DynamoDB: index items more than one DynamoDB transaction writes, written out after; items in parts; reads in pages;
DynamoDB's failures raised; the settings; a table written by an earlier version refused."""

from __future__ import annotations

import json

import pytest
from botocore.exceptions import ClientError

from mbse.Persistence.DynamoDB import Store as DynamoDB
from mbse.Persistence.DynamoDB import Table

from conftest import REGION
from test_store import first_note, keys, people, person, pet, process


def stopping(kept, method, code="InternalServerError"):
    """Makes the store's next call of `method` fail, as where its writer stopped, and the one after succeed."""
    original = getattr(kept.client, method)

    def stopped(**request):
        setattr(kept.client, method, original)
        raise ClientError({"Error": {"Code": code, "Message": "stopped"}}, method)
    setattr(kept.client, method, stopped)


def test_index_items_too_many_for_one_transaction_are_written_out_after(aws):
    """A transaction whose index items are more than one DynamoDB transaction writes beside its journal transaction:
    its journal transaction and its index items, kept apart, are committed first, then written out; a store that reads
    meanwhile writes them out first, as one does where the writer stopped before it did; two at once, the second
    finds `indexed` moved already."""
    kept = process(max_actions=6)
    kept.transaction(person("ada", "Ada"))
    assert any(key.startswith("P#") for key in keys(aws))
    stopping(kept, "batch_write_item")
    kept.transaction(person("bob", "Bob", knows=["ada"]))
    head = aws.get_item(TableName="t", Key={"store": {"S": "s"}, "key": {"S": "H"}})["Item"]
    assert int(head["indexed"]["N"]) == int(head["seq"]["N"]) - 1
    reader = process()
    assert people(reader) == [("Ada", None, [], []), ("Bob", None, ["Ada"], [])]
    head = aws.get_item(TableName="t", Key={"store": {"S": "s"}, "key": {"S": "H"}})["Item"]
    assert head["indexed"] == head["seq"]
    reader._kept_write_out(int(head["seq"]["N"]))  # as a reader that read the head before another wrote them out
    assert people(process()) == people(reader)


def test_a_failure_to_move_indexed_is_left_to_the_next_reader(aws):
    kept = process(max_actions=4)
    stopping(kept, "update_item")
    kept.transaction(person("ada", "Ada"))  # committed; its index items written out, `indexed` not moved
    assert people(process()) == [("Ada", None, [], [])]


def test_index_items_left_unprocessed_are_written_again(aws):
    kept = process(max_actions=4)
    batch = kept.client.batch_write_item

    def some_left(**request):
        (requests,) = request["RequestItems"].values()
        done = batch(RequestItems={kept.table: requests[:1]})
        return {**done, "UnprocessedItems": {kept.table: requests[1:]} if requests[1:] else {}}
    kept.client.batch_write_item = some_left
    kept.transaction(person("ada", "Ada"))
    kept.client.batch_write_item = batch
    assert people(process()) == [("Ada", None, [], [])]


def test_a_journal_transaction_and_an_object_in_parts(aws):
    kept = process(part_bytes=40)
    kept.transaction(person("ada", "Ada" * 20))
    assert len([key for key in keys(aws) if key.startswith("L#")]) > 3
    assert len([key for key in keys(aws) if key.startswith("X#crm.Person#")]) > 3
    assert kept.journal() == process().journal() and people(process()) == [("Ada" * 20, None, [], [])]


def test_read_in_pages(aws):
    """DynamoDB gives a query's items in pages: each is read."""
    kept = process(part_bytes=50)
    kept.transaction(person("ada", "Ada"))
    kept.transaction(pet("ada", "rex", "dog"))
    query = kept.client.query
    kept.client.query = lambda **request: query(**request, Limit=1)
    assert people(kept) == [("Ada", None, [], [("rex", "dog")])] and len(kept.journal()) == 2


def test_more_than_one_dynamodb_transaction_writes_is_refused(aws):
    kept = process(max_bytes=500)
    with pytest.raises(ValueError, match="more than one DynamoDB transaction writes"):
        kept.transaction(person("ada", "Ada"))
    assert kept.journal() == [] and keys(aws) == ["H"]


def test_a_failure_of_dynamodb_is_raised(aws):
    kept = process()
    stopping(kept, "transact_write_items", "ValidationException")
    with pytest.raises(ClientError, match="stopped"):
        kept.transaction(person("ada", "Ada"))
    assert kept.journal() == []

    def refused(**request):
        raise ClientError({"Error": {"Code": "AccessDeniedException", "Message": "denied"}}, "PutItem")
    put = kept.client.put_item
    kept.client.put_item = refused
    with pytest.raises(ClientError, match="denied"):
        DynamoDB.Store("t", store="other", client=kept.client)
    kept.client.put_item = put
    transact = kept.client.transact_write_items

    def cancelled(**request):  # cancelled with no reason given: another store, as DynamoDB Local reports one
        kept.client.transact_write_items = transact
        raise ClientError({"Error": {"Code": "TransactionCanceledException", "Message": "cancelled"}},
                          "TransactWriteItems")
    kept.client.transact_write_items = cancelled
    kept.transaction(person("ada", "Ada"))  # caught up, and interpreted again
    assert [t["id"] for t in kept.journal()] == ["person/ada"]


def test_configured_by_the_environment(aws):
    kept = DynamoDB.Store(environ={"MBSE_DYNAMODB_TABLE": "from-env", "MBSE_DYNAMODB_STORE": "demo",
                                   "MBSE_DYNAMODB_REGION": REGION})
    assert (kept.table, kept.store_name) == ("from-env", "demo")
    with pytest.raises(ClientError, match="ResourceNotFound"):
        DynamoDB.Store("absent", region=REGION, create=False)


def test_the_table_made_meanwhile_by_another(aws):
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


def test_a_table_written_by_an_earlier_version_is_refused(aws):
    """mbse-dynamodb 0.2 and 0.3 kept each object's document, `O#...`: the store refuses such a table where it is made,
    the layout of its index being another; a journal transaction written before 0.3.0, a mutation transaction, is
    refused where it is read."""
    kept = process()
    kept.transaction(first_note("n1", "first"))
    aws.put_item(TableName="t", Item={"store": {"S": "old"}, "key": {"S": "O#0123#D#000000"}, "data": {"S": "{}"}})
    with pytest.raises(ValueError, match="before 0.4.0"):
        DynamoDB.Store("t", store="old", region=REGION)
    effect = {"mutations": [{"upsert": {"exists": [{"symbols": {}, "spec": []}]}}]}
    aws.put_item(TableName="t", Item={"store": {"S": "s"}, "key": {"S": kept.layout.log("L", 1, 0)},
                                      "data": {"B": json.dumps(effect).encode()}})
    with pytest.raises(ValueError, match="before 0.3.0"):
        kept.journal()
