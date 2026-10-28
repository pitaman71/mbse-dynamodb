"""mbse-rest's persistence plugin over DynamoDB (mbse-basis [CAP-013: an in-memory store journaled to DynamoDB](../../../../../mbse-basis/docs/capabilities/CAP-013.md); mbse-rest docs/CONTRACT.md, Persistence).

A store's history is two append-only logs in one table, under one partition key (the store's name):

- its journal, each committed transaction that writes, numbered in commit order from 0;
- its snapshots, numbered in the order they are taken, each recording `since`, the journal's length when it was taken.

Each record is its plain form as UTF-8 JSON, cut into parts that each fit in an item (DynamoDB's 400 KB limit). Its
part 0 is written last, and only on condition that it is new: a record is there exactly when its part 0 is, so a
write that fails leaves nothing `load` reads, and a second writer is refused rather than interleaved. `load` gives
the latest snapshot and the journal from its `since` on. Nothing is ever deleted. docs/DESIGN.md has the design and
its reasons.
"""

from __future__ import annotations

import json
import os
from collections.abc import Iterator, Mapping
from typing import Any

import boto3
from botocore.exceptions import ClientError

from mbse.Services.Rest.Persistence import PlainData

__all__ = ["Plugin", "environment", "DEFAULT_TABLE", "DEFAULT_STORE", "PART_BYTES", "JOURNAL", "SNAPSHOT"]

DEFAULT_TABLE = "mbse-store"
DEFAULT_STORE = "store"
PART_BYTES = 350_000  # each part's data, leaving room under 400 KB for the item's keys and attributes
JOURNAL, SNAPSHOT = "J", "S"  # the two logs, the first letter of each record's sort key
WIDTH = 20  # digits in a record's number, so that sort keys sort as numbers
PART_WIDTH = 6
END = "~"  # sorts after every digit and '#'


def key(log: str, number: int, part: int) -> str:
    """A part's sort key: `J#00000000000000000042#000000`."""
    return f"{log}#{number:0{WIDTH}d}#{part:0{PART_WIDTH}d}"


def parse(sort_key: str) -> tuple[str, int, int]:
    log, number, part = sort_key.split("#")
    return log, int(number), int(part)


def environment(environ: Mapping[str, str]) -> dict[str, Any]:
    """The plugin's settings as the environment gives them, each where an argument does not:

    | Variable | Setting | Default |
    |---|---|---|
    | `MBSE_DYNAMODB_TABLE` | `table` | `mbse-store` |
    | `MBSE_DYNAMODB_STORE` | `store` | `store` |
    | `MBSE_DYNAMODB_REGION` | `region` | boto3's own (`AWS_REGION`, `AWS_DEFAULT_REGION`, its config) |
    | `MBSE_DYNAMODB_ENDPOINT_URL` | `endpoint_url` | AWS's; `http://localhost:8000` for DynamoDB Local |
    | `MBSE_DYNAMODB_CREATE` | `create` | `1`; `0`, `false` or `no` not to make the table |

    Credentials are boto3's own (`AWS_ACCESS_KEY_ID`, `AWS_PROFILE`, ...)."""
    return {
        "table": environ.get("MBSE_DYNAMODB_TABLE") or DEFAULT_TABLE,
        "store": environ.get("MBSE_DYNAMODB_STORE") or DEFAULT_STORE,
        "region": environ.get("MBSE_DYNAMODB_REGION") or None,
        "endpoint_url": environ.get("MBSE_DYNAMODB_ENDPOINT_URL") or None,
        "create": environ.get("MBSE_DYNAMODB_CREATE", "1").lower() not in ("0", "false", "no"),
    }


class Plugin:
    """`mbse.Services.Rest.Persistence.Plugin` over one DynamoDB table.

    `Plugin()`, with no arguments, takes every setting from the environment (`environment`, above), so that a service
    can make it by name (mbse-rest's `MBSE_REST_PLUGIN=mbse.Persistence.DynamoDB.Plugin:Plugin`). An argument given
    overrides the environment; `environ`, where given, is read in place of the process's.

    `table` is the table's name, `store` the partition key that holds this store, so that several stores may share a
    table. `region` and `endpoint_url` go to boto3 (an endpoint URL such as `http://localhost:8000` for DynamoDB
    Local); `client`, where given, is used instead. Where `create`, the table is made if absent (on-demand capacity).
    `part_bytes` bounds each part's data.
    """

    def __init__(self, table: str | None = None, *, store: str | None = None, region: str | None = None,
                 endpoint_url: str | None = None, client: Any = None, create: bool | None = None,
                 part_bytes: int = PART_BYTES, environ: Mapping[str, str] | None = None) -> None:
        settings = environment(os.environ if environ is None else environ)
        table = table if table is not None else settings["table"]
        store = store if store is not None else settings["store"]
        region = region if region is not None else settings["region"]
        endpoint_url = endpoint_url if endpoint_url is not None else settings["endpoint_url"]
        create = create if create is not None else settings["create"]
        if part_bytes < 1:
            raise ValueError(f"part_bytes must be positive, not {part_bytes}")
        self.table, self.store, self.part_bytes = table, store, part_bytes
        self.client = client if client is not None else boto3.client(
            "dynamodb", region_name=region, endpoint_url=endpoint_url)
        if create:
            self.create_table()
        self._next: dict[str, int] | None = None  # each log's next number, read from the table when first needed

    @classmethod
    def from_environment(cls, environ: Mapping[str, str] | None = None, **options: Any) -> Plugin:
        """A plugin configured by `environ` (the process's environment by default), `options` overriding it; the
        same as `Plugin(**options, environ=environ)`."""
        return cls(**options, environ=environ)

    # --- The table ---

    def create_table(self) -> bool:
        """Makes the table if it is absent, and waits until it is active. Whether it was made."""
        try:
            self.client.describe_table(TableName=self.table)
            return False
        except ClientError as error:
            if error.response["Error"]["Code"] != "ResourceNotFoundException":
                raise
        try:
            self.client.create_table(
                TableName=self.table, BillingMode="PAY_PER_REQUEST",
                AttributeDefinitions=[{"AttributeName": "store", "AttributeType": "S"},
                                      {"AttributeName": "key", "AttributeType": "S"}],
                KeySchema=[{"AttributeName": "store", "KeyType": "HASH"},
                           {"AttributeName": "key", "KeyType": "RANGE"}])
        except ClientError as error:
            if error.response["Error"]["Code"] != "ResourceInUseException":  # made meanwhile by another
                raise
        self.client.get_waiter("table_exists").wait(TableName=self.table)
        return True

    # --- The protocol ---

    def load(self) -> tuple[list[PlainData] | None, list[PlainData]]:
        snapshot, since = None, 0
        latest = self._latest(SNAPSHOT)
        if latest is not None:
            item = self._record(SNAPSHOT, latest)
            snapshot, since = item["data"], item["since"]
        journal = [record["data"] for record in self._records(JOURNAL, since)]
        # what the service writes next follows what it loaded, so that another writer since is refused
        self._next = {JOURNAL: since + len(journal), SNAPSHOT: 0 if latest is None else latest + 1}
        return snapshot, journal

    def journal(self, transaction: PlainData) -> None:
        self._append(JOURNAL, transaction, {})

    def snapshot(self, snapshot: list[PlainData]) -> None:
        self._append(SNAPSHOT, snapshot, {"since": {"N": str(self._numbers()[JOURNAL])}})

    # --- Records ---

    def _numbers(self) -> dict[str, int]:
        if self._next is None:
            self._next = {log: (lambda n: 0 if n is None else n + 1)(self._latest(log)) for log in (JOURNAL, SNAPSHOT)}
        return self._next

    def _append(self, log: str, data: PlainData, attributes: dict[str, Any]) -> None:
        """Writes `data` as the log's next record: parts 1 and on first, then part 0, the record's presence, on
        condition that it is new. Where a write fails, the record is not there."""
        number = self._numbers()[log]
        encoded = json.dumps(data, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode()
        parts = [encoded[i:i + self.part_bytes] for i in range(0, len(encoded), self.part_bytes)] or [b""]
        for part in range(1, len(parts)):  # left over by a write that failed, these are written over
            self.client.put_item(TableName=self.table, Item=self._item(log, number, part, parts[part]))
        try:
            self.client.put_item(
                TableName=self.table, ConditionExpression="attribute_not_exists(#k)",
                ExpressionAttributeNames={"#k": "key"},
                Item={**self._item(log, number, 0, parts[0]), "parts": {"N": str(len(parts))}, **attributes})
        except ClientError as error:
            if error.response["Error"]["Code"] == "ConditionalCheckFailedException":
                self._next = None  # read the numbers again before another write
                raise RuntimeError(f"{self.table}/{self.store}: {key(log, number, 0)} was written by another "
                                   "writer; one service writes a store at a time") from error
            raise
        self._numbers()[log] = number + 1

    def _item(self, log: str, number: int, part: int, data: bytes) -> dict[str, Any]:
        return {"store": {"S": self.store}, "key": {"S": key(log, number, part)}, "data": {"B": data}}

    def _query(self, low: str, high: str, forward: bool = True, keys_only: bool = False) -> Iterator[dict[str, Any]]:
        """Each item of this store whose key is from `low` to `high`, in key order (or its reverse), strongly
        consistent; its key alone where `keys_only`."""
        start = None
        projection = {"ProjectionExpression": "#k"} if keys_only else {}
        while True:
            page = self.client.query(
                TableName=self.table, ConsistentRead=True, ScanIndexForward=forward,
                KeyConditionExpression="#s = :s AND #k BETWEEN :low AND :high",
                ExpressionAttributeNames={"#s": "store", "#k": "key"},
                ExpressionAttributeValues={":s": {"S": self.store}, ":low": {"S": low}, ":high": {"S": high}},
                **projection, **({"ExclusiveStartKey": start} if start else {}))
            yield from page.get("Items", [])
            start = page.get("LastEvaluatedKey")
            if not start:
                return

    def _latest(self, log: str) -> int | None:
        """The number of the log's latest record, the highest with a part 0."""
        for item in self._query(f"{log}#", f"{log}#{END}", forward=False, keys_only=True):
            _, number, part = parse(item["key"]["S"])
            if part == 0:
                return number
        return None

    def _records(self, log: str, first: int, last: int | None = None) -> Iterator[dict[str, Any]]:
        """The log's records from number `first` to `last` (or the end), in order, each with its `data` decoded. A
        record's parts without a part 0, or beyond the count its part 0 gives, are left by a write that failed, and are
        passed over."""
        high = f"{log}#{END}" if last is None else f"{log}#{last:0{WIDTH}d}#{END}"
        record: dict[str, Any] | None = None
        for item in self._query(key(log, first, 0), high):
            _, number, part = parse(item["key"]["S"])
            if part == 0:
                if record is not None:
                    yield self._decode(record)
                record = {"number": number, "head": item, "data": [bytes(item["data"]["B"])]}
            elif record is not None and number == record["number"] and part < int(record["head"]["parts"]["N"]):
                record["data"].append(bytes(item["data"]["B"]))
        if record is not None:
            yield self._decode(record)

    def _record(self, log: str, number: int) -> dict[str, Any]:
        [record] = self._records(log, number, number)
        return record

    def _decode(self, record: dict[str, Any]) -> dict[str, Any]:
        head, data = record["head"], record["data"]
        if len(data) != int(head["parts"]["N"]):
            raise RuntimeError(f"{self.table}/{self.store}: {head['key']['S']} has {len(data)} of its "
                               f"{head['parts']['N']} parts")
        decoded = {"data": json.loads(b"".join(data))}
        if "since" in head:
            decoded["since"] = int(head["since"]["N"])
        return decoded
