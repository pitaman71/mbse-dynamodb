"""The DynamoDB table a store is kept in, and the settings that name it, shared by `Store` and the `Plugin` it
supersedes ([CAP-289: journals kept in DynamoDB](mbse:mbse-basis/docs/capabilities/README.md#CAP-289)).

A table holds any number of stores, each under its own partition key (`store`), its items sorted by `key`."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from botocore.exceptions import ClientError

__all__ = ["environment", "create_table", "DEFAULT_TABLE", "DEFAULT_STORE"]

DEFAULT_TABLE = "mbse-store"
DEFAULT_STORE = "store"


def environment(environ: Mapping[str, str]) -> dict[str, Any]:
    """The settings as the environment gives them, each where an argument does not:

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


def create_table(client: Any, table: str) -> bool:
    """Makes the table if it is absent, on-demand capacity, and waits until it is active. Whether it was made."""
    try:
        client.describe_table(TableName=table)
        return False
    except ClientError as error:
        if error.response["Error"]["Code"] != "ResourceNotFoundException":
            raise
    try:
        client.create_table(
            TableName=table, BillingMode="PAY_PER_REQUEST",
            AttributeDefinitions=[{"AttributeName": "store", "AttributeType": "S"},
                                  {"AttributeName": "key", "AttributeType": "S"}],
            KeySchema=[{"AttributeName": "store", "KeyType": "HASH"},
                       {"AttributeName": "key", "KeyType": "RANGE"}])
    except ClientError as error:
        if error.response["Error"]["Code"] != "ResourceInUseException":  # made meanwhile by another
            raise
    client.get_waiter("table_exists").wait(TableName=table)
    return True
