"""DynamoDB, mocked by moto: no AWS account, no network."""

from __future__ import annotations

import boto3
import pytest
from moto import mock_aws

from mbse.Persistence.DynamoDB import Plugin

REGION = "us-east-1"


@pytest.fixture
def aws(monkeypatch):
    """A mocked AWS, with credentials that are not real, so that nothing can reach one."""
    for name, value in {"AWS_ACCESS_KEY_ID": "testing", "AWS_SECRET_ACCESS_KEY": "testing",
                        "AWS_SESSION_TOKEN": "testing", "AWS_DEFAULT_REGION": REGION, "AWS_REGION": REGION}.items():
        monkeypatch.setenv(name, value)
    with mock_aws():
        yield boto3.client("dynamodb", region_name=REGION)


@pytest.fixture
def plugin(aws):
    return Plugin.Plugin("mbse-test", region=REGION)
