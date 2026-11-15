"""DynamoDB, mocked by moto: no AWS account, no network."""

from __future__ import annotations

import boto3
import pytest
from moto import mock_aws

REGION = "us-east-1"
CREDENTIALS = {"AWS_ACCESS_KEY_ID": "testing", "AWS_SECRET_ACCESS_KEY": "testing", "AWS_SESSION_TOKEN": "testing",
               "AWS_DEFAULT_REGION": REGION, "AWS_REGION": REGION}
SETTINGS = ("TABLE", "STORE", "REGION", "ENDPOINT_URL", "CREATE")  # the settings: none from the shell


@pytest.fixture
def aws(monkeypatch):
    """A mocked AWS, with credentials that are not real, so that nothing can reach one."""
    for name, value in CREDENTIALS.items():
        monkeypatch.setenv(name, value)
    for name in SETTINGS:
        monkeypatch.delenv(f"MBSE_DYNAMODB_{name}", raising=False)
    with mock_aws():
        yield boto3.client("dynamodb", region_name=REGION)


@pytest.fixture(scope="module")
def module_aws():
    """A mocked AWS kept for a module's cases, which run a notebook's cells one after another."""
    with pytest.MonkeyPatch.context() as patch:
        for name, value in CREDENTIALS.items():
            patch.setenv(name, value)
        for name in SETTINGS:
            patch.delenv(f"MBSE_DYNAMODB_{name}", raising=False)
        with mock_aws():
            yield boto3.client("dynamodb", region_name=REGION)

