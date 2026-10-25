"""The WorkTracker demo service (mbse-rest's `Service.work_tracker`) persisted in DynamoDB (CAP-013), configured by
the environment as `Plugin.Plugin()` reads it. Until mbse-rest's service selects its plugin by configuration of
its own, serve this factory instead of mbse-rest's:

    MBSE_DYNAMODB_ENDPOINT_URL=http://localhost:8000 AWS_REGION=us-east-1 \\
        uv run --with uvicorn uvicorn --factory mbse.Persistence.DynamoDB.Demo:work_tracker

It needs mbse-demo-apps, which `uv sync` installs with the development dependencies.
"""

from __future__ import annotations

from typing import Any

from mbse.Services.Rest import Service

from . import Plugin

__all__ = ["work_tracker"]


def work_tracker() -> Any:
    """mbse-rest's WorkTracker application, its store loaded from, and journaled to, DynamoDB."""
    return Service.work_tracker(Plugin.Plugin())
