"""mbse-rest's logical views (mbse-basis CAP-012) over a store kept in DynamoDB, DynamoDB mocked by moto, against the
same views over a store in memory: each case of mbse-rest's golden corpus that asks a view of the WorkTracker's seed
(its `conformance/`, in the sibling checkout), the detail and adjacency views among them, whose entries carry their
properties (`work.Plans`' `original`, `forecast`, `completed` and `progress`).

Expected to fail, strictly, until mbse-rest reads its views through the store's interpretation: its views read the
objects a frame binds through the builders of a store in memory (`Views.witness`), which a store kept in DynamoDB has
not, and reading them through `txn.exec` waits on a clause that names a property without the value it must hold, which
mbse-schemas 0.38.0 has none of (mbse-rest's `Views.py`). Once it passes, the mark goes."""

from __future__ import annotations

import json

import pytest
from fastapi.testclient import TestClient

from mbse.Persistence.DynamoDB import Store as DynamoDB
from mbse.Services.Rest import Service, Views

from conftest import REGION
from notebooks import ROOT

CORPUS = ROOT / "mbse-rest" / "conformance"
CASES = ("label-capabilities", "chip-tasks", "chip-comments", "detail-goal", "detail-feature",
         "adjacency-goal-capabilities", "adjacency-feature-capabilities", "adjacency-comment-about")


def answered(client, case):
    """The view a corpus case asks, as `client`'s service answers it: its status and its frames, as plain data."""
    body = json.loads((CORPUS / f"{case}.request.json").read_text())
    response = client.post(f"/views/{case.split('-')[0]}", json=body)
    return response.status_code, [json.loads(line) for line in response.text.splitlines()]


def test_the_views_over_a_store_in_memory_carry_what_is_compared():
    """What the comparison below holds a store kept in DynamoDB to: each view answered, with frames; the goal's detail
    with its plans' entry properties."""
    memory = TestClient(Service.work_tracker())
    for case in CASES:
        status, frames = answered(memory, case)
        assert status == 200 and frames, case
    _, [goal] = answered(memory, "detail-goal")
    plans = [entry for entry in Views.entries(goal) if entry["adjacency"] == "capabilities"]
    assert [sorted(entry["properties"]) for entry in plans] == [["forecast", "original", "progress"]] * 2


@pytest.mark.xfail(strict=True, reason="mbse-rest's views read a store in memory's builders; through txn.exec, "
                                       "mbse-schemas 0.38.0 names no property without its value")
def test_the_views_over_a_store_kept_in_dynamodb_are_a_store_in_memorys(aws):
    memory = TestClient(Service.work_tracker())
    kept = TestClient(Service.work_tracker(DynamoDB.Store("t", store="views", region=REGION)),
                      raise_server_exceptions=False)
    for case in CASES:
        assert answered(kept, case) == answered(memory, case), case
