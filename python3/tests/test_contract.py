"""The persistence plugin interface this package implements: mbse-rest's (mbse-basis CAP-013: an in-memory store journaled to DynamoDB, the persistence plugin)."""

from mbse.Services.Rest import Persistence


def test_the_reference_plugin_meets_the_protocol():
    assert isinstance(Persistence.Memory(), Persistence.Plugin)
    assert Persistence.Memory().load() == (None, [])
