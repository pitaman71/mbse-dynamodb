"""The persistence plugin interface this package implements: mbse-rest's (docs/CONTRACT.md, Persistence; CAP-013)."""

from mbse.Services.Rest import Persistence


def test_the_reference_plugin_meets_the_protocol():
    assert isinstance(Persistence.Memory(), Persistence.Plugin)
    assert Persistence.Memory().load() == (None, [])
