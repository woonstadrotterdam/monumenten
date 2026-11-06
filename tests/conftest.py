"""Shared fixtures for the tests."""

from unittest.mock import patch

import pytest


class _Klok:
    """Stands in for the time and asyncio modules in _bronfouten: time only passes
    when a test says so, or when the code under test sleeps."""

    def __init__(self):
        self.nu = 1000.0

    def monotonic(self):
        return self.nu

    async def sleep(self, seconden):
        self.nu += seconden


@pytest.fixture
def klok():
    """Freeze the clock that decides when a source with an outage gets a probe
    request, and the sleep before a retry, so that no test depends on how fast it
    runs or really waits."""
    klok = _Klok()
    with (
        patch("monumenten._bronfouten.time", klok),
        patch("monumenten._bronfouten.asyncio", klok),
        patch("monumenten._api._backoff.asyncio", klok),
    ):
        yield klok
