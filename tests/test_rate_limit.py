from unittest.mock import AsyncMock, MagicMock, patch

import aiohttp
import pytest

from monumenten._api import _kadaster
from monumenten._api._backoff import (
    KKG_MAX_REQUESTS_PER_MINUTE,
    RATE_LIMIT_SLEEP_SECONDS,
)
from monumenten._api._kadaster import (
    _BAG_LV_ENDPOINT,
    _post_sparql_json,
    _query_kkg,
    _query_verblijfsobjecten,
    _wait_for_kkg_rate_limit,
)


def _error(status, headers=None):
    return aiohttp.ClientResponseError(
        request_info=MagicMock(), history=(), status=status, headers=headers
    )


def _response(error=None):
    resp = MagicMock()
    resp.raise_for_status = MagicMock(side_effect=error)
    resp.json = AsyncMock(return_value=[])
    resp.__aenter__ = AsyncMock(return_value=resp)
    resp.__aexit__ = AsyncMock(return_value=None)
    return resp


@pytest.mark.asyncio
async def test_kkg_rate_limit_waits_when_minute_is_full():
    sleep = AsyncMock()
    with (
        patch.object(_kadaster, "_kkg_request_times", _kadaster.collections.deque()),
        patch("monumenten._api._kadaster.time.monotonic", return_value=100.0),
        patch("monumenten._api._kadaster.asyncio.sleep", sleep),
    ):
        _kadaster._kkg_request_times.extend([50.0] * KKG_MAX_REQUESTS_PER_MINUTE)
        # Simuleer dat de oude requests na het wachten uit het venster zijn gevallen
        sleep.side_effect = lambda _: _kadaster._kkg_request_times.clear()
        await _wait_for_kkg_rate_limit()

    sleep.assert_awaited_once_with(pytest.approx(10.0))  # 50 + 60 - 100


@pytest.mark.asyncio
async def test_kkg_rate_limit_does_not_wait_below_limit():
    sleep = AsyncMock()
    with (
        patch.object(_kadaster, "_kkg_request_times", _kadaster.collections.deque()),
        patch("monumenten._api._kadaster.asyncio.sleep", sleep),
    ):
        for _ in range(KKG_MAX_REQUESTS_PER_MINUTE):
            await _wait_for_kkg_rate_limit()

    sleep.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("headers", "expected_sleep"),
    [(None, RATE_LIMIT_SLEEP_SECONDS), ({"Retry-After": "7"}, 7)],
)
async def test_429_waits_for_retry_after_or_default(headers, expected_sleep):
    session = MagicMock()
    session.post = MagicMock(side_effect=[_response(_error(429, headers)), _response()])
    sleep = AsyncMock()

    with patch("monumenten._api._kadaster.asyncio.sleep", sleep):
        assert await _post_sparql_json(session, _BAG_LV_ENDPOINT, "q", "test") == []

    sleep.assert_awaited_once_with(expected_sleep)


@pytest.mark.asyncio
async def test_no_split_on_429():
    calls = []

    async def mock_post_sparql(session, endpoint, query, context):
        calls.append(context)
        raise _error(429)

    with patch(
        "monumenten._api._kadaster._post_sparql_json", side_effect=mock_post_sparql
    ):
        assert await _query_verblijfsobjecten(AsyncMock(), ["id1", "id2"]) == []
        assert await _query_kkg(AsyncMock(), {"na/1": ["id1"], "na/2": ["id2"]}) == []

    assert calls == ["BAG nummeraanduiding query", "KKG verblijfsobjecten query"]
