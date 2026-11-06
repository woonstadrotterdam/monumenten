from unittest.mock import AsyncMock, MagicMock, patch

import aiohttp
import pytest

from monumenten._api._backoff import RATE_LIMIT_SLEEP_SECONDS
from monumenten._api._kadaster import (
    _BAG_ENDPOINT,
    _post_wfs_json,
    _query_adrespunten,
    _query_beperkingen,
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
@pytest.mark.parametrize(
    ("headers", "expected_sleep"),
    [(None, RATE_LIMIT_SLEEP_SECONDS), ({"Retry-After": "7"}, 7)],
)
async def test_429_waits_for_retry_after_or_default(headers, expected_sleep):
    session = MagicMock()
    session.post = MagicMock(side_effect=[_response(_error(429, headers)), _response()])
    sleep = AsyncMock()

    with patch("monumenten._api._kadaster.asyncio.sleep", sleep):
        assert await _post_wfs_json(session, _BAG_ENDPOINT, "q", "test") == []

    sleep.assert_awaited_once_with(expected_sleep)


@pytest.mark.asyncio
async def test_no_split_on_429():
    calls = []

    async def mock_post_wfs(session, endpoint, query, context):
        calls.append(context)
        raise _error(429)

    with patch("monumenten._api._kadaster._post_wfs_json", side_effect=mock_post_wfs):
        assert await _query_adrespunten(AsyncMock(), ["id1", "id2"]) == {}
        assert (
            await _query_beperkingen(
                AsyncMock(), {"id1": (0.0, 0.0), "id2": (1.0, 1.0)}
            )
            == []
        )

    assert calls == ["BAG verblijfsobjecten query", "BRK-PB beperkingen query"]
