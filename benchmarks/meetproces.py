"""Verwerk één ronde adressen met één versie van de package en leg elk HTTP-verzoek vast.

Dit script draait in de omgeving van de versie die gemeten wordt, en wordt gestart
door snelheid.py. Het gebruikt alleen de publieke API van de package: een eigen
aiohttp-sessie met trace-hooks, meegegeven aan MonumentenClient.
"""

import argparse
import asyncio
import json
import re
import time
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Dict, List

import aiohttp

from monumenten import MonumentenClient

_SERVER_TIMING_DUUR = re.compile(r"dur=([0-9.]+)")


def _trace_config(verzoeken: List[Dict[str, Any]]) -> aiohttp.TraceConfig:
    """Maak trace-hooks die per verzoek start, einde, status en cacheheaders vastleggen."""
    trace = aiohttp.TraceConfig()

    async def start(
        _: aiohttp.ClientSession,
        ctx: SimpleNamespace,
        params: aiohttp.TraceRequestStartParams,
    ) -> None:
        ctx.verzoek = {
            "host": params.url.host,
            "start_epoch": time.time(),
            "start": time.perf_counter(),
            "status": None,
            "server_ms": None,
            "cache": None,
            "fout": None,
        }
        verzoeken.append(ctx.verzoek)

    async def einde(
        _: aiohttp.ClientSession,
        ctx: SimpleNamespace,
        params: aiohttp.TraceRequestEndParams,
    ) -> None:
        verzoek = ctx.verzoek
        verzoek["status"] = params.response.status
        verzoek["eind"] = time.perf_counter()
        server_timing = _SERVER_TIMING_DUUR.search(
            params.response.headers.get("server-timing", "")
        )
        if server_timing:
            verzoek["server_ms"] = float(server_timing.group(1))
        verzoek["cache"] = params.response.headers.get("x-t-cache")

    async def body_ontvangen(
        _: aiohttp.ClientSession,
        ctx: SimpleNamespace,
        params: aiohttp.TraceResponseChunkReceivedParams,
    ) -> None:
        # aiohttp meldt dit pas als de hele body binnen is
        ctx.verzoek["eind"] = time.perf_counter()

    async def uitzondering(
        _: aiohttp.ClientSession,
        ctx: SimpleNamespace,
        params: aiohttp.TraceRequestExceptionParams,
    ) -> None:
        ctx.verzoek["fout"] = type(params.exception).__name__
        ctx.verzoek["eind"] = time.perf_counter()

    trace.on_request_start.append(start)
    trace.on_request_end.append(einde)
    trace.on_response_chunk_received.append(body_ontvangen)
    trace.on_request_exception.append(uitzondering)
    return trace


async def _meet(ids: List[str]) -> Dict[str, Any]:
    """Verwerk de ID's en geef totale duur en alle verzoeken terug."""
    verzoeken: List[Dict[str, Any]] = []
    async with aiohttp.ClientSession(
        trace_configs=[_trace_config(verzoeken)]
    ) as sessie:
        async with MonumentenClient(session=sessie) as client:
            start = time.perf_counter()
            fout = None
            try:
                await client.process_from_list(ids)
            except Exception as e:
                fout = repr(e)
            totaal_s = time.perf_counter() - start

    for verzoek in verzoeken:
        eind = verzoek.pop("eind", None)
        begin = verzoek.pop("start")
        verzoek["duur_ms"] = None if eind is None else (eind - begin) * 1000
    return {"totaal_s": totaal_s, "fout": fout, "verzoeken": verzoeken}


def main() -> None:
    """Lees de ID's, meet en schrijf het resultaat als JSON."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ids", type=Path, required=True)
    parser.add_argument("--uit", type=Path, required=True)
    args = parser.parse_args()

    ids = args.ids.read_text().split()
    resultaat = asyncio.run(_meet(ids))
    args.uit.write_text(json.dumps(resultaat))


if __name__ == "__main__":
    main()
