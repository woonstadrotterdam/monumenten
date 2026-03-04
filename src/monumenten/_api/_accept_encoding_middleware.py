"""Accept-Encoding filtering

Some endpoints (e.g. Kadaster SPARQL) can fail when the server uses certain
encodings. This middleware strips those from the Accept-Encoding header so
the server does not choose them.

zstd support landed in Python 3.14. Currently we strip zstd because aiohttp
mis-handles Kadaster's zstd stream on large responses (e.g. a trailing 9-byte
chunk: `28 b5 2f fd 20 00 01 00 00`).
"""

from aiohttp import ClientRequest, ClientResponse, ClientHandlerType
from aiohttp.hdrs import ACCEPT_ENCODING

_STRIP_ENCODINGS = frozenset({"zstd"})


async def accept_encoding_middleware(
    req: ClientRequest, handler: ClientHandlerType
) -> ClientResponse:
    """Client middleware: strip encodings from Accept-Encoding header."""
    accept_encoding = req.headers.get(ACCEPT_ENCODING, "")
    filtered_encodings = [
        e.strip()
        for e in accept_encoding.split(",")
        if e.strip().lower() not in _STRIP_ENCODINGS
    ]
    if filtered_encodings:
        req.headers[ACCEPT_ENCODING] = ", ".join(filtered_encodings)
    else:
        req.headers.pop(ACCEPT_ENCODING, None)
    return await handler(req)
