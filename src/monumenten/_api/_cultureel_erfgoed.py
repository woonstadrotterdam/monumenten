import logging
from typing import Any, Dict, List

import aiohttp

from monumenten._api._backoff import (
    MAX_SPLIT_DEPTH,
    MIN_BATCH_SIZE,
    RETRYABLE_NETWORK_EXCEPTIONS,
    is_rate_limited,
    met_retries,
)
from monumenten._bronfouten import RCE, Bronuitval

logger = logging.getLogger("monumenten.api.cultureel_erfgoed")

_CULTUREEL_ERFGOED_SPARQL_ENDPOINT = (
    "https://api.linkeddata.cultureelerfgoed.nl/datasets/rce/cho/sparql"
)

_RIJKSMONUMENTEN_QUERY_TEMPLATE = """
PREFIX ceo:<https://linkeddata.cultureelerfgoed.nl/def/ceo#>
PREFIX bag:<http://bag.basisregistraties.overheid.nl/bag/id/>
PREFIX rn2:<https://data.cultureelerfgoed.nl/term/id/rn/2/>
SELECT ?identificatie (MAX(?nummer) as ?rijksmonument_nummer)
WHERE {{
    ?monument ceo:heeftJuridischeStatus rn2:b2d9a59a-fe1e-4552-9a05-3c2acddff864 ;
              ceo:rijksmonumentnummer ?nummer ;
              ceo:heeftBasisregistratieRelatie ?basisregistratieRelatie .
    ?basisregistratieRelatie ceo:heeftBAGRelatie ?bagRelatie .
    ?bagRelatie ceo:verblijfsobjectIdentificatie ?identificatie .
    VALUES ?identificatie {{ {identificaties} }}
}}
GROUP BY ?identificatie
"""

_BESCHERMDE_GEZICHTEN_QUERY = """
PREFIX ceo:<https://linkeddata.cultureelerfgoed.nl/def/ceo#>
PREFIX rn2:<https://data.cultureelerfgoed.nl/term/id/rn/2/>
PREFIX geo: <http://www.opengis.net/ont/geosparql#>
SELECT DISTINCT ?gezicht ?rijksbeschermd_gezicht_naam ?gezichtWKT
WHERE {{
  ?gezicht
      ceo:heeftGeometrie ?gezichtGeometrie ;
      ceo:heeftGezichtsstatus rn2:fd968529-bf70-4afa-8564-7c6c2fcfcc54;
      ceo:heeftNaam/ceo:naam ?rijksbeschermd_gezicht_naam.
  ?gezichtGeometrie geo:asWKT ?gezichtWKT.
}}
"""


class _GeenLijstError(RuntimeError):
    """De RCE gaf geen lijst terug: dat is geen antwoord, dus ook geen leeg resultaat."""


async def _sparql(
    session: aiohttp.ClientSession, query: str, context: str
) -> List[Dict[str, Any]]:
    """Voer een SPARQL-query uit bij de RCE, met retries (zie _backoff).

    Args:
        session (aiohttp.ClientSession): De sessie voor HTTP requests
        query (str): De SPARQL-query
        context (str): Wat er wordt opgehaald, voor de logregels

    Returns:
        List[Dict[str, Any]]: De rijen van het antwoord

    Raises:
        Exception: De fout van de laatste poging, ook als het antwoord geen lijst is
    """

    async def post() -> List[Dict[str, Any]]:
        async with session.post(
            _CULTUREEL_ERFGOED_SPARQL_ENDPOINT, data={"query": query, "format": "json"}
        ) as response:
            response.raise_for_status()
            resultaat = await response.json()
        if not isinstance(resultaat, list):
            raise _GeenLijstError(f"Onverwacht antwoord van de RCE: {resultaat!r:.200}")
        return resultaat

    return await met_retries(
        post, context, herhaalbaar=(*RETRYABLE_NETWORK_EXCEPTIONS, _GeenLijstError)
    )


async def _query_rijksmonumenten(
    session: aiohttp.ClientSession,
    identificaties: List[str],
    uitval: Bronuitval,
    *,
    _depth: int = 0,
) -> List[Dict[str, Any]]:
    """Haal de rijksmonumenten op voor BAG-identificaties.

    Bij falen na retries wordt de batch in tweeën gesplitst en opnieuw geprobeerd,
    behalve bij een rate limit (429) of als de RCE een storing heeft: dan wordt de
    request overgeslagen (zie Bronuitval). In uitval wordt vastgelegd voor welke
    identificaties de RCE wel en, ook na retries en splitsen, geen antwoord gaf.

    Args:
        session (aiohttp.ClientSession): De sessie voor HTTP requests
        identificaties (List[str]): De verblijfsobject ID's
        uitval (Bronuitval): Hierin legt de RCE vast voor welke identificaties zij
            wel en geen antwoord gaf
        _depth (int): Interne recursiediepte bij batch-splitsing; standaard 0.

    Returns:
        List[Dict[str, Any]]: Per gevonden rijksmonument de identificatie en het
            rijksmonument_nummer
    """
    if not identificaties:
        # Bijvoorbeeld een batch in de herkansing die alleen de BRK-PB nodig heeft
        return []
    try:
        async with uitval.semaphore(RCE):
            if uitval.sla_over_bij_storing(RCE, identificaties):
                return []
            query = _RIJKSMONUMENTEN_QUERY_TEMPLATE.format(
                identificaties=" ".join(f'"{i}"' for i in identificaties)
            )
            resultaat = await _sparql(session, query, "rijksmonumenten query")
    except Exception as e:
        kan_kleiner = (
            not is_rate_limited(e)
            and len(identificaties) > MIN_BATCH_SIZE
            and _depth < MAX_SPLIT_DEPTH
        )
        uitval.registreer_mislukte_request(RCE, e, kan_kleiner=kan_kleiner)
        if kan_kleiner and not uitval.in_storing(RCE):
            mid = len(identificaties) // 2
            logger.info(
                "Rijksmonumenten query mislukt, splitsen in 2 batches van %d en %d IDs (depth %d)",
                mid,
                len(identificaties) - mid,
                _depth + 1,
            )
            left = await _query_rijksmonumenten(
                session, identificaties[:mid], uitval, _depth=_depth + 1
            )
            right = await _query_rijksmonumenten(
                session, identificaties[mid:], uitval, _depth=_depth + 1
            )
            return left + right
        logger.warning(
            "Rijksmonumenten query definitief overgeslagen voor %d IDs",
            len(identificaties),
        )
        uitval.registreer_fout(RCE, identificaties, e)
        return []

    if _depth > 0:
        logger.info("Rijksmonumenten query geslaagd na splitsing (depth %d)", _depth)
    uitval.registreer_antwoord(RCE, len(identificaties))
    return resultaat


async def _query_beschermde_gezichten(
    session: aiohttp.ClientSession,
) -> List[Dict[str, Any]]:
    """Haal alle rijksbeschermde stads- en dorpsgezichten op, met hun geometrie.

    Args:
        session (aiohttp.ClientSession): De sessie voor HTTP requests

    Returns:
        List[Dict[str, Any]]: Per gezicht de naam en de WKT-geometrie

    Raises:
        Exception: De fout van de laatste poging
    """
    return await _sparql(
        session, _BESCHERMDE_GEZICHTEN_QUERY, "beschermde gezichten query"
    )
