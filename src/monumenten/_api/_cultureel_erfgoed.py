import asyncio
import logging
import re
from typing import Any, Dict, List, Tuple

import aiohttp

from monumenten._api._backoff import RETRYABLE_NETWORK_EXCEPTIONS, met_retries
from monumenten._api._kadaster import _in_groepen
from monumenten._bronfouten import RCE, Bronuitval

logger = logging.getLogger("monumenten.api.cultureel_erfgoed")

_CULTUREEL_ERFGOED_SPARQL_ENDPOINT = (
    "https://api.linkeddata.cultureelerfgoed.nl/datasets/rce/cho/sparql"
)

# Noemt de RCE een verblijfsobject bij meer dan één rijksmonument, dan telt het
# numeriek laagste nummer. Dat is meestal de eerste inschrijving en het hoofdobject;
# hogere nummers zijn vaak later ingeschreven onderdelen, zoals een tuin, beeld of
# bijgebouw. Bij Dorpsstraat 5 in Aarle-Rixtel is 6936 de kerk en 520165 het beeld
# ervoor. ceo:rijksmonumentnummer is een string, dus de vergelijking moet numeriek:
# alfabetisch is "520165" lager dan "6936".
_RIJKSMONUMENTEN_QUERY_TEMPLATE = """
PREFIX ceo:<https://linkeddata.cultureelerfgoed.nl/def/ceo#>
PREFIX bag:<http://bag.basisregistraties.overheid.nl/bag/id/>
PREFIX rn2:<https://data.cultureelerfgoed.nl/term/id/rn/2/>
PREFIX xsd:<http://www.w3.org/2001/XMLSchema#>
SELECT ?identificatie (STR(MIN(xsd:integer(?nummer))) as ?rijksmonument_nummer)
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

# Het monumentnummer bij de akte waarmee het rijksmonument in de openbare registers
# van het Kadaster is ingeschreven. Zo krijgt een rijksmonument dat alleen het
# Kadaster vindt toch een nummer.
_RIJKSMONUMENTEN_VIA_AKTE_QUERY_TEMPLATE = """
PREFIX ceo:<https://linkeddata.cultureelerfgoed.nl/def/ceo#>
PREFIX rn2:<https://data.cultureelerfgoed.nl/term/id/rn/2/>
SELECT DISTINCT ?deel ?deelnummer ?rijksmonument_nummer
WHERE {{
    VALUES (?deel ?deelnummer) {{ {aktes} }}
    ?monument ceo:kadasterdeel ?deel ;
              ceo:kadasterdeelnummer ?deelnummer ;
              ceo:heeftJuridischeStatus rn2:b2d9a59a-fe1e-4552-9a05-3c2acddff864 ;
              ceo:rijksmonumentnummer ?rijksmonument_nummer .
}}
"""

# Het Kadaster schrijft een akte als "HYP4 {deel}/{deelnummer}", met het deelnummer
# in drie cijfers. De RCE legt deel en deelnummer los vast, zonder voorloopnullen.
_AKTE_PATROON = re.compile(r"HYP4 (\d+)/(\d+)")
_MAX_AKTES_PER_REQUEST = 500
# Onder deze naam leggen de akte-requests hun uitval vast, in een eigen Bronuitval.
# Bronuitval spreekt in haar logregels van verblijfsobjecten; hier zijn het aktes.
_RCE_AKTES = "RCE (aktes)"

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
        List[Dict[str, Any]]: De rijen van het antwoord. Een antwoord dat geen lijst
            is telt als mislukte poging; blijft het uit, dan volgt de fout van de
            laatste poging.
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
) -> List[Dict[str, Any]]:
    """Haal de rijksmonumenten op voor BAG-identificaties.

    Retries, splitsen bij falen en overslaan bij een storing: zie Bronuitval.bevraag.

    Args:
        session (aiohttp.ClientSession): De sessie voor HTTP requests
        identificaties (List[str]): De verblijfsobject ID's
        uitval (Bronuitval): Hierin legt de RCE vast voor welke identificaties zij
            wel en geen antwoord gaf

    Returns:
        List[Dict[str, Any]]: Per gevonden rijksmonument de identificatie en het
            rijksmonument_nummer
    """

    async def vraag(ids: List[str]) -> List[Dict[str, Any]]:
        query = _RIJKSMONUMENTEN_QUERY_TEMPLATE.format(
            identificaties=" ".join(f'"{i}"' for i in ids)
        )
        return await _sparql(session, query, "rijksmonumenten query")

    # Een lege lijst: bijvoorbeeld een batch in de herkansing die alleen de BRK-PB
    # nodig heeft
    return await uitval.bevraag(RCE, identificaties, vraag)


def _deel_en_deelnummer(akte: str) -> Tuple[str, str] | None:
    """Splits een akte van het Kadaster in het deel en deelnummer zoals de RCE ze
    vastlegt; None als de akte een andere vorm heeft."""
    gevonden = _AKTE_PATROON.fullmatch(akte)
    if gevonden is None:
        return None
    kadasterdeel, deelnummer = gevonden.groups()
    # Alleen een akte die de RCE-waarden exact teruggeven: "052" hoort bij
    # deelnummer 52, maar "52" niet, want 52 wordt geschreven als "052"
    if f"{int(deelnummer):03d}" != deelnummer:
        return None
    return kadasterdeel, str(int(deelnummer))


async def _query_rijksmonumenten_via_aktes(
    session: aiohttp.ClientSession, aktes: List[str]
) -> List[Dict[str, str]]:
    """Haal bij de RCE de rijksmonumentnummers op die bij aktes van het Kadaster horen.

    Per request gaan er maximaal _MAX_AKTES_PER_REQUEST aktes mee. Retries en
    splitsen bij falen gaan zoals bij de rijksmonumenten (zie Bronuitval.bevraag), met
    een eigen Bronuitval: zonder nummer blijft de status gelijk, dus uitval maakt geen
    status onbepaald. Wat niet op te halen was, volgt alleen in een waarschuwing in
    de log.

    Args:
        session (aiohttp.ClientSession): De sessie voor HTTP requests
        aktes (List[str]): De aktes, zoals "HYP4 82963/052"

    Returns:
        List[Dict[str, str]]: Per akte en monument de akte en het
            rijksmonument_nummer. Een akte kan bij meer dan één monument horen; een
            akte die de RCE niet kent of die niet op te halen was, ontbreekt.
    """
    sleutels = {
        akte: sleutel
        for akte in aktes
        if (sleutel := _deel_en_deelnummer(akte)) is not None
    }
    te_vragen = list(sleutels)
    uitval = Bronuitval()

    async def vraag(groep: List[str]) -> List[Dict[str, str]]:
        akte_per_sleutel = {sleutels[akte]: akte for akte in groep}
        query = _RIJKSMONUMENTEN_VIA_AKTE_QUERY_TEMPLATE.format(
            aktes=" ".join(
                f'("{kadasterdeel}" "{deelnummer}")'
                for kadasterdeel, deelnummer in akte_per_sleutel
            )
        )
        rijen = await _sparql(session, query, "rijksmonumenten via akte query")
        return [
            {
                "akte": akte_per_sleutel[(rij["deel"], rij["deelnummer"])],
                "rijksmonument_nummer": rij["rijksmonument_nummer"],
            }
            for rij in rijen
        ]

    groepen = await asyncio.gather(
        *(
            uitval.bevraag(_RCE_AKTES, groep, vraag)
            for groep in _in_groepen(te_vragen, _MAX_AKTES_PER_REQUEST)
        )
    )
    mislukt = uitval.mislukt.get(_RCE_AKTES, set())
    if mislukt:
        logger.warning(
            "Rijksmonumentnummers via de akte niet op te halen voor %d van %d aktes "
            "(%s); rijksmonumenten die alleen het Kadaster vindt, houden daar een "
            "leeg nummer",
            len(mislukt),
            len(te_vragen),
            uitval.laatste_fout[_RCE_AKTES],
        )
    return [rij for groep in groepen for rij in groep]


async def _query_beschermde_gezichten(
    session: aiohttp.ClientSession,
) -> List[Dict[str, Any]]:
    """Haal alle rijksbeschermde stads- en dorpsgezichten op, met hun geometrie.

    Args:
        session (aiohttp.ClientSession): De sessie voor HTTP requests

    Returns:
        List[Dict[str, Any]]: Per gezicht de naam en de WKT-geometrie
    """
    return await _sparql(
        session, _BESCHERMDE_GEZICHTEN_QUERY, "beschermde gezichten query"
    )
