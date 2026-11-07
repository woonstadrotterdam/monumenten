"""De BRK-PB kent geen erfgoedcodes die de package niet bevraagt.

Komt er een nieuwe grondslagcode voor een monument bij (bijvoorbeeld onder de
Omgevingswet), dan zou de package die stil overslaan en zulke monumenten False
geven. De live test draait dagelijks mee tegen de echte BRK-PB.
"""

import aiohttp
import pytest

from monumenten._api._kadaster import (
    _BRK_PB_BEPERKINGEN_QUERY_TEMPLATE,
    _BRK_PB_ENDPOINT,
    _RELEVANTE_GRONDSLAGCODES,
    _grondslagcode_filter,
)

_LAGEN = ("pb_multipolygon", "pb_multipoint", "pb_multilinestring")

# Woorden waaraan een erfgoedbeperking in de grondslagomschrijving te herkennen is.
# Op 6 november 2026 gaf alleen "monument" en "erfgoed" treffers buiten de bevraagde
# codes; de andere staan erbij voor een code die anders wordt omschreven.
_ZOEKWOORDEN = ("monument", "erfgoed", "beschermd", "cultu", "gezicht")

# Erfgoedcodes die bewust niet bevraagd worden, omdat ze geen monumentstatus geven
_GEEN_MONUMENT = {
    "EW": "Gedoogplicht archeologisch onderzoek, Erfgoedwet",
    "EWC": "Erfgoedwet: Gedoogplicht archeologisch onderzoek opgelegd door bestuursorgaan",
}

_QUERY = """<?xml version="1.0" encoding="UTF-8"?>
<wfs:GetFeature service="WFS" version="2.0.0" outputFormat="application/json" count="20"
    xmlns:wfs="http://www.opengis.net/wfs/2.0"
    xmlns:fes="http://www.opengis.net/fes/2.0"
    xmlns:wkpb="http://wkpb.geonovum.nl">
  <wfs:Query typeNames="wkpb:{laag}">
    <wfs:PropertyName>grondslagCode</wfs:PropertyName>
    <wfs:PropertyName>grondslagOmschrijving</wfs:PropertyName>
    <fes:Filter>
      <fes:And>
        <fes:Or>{zoekwoorden}</fes:Or>
        <fes:Not>{bekend}</fes:Not>
      </fes:And>
    </fes:Filter>
  </wfs:Query>
</wfs:GetFeature>
"""

_ZOEKWOORD = (
    '<fes:PropertyIsLike wildCard="*" singleChar="." escapeChar="!" matchCase="false">'
    "<fes:ValueReference>grondslagOmschrijving</fes:ValueReference>"
    "<fes:Literal>*{woord}*</fes:Literal>"
    "</fes:PropertyIsLike>"
)


def test_beperkingenquery_bevraagt_de_relevante_grondslagcodes():
    """De live test hieronder controleert _RELEVANTE_GRONDSLAGCODES; dat zegt alleen
    iets als de BRK-PB-query precies die codes opvraagt."""
    assert _grondslagcode_filter(_RELEVANTE_GRONDSLAGCODES) in (
        _BRK_PB_BEPERKINGEN_QUERY_TEMPLATE
    )


def test_grondslagcode_filter_met_een_code_zonder_or():
    """Een fes:Or vereist minimaal twee filters; PDOK weigert er een met één."""
    assert _grondslagcode_filter(["EWE"]) == (
        "<fes:PropertyIsEqualTo><fes:ValueReference>grondslagCode</fes:ValueReference>"
        "<fes:Literal>EWE</fes:Literal></fes:PropertyIsEqualTo>"
    )
    assert _grondslagcode_filter(["EWE", "GWA"]).startswith("<fes:Or>")


@pytest.mark.asyncio
@pytest.mark.parametrize("laag", _LAGEN)
async def test_brk_pb_heeft_geen_onbekende_erfgoedcodes(laag: str):
    query = _QUERY.format(
        laag=laag,
        zoekwoorden="".join(_ZOEKWOORD.format(woord=w) for w in _ZOEKWOORDEN),
        bekend=_grondslagcode_filter([*_RELEVANTE_GRONDSLAGCODES, *_GEEN_MONUMENT]),
    )
    async with aiohttp.ClientSession() as session:
        async with session.post(
            _BRK_PB_ENDPOINT,
            data=query,
            headers={"Content-Type": "application/xml"},
        ) as response:
            response.raise_for_status()
            features = (await response.json())["features"]

    onbekend = {
        (f["properties"]["grondslagCode"], f["properties"]["grondslagOmschrijving"])
        for f in features
    }
    assert not onbekend, (
        f"De BRK-PB ({laag}) heeft erfgoedbeperkingen met een grondslagcode die de "
        f"package niet bevraagt en niet als 'geen monument' kent: {sorted(onbekend)}"
    )
