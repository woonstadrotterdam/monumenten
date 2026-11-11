"""Bijhouden en melden voor welke verblijfsobjecten een bron geen antwoord gaf."""

from __future__ import annotations

import asyncio
import heapq
import logging
import time
from asyncio import Semaphore
from dataclasses import dataclass, field
from typing import (
    Awaitable,
    Callable,
    Dict,
    FrozenSet,
    Iterable,
    List,
    Optional,
    Set,
    TypeVar,
)

from monumenten._api._backoff import (
    MAX_CONSECUTIVE_FAILURES,
    MAX_PARALLELLE_REQUESTS,
    MAX_SPLIT_DEPTH,
    MIN_BATCH_SIZE,
    OUTAGE_PROBE_SECONDS,
    is_fout_van_de_request,
    is_rate_limited,
)

logger = logging.getLogger("monumenten.bronfouten")

RCE = "RCE"
BAG = "BAG"
BRK_PB = "BRK-PB"
# Geen externe bron: een onverwachte fout bij het verwerken van een batch. Zo'n fout
# maakt alle statussen van de batch onbepaald in plaats van de aanroep af te breken.
VERWERKING = "verwerking"

# De bronnen die per batch worden bevraagd. Gaf zo'n bron voor niets antwoord, dan
# stopt de aanroep, en haar uitval krijgt een herkansing. Een provincie die niet op te
# halen is, maakt alleen provinciaal_monument onbepaald.
BATCHBRONNEN: FrozenSet[str] = frozenset({RCE, BAG, BRK_PB, VERWERKING})
# De externe bronnen onder de batchbronnen: alleen die kunnen een storing hebben.
# De verwerking kent geen storing en geen proefrequest.
_EXTERNE_BATCHBRONNEN: FrozenSet[str] = BATCHBRONNEN - {VERWERKING}

_MAX_IDS_IN_MELDING = 10

T = TypeVar("T")


class _MetVerblijfsobjectIds(Exception):
    """Geeft een fout of waarschuwing per bron de betrokken verblijfsobject ID's.

    Alle fouten en waarschuwingen van de package hebben zo hetzelfde attribuut
    ``verblijfsobject_ids``: een dict van bron naar gesorteerde ID's.
    """

    def __init__(
        self,
        melding: str,
        verblijfsobject_ids: Optional[Dict[str, List[str]]] = None,
    ) -> None:
        super().__init__(melding)
        self.verblijfsobject_ids = verblijfsobject_ids or {}


class BronFoutError(_MetVerblijfsobjectIds, RuntimeError):
    """Een bron gaf voor geen enkel bevraagd verblijfsobject antwoord.

    Deze fout wordt niet stil overgeslagen: zonder antwoord van de bron zou elk
    verblijfsobject onterecht ``False`` krijgen voor de statussen uit die bron.

    Args:
        melding (str): Omschrijving van de fout
        verblijfsobject_ids (Optional[Dict[str, List[str]]]): Per bron zonder
            antwoord ("RCE", "BAG" of "BRK-PB") de gesorteerde verblijfsobject ID's
            zonder antwoord
    """


class MonumentStatusOnbepaaldWarning(_MetVerblijfsobjectIds, UserWarning):
    """Een bron gaf voor een deel van de verblijfsobjecten geen antwoord.

    De statussen die van die bron afhangen zijn voor deze verblijfsobjecten
    ``pd.NA`` in plaats van ``False``.

    Args:
        melding (str): Omschrijving van de waarschuwing
        verblijfsobject_ids (Optional[Dict[str, List[str]]]): Per bron ("RCE",
            "BAG", "BRK-PB", "verwerking" of een provincie zoals "Noord-Holland")
            de gesorteerde verblijfsobject ID's zonder antwoord
    """


class GeenAdrespuntWarning(_MetVerblijfsobjectIds, UserWarning):
    """Voor een deel van de verblijfsobjecten gaf de BAG van PDOK geen adrespunt.

    De BAG gaf wel antwoord, maar zonder adrespunt: meestal is het verblijfsobject
    nooit gebouwd (status "Niet gerealiseerd verblijfsobject"), en die staan niet in
    de BAG WFS. Voor deze ID's kunnen de monumentstatussen niet via de locatie
    worden gecontroleerd. Een koppeling aan een rijksmonument via de RCE is wel
    mogelijk.

    De waarschuwing noemt geen statuswaarden: die staan in het resultaat. Mislukte
    de verwerking van een ID, dan meldt ``MonumentStatusOnbepaaldWarning`` dat.

    Args:
        melding (str): Omschrijving van de waarschuwing
        verblijfsobject_ids (Optional[Dict[str, List[str]]]): Onder "BAG" de
            gesorteerde verblijfsobject ID's zonder adrespunt
    """


@dataclass
class _Bron:
    """De toestand van één bron binnen één aanroep."""

    # Voor zoveel verblijfsobjecten gaf de bron antwoord
    beantwoord: int = 0
    # Zoveel requests mislukten er achter elkaar, zonder antwoord ertussen
    fouten_op_rij: int = 0
    # De laatste fout van de reeks mislukte requests; None zonder storing
    storing: Optional[Exception] = None
    # Tijdstip (time.monotonic) vanaf wanneer een proefrequest mag
    proef_vanaf: float = 0.0
    # De verblijfsobject ID's waarvoor de bron, ook na retries en splitsen, geen
    # antwoord gaf
    mislukt: Set[str] = field(default_factory=set)
    # De laatste fout waardoor de bron geen antwoord gaf
    laatste_fout: Optional[Exception] = None
    # Beperkt het aantal gelijktijdige requests aan de bron. Per aanroep en niet per
    # proces: een semaphore bindt zich aan de event loop waarin zij voor het eerst
    # moet wachten, en een tweede asyncio.run in hetzelfde proces zou op een
    # gedeelde semaphore een RuntimeError krijgen.
    semaphore: Semaphore = field(
        default_factory=lambda: Semaphore(MAX_PARALLELLE_REQUESTS)
    )


class Bronuitval:
    """Houdt per bron bij voor welke verblijfsobjecten zij wel en geen antwoord gaf.

    Een leeg antwoord (het verblijfsobject is bij de bron niet bekend) telt als
    antwoord; alleen een fout die na retries en splitsen overblijft is uitval.

    Splitsen helpt alleen als een kleinere query wel lukt. Een bron waarvan
    MAX_CONSECUTIVE_FAILURES requests achter elkaar mislukken heeft daarom een
    storing: er wordt niet meer gesplitst en zij wordt overgeslagen, op één
    proefrequest per OUTAGE_PROBE_SECONDS na. Een antwoord beëindigt de storing. Wat
    tijdens de storing is overgeslagen, krijgt aan het einde van de aanroep een
    herkansing (zie _processing._query).

    Per bron wordt alleen de laatste fout bewaard. Een fout houdt via zijn traceback
    de frames van zijn request vast; bij een lange storing in een aanroep met
    miljoenen verblijfsobjecten zou één fout per verblijfsobject veel geheugen kosten.
    """

    def __init__(self) -> None:
        self._bronnen: Dict[str, _Bron] = {}
        # Geen uitval: de BAG gaf antwoord, maar kende deze verblijfsobjecten niet.
        # Hier vastgelegd zodat de client ze in een eigen waarschuwing kan noemen.
        self.zonder_adrespunt: Set[str] = set()

    def _bron(self, naam: str) -> _Bron:
        return self._bronnen.setdefault(naam, _Bron())

    @property
    def mislukt(self) -> Dict[str, Set[str]]:
        """Per bron de verblijfsobject ID's waarvoor zij, ook na retries en splitsen,
        geen antwoord gaf; alleen bronnen met zulke ID's."""
        return {
            naam: bron.mislukt for naam, bron in self._bronnen.items() if bron.mislukt
        }

    @property
    def laatste_fout(self) -> Dict[str, Exception]:
        """Per bron in ``mislukt`` de laatste fout waardoor zij geen antwoord gaf."""
        return {
            naam: bron.laatste_fout
            for naam, bron in self._bronnen.items()
            if bron.mislukt and bron.laatste_fout is not None
        }

    def semaphore(self, bron: str) -> Semaphore:
        """Geef de semaphore die het aantal gelijktijdige requests aan een bron beperkt.

        Args:
            bron (str): De bron

        Returns:
            Semaphore: De semaphore van de bron, met MAX_PARALLELLE_REQUESTS plaatsen
        """
        return self._bron(bron).semaphore

    async def bevraag(
        self,
        bron: str,
        verblijfsobject_ids: List[str],
        vraag: Callable[[List[str]], Awaitable[List[T]]],
        *,
        _diepte: int = 0,
    ) -> List[T]:
        """Bevraag een bron voor verblijfsobjecten, met splitsen bij falen.

        ``vraag`` doet één request voor de gegeven ID's en geeft de rijen van het
        antwoord. Er lopen hooguit MAX_PARALLELLE_REQUESTS requests per bron tegelijk.
        Heeft de bron een storing, dan wordt de request overgeslagen, op de
        proefrequest na. Mislukt de request, ook na haar retries, dan wordt de set
        ID's gehalveerd en elk deel opnieuw gevraagd, behalve bij een rate limit (429),
        omdat splitsen dan alleen meer requests oplevert, bij een storing, bij één ID
        of op de maximale splitsdiepte: dan krijgen de ID's geen antwoord. Antwoord en
        uitval worden hier vastgelegd.

        Args:
            bron (str): De bron
            verblijfsobject_ids (List[str]): De verblijfsobject ID's van de request
            vraag (Callable[[List[str]], Awaitable[List[T]]]): Doet de request voor
                een deel van de ID's en geeft de rijen van het antwoord
            _diepte (int): Interne recursiediepte bij het splitsen; standaard 0

        Returns:
            List[T]: De rijen van alle requests die antwoord gaven, in de volgorde van
                de ID's
        """
        if not verblijfsobject_ids:
            return []
        try:
            async with self.semaphore(bron):
                if self.sla_over_bij_storing(bron, verblijfsobject_ids):
                    return []
                rijen = await vraag(verblijfsobject_ids)
        except Exception as fout:
            kan_kleiner = (
                not is_rate_limited(fout)
                and len(verblijfsobject_ids) > MIN_BATCH_SIZE
                and _diepte < MAX_SPLIT_DEPTH
            )
            self.registreer_mislukte_request(bron, fout, kan_kleiner=kan_kleiner)
            if kan_kleiner and not self.in_storing(bron):
                mid = len(verblijfsobject_ids) // 2
                logger.info(
                    "%s: request mislukt (%s: %s), splitsen in twee requests van %d en "
                    "%d verblijfsobjecten (diepte %d)",
                    bron,
                    type(fout).__name__,
                    fout,
                    mid,
                    len(verblijfsobject_ids) - mid,
                    _diepte + 1,
                )
                return [
                    *await self.bevraag(
                        bron, verblijfsobject_ids[:mid], vraag, _diepte=_diepte + 1
                    ),
                    *await self.bevraag(
                        bron, verblijfsobject_ids[mid:], vraag, _diepte=_diepte + 1
                    ),
                ]
            logger.warning(
                "%s: request definitief overgeslagen voor %d verblijfsobjecten",
                bron,
                len(verblijfsobject_ids),
            )
            self.registreer_fout(bron, verblijfsobject_ids, fout)
            return []
        if _diepte > 0:
            logger.info("%s: request geslaagd na splitsen (diepte %d)", bron, _diepte)
        self.registreer_antwoord(bron, len(verblijfsobject_ids))
        return rijen

    def registreer_antwoord(self, naam: str, aantal: int) -> None:
        """Leg vast dat een bron voor een aantal verblijfsobjecten antwoord gaf.

        Een antwoord beëindigt een storing.

        Args:
            naam (str): De bron die antwoord gaf
            aantal (int): Aantal verblijfsobjecten waarvoor het antwoord geldt
        """
        bron = self._bron(naam)
        bron.beantwoord += aantal
        bron.fouten_op_rij = 0
        if bron.storing is not None:
            bron.storing = None
            logger.info("%s geeft weer antwoord en wordt weer bevraagd.", naam)

    def registreer_mislukte_request(
        self, naam: str, fout: Exception, *, kan_kleiner: bool = False
    ) -> None:
        """Tel een request die na retries mislukte.

        Een fout die bij de request hoort, zoals een 4xx of een afgekapt antwoord,
        telt niet zolang de request nog kleiner kan: splitsen zoekt dan het
        verblijfsobject waar het aan ligt. Anders zou één verblijfsobject dat altijd
        faalt, op weg naar beneden, de hele bron een storing geven.

        Na MAX_CONSECUTIVE_FAILURES mislukte requests achter elkaar, zonder één
        antwoord ertussen, heeft de bron een storing. Mislukt daarna ook een
        proefrequest, dan begint de pauze tot de volgende proefrequest opnieuw.

        Args:
            naam (str): De bron die geen antwoord gaf
            fout (Exception): De fout die na retries overbleef
            kan_kleiner (bool): Of de request bij een fout nog gesplitst kan worden
        """
        if kan_kleiner and is_fout_van_de_request(fout):
            return
        bron = self._bron(naam)
        bron.fouten_op_rij += 1
        had_storing = bron.storing is not None
        if not had_storing and bron.fouten_op_rij < MAX_CONSECUTIVE_FAILURES:
            return
        bron.storing = fout
        bron.proef_vanaf = time.monotonic() + OUTAGE_PROBE_SECONDS
        if not had_storing:
            logger.warning(
                "%s heeft een storing: %d requests achter elkaar mislukt (%s: %s). De "
                "bron wordt overgeslagen; elke %d seconden volgt een proefrequest.",
                naam,
                MAX_CONSECUTIVE_FAILURES,
                type(fout).__name__,
                fout,
                OUTAGE_PROBE_SECONDS,
            )

    def registreer_fout(
        self, naam: str, verblijfsobject_ids: Iterable[str], fout: Exception
    ) -> None:
        """Leg vast dat een bron voor verblijfsobjecten definitief geen antwoord gaf.

        Args:
            naam (str): De bron die geen antwoord gaf
            verblijfsobject_ids (Iterable[str]): De betrokken verblijfsobject ID's
            fout (Exception): De fout die na retries en splitsen overbleef
        """
        bron = self._bron(naam)
        bron.mislukt.update(verblijfsobject_ids)
        bron.laatste_fout = fout

    def in_storing(self, naam: str) -> bool:
        """Geef aan of een bron een storing heeft; er wordt dan niet gesplitst.

        Args:
            naam (str): De bron

        Returns:
            bool: Of de bron een storing heeft
        """
        return self._bron(naam).storing is not None

    def bronnen_in_storing(self) -> List[str]:
        """Geef de bronnen die op dit moment een storing hebben.

        Returns:
            List[str]: De bronnen met een storing
        """
        return [naam for naam, bron in self._bronnen.items() if bron.storing]

    def sla_over_bij_storing(
        self, naam: str, verblijfsobject_ids: Iterable[str]
    ) -> bool:
        """Registreer verblijfsobjecten direct als mislukt als de bron een storing heeft.

        Eén request per OUTAGE_PROBE_SECONDS wordt niet overgeslagen: die proefrequest
        laat zien of de bron weer antwoord geeft.

        Args:
            naam (str): De bron die bevraagd zou worden
            verblijfsobject_ids (Iterable[str]): De verblijfsobject ID's van de request

        Returns:
            bool: Of de request moet worden overgeslagen
        """
        bron = self._bron(naam)
        if bron.storing is None:
            return False
        if time.monotonic() >= bron.proef_vanaf:
            # Dit is de proefrequest; de volgende mag pas na een nieuwe pauze
            bron.proef_vanaf = time.monotonic() + OUTAGE_PROBE_SECONDS
            return False
        self.registreer_fout(naam, verblijfsobject_ids, bron.storing)
        return True

    async def wacht_op_proefrequest(self) -> None:
        """Wacht tot elke bron met een storing haar volgende proefrequest mag krijgen."""
        wachttijd = (
            max(
                (bron.proef_vanaf for bron in self._bronnen.values() if bron.storing),
                default=0.0,
            )
            - time.monotonic()
        )
        if wachttijd > 0:
            await asyncio.sleep(wachttijd)

    def herkansbaar(self) -> Dict[str, Set[str]]:
        """Geef per bron de verblijfsobjecten die bij haar een herkansing verdienen.

        Dat zijn de verblijfsobjecten waarvoor een batchbron geen antwoord gaf die op
        dit moment geen storing heeft: een bron met een storing opnieuw bevragen heeft
        geen zin. Alleen de bron die geen antwoord gaf wordt opnieuw bevraagd; wat de
        andere bronnen al antwoordden blijft staan.

        Returns:
            Dict[str, Set[str]]: Per bron de verblijfsobject ID's
        """
        return {
            naam: set(bron.mislukt)
            for naam, bron in self._bronnen.items()
            if naam in BATCHBRONNEN and bron.mislukt and bron.storing is None
        }

    def vergeet(self, naam: str, verblijfsobject_ids: Iterable[str]) -> None:
        """Wis de uitval van een bron voor verblijfsobjecten die een herkansing krijgen.

        De bron legt tijdens de herkansing opnieuw vast voor welke daarvan zij geen
        antwoord gaf.

        Args:
            naam (str): De bron die de verblijfsobjecten opnieuw krijgt
            verblijfsobject_ids (Iterable[str]): De verblijfsobject ID's
        """
        self._bron(naam).mislukt.difference_update(verblijfsobject_ids)

    def aantal_onvolledig_bepaald(self, verblijfsobject_ids: Iterable[str]) -> int:
        """Tel de onvolledig bepaalde verblijfsobjecten: een bron gaf tot nu toe geen
        antwoord, of hun verwerking mislukte.

        Args:
            verblijfsobject_ids (Iterable[str]): De verblijfsobject ID's

        Returns:
            int: Hoeveel daarvan onvolledig bepaald zijn
        """
        mislukt = list(self.mislukt.values())
        return sum(1 for i in verblijfsobject_ids if any(i in ids for ids in mislukt))

    def aantal_onvolledig_bepaald_totaal(self) -> int:
        """Tel alle onvolledig bepaalde verblijfsobjecten, zonder een kopie van hun ID's.

        Een verblijfsobject telt bij de eerste bron, van groot naar klein, waarvoor
        het in mislukt staat. Bij een uitval voor miljoenen verblijfsobjecten zou een
        unie van alle ID's alleen om te tellen veel geheugen kosten.

        Returns:
            int: Hoeveel verblijfsobjecten onvolledig bepaald zijn
        """
        sets = sorted(self.mislukt.values(), key=len, reverse=True)
        return sum(
            sum(1 for i in ids if not any(i in eerder for eerder in sets[:nummer]))
            for nummer, ids in enumerate(sets)
        )

    def controleer(self) -> None:
        """Stop als een bron of de verwerking voor geen enkel verblijfsobject slaagde.

        Een bron zonder enig antwoord gaat voor: zij kan de reden zijn dat geen batch
        werd verwerkt. Gaf elke bron antwoord en lukte de verwerking van geen enkele
        batch, dan is dat vrijwel zeker een programmeerfout: die fout zelf bereikt de
        aanroeper.

        Raises:
            BronFoutError: Als een bron voor alle bevraagde verblijfsobjecten faalde
            Exception: De oorspronkelijke fout als de verwerking van geen enkele
                batch lukte
        """
        zonder_antwoord = {
            naam: ids
            for naam, ids in self.mislukt.items()
            if naam in _EXTERNE_BATCHBRONNEN and not self._bron(naam).beantwoord
        }
        if zonder_antwoord:
            raise BronFoutError(
                "De monumentstatus kon niet worden bepaald: een bron gaf, ook na "
                "retries, voor geen enkel bevraagd verblijfsobject antwoord. "
                + self._beschrijf(zonder_antwoord),
                _ids_per_bron(zonder_antwoord),
            ) from self.laatste_fout[next(iter(zonder_antwoord))]
        verwerking = self._bron(VERWERKING)
        if verwerking.mislukt and not verwerking.beantwoord:
            raise self.laatste_fout[VERWERKING]

    def waarschuwing(self) -> Optional[MonumentStatusOnbepaaldWarning]:
        """Geef de waarschuwing voor de verblijfsobjecten zonder antwoord.

        Returns:
            Optional[MonumentStatusOnbepaaldWarning]: De waarschuwing, of ``None``
                als elke bron voor elk verblijfsobject antwoord gaf
        """
        mislukt = self.mislukt
        if not mislukt:
            return None
        aantal = self.aantal_onvolledig_bepaald_totaal()
        return MonumentStatusOnbepaaldWarning(
            f"De monumentstatus van {aantal} verblijfsobject(en) kon niet volledig "
            "worden bepaald: een bron gaf, ook na retries, geen antwoord. De "
            "statussen die van die bron afhangen zijn pd.NA. "
            + self._beschrijf(mislukt),
            _ids_per_bron(mislukt),
        )

    def waarschuwing_zonder_adrespunt(self) -> Optional[GeenAdrespuntWarning]:
        """Geef de waarschuwing voor de verblijfsobjecten zonder adrespunt.

        Returns:
            Optional[GeenAdrespuntWarning]: De waarschuwing, of ``None`` als elk
                verblijfsobject een adrespunt kreeg
        """
        if not self.zonder_adrespunt:
            return None
        ids = sorted(self.zonder_adrespunt)
        genoemd = ", ".join(ids[:_MAX_IDS_IN_MELDING])
        if len(ids) > _MAX_IDS_IN_MELDING:
            genoemd += f" en {len(ids) - _MAX_IDS_IN_MELDING} andere"
        return GeenAdrespuntWarning(
            f"Voor {len(ids)} verblijfsobject(en) heeft de BAG van PDOK geen "
            "adrespunt teruggegeven. Voor deze ID's kunnen monumentstatussen niet "
            "via locatie worden gecontroleerd. Een koppeling aan een rijksmonument "
            f"via de RCE is wel mogelijk. Betrokken ID's: {genoemd}",
            {BAG: ids},
        )

    def _beschrijf(self, mislukt: Dict[str, Set[str]]) -> str:
        """Som per bron de laatste fout en de verblijfsobjecten zonder antwoord op."""
        delen = []
        for naam, ids in mislukt.items():
            fout = self.laatste_fout[naam]
            genoemd = ", ".join(heapq.nsmallest(_MAX_IDS_IN_MELDING, ids))
            if len(ids) > _MAX_IDS_IN_MELDING:
                genoemd += f" en {len(ids) - _MAX_IDS_IN_MELDING} andere"
            delen.append(
                f"{naam} ({len(ids)} verblijfsobject(en), "
                f"{type(fout).__name__}: {fout}): {genoemd}"
            )
        return "; ".join(delen)


def _ids_per_bron(mislukt: Dict[str, Set[str]]) -> Dict[str, List[str]]:
    return {bron: sorted(ids) for bron, ids in mislukt.items()}
