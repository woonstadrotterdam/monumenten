"""Bijhouden en melden voor welke verblijfsobjecten een bron geen antwoord gaf."""

from __future__ import annotations

import asyncio
import heapq
import logging
import time
from collections import Counter
from typing import Dict, FrozenSet, Iterable, List, NoReturn, Optional, Set

from monumenten._api._backoff import (
    MAX_CONSECUTIVE_FAILURES,
    OUTAGE_GIVE_UP_SECONDS,
    OUTAGE_PROBE_SECONDS,
    is_fout_van_de_request,
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
# De externe bronnen onder de batchbronnen: alleen die kunnen gestoord raken of plat
# liggen. De verwerking kent geen storing en geen proefrequest.
_EXTERNE_BATCHBRONNEN: FrozenSet[str] = BATCHBRONNEN - {VERWERKING}

_MAX_IDS_IN_MELDING = 10


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
            zonder antwoord, ook uit batches die daarna zijn overgeslagen
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
    """Een deel van de verblijfsobjecten staat niet in de BAG van PDOK.

    De BAG gaf wel antwoord, maar zonder adrespunt: meestal is het verblijfsobject
    nooit gebouwd (status "Niet gerealiseerd verblijfsobject"), en die staan niet in
    de BAG WFS. Zonder adrespunt zijn er geen Kadaster-beperkingen, geen
    rijksbeschermd gezicht en geen provinciaal monument te vinden; die statussen
    zijn ``False`` en niet ``pd.NA``, omdat de bron wel antwoord gaf.
    ``rijksmonument`` kan nog ``True`` zijn via de RCE.

    Args:
        melding (str): Omschrijving van de waarschuwing
        verblijfsobject_ids (Optional[Dict[str, List[str]]]): Onder "BAG" de
            gesorteerde verblijfsobject ID's zonder adrespunt
    """


class Bronuitval:
    """Houdt per bron bij voor welke verblijfsobjecten zij wel en geen antwoord gaf.

    Een leeg antwoord (het verblijfsobject is bij de bron niet bekend) telt als
    antwoord; alleen een fout die na retries en splitsen overblijft is uitval.

    Splitsen helpt alleen als een kleinere query wel lukt. Een bron waarvan
    MAX_CONSECUTIVE_FAILURES requests achter elkaar mislukken is daarom gestoord: er
    wordt niet meer gesplitst en zij wordt overgeslagen, op één proefrequest per
    OUTAGE_PROBE_SECONDS na. Een antwoord beëindigt de storing. Is een bron die nog
    nooit antwoord gaf na OUTAGE_GIVE_UP_SECONDS nog steeds gestoord, dan ligt zij
    plat: de aanroep kan niet meer slagen en er volgt geen proefrequest meer. Tot dan
    kan een proefrequest haar redden; een korte hapering aan het begin van een
    aanroep, waarin de requests die tegelijk onderweg zijn samen falen, legt de bron
    zo niet plat.

    Per bron wordt alleen de laatste fout bewaard. Een fout houdt via zijn traceback
    de frames van zijn request vast; bij een lange storing in een aanroep met
    miljoenen verblijfsobjecten zou één fout per verblijfsobject veel geheugen kosten.
    """

    def __init__(self) -> None:
        self._beantwoord: Counter[str] = Counter()
        self._fouten_op_rij: Counter[str] = Counter()
        # Gestoorde bron -> laatste fout van de reeks mislukte requests
        self._storing: Dict[str, Exception] = {}
        # Gestoorde bron -> tijdstip (time.monotonic) vanaf wanneer een proefrequest mag
        self._proef_vanaf: Dict[str, float] = {}
        # Gestoorde bron -> tijdstip (time.monotonic) waarop de storing begon
        self._gestoord_sinds: Dict[str, float] = {}
        self._plat: Set[str] = set()
        # Bron -> verblijfsobject ID's waarvoor zij, ook na retries en splitsen,
        # geen antwoord gaf
        self.mislukt: Dict[str, Set[str]] = {}
        # Bron in mislukt -> de laatste fout waardoor zij geen antwoord gaf
        self.laatste_fout: Dict[str, Exception] = {}
        # Geen uitval: de BAG gaf antwoord, maar kende deze verblijfsobjecten niet.
        # Hier vastgelegd zodat de client ze in een eigen waarschuwing kan noemen.
        self.zonder_adrespunt: Set[str] = set()

    def registreer_antwoord(self, bron: str, aantal: int) -> None:
        """Leg vast dat een bron voor een aantal verblijfsobjecten antwoord gaf.

        Een antwoord beëindigt een storing, behalve bij een bron die plat ligt: daar
        zijn al batches voor overgeslagen.

        Args:
            bron (str): De bron die antwoord gaf
            aantal (int): Aantal verblijfsobjecten waarvoor het antwoord geldt
        """
        self._beantwoord[bron] += aantal
        self._fouten_op_rij[bron] = 0
        if bron in self._storing and bron not in self._plat:
            del self._storing[bron]
            del self._proef_vanaf[bron]
            del self._gestoord_sinds[bron]
            logger.info("%s geeft weer antwoord en wordt weer bevraagd.", bron)

    def registreer_mislukte_request(
        self, bron: str, fout: Exception, *, kan_kleiner: bool = False
    ) -> None:
        """Tel een request die na retries mislukte.

        Een fout die bij de request hoort, zoals een 4xx of een afgekapt antwoord,
        telt niet zolang de request nog kleiner kan: splitsen zoekt dan het
        verblijfsobject waar het aan ligt. Anders zou één verblijfsobject dat altijd
        faalt, op weg naar beneden, de hele bron als gestoord laten gelden.

        Na MAX_CONSECUTIVE_FAILURES mislukte requests achter elkaar, zonder één
        antwoord ertussen, is de bron gestoord. Mislukt daarna ook een proefrequest,
        dan begint de pauze tot de volgende proefrequest opnieuw. Gaf de bron nog nooit
        antwoord en duurt de storing dan al OUTAGE_GIVE_UP_SECONDS, dan ligt zij plat.

        Args:
            bron (str): De bron die geen antwoord gaf
            fout (Exception): De fout die na retries overbleef
            kan_kleiner (bool): Of de request bij een fout nog gesplitst kan worden
        """
        if kan_kleiner and is_fout_van_de_request(fout):
            return
        self._fouten_op_rij[bron] += 1
        was_gestoord = bron in self._storing
        if not was_gestoord and self._fouten_op_rij[bron] < MAX_CONSECUTIVE_FAILURES:
            return
        nu = time.monotonic()
        self._storing[bron] = fout
        self._proef_vanaf[bron] = nu + OUTAGE_PROBE_SECONDS
        if not was_gestoord:
            self._gestoord_sinds[bron] = nu
            logger.warning(
                "%s is gestoord: %d requests achter elkaar mislukt (%s: %s). De bron "
                "wordt overgeslagen; elke %d seconden volgt een proefrequest.",
                bron,
                MAX_CONSECUTIVE_FAILURES,
                type(fout).__name__,
                fout,
                OUTAGE_PROBE_SECONDS,
            )
            return
        if (
            bron not in self._plat
            and not self._beantwoord[bron]
            and nu - self._gestoord_sinds[bron] >= OUTAGE_GIVE_UP_SECONDS
        ):
            self._plat.add(bron)
            logger.warning(
                "%s gaf nog geen enkel antwoord en is al %d seconden gestoord (%s: %s); "
                "de aanroep stopt.",
                bron,
                OUTAGE_GIVE_UP_SECONDS,
                type(fout).__name__,
                fout,
            )

    def registreer_mislukte_verwerking(
        self, verblijfsobject_ids: Iterable[str], fout: Exception
    ) -> None:
        """Leg vast dat het verwerken van een batch onverwacht mislukte.

        De statussen van de batch worden onbepaald en de batch krijgt een herkansing.
        Een mislukte verwerking is geen storing van een bron: er volgt geen
        proefrequest, en de andere batches worden gewoon verwerkt, tenzij de
        verwerking kansloos is (zie verwerking_kansloos).

        Args:
            verblijfsobject_ids (Iterable[str]): De verblijfsobject ID's van de batch
            fout (Exception): De onverwachte fout
        """
        self.registreer_fout(VERWERKING, verblijfsobject_ids, fout)
        self._fouten_op_rij[VERWERKING] += 1
        if (
            self.verwerking_kansloos()
            and self._fouten_op_rij[VERWERKING] == MAX_CONSECUTIVE_FAILURES
        ):
            logger.error(
                "De verwerking van de eerste %d batches mislukte; de resterende "
                "batches worden overgeslagen en de aanroep stopt.",
                MAX_CONSECUTIVE_FAILURES,
            )

    def verwerking_kansloos(self) -> bool:
        """Geef aan of de eerste MAX_CONSECUTIVE_FAILURES batches allemaal mislukten.

        Dan is het vrijwel zeker een programmeerfout en geen probleem met de data van
        één batch: de resterende batches hoeven niet meer verwerkt te worden, en
        controleer geeft de oorspronkelijke fout.

        Returns:
            bool: Of de verwerking kansloos is
        """
        return (
            not self._beantwoord[VERWERKING]
            and self._fouten_op_rij[VERWERKING] >= MAX_CONSECUTIVE_FAILURES
        )

    def sla_over_als_verwerking_kansloos(
        self, verblijfsobject_ids: Iterable[str]
    ) -> bool:
        """Registreer een batch direct als mislukt als de verwerking kansloos is.

        De batch wordt vastgelegd met de laatste verwerkingsfout. Dat moet: slaagt
        daarna toch nog een batch die al liep, dan is de verwerking niet meer
        kansloos, geeft controleer geen fout, en zou een niet vastgelegde batch
        voor al zijn verblijfsobjecten stil False opleveren.

        Args:
            verblijfsobject_ids (Iterable[str]): De verblijfsobject ID's van de batch

        Returns:
            bool: Of de verwerking kansloos is; de batch moet dan worden overgeslagen
        """
        if not self.verwerking_kansloos():
            return False
        self.registreer_fout(
            VERWERKING, verblijfsobject_ids, self.laatste_fout[VERWERKING]
        )
        return True

    def aantal_onvolledig_bepaald(self, verblijfsobject_ids: Iterable[str]) -> int:
        """Tel de onvolledig bepaalde verblijfsobjecten: een bron gaf tot nu toe geen
        antwoord, of hun verwerking mislukte.

        Args:
            verblijfsobject_ids (Iterable[str]): De verblijfsobject ID's

        Returns:
            int: Hoeveel daarvan onvolledig bepaald zijn
        """
        return sum(
            1
            for verblijfsobject_id in verblijfsobject_ids
            if any(verblijfsobject_id in ids for ids in self.mislukt.values())
        )

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

    def gestoord_zonder_antwoord(self, bron: str) -> bool:
        """Geef aan of een bron gestoord is voor zij één antwoord gaf, maar nog niet
        plat ligt: alleen een proefrequest kan haar nog redden.

        Args:
            bron (str): De bron

        Returns:
            bool: Of de bron gestoord is zonder ooit antwoord te hebben gegeven
        """
        return (
            bron in self._storing
            and not self._beantwoord[bron]
            and bron not in self._plat
        )

    def is_gestoord(self, bron: str) -> bool:
        """Geef aan of een bron gestoord is; er wordt dan niet gesplitst.

        Args:
            bron (str): De bron

        Returns:
            bool: Of de bron gestoord is
        """
        return bron in self._storing

    def sla_over_bij_storing(
        self, bron: str, verblijfsobject_ids: Iterable[str]
    ) -> bool:
        """Registreer verblijfsobjecten direct als mislukt als de bron gestoord is.

        Eén request per OUTAGE_PROBE_SECONDS wordt niet overgeslagen: die proefrequest
        laat zien of de bron weer antwoord geeft.

        Args:
            bron (str): De bron die bevraagd zou worden
            verblijfsobject_ids (Iterable[str]): De verblijfsobject ID's van de request

        Returns:
            bool: Of de request moet worden overgeslagen
        """
        fout = self._storing.get(bron)
        if fout is None:
            return False
        if bron not in self._plat and time.monotonic() >= self._proef_vanaf[bron]:
            # Dit is de proefrequest; de volgende mag pas na een nieuwe pauze
            self._proef_vanaf[bron] = time.monotonic() + OUTAGE_PROBE_SECONDS
            return False
        self.registreer_fout(bron, verblijfsobject_ids, fout)
        return True

    def sla_over_als_bron_plat_ligt(self, verblijfsobject_ids: Iterable[str]) -> bool:
        """Registreer verblijfsobjecten direct als mislukt als een bron plat ligt.

        Een bron die plat ligt maakt de aanroep kansloos, dus ook de andere bronnen
        hoeven voor deze verblijfsobjecten niet meer bevraagd te worden. Dat is alleen
        veilig omdat controleer daarna een BronFoutError geeft.

        Args:
            verblijfsobject_ids (Iterable[str]): De verblijfsobject ID's van de batch

        Returns:
            bool: Of een bron plat ligt; de batch moet dan worden overgeslagen
        """
        ids = list(verblijfsobject_ids)
        for bron in self._plat:
            self.registreer_fout(bron, ids, self._storing[bron])
        return bool(self._plat)

    def registreer_fout(
        self, bron: str, verblijfsobject_ids: Iterable[str], fout: Exception
    ) -> None:
        """Leg vast dat een bron voor verblijfsobjecten definitief geen antwoord gaf.

        Args:
            bron (str): De bron die geen antwoord gaf
            verblijfsobject_ids (Iterable[str]): De betrokken verblijfsobject ID's
            fout (Exception): De fout die na retries en splitsen overbleef
        """
        self.mislukt.setdefault(bron, set()).update(verblijfsobject_ids)
        self.laatste_fout[bron] = fout

    def herkansbaar(self) -> Dict[str, Set[str]]:
        """Geef per bron de verblijfsobjecten die bij haar een herkansing verdienen.

        Dat zijn de verblijfsobjecten waarvoor een batchbron geen antwoord gaf die op
        dit moment niet gestoord is: een gestoorde bron opnieuw bevragen heeft geen zin.
        Alleen de bron die geen antwoord gaf wordt opnieuw bevraagd; wat de andere
        bronnen al antwoordden blijft staan.

        Returns:
            Dict[str, Set[str]]: Per bron de verblijfsobject ID's
        """
        return {
            bron: set(ids)
            for bron, ids in self.mislukt.items()
            if bron in BATCHBRONNEN and bron not in self._storing
        }

    def vergeet(self, bron: str, verblijfsobject_ids: Iterable[str]) -> None:
        """Wis de uitval van een bron voor verblijfsobjecten die een herkansing krijgen.

        De bron legt tijdens de herkansing opnieuw vast voor welke daarvan zij geen
        antwoord gaf.

        Args:
            bron (str): De bron die de verblijfsobjecten opnieuw krijgt
            verblijfsobject_ids (Iterable[str]): De verblijfsobject ID's
        """
        if bron not in self.mislukt:
            return
        self.mislukt[bron].difference_update(verblijfsobject_ids)
        if not self.mislukt[bron]:
            del self.mislukt[bron]
            del self.laatste_fout[bron]

    def gestoorde_bronnen(self) -> List[str]:
        """Geef de bronnen die op dit moment gestoord zijn.

        Returns:
            List[str]: De gestoorde bronnen
        """
        return list(self._storing)

    async def wacht_op_proefrequest(self) -> None:
        """Wacht tot elke gestoorde bron haar volgende proefrequest mag krijgen."""
        wachttijd = max(self._proef_vanaf.values(), default=0.0) - time.monotonic()
        if wachttijd > 0:
            await asyncio.sleep(wachttijd)

    def controleer_tussentijds(self) -> None:
        """Stop als de aanroep niet meer kan slagen, ook niet met de herkansing.

        Dat is zo als een bron plat ligt, of als de verwerking kansloos is. Een bron
        die alleen nog geen antwoord gaf, stopt de aanroep hier niet: bij een korte
        hapering in een kleine aanroep, zoals voor één verblijfsobject, kan de
        herkansing haar alsnog redden. Dat beoordeelt controleer na de herkansing.

        Raises:
            BronFoutError: Als een bron plat ligt
            Exception: De oorspronkelijke fout als de verwerking kansloos is
        """
        plat = {bron: ids for bron, ids in self.mislukt.items() if bron in self._plat}
        if plat:
            self._stop_voor_bronnen(plat)
        if self.verwerking_kansloos():
            raise self.laatste_fout[VERWERKING]

    def controleer(self) -> None:
        """Stop als een bron of de verwerking voor geen enkel verblijfsobject slaagde.

        Een bron die plat ligt gaat voor: zij kan de reden zijn dat geen batch werd
        verwerkt. Ligt geen bron plat en lukte de verwerking van geen enkele batch,
        dan is dat vrijwel zeker een programmeerfout: die fout zelf bereikt de
        aanroeper.

        Raises:
            BronFoutError: Als een bron voor alle bevraagde verblijfsobjecten faalde
                of plat ligt
            Exception: De oorspronkelijke fout als de verwerking van geen enkele
                batch lukte
        """
        plat = {
            bron: ids
            for bron, ids in self.mislukt.items()
            if bron in _EXTERNE_BATCHBRONNEN
            and (bron in self._plat or not self._beantwoord[bron])
        }
        if plat:
            self._stop_voor_bronnen(plat)
        if VERWERKING in self.mislukt and not self._beantwoord[VERWERKING]:
            raise self.laatste_fout[VERWERKING]

    def _stop_voor_bronnen(self, plat: Dict[str, Set[str]]) -> NoReturn:
        raise BronFoutError(
            "De monumentstatus kon niet worden bepaald: een bron gaf, ook na "
            "retries, voor geen enkel bevraagd verblijfsobject antwoord. "
            + self._beschrijf(plat),
            _ids_per_bron(plat),
        ) from self.laatste_fout[next(iter(plat))]

    def waarschuwing(self) -> Optional[MonumentStatusOnbepaaldWarning]:
        """Geef de waarschuwing voor de verblijfsobjecten zonder antwoord.

        Returns:
            Optional[MonumentStatusOnbepaaldWarning]: De waarschuwing, of ``None``
                als elke bron voor elk verblijfsobject antwoord gaf
        """
        if not self.mislukt:
            return None
        aantal = self.aantal_onvolledig_bepaald_totaal()
        return MonumentStatusOnbepaaldWarning(
            f"De monumentstatus van {aantal} verblijfsobject(en) kon niet volledig "
            "worden bepaald: een bron gaf, ook na retries, geen antwoord. De "
            "statussen die van die bron afhangen zijn pd.NA. "
            + self._beschrijf(self.mislukt),
            _ids_per_bron(self.mislukt),
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
            f"{len(ids)} verblijfsobject(en) staan niet in de BAG van PDOK, "
            "meestal omdat ze nooit gebouwd zijn (status 'Niet gerealiseerd "
            "verblijfsobject'). Zonder adrespunt zijn rijksbeschermd_gezicht, "
            "gemeentelijk_monument, provinciaal_monument, "
            "rijksmonument_voorbescherming en de Kadaster-bron van rijksmonument "
            f"False; rijksmonument kan nog True zijn via de RCE: {genoemd}",
            {BAG: ids},
        )

    def _beschrijf(self, mislukt: Dict[str, Set[str]]) -> str:
        """Som per bron de laatste fout en de verblijfsobjecten zonder antwoord op."""
        delen = []
        for bron, ids in mislukt.items():
            fout = self.laatste_fout[bron]
            genoemd = ", ".join(heapq.nsmallest(_MAX_IDS_IN_MELDING, ids))
            if len(ids) > _MAX_IDS_IN_MELDING:
                genoemd += f" en {len(ids) - _MAX_IDS_IN_MELDING} andere"
            delen.append(
                f"{bron} ({len(ids)} verblijfsobject(en), "
                f"{type(fout).__name__}: {fout}): {genoemd}"
            )
        return "; ".join(delen)


def _ids_per_bron(mislukt: Dict[str, Set[str]]) -> Dict[str, List[str]]:
    return {bron: sorted(ids) for bron, ids in mislukt.items()}
