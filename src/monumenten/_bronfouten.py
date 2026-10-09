"""Bijhouden en melden voor welke verblijfsobjecten een bron geen antwoord gaf."""

from __future__ import annotations

import asyncio
import logging
import time
from collections import Counter
from typing import Dict, Iterable, List, Optional, Set

from monumenten._api._backoff import MAX_CONSECUTIVE_FAILURES, OUTAGE_PROBE_SECONDS

logger = logging.getLogger("monumenten.bronfouten")

RCE = "RCE"
BAG = "BAG"
BRK_PB = "BRK-PB"

# Verblijfsobject ID -> fout waardoor de bron, ook na retries en splitsen, geen antwoord gaf
Mislukt = Dict[str, Exception]

_MAX_IDS_IN_MELDING = 10


class BronFoutError(RuntimeError):
    """Een bron gaf voor geen enkel bevraagd verblijfsobject antwoord.

    Deze fout wordt niet stil overgeslagen: zonder antwoord van de bron zou elk
    verblijfsobject onterecht ``False`` krijgen voor de statussen uit die bron.

    Args:
        melding (str): Omschrijving van de fout
        verblijfsobject_ids (Optional[Dict[str, List[str]]]): Per bron zonder
            antwoord ("RCE", "BAG" of "BRK-PB") de verblijfsobject ID's zonder
            antwoord, ook uit batches die daarna zijn overgeslagen
    """

    def __init__(
        self,
        melding: str,
        verblijfsobject_ids: Optional[Dict[str, List[str]]] = None,
    ) -> None:
        super().__init__(melding)
        self.verblijfsobject_ids = verblijfsobject_ids or {}


class MonumentStatusOnbepaaldWarning(UserWarning):
    """Een bron gaf voor een deel van de verblijfsobjecten geen antwoord.

    De statussen die van die bron afhangen zijn voor deze verblijfsobjecten
    ``pd.NA`` in plaats van ``False``.

    Args:
        melding (str): Omschrijving van de waarschuwing
        verblijfsobject_ids (Optional[Dict[str, List[str]]]): Per bron ("RCE",
            "BAG" of "BRK-PB") de verblijfsobject ID's zonder antwoord
    """

    def __init__(
        self,
        melding: str,
        verblijfsobject_ids: Optional[Dict[str, List[str]]] = None,
    ) -> None:
        super().__init__(melding)
        self.verblijfsobject_ids = verblijfsobject_ids or {}


class Bronuitval:
    """Houdt per bron bij voor welke verblijfsobjecten zij wel en geen antwoord gaf.

    Een leeg antwoord (het verblijfsobject is bij de bron niet bekend) telt als
    antwoord; alleen een fout die na retries en splitsen overblijft is uitval.

    Splitsen helpt alleen als een kleinere query wel lukt. Een bron waarvan
    MAX_CONSECUTIVE_FAILURES requests achter elkaar mislukken is daarom gestoord: er
    wordt niet meer gesplitst en zij wordt overgeslagen, op één proefrequest per
    OUTAGE_PROBE_SECONDS na. Een antwoord beëindigt de storing. Raakt een bron gestoord
    voor zij één antwoord gaf, dan ligt zij plat: de aanroep kan niet meer slagen en
    er volgt geen proefrequest.
    """

    def __init__(self) -> None:
        self._beantwoord: Counter[str] = Counter()
        self._fouten_op_rij: Counter[str] = Counter()
        # Gestoorde bron -> laatste fout van de reeks mislukte requests
        self._storing: Dict[str, Exception] = {}
        # Gestoorde bron -> tijdstip (time.monotonic) vanaf wanneer een proefrequest mag
        self._proef_vanaf: Dict[str, float] = {}
        self._plat: Set[str] = set()
        self.mislukt: Dict[str, Mislukt] = {}

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
            logger.info("%s geeft weer antwoord en wordt weer bevraagd.", bron)

    def registreer_mislukte_request(self, bron: str, fout: Exception) -> None:
        """Tel een request die na retries mislukte.

        Na MAX_CONSECUTIVE_FAILURES mislukte requests achter elkaar, zonder één
        antwoord ertussen, is de bron gestoord. Mislukt daarna ook een proefrequest,
        dan begint de pauze tot de volgende proefrequest opnieuw.

        Args:
            bron (str): De bron die geen antwoord gaf
            fout (Exception): De fout die na retries overbleef
        """
        self._fouten_op_rij[bron] += 1
        was_gestoord = bron in self._storing
        if not was_gestoord and self._fouten_op_rij[bron] < MAX_CONSECUTIVE_FAILURES:
            return
        self._storing[bron] = fout
        self._proef_vanaf[bron] = time.monotonic() + OUTAGE_PROBE_SECONDS
        if was_gestoord:
            return
        if not self._beantwoord[bron]:
            self._plat.add(bron)
        logger.warning(
            "%s is gestoord: %d requests achter elkaar mislukt (%s: %s). %s",
            bron,
            MAX_CONSECUTIVE_FAILURES,
            type(fout).__name__,
            fout,
            "De bron gaf nog geen enkel antwoord, dus de aanroep stopt."
            if bron in self._plat
            else "De bron wordt overgeslagen; elke "
            f"{OUTAGE_PROBE_SECONDS} seconden volgt een proefrequest.",
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
        self.mislukt.setdefault(bron, {}).update(
            dict.fromkeys(verblijfsobject_ids, fout)
        )

    def herkansbaar(self) -> List[str]:
        """Geef de verblijfsobjecten die een herkansing verdienen.

        Dat zijn de verblijfsobjecten waarvoor een bron geen antwoord gaf die op dit
        moment niet gestoord is: een gestoorde bron opnieuw bevragen heeft geen zin.

        Returns:
            List[str]: De verblijfsobject ID's, elk één keer
        """
        return list(
            dict.fromkeys(
                verblijfsobject_id
                for bron, fouten in self.mislukt.items()
                if bron not in self._storing
                for verblijfsobject_id in fouten
            )
        )

    def vergeet(self, verblijfsobject_ids: Iterable[str]) -> None:
        """Wis de uitval van verblijfsobjecten die een herkansing krijgen.

        De bronnen leggen tijdens de herkansing opnieuw vast voor welke daarvan zij
        geen antwoord gaven.

        Args:
            verblijfsobject_ids (Iterable[str]): De verblijfsobject ID's van de batch
        """
        ids = list(verblijfsobject_ids)
        for bron in list(self.mislukt):
            fouten = self.mislukt[bron]
            for verblijfsobject_id in ids:
                fouten.pop(verblijfsobject_id, None)
            if not fouten:
                del self.mislukt[bron]

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

    def controleer(self) -> None:
        """Stop als een bron voor geen enkel bevraagd verblijfsobject antwoord gaf.

        Raises:
            BronFoutError: Als een bron voor alle bevraagde verblijfsobjecten faalde
                of plat ligt
        """
        plat = {
            bron: fouten
            for bron, fouten in self.mislukt.items()
            if bron in self._plat or not self._beantwoord[bron]
        }
        if not plat:
            return
        laatste_fout = list(next(iter(plat.values())).values())[-1]
        raise BronFoutError(
            "De monumentstatus kon niet worden bepaald: een bron gaf, ook na "
            "retries, voor geen enkel bevraagd verblijfsobject antwoord. "
            + _beschrijf(plat),
            _ids_per_bron(plat),
        ) from laatste_fout

    def waarschuwing(self) -> Optional[MonumentStatusOnbepaaldWarning]:
        """Geef de waarschuwing voor de verblijfsobjecten zonder antwoord.

        Returns:
            Optional[MonumentStatusOnbepaaldWarning]: De waarschuwing, of ``None``
                als elke bron voor elk verblijfsobject antwoord gaf
        """
        if not self.mislukt:
            return None
        aantal = len({i for fouten in self.mislukt.values() for i in fouten})
        return MonumentStatusOnbepaaldWarning(
            f"De monumentstatus van {aantal} verblijfsobject(en) kon niet volledig "
            "worden bepaald: een bron gaf, ook na retries, geen antwoord. De "
            "statussen die van die bron afhangen zijn pd.NA. "
            + _beschrijf(self.mislukt),
            _ids_per_bron(self.mislukt),
        )


def _ids_per_bron(mislukt: Dict[str, Mislukt]) -> Dict[str, List[str]]:
    return {bron: list(fouten) for bron, fouten in mislukt.items()}


def _beschrijf(mislukt: Dict[str, Mislukt]) -> str:
    """Som per bron de laatste fout en de verblijfsobjecten zonder antwoord op."""
    delen = []
    for bron, fouten in mislukt.items():
        ids = list(fouten)
        fout = fouten[ids[-1]]
        genoemd = ", ".join(ids[:_MAX_IDS_IN_MELDING])
        if len(ids) > _MAX_IDS_IN_MELDING:
            genoemd += f" en {len(ids) - _MAX_IDS_IN_MELDING} andere"
        delen.append(
            f"{bron} ({len(ids)} verblijfsobject(en), {type(fout).__name__}: {fout}): "
            f"{genoemd}"
        )
    return "; ".join(delen)
