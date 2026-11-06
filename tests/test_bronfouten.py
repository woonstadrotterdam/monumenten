"""Tests for Bronuitval: which sources gave no answer, and when a source is down."""

import pytest

from monumenten._api._backoff import (
    MAX_CONSECUTIVE_FAILURES,
    OUTAGE_GIVE_UP_SECONDS,
    OUTAGE_PROBE_SECONDS,
)
from monumenten._bronfouten import (
    BAG,
    BRK_PB,
    RCE,
    VERWERKING,
    BronFoutError,
    Bronuitval,
)

pytestmark = pytest.mark.usefixtures("klok")


def test_source_is_down_after_consecutive_failed_requests():
    """MAX_CONSECUTIVE_FAILURES failed requests in a row mark a source as down. An
    answer in between resets the count."""
    uitval = Bronuitval()
    fout = RuntimeError("503")

    for _ in range(MAX_CONSECUTIVE_FAILURES - 1):
        uitval.registreer_mislukte_request(BRK_PB, fout)
    uitval.registreer_antwoord(BRK_PB, 1)
    for _ in range(MAX_CONSECUTIVE_FAILURES - 1):
        uitval.registreer_mislukte_request(BRK_PB, fout)
    assert not uitval.is_gestoord(BRK_PB)
    assert not uitval.sla_over_bij_storing(BRK_PB, ["id1"])
    assert uitval.mislukt == {}

    uitval.registreer_mislukte_request(BRK_PB, fout)
    assert uitval.is_gestoord(BRK_PB)
    assert not uitval.is_gestoord(RCE)

    # BRK-PB answered before it went down, so the call itself can still succeed
    assert uitval.sla_over_bij_storing(BRK_PB, ["id1"])
    assert uitval.mislukt == {BRK_PB: {"id1"}}
    assert uitval.laatste_fout[BRK_PB] is fout
    assert not uitval.sla_over_als_bron_plat_ligt(["id2"])
    uitval.controleer()


def test_source_that_went_down_halfway_is_probed_until_it_answers(klok):
    """A source that answered before it went down gets one probe request per
    OUTAGE_PROBE_SECONDS. Every other request is skipped until one is answered."""
    uitval = Bronuitval()
    fout, proef_fout = RuntimeError("503"), RuntimeError("still 503")
    uitval.registreer_antwoord(BRK_PB, 1)
    for _ in range(MAX_CONSECUTIVE_FAILURES):
        uitval.registreer_mislukte_request(BRK_PB, fout)

    assert uitval.sla_over_bij_storing(BRK_PB, ["id1"])
    klok.nu += OUTAGE_PROBE_SECONDS
    assert not uitval.sla_over_bij_storing(BRK_PB, ["proef1"])  # the probe request
    assert uitval.sla_over_bij_storing(BRK_PB, ["id2"])

    # The probe fails: still down, and the pause starts again
    uitval.registreer_mislukte_request(BRK_PB, proef_fout)
    assert uitval.is_gestoord(BRK_PB)
    klok.nu += OUTAGE_PROBE_SECONDS - 1
    assert uitval.sla_over_bij_storing(BRK_PB, ["id3"])
    klok.nu += 1
    assert not uitval.sla_over_bij_storing(BRK_PB, ["proef2"])

    # The probe is answered: the source is asked again
    uitval.registreer_antwoord(BRK_PB, 1)
    assert not uitval.is_gestoord(BRK_PB)
    assert not uitval.sla_over_bij_storing(BRK_PB, ["id4"])
    assert uitval.mislukt == {BRK_PB: {"id1", "id2", "id3"}}
    assert uitval.laatste_fout[BRK_PB] is proef_fout

    # It is back for real: going down again takes a full run of failed requests
    for _ in range(MAX_CONSECUTIVE_FAILURES - 1):
        uitval.registreer_mislukte_request(BRK_PB, fout)
    assert not uitval.is_gestoord(BRK_PB)


def _leg_plat(uitval, bron, klok, fout):
    """Laat een bron die nog nooit antwoord gaf gestoord raken en blijven."""
    for _ in range(MAX_CONSECUTIVE_FAILURES):
        uitval.registreer_mislukte_request(bron, fout)
    klok.nu += OUTAGE_GIVE_UP_SECONDS
    uitval.registreer_mislukte_request(bron, fout)  # de laatste proefrequest


def test_source_down_before_its_first_answer_is_probed_until_it_lies_down(klok):
    """Requests that were under way together can fail together at the start of a
    call. So a source that went down before its first answer gets probe requests
    too, and only lies down when it is still down after OUTAGE_GIVE_UP_SECONDS."""
    uitval = Bronuitval()
    fout = RuntimeError("503")
    for _ in range(MAX_CONSECUTIVE_FAILURES):
        uitval.registreer_mislukte_request(RCE, fout)

    assert uitval.gestoord_zonder_antwoord(RCE)
    assert not uitval.sla_over_als_bron_plat_ligt(["id1"])
    assert uitval.sla_over_bij_storing(RCE, ["id1"])
    klok.nu += OUTAGE_PROBE_SECONDS
    assert not uitval.sla_over_bij_storing(RCE, ["proef1"])  # the probe request

    # A probe request that fails before OUTAGE_GIVE_UP_SECONDS: still a chance
    klok.nu += OUTAGE_GIVE_UP_SECONDS - OUTAGE_PROBE_SECONDS - 1
    uitval.registreer_mislukte_request(RCE, fout)
    assert not uitval.sla_over_als_bron_plat_ligt(["id2"])
    uitval.controleer_tussentijds()

    # The one after it: the source lies down, and gets no more probe requests
    klok.nu += 1
    uitval.registreer_mislukte_request(RCE, fout)
    assert not uitval.gestoord_zonder_antwoord(RCE)
    assert uitval.sla_over_als_bron_plat_ligt(["id3"])
    klok.nu += OUTAGE_PROBE_SECONDS
    assert uitval.sla_over_bij_storing(RCE, ["id4"])
    with pytest.raises(BronFoutError, match="RCE"):
        uitval.controleer_tussentijds()


def test_source_down_before_its_first_answer_is_saved_by_a_probe(klok):
    """A short hiccup at the start of a call: the probe request is answered, the
    source is asked again, and what it skipped gets the retry at the end."""
    uitval = Bronuitval()
    for _ in range(MAX_CONSECUTIVE_FAILURES):
        uitval.registreer_mislukte_request(BAG, RuntimeError("503"))
    assert uitval.sla_over_bij_storing(BAG, ["id1"])

    klok.nu += OUTAGE_PROBE_SECONDS
    assert not uitval.sla_over_bij_storing(BAG, ["proef1"])
    uitval.registreer_antwoord(BAG, 1)

    assert not uitval.is_gestoord(BAG)
    assert not uitval.gestoord_zonder_antwoord(BAG)
    assert uitval.herkansbaar() == {BAG: {"id1"}}
    uitval.controleer_tussentijds()
    uitval.controleer()


def test_source_down_before_its_first_answer_fails_the_call(klok):
    """A source that lies down fails the call, also when a request that was still
    under way answers after all: batches have been skipped."""
    uitval = Bronuitval()
    fout = RuntimeError("503")
    _leg_plat(uitval, RCE, klok, fout)

    assert uitval.sla_over_als_bron_plat_ligt(["id1", "id2"])
    uitval.registreer_antwoord(RCE, 1)

    with pytest.raises(BronFoutError, match="RCE") as exc_info:
        uitval.controleer()
    assert exc_info.value.verblijfsobject_ids == {RCE: ["id1", "id2"]}
    assert exc_info.value.__cause__ is fout


def test_retry_is_only_for_sources_that_are_not_down():
    """An ID gets a retry when a source that is up again gave no answer for it. Asking
    a source that is still down has no use."""
    uitval = Bronuitval()
    fout = RuntimeError("503")
    uitval.registreer_antwoord(BRK_PB, 1)
    for _ in range(MAX_CONSECUTIVE_FAILURES):
        uitval.registreer_mislukte_request(BRK_PB, fout)
    uitval.registreer_fout(BRK_PB, ["id1", "id2"], fout)
    uitval.registreer_fout(RCE, ["id2", "id3"], fout)

    assert uitval.gestoorde_bronnen() == [BRK_PB]
    assert uitval.herkansbaar() == {RCE: {"id2", "id3"}}

    uitval.vergeet(RCE, ["id2", "id3"])
    assert uitval.mislukt == {BRK_PB: {"id1", "id2"}}
    assert uitval.laatste_fout[BRK_PB] is fout


def test_controleer_bron_die_plat_ligt_gaat_voor_verwerkingsfout(klok):
    """Faalde een batch onverwacht en ligt daarna een bron plat, dan is de bron de
    reden dat geen batch slaagde: de aanroeper krijgt de BronFoutError."""
    uitval = Bronuitval()
    uitval.registreer_mislukte_verwerking(["id1"], ValueError("onverwacht"))
    _leg_plat(uitval, RCE, klok, RuntimeError("RCE 503"))
    assert uitval.sla_over_als_bron_plat_ligt(["id2"])

    with pytest.raises(BronFoutError, match="RCE"):
        uitval.controleer()


def test_verwerking_kansloos_is_geen_storing():
    """Een mislukte verwerking krijgt geen proefrequest en telt niet als gestoorde
    bron; na MAX_CONSECUTIVE_FAILURES mislukte batches zonder succes is zij kansloos."""
    uitval = Bronuitval()
    for nummer in range(MAX_CONSECUTIVE_FAILURES):
        assert not uitval.verwerking_kansloos()
        uitval.registreer_mislukte_verwerking([f"id{nummer}"], ValueError("bug"))

    assert uitval.verwerking_kansloos()
    assert uitval.gestoorde_bronnen() == []
    with pytest.raises(ValueError, match="bug"):
        uitval.controleer()

    # Lukte eerder wel een batch, dan is het geen programmeerfout maar data
    uitval = Bronuitval()
    uitval.registreer_antwoord(VERWERKING, 1)
    for nummer in range(MAX_CONSECUTIVE_FAILURES):
        uitval.registreer_mislukte_verwerking([f"id{nummer}"], ValueError("data"))
    assert not uitval.verwerking_kansloos()
    uitval.controleer()


def test_overgeslagen_batch_bij_kansloze_verwerking_wordt_vastgelegd():
    """Slaagt na de kansloze start toch nog een batch die al liep, dan is de
    verwerking niet meer kansloos en volgt er geen fout. De batches die intussen zijn
    overgeslagen moeten daarom als onvolledig bepaald vastliggen, en niet stil False geven."""
    uitval = Bronuitval()
    for nummer in range(MAX_CONSECUTIVE_FAILURES):
        uitval.registreer_mislukte_verwerking([f"id{nummer}"], ValueError("data"))

    assert uitval.sla_over_als_verwerking_kansloos(["overgeslagen"])
    uitval.registreer_antwoord(VERWERKING, 1)  # de batch die al liep

    assert not uitval.sla_over_als_verwerking_kansloos(["volgende"])
    uitval.controleer()
    assert "overgeslagen" in uitval.mislukt[VERWERKING]
    assert "overgeslagen" in uitval.herkansbaar()[VERWERKING]
    assert "volgende" not in uitval.mislukt[VERWERKING]


def test_aantal_onvolledig_bepaald_totaal_telt_elk_verblijfsobject_een_keer():
    uitval = Bronuitval()
    fout = RuntimeError("503")
    uitval.registreer_fout(RCE, ["id1", "id2", "id3"], fout)
    uitval.registreer_fout(BRK_PB, ["id2", "id4"], fout)
    uitval.registreer_fout(VERWERKING, ["id4"], fout)

    assert uitval.aantal_onvolledig_bepaald_totaal() == 4
    assert "van 4 verblijfsobject(en)" in str(uitval.waarschuwing())
