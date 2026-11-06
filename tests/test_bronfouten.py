"""Tests for Bronuitval: which sources gave no answer, and when a source has an
outage."""

import pytest

from monumenten._api._backoff import (
    MAX_CONSECUTIVE_FAILURES,
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
    assert not uitval.in_storing(BRK_PB)
    assert not uitval.sla_over_bij_storing(BRK_PB, ["id1"])
    assert uitval.mislukt == {}

    uitval.registreer_mislukte_request(BRK_PB, fout)
    assert uitval.in_storing(BRK_PB)
    assert not uitval.in_storing(RCE)

    # BRK-PB answered before it went down, so the call itself can still succeed
    assert uitval.sla_over_bij_storing(BRK_PB, ["id1"])
    assert uitval.mislukt == {BRK_PB: {"id1"}}
    assert uitval.laatste_fout[BRK_PB] is fout
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
    assert uitval.in_storing(BRK_PB)
    klok.nu += OUTAGE_PROBE_SECONDS - 1
    assert uitval.sla_over_bij_storing(BRK_PB, ["id3"])
    klok.nu += 1
    assert not uitval.sla_over_bij_storing(BRK_PB, ["proef2"])

    # The probe is answered: the source is asked again
    uitval.registreer_antwoord(BRK_PB, 1)
    assert not uitval.in_storing(BRK_PB)
    assert not uitval.sla_over_bij_storing(BRK_PB, ["id4"])
    assert uitval.mislukt == {BRK_PB: {"id1", "id2", "id3"}}
    assert uitval.laatste_fout[BRK_PB] is proef_fout

    # It is back for real: going down again takes a full run of failed requests
    for _ in range(MAX_CONSECUTIVE_FAILURES - 1):
        uitval.registreer_mislukte_request(BRK_PB, fout)
    assert not uitval.in_storing(BRK_PB)


def test_source_down_before_its_first_answer_is_saved_by_a_probe(klok):
    """Requests that were under way together can fail together at the start of a
    call. A source that went down before its first answer gets probe requests too:
    the probe is answered, the source is asked again, and what it skipped gets the
    retry at the end."""
    uitval = Bronuitval()
    for _ in range(MAX_CONSECUTIVE_FAILURES):
        uitval.registreer_mislukte_request(BAG, RuntimeError("503"))
    assert uitval.sla_over_bij_storing(BAG, ["id1"])

    klok.nu += OUTAGE_PROBE_SECONDS
    assert not uitval.sla_over_bij_storing(BAG, ["proef1"])
    uitval.registreer_antwoord(BAG, 1)

    assert not uitval.in_storing(BAG)
    assert uitval.herkansbaar() == {BAG: {"id1"}}
    uitval.controleer()


def test_source_without_any_answer_fails_the_call():
    """A source that gave no answer at all fails the call, before an unexpected
    processing error: without the source no batch could be processed."""
    uitval = Bronuitval()
    fout = RuntimeError("503")
    uitval.registreer_fout(VERWERKING, ["id0"], ValueError("onverwacht"))
    for _ in range(MAX_CONSECUTIVE_FAILURES):
        uitval.registreer_mislukte_request(RCE, fout)
    assert uitval.sla_over_bij_storing(RCE, ["id1", "id2"])

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

    assert uitval.bronnen_in_storing() == [BRK_PB]
    assert uitval.herkansbaar() == {RCE: {"id2", "id3"}}

    uitval.vergeet(RCE, ["id2", "id3"])
    assert uitval.mislukt == {BRK_PB: {"id1", "id2"}}
    assert uitval.laatste_fout[BRK_PB] is fout


def test_aantal_onvolledig_bepaald_totaal_telt_elk_verblijfsobject_een_keer():
    uitval = Bronuitval()
    fout = RuntimeError("503")
    uitval.registreer_fout(RCE, ["id1", "id2", "id3"], fout)
    uitval.registreer_fout(BRK_PB, ["id2", "id4"], fout)
    uitval.registreer_fout(VERWERKING, ["id4"], fout)

    assert uitval.aantal_onvolledig_bepaald_totaal() == 4
    assert "van 4 verblijfsobject(en)" in str(uitval.waarschuwing())
