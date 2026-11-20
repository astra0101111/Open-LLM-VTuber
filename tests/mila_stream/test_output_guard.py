from mila_stream.output_guard import FILTERED, guard


def test_clean_sentence_unchanged():
    g = guard("Bok svima, danas je lijep dan!")
    assert g.reason == "" and g.display == "Bok svima, danas je lijep dan!"


def test_hate_and_threat_filtered():
    assert guard("ubit cu te").display == FILTERED
    assert guard("Ti si ciganin").reason == "govor_mrznje_ili_prijetnja"


def test_profanity_level_switch():
    assert guard("kakav debil", level=1).display == FILTERED
    assert guard("kakav debil", level=2).reason == ""


def test_personal_data_and_links_removed():
    g = guard("Piši mi na mila@primjer.hr ili posjeti www.nesto.com/x")
    assert "mila@primjer.hr" not in g.display and "[uklonjeno]" in g.display
    assert "[poveznica]" in g.display
    assert "@" not in g.tts and "www" not in g.tts
    assert g.reason == "osobni_podaci,poveznica"


def test_env_switch(monkeypatch):
    from mila_stream import output_guard

    monkeypatch.setenv("MILA_OUTPUT_GUARD", "0")
    assert not output_guard.enabled()
    monkeypatch.setenv("MILA_OUTPUT_GUARD", "1")
    assert output_guard.enabled()
