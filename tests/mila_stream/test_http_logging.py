import logging

from mila_stream.http import quiet_http_loggers


def test_httpx_request_urls_stay_out_of_info_logs():
    logging.getLogger("httpx").setLevel(logging.INFO)
    quiet_http_loggers()
    assert logging.getLogger("httpx").getEffectiveLevel() >= logging.WARNING
    assert logging.getLogger("httpcore").getEffectiveLevel() >= logging.WARNING


def test_chat_entry_point_silences_httpx(monkeypatch, tmp_path):
    import mila_stream.__main__ as entry

    logging.getLogger("httpx").setLevel(logging.INFO)
    seen = {}

    def fake_load(path):
        seen["level"] = logging.getLogger("httpx").level
        raise SystemExit(0)

    monkeypatch.setattr(entry, "load_config", fake_load)
    try:
        entry.main(["--config", str(tmp_path / "x.yaml")])
    except SystemExit:
        pass
    assert seen["level"] >= logging.WARNING
