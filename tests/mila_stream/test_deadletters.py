"""Paid events never disappear silently (plan v0.2, part 4, dead-letter queue)."""

import json

from mila_stream import events as ev
from mila_stream.deadletters import PaidLedger
from mila_stream.events import ChatEvent
from mila_stream.filters import InputFilter
from mila_stream.journal import Journal
from mila_stream.pipeline import Pipeline
from mila_stream.runner import Health, write_status
from mila_stream.config import StreamConfig
from mila_stream.selector import MessageQueue, QueueConfig


def sc(i, t, author="a1", text="hvala za stream"):
    return ChatEvent(
        id=f"sc{i}",
        platform="youtube",
        kind=ev.SUPERCHAT,
        author_id=author,
        author_name="Ana",
        text=text,
        amount=5.0,
        currency="EUR",
        amount_display="5,00 €",
        published_at=t,
        received_at=t,
    )


def make(tmp_path, ttl_paid=180.0):
    ledger = PaidLedger(tmp_path / "open.json", tmp_path / "dead.jsonl")
    clock = {"t": 1000.0}
    pipe = Pipeline(
        InputFilter(),
        MessageQueue(QueueConfig(ttl_paid_s=ttl_paid)),
        Journal(None, clock=lambda: clock["t"]),
        ledger=ledger,
    )
    return pipe, ledger


def dead_reasons(tmp_path):
    p = tmp_path / "dead.jsonl"
    if not p.exists():
        return []
    return [json.loads(x)["reason"] for x in p.read_text(encoding="utf-8").splitlines()]


def test_answered_superchat_leaves_the_ledger(tmp_path):
    pipe, ledger = make(tmp_path)
    pipe.ingest(sc(1, 1000), 1000)
    assert "sc1" in json.loads((tmp_path / "open.json").read_text(encoding="utf-8"))
    out = pipe.next_text(1001, busy=False)
    pipe.sent(out, 1001, True)
    assert "sc1" in ledger.open  # sent is not yet spoken
    pipe.next_text(1010, busy=False)  # Mila finished
    assert ledger.open == {}
    assert dead_reasons(tmp_path) == []


def test_expired_superchat_becomes_dead_letter(tmp_path):
    pipe, ledger = make(tmp_path, ttl_paid=10)
    pipe.ingest(sc(1, 1000), 1000)
    assert pipe.next_text(1020, busy=True) is None  # Mila busy the whole time
    pipe.next_text(1020, busy=False)
    assert dead_reasons(tmp_path) == ["zastarjelo"]
    assert ledger.dead_count == 1


def test_hard_kill_is_recovered_on_next_start(tmp_path):
    pipe, _ = make(tmp_path)
    pipe.ingest(sc(1, 1000), 1000)
    pipe.ingest(sc(2, 1000, author="a2"), 1000)
    # process killed here: nothing else runs; a new run starts
    fresh = PaidLedger(tmp_path / "open.json", tmp_path / "dead.jsonl")
    assert fresh.recover() == 2
    assert dead_reasons(tmp_path) == ["neodgovoreno_prije_pada"] * 2
    assert json.loads((tmp_path / "open.json").read_text(encoding="utf-8")) == {}


def test_interrupted_answer_and_ban_and_stop(tmp_path):
    pipe, ledger = make(tmp_path)
    pipe.ingest(sc(1, 1000), 1000)
    pipe.ingest(sc(2, 1000, author="zli"), 1000)
    pipe.ingest(sc(3, 1000, author="a3"), 1000)
    out = pipe.next_text(1001, busy=False)
    pipe.sent(out, 1001, True)
    pipe.interrupted("veza")  # OLLV went down while she was thanking sc1
    pipe.ingest(
        ChatEvent(id="b", platform="youtube", kind=ev.BAN, target_id="zli"), 1002
    )
    n = pipe.close()
    assert dead_reasons(tmp_path) == ["prekid_dok_govori", "zabrana", "kraj_rada"]
    assert n == 1 and ledger.open == {}


def test_personal_data_is_removed_from_dead_letters(tmp_path):
    pipe, _ = make(tmp_path)
    pipe.ingest(sc(1, 1000, text="pisi mi na ana.k@gmail.com"), 1000)
    pipe.close()
    rec = json.loads((tmp_path / "dead.jsonl").read_text(encoding="utf-8"))
    assert "gmail" not in rec["text"]


def test_status_file(tmp_path):
    pipe, _ = make(tmp_path)
    pipe.ingest(sc(1, 1000), 1000)
    cfg = StreamConfig(platform="sim", status_file=str(tmp_path / "status.json"))
    h = Health()
    h.olv = {"ok": True, "clients": 1, "busy": False, "busy_for_s": 0.0}
    write_status(cfg, pipe, h, 1005)
    st = json.loads((tmp_path / "status.json").read_text(encoding="utf-8"))
    assert st["queue"]["placeno"] == 1
    assert st["paid_open"] == 1
    assert st["last_event_age_s"] == 5.0
    assert st["olv"]["clients"] == 1
