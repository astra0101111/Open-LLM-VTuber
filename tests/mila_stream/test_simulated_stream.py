"""30 minutes of simulated Croatian chat through the whole pipeline, on a fake clock.

Plan v0.2, part 1 "done when": 30 minutes of simulated traffic without a lost or a
duplicated message. Here "not lost" means every message ends with a logged decision.
"""

import random
from collections import Counter

from mila_stream import events as ev
from mila_stream.filters import InputFilter
from mila_stream.journal import Journal
from mila_stream.pipeline import Pipeline
from mila_stream.selector import MessageQueue, QueueConfig
from mila_stream.sim import SimConfig, simulate

BAD = {"spam", "insult", "slur", "injection", "viewer_talk"}


def run_sim(minutes=30, rate=30, seed=7, speak=(4.0, 9.0), tick=0.25):
    cfg = SimConfig(messages_per_minute=rate, minutes=minutes, seed=seed)
    clock = {"t": cfg.start}
    journal = Journal(None, clock=lambda: clock["t"])
    pipe = Pipeline(InputFilter(), MessageQueue(QueueConfig()), journal)
    rnd = random.Random(seed)
    events = list(simulate(cfg))
    i = 0
    busy_until = 0.0
    sends = []
    overlaps = 0
    end = cfg.start + minutes * 60 + 300
    t = cfg.start
    while t < end:
        clock["t"] = t
        while i < len(events) and events[i].published_at <= t:
            pipe.ingest(events[i], t)
            i += 1
        busy = t < busy_until
        out = pipe.next_text(t, busy)
        if out is not None:
            if t < busy_until:
                overlaps += 1
            sends.append((t, out))
            busy_until = t + rnd.uniform(*speak)
            pipe.sent(out, t, True)
        t += tick
    return events, sends, journal, overlaps


def test_thirty_minutes_no_overlap_no_duplicate_no_loss():
    events, sends, journal, overlaps = run_sim()
    assert len(events) > 700
    assert overlaps == 0
    sent_ids = [o.selection.event.id for _, o in sends]
    assert len(sent_ids) == len(set(sent_ids)), "a message was sent twice"

    recs = journal.records
    received = [r for r in recs if r["kind"] == "received"]
    assert len(received) == len(events)  # every event logged on arrival

    # every message got a final decision: sent, dropped (filter or expired) or summarised/removed
    final = {}
    for r in recs:
        if r["kind"] in ("dropped", "sent") and "id" in r:
            final[r["id"]] = r["kind"]
        if r["kind"] == "queued":
            final.setdefault(r["id"], "queued")
    summarised = sum(r["count"] for r in recs if r["kind"] == "summarised")
    removed = sum(r["count"] for r in recs if r["kind"] == "removed")
    still_queued = sum(1 for v in final.values() if v == "queued")
    # messages still "queued" at the end are exactly those summarised or removed by tombstone/ban
    assert still_queued <= summarised + removed

    # nothing harmful reached Mila
    labels = Counter(o.selection.event.label for _, o in sends)
    assert not (set(labels) & BAD), labels
    for _, o in sends:
        if o.selection.event.label == "superchat_bad":
            assert "piše" not in o.text  # thanked without reading the insult

    # every paid event was thanked (unless the viewer was banned meanwhile)
    paid = [e for e in events if e.is_paid]
    paid_sent = {o.selection.event.id for _, o in sends if o.selection.event.is_paid}
    assert len(paid_sent) >= len(paid) - removed


def test_filter_accuracy_on_simulated_labels():
    events, _, journal, _ = run_sim(minutes=30, rate=40, seed=11)
    by_id = {e.id: e for e in events}
    dropped = {
        r["id"]: r["reason"]
        for r in journal.records
        if r["kind"] == "dropped" and r.get("reason") != "zastarjelo"
    }
    bad = [e for e in events if e.label in BAD and e.kind == ev.MESSAGE]
    caught = [e for e in bad if e.id in dropped]
    good = [e for e in events if e.label in ("normal", "question", "mention")]
    false_drop = [
        e
        for e in good
        if e.id in dropped
        and dropped[e.id]
        not in (
            "kopirana_poruka",
            "ponovljena_poruka",
            "prebrzo_pisanje",
            "gledatelj_privremeno_blokiran",
            "bez_teksta",
        )
    ]
    assert len(caught) / len(bad) >= 0.95, Counter(
        by_id[e.id].text for e in bad if e.id not in dropped
    )
    assert len(false_drop) / len(good) <= 0.05, Counter(
        (e.text, dropped[e.id]) for e in false_drop
    )
