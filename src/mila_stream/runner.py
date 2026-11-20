"""Async runner: chat source -> pipeline -> Open-LLM-VTuber.

Two tasks: one reads chat and feeds the pipeline, one asks OLLV every tick_s whether
Mila is free and sends the next chosen text.
"""

from __future__ import annotations

import asyncio
import logging
import time
from pathlib import Path

from .config import StreamConfig
from .deadletters import PaidLedger, write_json_atomic
from .filters import InputFilter
from .http import HttpxClient
from .journal import Journal
from .pipeline import Pipeline
from .selector import MessageQueue
from .sim import simulate
from .sink import DryRunSink, OlvSink
from .youtube import AuthError, ChatClosed, QuotaExhausted, YouTubeReader

log = logging.getLogger("mila_stream")


async def _feed_sim(cfg: StreamConfig, pipe: Pipeline, realtime: bool = True):
    start = time.time()
    for e in simulate(cfg.sim):
        delay = (e.published_at - cfg.sim.start) - (time.time() - start)
        if realtime and delay > 0:
            await asyncio.sleep(delay)
        now = time.time()
        e.published_at = e.received_at = now
        pipe.ingest(e, now)
    pipe.chat_ended = True


async def _feed_youtube(reader: YouTubeReader, pipe: Pipeline, health: Health):
    try:
        async for e in reader.events():
            pipe.ingest(e, time.time())
        health.source = "stopped:ended"
    except (ChatClosed, QuotaExhausted, AuthError) as e:
        health.source = f"stopped:{type(e).__name__}"
        pipe.journal.write("source_stopped", reason=type(e).__name__, detail=str(e))
        log.error("YouTube reader stopped: %s: %s", type(e).__name__, e)
    finally:
        pipe.chat_ended = True


class Health:
    """What the runner last saw; written to status_file for the supervisor and overlay."""

    def __init__(self):
        self.source = "running"
        self.olv = {"ok": False, "clients": 0, "busy": False, "busy_for_s": 0.0}
        self.olv_error = ""
        self.stuck_logged = False
        self.reader = None


def write_status(cfg: StreamConfig, pipe: Pipeline, health: Health, now: float) -> dict:
    st = {
        "t": round(now, 3),
        "platform": cfg.platform,
        "source": health.source,
        "events_in": pipe.events_in,
        "last_event_age_s": round(now - pipe.last_event_t, 1)
        if pipe.last_event_t
        else None,
        "queue": pipe.lane_counts(),
        "speaking": pipe.in_flight is not None,
        "sent": pipe.sent_count,
        "olv": {**health.olv, "error": health.olv_error},
        "paid_open": len(pipe.ledger.open),
        "dead_letters": pipe.ledger.dead_count,
    }
    r = health.reader
    if r is not None:
        st["youtube"] = {
            "quota_used": r.state.quota_used,
            "live_chat_id_known": bool(r.state.live_chat_id),
        }
    if cfg.status_file:
        try:
            write_json_atomic(Path(cfg.status_file), st)
        except OSError as e:
            log.warning("status file not written: %s", e)
    return st


async def _speak(
    cfg: StreamConfig, pipe: Pipeline, sink, stop: asyncio.Event, health: Health
):
    warned = False
    next_status = 0.0
    while not stop.is_set():
        st = await sink.status()
        now = time.time()
        health.olv = {
            "ok": st.ok,
            "clients": st.clients,
            "busy": st.busy,
            "busy_for_s": st.busy_for_s,
        }
        health.olv_error = st.error
        if not st.ok:
            if pipe.in_flight is not None:
                pipe.interrupted(st.error or "olv")
            if not warned:
                log.warning("OLLV not reachable: %s", st.error)
                pipe.journal.write("olv_unreachable", reason=st.error)
                warned = True
        elif st.clients == 0:
            if pipe.in_flight is not None:
                pipe.interrupted("stranica_odspojena")
            if not warned:
                log.warning("no page with Mila is open")
                pipe.journal.write("olv_no_page")
                warned = True
        else:
            if warned:
                pipe.journal.write("olv_back")
            warned = False
            if st.busy and st.busy_for_s > cfg.olv_stuck_s:
                if not health.stuck_logged:
                    log.warning("Mila busy for %.0f s: answer stuck?", st.busy_for_s)
                    pipe.journal.write("olv_stuck", busy_for_s=st.busy_for_s)
                    health.stuck_logged = True
            else:
                health.stuck_logged = False
            out = pipe.next_text(now, st.busy)
            if out is not None:
                r = await sink.send(out.text)
                pipe.sent(out, time.time(), r.ok, r.error)
        if now >= next_status:
            write_status(cfg, pipe, health, now)
            next_status = now + cfg.status_every_s
        if pipe.chat_ended and not pipe.queue.pending and pipe.in_flight is None:
            stop.set()
        await asyncio.sleep(cfg.tick_s)


async def run(cfg: StreamConfig, dry_run: bool = False) -> dict:
    journal = Journal(Path(cfg.log_dir))
    ledger = PaidLedger(cfg.paid_ledger_file, cfg.dead_letter_file)
    recovered = ledger.recover()
    if recovered:
        journal.write("dead_letters_recovered", count=recovered)
        log.warning("%d paid events from an earlier run were not answered", recovered)
    pipe = Pipeline(
        InputFilter(cfg.filter), MessageQueue(cfg.queue), journal, ledger=ledger
    )
    health = Health()
    need_http = (not dry_run) or cfg.platform == "youtube"
    http = HttpxClient() if need_http else None
    sink = DryRunSink() if dry_run else OlvSink(cfg.olv, http)
    stop = asyncio.Event()
    journal.write("start", platform=cfg.platform, dry_run=dry_run)
    if cfg.platform == "sim":
        feeder = asyncio.create_task(_feed_sim(cfg, pipe))
    elif cfg.platform == "youtube":
        reader = YouTubeReader(cfg.youtube, http, Path(cfg.state_file))
        health.reader = reader
        feeder = asyncio.create_task(_feed_youtube(reader, pipe, health))
    else:
        raise ValueError(f"unknown platform {cfg.platform}")
    try:
        await _speak(cfg, pipe, sink, stop, health)
    finally:
        feeder.cancel()
        if http is not None:
            await http.aclose()
        lost = pipe.close()
        if lost:
            log.warning(
                "%d paid events not answered: see %s", lost, cfg.dead_letter_file
            )
        health.source = "stopped"
        write_status(cfg, pipe, health, time.time())
        journal.write("stop", counts=journal.summary())
    return journal.summary()
