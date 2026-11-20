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


async def _feed_youtube(reader: YouTubeReader, pipe: Pipeline):
    try:
        async for e in reader.events():
            pipe.ingest(e, time.time())
    except (ChatClosed, QuotaExhausted, AuthError) as e:
        pipe.journal.write("source_stopped", reason=type(e).__name__, detail=str(e))
        log.error("YouTube reader stopped: %s: %s", type(e).__name__, e)
    finally:
        pipe.chat_ended = True


async def _speak(cfg: StreamConfig, pipe: Pipeline, sink, stop: asyncio.Event):
    warned = False
    while not stop.is_set():
        st = await sink.status()
        now = time.time()
        if not st.ok:
            if not warned:
                log.warning("OLLV not reachable: %s", st.error)
                pipe.journal.write("olv_unreachable", reason=st.error)
                warned = True
        elif st.clients == 0:
            if not warned:
                log.warning("no page with Mila is open")
                warned = True
        else:
            warned = False
            out = pipe.next_text(now, st.busy)
            if out is not None:
                r = await sink.send(out.text)
                pipe.sent(out, time.time(), r.ok, r.error)
        if pipe.chat_ended and not pipe.queue.pending and pipe.in_flight is None:
            stop.set()
        await asyncio.sleep(cfg.tick_s)


async def run(cfg: StreamConfig, dry_run: bool = False) -> dict:
    journal = Journal(Path(cfg.log_dir))
    pipe = Pipeline(InputFilter(cfg.filter), MessageQueue(cfg.queue), journal)
    need_http = (not dry_run) or cfg.platform == "youtube"
    http = HttpxClient() if need_http else None
    sink = DryRunSink() if dry_run else OlvSink(cfg.olv, http)
    stop = asyncio.Event()
    journal.write("start", platform=cfg.platform, dry_run=dry_run)
    if cfg.platform == "sim":
        feeder = asyncio.create_task(_feed_sim(cfg, pipe))
    elif cfg.platform == "youtube":
        reader = YouTubeReader(cfg.youtube, http, Path(cfg.state_file))
        feeder = asyncio.create_task(_feed_youtube(reader, pipe))
    else:
        raise ValueError(f"unknown platform {cfg.platform}")
    try:
        await _speak(cfg, pipe, sink, stop)
    finally:
        feeder.cancel()
        if http is not None:
            await http.aclose()
        journal.write("stop", counts=journal.summary())
    return journal.summary()
