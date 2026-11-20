"""The stream layer's core, without I/O: event in -> filter -> queue -> text out.

Kept synchronous and clock-driven so a 30-minute stream can be simulated in a test.
"""

from __future__ import annotations

from dataclasses import dataclass

from . import events as ev
from .deadletters import PaidLedger
from .events import ChatEvent
from .filters import InputFilter
from .journal import Journal
from .prompt import format_selection
from .selector import LANE_NAMES, PAID, MessageQueue, Selection


@dataclass
class Outgoing:
    text: str
    selection: Selection


class Pipeline:
    def __init__(
        self,
        flt: InputFilter,
        queue: MessageQueue,
        journal: Journal,
        ledger: PaidLedger | None = None,
    ):
        self.filter = flt
        self.queue = queue
        self.journal = journal
        self.ledger = ledger or PaidLedger(None, None)
        self.events_in = 0
        self.last_event_t: float = 0.0
        self.sent_count = 0
        self.in_flight: Outgoing | None = None
        self.sent_at: float = 0.0
        self.chat_ended = False

    def ingest(self, e: ChatEvent, now: float) -> None:
        self.events_in += 1
        self.last_event_t = now
        self.journal.write(
            "received", event=e.short(), lag_s=round(now - e.published_at, 3)
        )
        if e.kind == ev.TOMBSTONE:
            n = self.queue.remove_message(e.target_id)
            self.ledger.dead_letter(e.target_id, "obrisana_poruka")
            self.journal.write(
                "removed", reason="obrisana_poruka", target=e.target_id, count=n
            )
            return
        if e.kind == ev.BAN:
            n = self.queue.remove_author(e.target_id)
            self.filter.block(e.target_id, now + 10 * 365 * 86400)
            self.ledger.dead_by_author(e.target_id, "zabrana")
            self.journal.write("removed", reason="zabrana", target=e.target_id, count=n)
            return
        if e.kind == ev.CHAT_ENDED:
            self.chat_ended = True
            self.journal.write("chat_ended")
            return
        d = self.filter.check(e, now)
        if d.verdict != "accept":
            self.journal.write("dropped", id=e.id, reason=d.reason, label=e.label)
            return
        lane = self.queue.add(e, d.text, now, d.flags)
        if lane == PAID:
            self.ledger.opened(e, now)
        self.journal.write(
            "queued",
            id=e.id,
            lane=LANE_NAMES[lane],
            reason=d.reason,
            flags=list(d.flags),
            label=e.label,
        )

    def next_text(self, now: float, busy: bool) -> Outgoing | None:
        """Called often. Returns text to send when Mila is free, else None."""
        if busy:
            return None
        if self.in_flight is not None:
            self.journal.write(
                "spoke",
                id=self.in_flight.selection.event.id,
                speak_s=round(now - self.sent_at, 3),
            )
            self.ledger.closed(self.in_flight.selection.event.id)
            self.in_flight = None
        for p in self.queue.expire(now):
            if p.lane == PAID:
                self.ledger.dead_letter(p.event.id, "zastarjelo")
            self.journal.write(
                "dropped",
                id=p.event.id,
                reason="zastarjelo",
                lane=LANE_NAMES[p.lane],
                label=p.event.label,
            )
        sel = self.queue.pick(now)
        if sel is None:
            return None
        out = Outgoing(format_selection(sel), sel)
        for cat, n in sel.skipped.items():
            self.journal.write("summarised", category=cat, count=n)
        return out

    def sent(self, out: Outgoing, now: float, ok: bool, error: str = "") -> None:
        e = out.selection.event
        if ok:
            self.queue.mark_answered(out.selection, now)
            self.in_flight = out
            self.sent_at = now
            self.sent_count += 1
            self.journal.write(
                "sent",
                id=e.id,
                lane=LANE_NAMES[out.selection.lane],
                score=round(out.selection.score, 3),
                text=out.text,
                wait_s=round(now - e.received_at, 3),
                label=e.label,
            )
        else:
            self.queue.requeue(out.selection, now)
            self.journal.write("send_failed", id=e.id, reason=error)

    def interrupted(self, reason: str) -> None:
        """OLLV went away while Mila was answering: the answer may not have been heard."""
        if self.in_flight is None:
            return
        e = self.in_flight.selection.event
        self.journal.write("interrupted", id=e.id, reason=reason)
        self.ledger.dead_letter(e.id, "prekid_dok_govori")
        self.in_flight = None

    def close(self) -> int:
        """End of the run: whatever is still waiting is logged; paid ones become dead letters."""
        left = len(self.queue.pending)
        # an answer cut off by the stop stays open, so it is listed for the operator
        self.in_flight = None
        if left:
            self.journal.write("unanswered", count=left)
        n = self.ledger.flush("kraj_rada")
        self.queue.pending.clear()
        return n

    def lane_counts(self) -> dict:
        out = {name: 0 for name in LANE_NAMES.values()}
        for p in self.queue.pending:
            out[LANE_NAMES[p.lane]] += 1
        return out
