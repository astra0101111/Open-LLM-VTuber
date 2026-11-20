"""Paid events that Mila did not answer are never lost (plan v0.2, part 4: "red mrtvih
pisama").

Every paid event (Super Chat, membership, gift) is written to an open ledger file the
moment it is queued and leaves it only when Mila has finished speaking the answer.
If it leaves any other way (time-to-live ran out, the message was deleted, the viewer
was banned, the stream ended, the program crashed or OLLV went down mid-answer) it is
appended to the dead-letter file with the reason, so the operator can thank the viewer
later. The ledger is rewritten atomically, so even a hard kill loses nothing: on the
next start every entry left in it becomes a dead letter "neodgovoreno_prije_pada".
"""

from __future__ import annotations

import json
import os
import time
from pathlib import Path

from . import rules
from .events import ChatEvent


class PaidLedger:
    def __init__(self, ledger_file: str | Path | None, dead_file: str | Path | None):
        self.ledger_path = Path(ledger_file) if ledger_file else None
        self.dead_path = Path(dead_file) if dead_file else None
        self.open: dict[str, dict] = {}
        self.dead_count = 0
        self.dead: list[dict] = []  # kept in memory too (tests, status page)

    # -- start

    def recover(self) -> int:
        """Entries left by a previous run that did not stop cleanly become dead letters."""
        if not self.ledger_path or not self.ledger_path.exists():
            return 0
        try:
            left = json.loads(self.ledger_path.read_text(encoding="utf-8")) or {}
        except (ValueError, OSError):
            left = {}
        for rec in left.values():
            self._dead_record(rec, "neodgovoreno_prije_pada")
        self.open = {}
        self._save()
        return len(left)

    # -- events

    def opened(self, e: ChatEvent, now: float) -> None:
        d = e.short()
        if d.get("text"):
            d["text"] = rules.redact(d["text"])
        if e.amount_display:
            d["amount_display"] = e.amount_display
        if e.count:
            d["count"] = e.count
        d["queued_at"] = round(now, 3)
        self.open[e.id] = d
        self._save()

    def closed(self, event_id: str) -> None:
        if self.open.pop(event_id, None) is not None:
            self._save()

    def dead_letter(self, event_id: str, reason: str) -> bool:
        rec = self.open.pop(event_id, None)
        if rec is None:
            return False
        self._dead_record(rec, reason)
        self._save()
        return True

    def dead_by_author(self, author_id: str, reason: str) -> int:
        ids = [k for k, r in self.open.items() if r.get("author_id") == author_id]
        for k in ids:
            self.dead_letter(k, reason)
        return len(ids)

    def flush(self, reason: str) -> int:
        ids = list(self.open)
        for k in ids:
            self.dead_letter(k, reason)
        return len(ids)

    # -- files

    def _dead_record(self, rec: dict, reason: str) -> None:
        out = {"t": round(time.time(), 3), "reason": reason, **rec}
        self.dead.append(out)
        self.dead_count += 1
        if self.dead_path:
            self.dead_path.parent.mkdir(parents=True, exist_ok=True)
            with open(self.dead_path, "a", encoding="utf-8") as f:
                f.write(json.dumps(out, ensure_ascii=False) + "\n")

    def _save(self) -> None:
        if not self.ledger_path:
            return
        write_json_atomic(self.ledger_path, self.open)


def write_json_atomic(path: Path, obj) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(obj, ensure_ascii=False, indent=1), encoding="utf-8")
    os.replace(tmp, path)
