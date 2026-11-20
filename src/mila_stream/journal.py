"""Per-message log (JSON Lines): what arrived, what the filter and the queue decided
and why, what was sent to Mila and when she finished. Personal data (e-mail, phone,
IBAN) is removed from logged text. One file per day.
"""

from __future__ import annotations

import json
import time
from collections import Counter
from datetime import datetime
from pathlib import Path

from . import rules


class Journal:
    def __init__(self, log_dir: Path | None, clock=time.time):
        self.dir = Path(log_dir) if log_dir else None
        self.clock = clock
        self.counts: Counter = Counter()
        self.records: list[dict] = []  # kept in memory too, for tests and the summary
        self.keep_in_memory = log_dir is None

    def write(self, kind: str, **data) -> dict:
        rec = {"t": round(self.clock(), 3), "kind": kind, **data}
        if "text" in rec and isinstance(rec["text"], str):
            rec["text"] = rules.redact(rec["text"])
        if (
            "event" in rec
            and isinstance(rec["event"], dict)
            and rec["event"].get("text")
        ):
            rec["event"] = {**rec["event"], "text": rules.redact(rec["event"]["text"])}
        key = kind if "reason" not in rec else f"{kind}:{rec['reason']}"
        self.counts[key] += 1
        if self.keep_in_memory:
            self.records.append(rec)
        if self.dir:
            self.dir.mkdir(parents=True, exist_ok=True)
            day = datetime.fromtimestamp(rec["t"]).strftime("%Y-%m-%d")
            with open(self.dir / f"{day}.jsonl", "a", encoding="utf-8") as f:
                f.write(json.dumps(rec, ensure_ascii=False) + "\n")
        return rec

    def summary(self) -> dict:
        return dict(sorted(self.counts.items()))
