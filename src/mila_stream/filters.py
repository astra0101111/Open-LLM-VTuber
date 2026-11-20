"""Input filter: wraps the rule filter (rules.py) and adds what a live stream needs.

- messages starting with "#" are viewer-to-viewer talk and are not read (idea: AITuberKit)
- a viewer who sends a high-risk message (hate speech, threat, prompt injection) is
  ignored for block_minutes, so a "normal" follow-up is skipped too (AITuber OnAir
  comment-intelligence blocks for 10 minutes; checked in their code 2026-11-20)
- paid events always pass, but their text goes through the same filter: a bad
  comment is removed and Mila only thanks for the gift
"""

from __future__ import annotations

from dataclasses import dataclass

from . import events as ev
from . import rules
from .events import ChatEvent

HIGH_RISK = {"govor_mrznje_ili_prijetnja"}


@dataclass
class Decision:
    verdict: str  # accept | drop
    reason: str
    flags: tuple = ()
    text: str = ""  # cleaned text (personal data removed)


@dataclass
class FilterConfig:
    block_minutes: float = 10.0
    max_len: int = 220
    block_on_injection: bool = True


class InputFilter:
    def __init__(self, cfg: FilterConfig | None = None):
        self.cfg = cfg or FilterConfig()
        self.state = rules.SpamState()
        self.blocked_until: dict[str, float] = {}

    def _viewer(self, e: ChatEvent) -> str:
        return e.author_id or e.author_name

    def is_blocked(self, e: ChatEvent, now: float) -> bool:
        until = self.blocked_until.get(self._viewer(e), 0.0)
        return now < until

    def block(self, viewer: str, now: float) -> None:
        if viewer:
            self.blocked_until[viewer] = now + self.cfg.block_minutes * 60

    def check(self, e: ChatEvent, now: float | None = None) -> Decision:
        now = e.received_at if now is None else now
        if e.is_control:
            return Decision("accept", "kontrola")
        if e.is_paid:
            return self._check_paid(e, now)
        raw = (e.text or "").strip()
        if raw.startswith("#"):
            return Decision("drop", "nije_za_milu", text=raw)
        if self.is_blocked(e, now):
            return Decision("drop", "gledatelj_privremeno_blokiran", text=raw)
        verdict, reason, flags, text = rules.check(
            e, self.state, now=now, max_len=self.cfg.max_len
        )
        if verdict == "drop" and reason in HIGH_RISK:
            self.block(self._viewer(e), now)
        if verdict == "accept" and "pokusaj_preuzimanja_uloge" in flags:
            if self.cfg.block_on_injection:
                self.block(self._viewer(e), now)
            return Decision("drop", "pokusaj_preuzimanja_uloge", flags, text)
        return Decision(verdict, reason, flags, text)

    def _check_paid(self, e: ChatEvent, now: float) -> Decision:
        raw = (e.text or "").strip()
        if not raw or e.kind in (ev.SUPERSTICKER, ev.GIFT):
            return Decision(
                "accept", "dar", text=raw if e.kind == ev.SUPERSTICKER else ""
            )
        lvl, _ = rules.toxicity(raw)
        injection = bool(rules.INJECTION_RE.search(raw))
        if lvl or injection or rules.URL_RE.search(raw) or rules.PROMO_RE.search(raw):
            if lvl == 2 or injection:
                self.block(self._viewer(e), now)
            return Decision("accept", "dar_tekst_uklonjen", ("tekst_uklonjen",), "")
        return Decision("accept", "dar", (), rules.redact(raw)[: self.cfg.max_len])
