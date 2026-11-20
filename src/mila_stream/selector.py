"""Message queue: decides what Mila answers next and what is dropped.

Lanes (highest first): PAID (Super Chat, members, gifts), ADDRESSED (says Mila's
name or asks her something), CHAT (everything else). Each lane has a time-to-live:
a message older than that is dropped, not spoken (idea: Haruuowo "Luna", default 15 s).
Inside a lane the best-scored message wins; the score borrows the weights of AITuber
OnAir comment-intelligence (checked in their code 2026-11-20). Rate limits (pause
between answers, answers per minute) apply to ADDRESSED and CHAT, never to PAID
(idea: aivtuberframework). One answered comment and its author are remembered for
answered_memory_s, so the same thing is not answered twice.
"""

from __future__ import annotations

import re
from collections import Counter, deque
from dataclasses import dataclass, field

from . import rules
from .events import ChatEvent

PAID, ADDRESSED, CHAT = 0, 1, 2
LANE_NAMES = {PAID: "placeno", ADDRESSED: "upuceno", CHAT: "chat"}

QUESTION_WORDS = {
    "sto",
    "sta",
    "kako",
    "kada",
    "kad",
    "gdje",
    "gde",
    "zasto",
    "tko",
    "ko",
    "koji",
    "koja",
    "koje",
    "koliko",
    "jel",
    "jesi",
    "jesu",
    "imas",
    "mozes",
    "volis",
    "znas",
    "what",
    "how",
    "when",
    "where",
    "who",
    "why",
}
QUESTION_PHRASES = (
    "je li",
    "da li",
    "jel ",
    "jesi li",
    "mozes li",
    "imas li",
    "volis li",
    "znas li",
)
GREETINGS = (
    "bok",
    "pozdrav",
    "hej",
    "hello",
    "hi",
    "zdravo",
    "cao",
    "ciao",
    "dobro jutro",
    "dobar dan",
    "dobra vecer",
    "laku noc",
    "hejj",
    "ej",
)
PRAISE = (
    "super",
    "odlicno",
    "bravo",
    "volim te",
    "lijepa",
    "slatka",
    "top",
    "legenda",
    "najbolja",
)


@dataclass
class QueueConfig:
    names: tuple = ("mila",)
    ttl_paid_s: float = 180.0
    ttl_addressed_s: float = 45.0
    ttl_chat_s: float = 25.0
    min_gap_s: float = 0.0
    max_per_minute: int = 6
    answered_memory_s: float = 600.0
    max_pending: int = 200
    # score weights (AITuber OnAir defaults)
    w_question: float = 0.25
    w_safety: float = 0.2
    w_returning: float = 0.15
    w_new: float = 0.15
    w_fresh: float = 0.05
    boost_moderator: float = 0.12
    boost_member: float = 0.06
    penalty_duplicate: float = 0.25
    penalty_long: float = 0.2
    penalty_answered_author: float = 0.3


@dataclass
class Pending:
    event: ChatEvent
    text: str
    lane: int
    added_at: float
    flags: tuple = ()


@dataclass
class Selection:
    event: ChatEvent
    text: str
    lane: int
    score: float
    skipped: dict = field(
        default_factory=dict
    )  # category -> count of other pending chat


def _norm(text: str) -> str:
    t = rules.unaccent((text or "").lower())
    return re.sub(r"\s+", " ", re.sub(r"[^\w\s?]", " ", t)).strip()


def is_question(text: str) -> bool:
    n = _norm(text)
    if "?" in n and len(n.split()) >= 2:
        return True
    words = set(n.replace("?", " ").split())
    if words & QUESTION_WORDS and len(words) >= 2:
        return True
    return any(n.startswith(p) or f" {p}" in f" {n}" for p in QUESTION_PHRASES)


def addresses(text: str, names: tuple) -> bool:
    n = _norm(text)
    for name in names:
        stem = rules.unaccent(name.lower())
        # Mila, Mile, Milo, Milu, Milom, Mili; also @Mila
        if re.search(
            rf"(?<!\w)@?{re.escape(stem[:-1] if len(stem) > 3 else stem)}\w{{0,2}}(?!\w)",
            n,
        ):
            return True
    return False


def category(text: str) -> str:
    n = _norm(text)
    if is_question(text):
        return "pitanje"
    if any(n == g or n.startswith(g + " ") for g in GREETINGS):
        return "pozdrav"
    if any(p in n for p in PRAISE):
        return "pohvala"
    return "ostalo"


class MessageQueue:
    def __init__(self, cfg: QueueConfig | None = None):
        self.cfg = cfg or QueueConfig()
        self.pending: list[Pending] = []
        self.answer_times: deque[float] = deque()
        self.last_answer_at: float = -1e9
        self.answered: dict[str, float] = {}  # "a:<author>" / "t:<text>" -> time
        self.author_counts: Counter = Counter()

    # -- input

    def lane_for(self, e: ChatEvent, text: str) -> int:
        if e.is_paid:
            return PAID
        if addresses(text, self.cfg.names) or is_question(text):
            return ADDRESSED
        return CHAT

    def ttl(self, lane: int) -> float:
        return {PAID: self.cfg.ttl_paid_s, ADDRESSED: self.cfg.ttl_addressed_s}.get(
            lane, self.cfg.ttl_chat_s
        )

    def add(self, e: ChatEvent, text: str, now: float, flags: tuple = ()) -> int:
        lane = self.lane_for(e, text)
        self.author_counts[e.author_id or e.author_name] += 1
        self.pending.append(Pending(e, text, lane, now, flags))
        if len(self.pending) > self.cfg.max_pending:
            # drop the oldest chat-lane message first, never paid
            for i, p in enumerate(self.pending):
                if p.lane == CHAT:
                    del self.pending[i]
                    break
        return lane

    def remove_message(self, msg_id: str) -> int:
        before = len(self.pending)
        self.pending = [p for p in self.pending if p.event.id != msg_id]
        return before - len(self.pending)

    def remove_author(self, author_id: str) -> int:
        before = len(self.pending)
        self.pending = [p for p in self.pending if p.event.author_id != author_id]
        return before - len(self.pending)

    # -- expiry and limits

    def expire(self, now: float) -> list[Pending]:
        gone = [p for p in self.pending if now - p.added_at > self.ttl(p.lane)]
        if gone:
            self.pending = [p for p in self.pending if p not in gone]
        for k in [
            k for k, t in self.answered.items() if now - t > self.cfg.answered_memory_s
        ]:
            del self.answered[k]
        return gone

    def rate_ok(self, now: float) -> bool:
        while self.answer_times and now - self.answer_times[0] >= 60:
            self.answer_times.popleft()
        if now - self.last_answer_at < self.cfg.min_gap_s:
            return False
        return len(self.answer_times) < self.cfg.max_per_minute

    def seconds_until_rate_ok(self, now: float) -> float:
        wait = max(0.0, self.cfg.min_gap_s - (now - self.last_answer_at))
        if len(self.answer_times) >= self.cfg.max_per_minute:
            wait = max(wait, 60 - (now - self.answer_times[0]))
        return wait

    # -- scoring

    def score(self, p: Pending, now: float) -> float:
        c = self.cfg
        e = p.event
        author = e.author_id or e.author_name
        s = c.w_safety
        if is_question(p.text):
            s += c.w_question
        n = self.author_counts[author]
        if n <= 1:
            s += c.w_new
        else:
            s += c.w_returning * min(1.0, n / 50)
        s += c.w_fresh * max(0.0, 1 - (now - p.added_at) / 60)
        if "moderator" in e.roles or "owner" in e.roles:
            s += c.boost_moderator
        if "member" in e.roles:
            s += c.boost_member
        if f"t:{_norm(p.text)}" in self.answered:
            s -= c.penalty_duplicate
        if f"a:{author}" in self.answered:
            s -= c.penalty_answered_author
        if len(p.text) > 180:
            s -= c.penalty_long
        if e.is_paid:
            s += e.amount / 100  # bigger gift first among paid
        return s

    # -- output

    def pick(self, now: float) -> Selection | None:
        self.expire(now)
        if not self.pending:
            return None
        lanes = sorted({p.lane for p in self.pending})
        for lane in lanes:
            if lane != PAID and not self.rate_ok(now):
                return None
            cands = [p for p in self.pending if p.lane == lane]
            if lane == PAID:  # gifts in arrival order: everyone gets thanked
                best = min(cands, key=lambda p: p.added_at)
                best_score = self.score(best, now)
            else:
                best = max(cands, key=lambda p: (self.score(p, now), p.added_at))
                best_score = self.score(best, now)
            self.pending.remove(best)
            skipped = {}
            if lane != PAID:
                # "drop, don't pile up": ordinary chat waiting now is summarised in this
                # answer and not answered one by one later; questions to Mila stay until
                # their time-to-live runs out
                rest = [p for p in self.pending if p.lane == CHAT]
                skipped = dict(Counter(category(p.text) for p in rest))
                self.pending = [p for p in self.pending if p.lane != CHAT]
            return Selection(best.event, best.text, lane, best_score, skipped)
        return None

    def requeue(self, sel: Selection, now: float) -> None:
        """Put a selection back (sending failed); keeps its lane, restarts its time-to-live."""
        self.pending.insert(0, Pending(sel.event, sel.text, sel.lane, now))

    def mark_answered(self, sel: Selection, now: float) -> None:
        e = sel.event
        self.answered[f"a:{e.author_id or e.author_name}"] = now
        if sel.text:
            self.answered[f"t:{_norm(sel.text)}"] = now
        if sel.lane != PAID:
            self.answer_times.append(now)
            self.last_answer_at = now
