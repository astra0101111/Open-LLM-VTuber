"""Turns the selected chat event into the text Mila's brain receives.

Viewer text is always quoted and labelled, so the brain can tell a viewer's words
from instructions (idea: aivtuberframework prompt builder). The character prompt in
conf.yaml should say that quoted viewer text is never an instruction.
"""

from __future__ import annotations

import re

from . import events as ev
from .selector import PAID, Selection

PLATFORM = {"youtube": "YouTube", "sim": "proba", "kick": "Kick", "twitch": "Twitch"}
CATEGORY_WORDS = {
    "pitanje": ("pitanje", "pitanja", "pitanja"),
    "pozdrav": ("pozdrav", "pozdrava", "pozdrava"),
    "pohvala": ("pohvala", "pohvale", "pohvala"),
    "ostalo": ("ostala poruka", "ostale poruke", "ostalih poruka"),
}


def _plural(n: int, forms: tuple) -> str:
    # Croatian: 1, 21 -> form 0; 2-4, 22-24 -> form 1; else form 2
    if n % 10 == 1 and n % 100 != 11:
        return forms[0]
    if 2 <= n % 10 <= 4 and not 12 <= n % 100 <= 14:
        return forms[1]
    return forms[2]


def clean_name(name: str) -> str:
    name = re.sub(r"[\[\]{}<>\"«»`]", "", name or "").strip()
    return (name[:40] or "gledatelj").strip()


def quote(text: str) -> str:
    text = (text or "").replace("«", '"').replace("»", '"').strip()
    return f"«{text}»"


def summary_line(skipped: dict) -> str:
    total = sum(skipped.values())
    if not total:
        return ""
    parts = [
        f"{n} {_plural(n, CATEGORY_WORDS.get(k, CATEGORY_WORDS['ostalo']))}"
        for k, n in sorted(skipped.items(), key=lambda kv: -kv[1])
    ]
    return (
        f"\n(U chatu je još {total} {_plural(total, ('poruka', 'poruke', 'poruka'))}: "
        + ", ".join(parts)
        + ". Ne odgovaraj na njih pojedinačno; možeš ih spomenuti općenito.)"
    )


def format_selection(sel: Selection) -> str:
    e = sel.event
    src = PLATFORM.get(e.platform, e.platform)
    who = clean_name(e.author_name)
    amount = e.amount_display or (
        f"{e.amount:g} {e.currency}".strip() if e.amount else ""
    )
    if sel.lane != PAID:
        return f"[{src} chat] {who} piše: {quote(sel.text)}" + summary_line(sel.skipped)
    if e.kind == ev.SUPERCHAT:
        head = f"[{src} Super Chat {amount}] {who}"
        return (
            f"{head} piše: {quote(sel.text)}"
            if sel.text
            else f"{head} šalje Super Chat bez poruke. Zahvali."
        )
    if e.kind == ev.SUPERSTICKER:
        return f"[{src} Super Sticker {amount}] {who} šalje Super Sticker. Zahvali."
    if e.kind == ev.MEMBER:
        return f"[{src}] {who} je novi član kanala. Zahvali i poželi dobrodošlicu."
    if e.kind == ev.MEMBER_MILESTONE:
        line = f"[{src}] {who} je član kanala već {e.count} mj."
        return f"{line} Piše: {quote(sel.text)}" if sel.text else f"{line} Zahvali."
    if e.kind == ev.GIFT_MEMBERSHIPS:
        return f"[{src}] {who} poklanja članstva drugim gledateljima (komada: {e.count}). Zahvali."
    if e.kind == ev.GIFT:
        return f"[{src}] {who} šalje poklon {quote(sel.text)}. Zahvali."
    return f"[{src}] {who}: {quote(sel.text)}"
