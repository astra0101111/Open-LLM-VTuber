"""Output filter: checks every sentence Mila is about to say (plan v0.2, part 3, layer 3).

The brain can still produce something it must not say on a public stream (a slur
repeated from chat, a phone number, a link). Each sentence is checked with the same
Croatian rules as the input before it reaches the voice and the subtitles:

- hate speech or a threat (level 2), and by default also profanity (level 1):
  the whole sentence becomes "Filtrirano." (Neuro-sama's "filtered" became a running
  joke, so a visible filter can be part of the character)
- e-mail, phone, IBAN: removed from speech, shown as [uklonjeno]
- links: removed from speech, shown as [poveznica]

Used by open_llm_vtuber.agent.transformers.tts_filter. Off with MILA_OUTPUT_GUARD=0;
MILA_OUTPUT_GUARD_LEVEL=2 lets mild profanity through.
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass

from . import rules

FILTERED = "Filtrirano."


@dataclass
class Guarded:
    display: str
    tts: str
    reason: str = ""  # empty when the sentence was not changed


def enabled() -> bool:
    return os.environ.get("MILA_OUTPUT_GUARD", "1").strip() not in (
        "0",
        "false",
        "no",
        "off",
    )


def block_level() -> int:
    try:
        return int(os.environ.get("MILA_OUTPUT_GUARD_LEVEL", "1"))
    except ValueError:
        return 1


def guard(display: str, tts: str | None = None, level: int | None = None) -> Guarded:
    """display: subtitle text; tts: text for the voice (defaults to display)."""
    tts = display if tts is None else tts
    level = block_level() if level is None else level
    lvl, _ = rules.toxicity(display)
    if lvl and lvl >= level:
        return Guarded(
            FILTERED, FILTERED, "govor_mrznje_ili_prijetnja" if lvl == 2 else "psovka"
        )
    reasons = []
    new_display, new_tts = display, tts
    for rx, label, reason in (
        (rules.EMAIL_RE, "[uklonjeno]", "osobni_podaci"),
        (rules.IBAN_RE, "[uklonjeno]", "osobni_podaci"),
        (rules.PHONE_RE, "[uklonjeno]", "osobni_podaci"),
        (rules.URL_RE, "[poveznica]", "poveznica"),
    ):
        if rx.search(new_display) or rx.search(new_tts):
            new_display = rx.sub(label, new_display)
            new_tts = rx.sub("", new_tts)
            if reason not in reasons:
                reasons.append(reason)
    if reasons:
        new_tts = re.sub(r"\s{2,}", " ", new_tts).strip()
    return Guarded(new_display, new_tts, ",".join(reasons))
