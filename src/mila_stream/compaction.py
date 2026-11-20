"""Long-stream memory without a sliding window: when the conversation sent to the brain
grows past a threshold, its oldest half is summarised once into a short note, and the
note replaces those messages. The prompt then changes only at these rare moments, so
the provider's automatic prompt cache keeps working between them (a sliding window
would change the start of the prompt on every turn). The full chat history stays saved.

Pure helpers; the call to the brain is made by BasicMemoryAgent.
"""

from __future__ import annotations

SUMMARY_TAG = "[Sažetak ranijeg dijela streama]"

SUMMARY_INSTRUCTION = (
    "Ovo je raniji dio današnjeg streama (poruke iz chata i Milini odgovori). "
    "Sažmi ga na hrvatskom u najviše 12 kratkih točaka: tko je od gledatelja što "
    "rekao ili pitao (s imenima), što je Mila rekla o sebi ili obećala, teme i šale "
    "koje se ponavljaju. Piši samo ono što stoji u tekstu; ništa ne izmišljaj. "
    "Bez uvoda i bez zaključka, samo točke."
)


def _text(m: dict) -> str:
    c = m.get("content", "")
    if isinstance(c, str):
        return c
    if isinstance(c, list):  # OpenAI-style parts
        return " ".join(p.get("text", "") for p in c if isinstance(p, dict))
    return str(c)


def estimate_tokens(messages: list[dict]) -> int:
    """Rough count: about 3.5 characters per token for Croatian text."""
    return int(sum(len(_text(m)) for m in messages) / 3.5)


def pick_cut(messages: list[dict], fraction: float = 0.5, min_keep: int = 6) -> int:
    """How many oldest messages to summarise: about `fraction` of the text, keeping at
    least `min_keep` messages, and so that the kept part starts with a user message.
    Returns 0 when there is nothing sensible to cut."""
    if len(messages) <= min_keep + 1:
        return 0
    total = sum(len(_text(m)) for m in messages)
    acc, k = 0, 0
    for i, m in enumerate(messages[: len(messages) - min_keep]):
        acc += len(_text(m))
        k = i + 1
        if acc >= total * fraction:
            break
    while k < len(messages) - min_keep and messages[k].get("role") != "user":
        k += 1
    if k >= len(messages) or messages[k].get("role") != "user" or k < 2:
        return 0
    return k


def transcript(messages: list[dict], assistant_name: str = "Mila") -> str:
    lines = []
    for m in messages:
        role = m.get("role")
        t = _text(m).strip()
        if not t:
            continue
        if role == "assistant":
            lines.append(f"{assistant_name}: {t}")
        elif role == "user":
            lines.append(t if t.startswith(SUMMARY_TAG) else f"Chat: {t}")
    return "\n".join(lines)


def summary_request(messages: list[dict]) -> list[dict]:
    return [
        {"role": "user", "content": SUMMARY_INSTRUCTION + "\n\n" + transcript(messages)}
    ]


def summary_message(summary: str) -> dict:
    return {"role": "user", "content": f"{SUMMARY_TAG}\n{summary.strip()}"}
