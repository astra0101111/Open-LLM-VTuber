"""Sends chosen text into Open-LLM-VTuber through the /inject-text route of our fork.

/inject-status reports busy while a conversation task runs; OLLV ends that task only
after the page reports frontend-playback-complete, so "not busy" means Mila has
actually finished speaking (one speaking slot).
"""

from __future__ import annotations

import os
from dataclasses import dataclass

from .http import HttpClient


@dataclass
class OlvConfig:
    base_url: str = "http://127.0.0.1:12393"
    token_env: str = "MILA_INJECT_TOKEN"


@dataclass
class Status:
    ok: bool
    clients: int = 0
    busy: bool = False
    error: str = ""
    busy_for_s: float = 0.0


class OlvSink:
    def __init__(self, cfg: OlvConfig, http: HttpClient, token: str | None = None):
        self.cfg = cfg
        self.http = http
        self.token = token if token is not None else os.environ.get(cfg.token_env, "")

    def _headers(self) -> dict:
        return {"x-mila-token": self.token}

    async def status(self) -> Status:
        try:
            r = await self.http.get(
                f"{self.cfg.base_url}/inject-status", {}, self._headers()
            )
        except Exception as e:
            return Status(False, error=f"veza: {e}")
        if r.status != 200 or not isinstance(r.data, dict):
            return Status(False, error=f"HTTP {r.status}")
        return Status(
            True,
            int(r.data.get("clients", 0)),
            bool(r.data.get("busy")),
            busy_for_s=float(r.data.get("busy_for_s") or 0.0),
        )

    async def send(self, text: str) -> Status:
        try:
            r = await self.http.post(
                f"{self.cfg.base_url}/inject-text", {"text": text}, self._headers()
            )
        except Exception as e:
            return Status(False, error=f"veza: {e}")
        if r.status == 200:
            return Status(True, busy=True)
        err = (r.data or {}).get("error", "") if isinstance(r.data, dict) else ""
        return Status(
            False, busy=r.status == 429, error=f"HTTP {r.status} {err}".strip()
        )


class DryRunSink:
    """Prints instead of sending; speaking takes speak_s seconds."""

    def __init__(self, speak_s: float = 6.0, clock=None, out=print):
        import time

        self.speak_s = speak_s
        self.clock = clock or time.time
        self.until = 0.0
        self.sent: list[str] = []
        self.out = out

    async def status(self) -> Status:
        busy = self.clock() < self.until
        left = self.speak_s - (self.until - self.clock())
        return Status(True, 1, busy, busy_for_s=max(0.0, left) if busy else 0.0)

    async def send(self, text: str) -> Status:
        if self.clock() < self.until:
            return Status(False, busy=True, error="Mila jos govori")
        self.sent.append(text)
        self.until = self.clock() + self.speak_s
        self.out(f"[MILA <-] {text}")
        return Status(True, busy=True)
