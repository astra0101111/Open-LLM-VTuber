"""Small HTTP seam so the YouTube reader and the OLLV sink can be tested without a network.

The default client uses httpx, which is already an Open-LLM-VTuber dependency.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol


@dataclass
class HttpResponse:
    status: int
    data: Any  # parsed JSON, or None


class HttpClient(Protocol):
    async def get(
        self, url: str, params: dict, headers: dict | None = None
    ) -> HttpResponse: ...

    async def post(
        self, url: str, json: dict, headers: dict | None = None
    ) -> HttpResponse: ...

    async def aclose(self) -> None: ...


class HttpxClient:
    """HttpClient on top of httpx.AsyncClient."""

    def __init__(self, timeout: float = 20.0):
        import httpx  # imported here so tests do not need httpx

        self._client = httpx.AsyncClient(timeout=timeout)

    async def get(self, url, params, headers=None):
        r = await self._client.get(url, params=params, headers=headers)
        return HttpResponse(r.status_code, _json_or_none(r))

    async def post(self, url, json, headers=None):
        r = await self._client.post(url, json=json, headers=headers)
        return HttpResponse(r.status_code, _json_or_none(r))

    async def aclose(self):
        await self._client.aclose()


def _json_or_none(r):
    try:
        return r.json()
    except Exception:
        return None
