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


def system_ssl_context():
    """Certificates from the operating system's store when truststore is installed.

    On Oero's PC Google's HTTPS works only with the Windows store (the demo's Gemini
    connector needed truststore for the same reason). Verification stays on; only the
    list of trusted roots changes. Returns True (httpx default: certifi) otherwise.
    """
    try:
        import ssl

        import truststore

        return truststore.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    except Exception:
        return True


class HttpxClient:
    """HttpClient on top of httpx.AsyncClient."""

    def __init__(self, timeout: float = 20.0):
        import httpx  # imported here so tests do not need httpx

        self._client = httpx.AsyncClient(timeout=timeout, verify=system_ssl_context())

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


def quiet_http_loggers() -> None:
    """httpx logs every request URL at INFO. The YouTube API key travels in the URL,
    so at INFO it would land in the chat log (found in the 2026-11-21 live test).
    Keep httpx and httpcore at WARNING."""
    import logging

    for name in ("httpx", "httpcore"):
        logging.getLogger(name).setLevel(logging.WARNING)
