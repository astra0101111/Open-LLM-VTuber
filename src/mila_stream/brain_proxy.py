"""Brain connector (plan v0.2, part 14): one local OpenAI-compatible address for OLLV,
in front of any number of OpenAI-compatible brains. The brain is chosen in config,
not in code; which brain Mila gets is decided separately.

- fallback chain (part 4): on timeout, network error, HTTP 429 or 5xx the next
  upstream in the list answers; the first one is retried after `cooldown_s`
- reasoning effort per upstream: tried in order (e.g. minimal, low, none) until the
  upstream accepts one; remembered
- spoken fallback (part 4): when every upstream fails and `fallback_reply` is set,
  the connector answers with that line itself (e.g. "Ups, malo zapinjem, odmah se
  vraćam."), so Mila says something in character instead of going silent
- GET /health: requests, failures in a row, when the last answer succeeded, which
  upstreams are cooling down; read by the supervisor. It is passive: it spends
  nothing and says nothing about an upstream that has not been asked yet.
- measurement (part 13): one JSON line per request with time to first token, total
  time, characters, and token usage including cached prompt tokens when the upstream
  reports them. Message content is never logged; API keys are read from environment
  variables and never logged.

Standard library only. Run: python -m mila_stream.brain_proxy --config mila_stream.yaml
OLLV then uses openai_compatible_llm with base_url http://127.0.0.1:<port>/v1.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import threading
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import yaml

log = logging.getLogger("mila_stream.brain")

RETRY_STATUSES = {429, 500, 502, 503, 504}


@dataclass
class Upstream:
    name: str
    base_url: str
    model: str
    key_env: str = ""
    reasoning_effort: list = field(default_factory=lambda: [None])
    timeout_s: float = 30.0
    max_tokens: int = 300
    include_usage: bool = True
    # runtime
    effort_index: int = 0
    down_until: float = 0.0

    @property
    def key(self) -> str:
        return os.environ.get(self.key_env, "") if self.key_env else ""


@dataclass
class BrainConfig:
    host: str = "127.0.0.1"
    port: int = 8089
    log_file: str = "logs/mila_brain.jsonl"
    cooldown_s: float = 60.0
    fallback_reply: str = ""
    upstreams: list = field(default_factory=list)


def load_brain_config(path: str | Path) -> BrainConfig:
    raw = yaml.safe_load(Path(path).read_text(encoding="utf-8")) or {}
    b = dict(raw.get("brain") or {})
    ups = [Upstream(**u) for u in b.pop("upstreams", [])]
    cfg = BrainConfig(**b)
    cfg.upstreams = ups
    return cfg


class UpstreamError(Exception):
    def __init__(self, status: int, body: str):
        super().__init__(f"HTTP {status}")
        self.status = status
        self.body = body


def _open(up: Upstream, body: dict):
    req = urllib.request.Request(
        up.base_url.rstrip("/") + "/chat/completions",
        data=json.dumps(body).encode("utf-8"),
        method="POST",
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {up.key}",
        },
    )
    try:
        return urllib.request.urlopen(req, timeout=up.timeout_s)
    except urllib.error.HTTPError as e:
        raise UpstreamError(e.code, e.read().decode("utf-8", "replace")[:600]) from e


class Brain:
    """Chooses an upstream and opens a streamed completion. Thread-safe enough for OLLV."""

    def __init__(self, cfg: BrainConfig, opener=_open, clock=time.time):
        self.cfg = cfg
        self.opener = opener
        self.clock = clock
        self.lock = threading.Lock()
        self.requests = 0
        self.failures_in_row = 0
        self.fallback_replies = 0
        self.last_ok_t: float | None = None
        self.last_error_t: float | None = None
        self.last_error = ""

    def record(self, ok: bool, error: str = "") -> None:
        with self.lock:
            self.requests += 1
            if ok:
                self.failures_in_row = 0
                self.last_ok_t = self.clock()
            else:
                self.failures_in_row += 1
                self.last_error_t = self.clock()
                self.last_error = error[:200]

    def health(self) -> dict:
        now = self.clock()
        return {
            "ok": self.failures_in_row < 3,
            "requests": self.requests,
            "failures_in_row": self.failures_in_row,
            "fallback_replies": self.fallback_replies,
            "last_ok_age_s": round(now - self.last_ok_t, 1) if self.last_ok_t else None,
            "last_error_age_s": round(now - self.last_error_t, 1)
            if self.last_error_t
            else None,
            "last_error": self.last_error,
            "upstreams": [
                {
                    "name": u.name,
                    "model": u.model,
                    "cooling_s": round(max(0.0, u.down_until - now), 1),
                }
                for u in self.cfg.upstreams
            ],
        }

    def open(self, request: dict):
        """Returns (response, upstream, effort). Raises the last error if all fail."""
        last = None
        now = self.clock()
        order = [u for u in self.cfg.upstreams if u.down_until <= now] or list(
            self.cfg.upstreams
        )
        for up in order:
            while True:
                efforts = up.reasoning_effort or [None]
                effort = efforts[min(up.effort_index, len(efforts) - 1)]
                body = dict(request)
                body["model"] = up.model
                body["stream"] = True
                body.setdefault("max_tokens", up.max_tokens)
                if effort:
                    body["reasoning_effort"] = effort
                else:
                    body.pop("reasoning_effort", None)
                if up.include_usage:
                    body["stream_options"] = {"include_usage": True}
                try:
                    resp = self.opener(up, body)
                    return resp, up, effort
                except UpstreamError as e:
                    last = e
                    low = e.body.lower()
                    if e.status == 400 and "stream_options" in low and up.include_usage:
                        up.include_usage = False
                        continue
                    if (
                        e.status == 400
                        and ("reason" in low or "thinking" in low)
                        and up.effort_index < len(efforts) - 1
                    ):
                        up.effort_index += 1
                        continue
                    if e.status in RETRY_STATUSES:
                        up.down_until = self.clock() + self.cfg.cooldown_s
                        break  # next upstream
                    raise  # 400/401/403: our request or key is wrong; do not hide it
                except Exception as e:  # timeout, network
                    last = e
                    up.down_until = self.clock() + self.cfg.cooldown_s
                    break
        raise last or RuntimeError("no upstream configured")


def make_handler(brain: Brain, log_path: Path | None):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *a):
            pass

        def _json(self, code: int, obj: dict):
            data = json.dumps(obj).encode()
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def do_GET(self):
            if self.path.rstrip("/") == "/health":
                self._json(200, brain.health())
                return
            # /v1/models
            models = [{"id": u.model, "object": "model"} for u in brain.cfg.upstreams]
            self._json(200, {"object": "list", "data": models})

        def do_POST(self):
            n = int(self.headers.get("Content-Length", 0))
            req = json.loads(self.rfile.read(n) or b"{}")
            t0 = time.perf_counter()
            try:
                resp, up, effort = brain.open(req)
            except Exception as e:
                status = getattr(e, "status", 502)
                brain.record(False, f"{type(e).__name__} {status}")
                if brain.cfg.fallback_reply and (
                    status in RETRY_STATUSES or status == 502
                ):
                    brain.fallback_replies += 1
                    _write_log(
                        log_path,
                        {"error": type(e).__name__, "status": status, "spoken": True},
                    )
                    self.send_response(200)
                    self.send_header("Content-Type", "text/event-stream")
                    self.send_header("Cache-Control", "no-cache")
                    self.end_headers()
                    write_canned(self.wfile, brain.cfg.fallback_reply)
                    return
                _write_log(log_path, {"error": type(e).__name__, "status": status})
                self._json(
                    502 if status in RETRY_STATUSES or status == 502 else status,
                    {"error": {"message": str(e), "type": "upstream"}},
                )
                return
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.send_header("Cache-Control", "no-cache")
            self.end_headers()
            stats = relay(resp, self.wfile, t0)
            brain.record(stats["chars"] > 0, "prazan odgovor")
            rec = {
                "upstream": up.name,
                "model": up.model,
                "effort": effort,
                "fallback": up is not brain.cfg.upstreams[0],
                **stats,
            }
            _write_log(log_path, rec)

    return Handler


def relay(resp, out, t0: float) -> dict:
    """Copy the SSE stream to the client; measure without keeping any content."""
    ttft = None
    chars = 0
    usage = {}
    for raw in resp:
        line = raw.decode("utf-8", "replace") if isinstance(raw, bytes) else raw
        if line.startswith("data:") and "[DONE]" not in line:
            try:
                d = json.loads(line[5:].strip())
                for ch in d.get("choices") or []:
                    c = (ch.get("delta") or {}).get("content") or ""
                    if c and ttft is None:
                        ttft = time.perf_counter() - t0
                    chars += len(c)
                if d.get("usage"):
                    usage = d["usage"]
            except (ValueError, AttributeError):
                pass
        try:
            out.write(raw if isinstance(raw, bytes) else raw.encode())
            out.flush()
        except OSError:
            break
    details = usage.get("prompt_tokens_details") or {}
    return {
        "ttft_s": round(ttft, 3) if ttft is not None else None,
        "total_s": round(time.perf_counter() - t0, 3),
        "chars": chars,
        "prompt_tokens": usage.get("prompt_tokens"),
        "cached_tokens": details.get("cached_tokens"),
        "completion_tokens": usage.get("completion_tokens"),
    }


def write_canned(out, text: str) -> None:
    """One OpenAI-style streamed answer with fixed text."""
    chunk = {
        "id": "mila-fallback",
        "object": "chat.completion.chunk",
        "model": "mila-fallback",
        "choices": [{"index": 0, "delta": {"role": "assistant", "content": text}}],
    }
    end = {
        "id": "mila-fallback",
        "object": "chat.completion.chunk",
        "model": "mila-fallback",
        "choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}],
    }
    for obj in (chunk, end):
        out.write(f"data: {json.dumps(obj, ensure_ascii=False)}\n\n".encode("utf-8"))
    out.write(b"data: [DONE]\n\n")
    out.flush()


def _write_log(path: Path | None, rec: dict):
    if not path:
        return
    rec = {"t": round(time.time(), 3), **rec}
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(rec) + "\n")


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="mila_stream.brain_proxy")
    ap.add_argument("--config", default="mila_stream.yaml")
    a = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO)
    cfg = load_brain_config(a.config)
    if not cfg.upstreams:
        print("no brain.upstreams in config")
        return 2
    missing = [u.key_env for u in cfg.upstreams if u.key_env and not u.key]
    if missing:
        print("missing environment variables:", ", ".join(missing))
        return 2
    srv = ThreadingHTTPServer(
        (cfg.host, cfg.port), make_handler(Brain(cfg), Path(cfg.log_file))
    )
    names = ", ".join(f"{u.name}={u.model}" for u in cfg.upstreams)
    print(f"brain proxy on http://{cfg.host}:{cfg.port}/v1 -> {names}", flush=True)
    srv.serve_forever()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
