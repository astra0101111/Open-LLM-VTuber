import json
import threading
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from mila_stream.brain_proxy import (
    Brain,
    BrainConfig,
    Upstream,
    UpstreamError,
    make_handler,
    relay,
)


def ups(*names):
    return [
        Upstream(
            name=n,
            base_url=f"http://{n}",
            model=f"m-{n}",
            reasoning_effort=["minimal", "low", None],
        )
        for n in names
    ]


def test_effort_steps_down_and_is_remembered():
    calls = []

    def opener(up, body):
        calls.append(body.get("reasoning_effort"))
        if body.get("reasoning_effort") == "minimal":
            raise UpstreamError(400, "reasoning_effort minimal is not supported")
        return ["ok"]

    b = Brain(BrainConfig(upstreams=ups("a")), opener=opener)
    assert b.open({"messages": []})[2] == "low"
    assert b.open({"messages": []})[2] == "low"
    assert calls == ["minimal", "low", "low"]


def test_fallback_on_429_and_timeout_then_cooldown():
    t = {"now": 0.0}
    seen = []

    def opener(up, body):
        seen.append(up.name)
        if up.name == "a":
            raise UpstreamError(429, "quota")
        if up.name == "b":
            raise TimeoutError("slow")
        return ["ok"]

    b = Brain(
        BrainConfig(cooldown_s=60, upstreams=ups("a", "b", "c")),
        opener=opener,
        clock=lambda: t["now"],
    )
    assert b.open({})[1].name == "c"
    seen.clear()
    t["now"] = 30  # a and b still cooling down
    assert b.open({})[1].name == "c" and seen == ["c"]
    t["now"] = 61  # a tried again
    seen.clear()
    b.open({})
    assert seen[0] == "a"


def test_bad_request_is_not_hidden():
    def opener(up, body):
        raise UpstreamError(401, "API key not valid")

    with pytest.raises(UpstreamError):
        Brain(BrainConfig(upstreams=ups("a", "b")), opener=opener).open({})


def test_stream_options_dropped_if_upstream_rejects_it():
    def opener(up, body):
        if "stream_options" in body:
            raise UpstreamError(400, "Unknown name stream_options")
        return ["ok"]

    b = Brain(BrainConfig(upstreams=ups("a")), opener=opener)
    b.open({})
    assert b.cfg.upstreams[0].include_usage is False


def test_relay_measures_without_content():
    lines = [
        b'data: {"choices":[{"delta":{"content":"Bok "}}]}\n',
        b"\n",
        b'data: {"choices":[{"delta":{"content":"svima!"}}]}\n',
        b"\n",
        b'data: {"choices":[],"usage":{"prompt_tokens":5000,"completion_tokens":7,'
        b'"prompt_tokens_details":{"cached_tokens":4096}}}\n',
        b"\n",
        b"data: [DONE]\n",
    ]

    class Out:
        data = b""

        def write(self, b):
            self.data += b

        def flush(self):
            pass

    out = Out()
    s = relay(lines, out, 0.0)
    assert out.data == b"".join(lines)
    assert (
        s["chars"],
        s["prompt_tokens"],
        s["cached_tokens"],
        s["completion_tokens"],
    ) == (10, 5000, 4096, 7)


def test_end_to_end_over_http(tmp_path):
    class FakeUpstream(BaseHTTPRequestHandler):
        def log_message(self, *a):
            pass

        def do_POST(self):
            body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            assert body["model"] == "m-real" and body["stream"] is True
            assert self.headers["Authorization"] == "Bearer SECRET"
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.end_headers()
            self.wfile.write(
                b'data: {"choices":[{"delta":{"content":"Pozdrav!"}}]}\n\n'
            )
            self.wfile.write(b"data: [DONE]\n\n")

    up_srv = ThreadingHTTPServer(("127.0.0.1", 0), FakeUpstream)
    threading.Thread(target=up_srv.serve_forever, daemon=True).start()
    import os

    os.environ["TEST_BRAIN_KEY"] = "SECRET"
    cfg = BrainConfig(
        upstreams=[
            Upstream(
                name="real",
                base_url=f"http://127.0.0.1:{up_srv.server_port}/v1",
                model="m-real",
                key_env="TEST_BRAIN_KEY",
            )
        ]
    )
    logf = tmp_path / "brain.jsonl"
    srv = ThreadingHTTPServer(("127.0.0.1", 0), make_handler(Brain(cfg), logf))
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    req = urllib.request.Request(
        f"http://127.0.0.1:{srv.server_port}/v1/chat/completions",
        data=json.dumps({"messages": [{"role": "user", "content": "bok"}]}).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    text = urllib.request.urlopen(req, timeout=5).read().decode()
    assert "Pozdrav!" in text
    rec = json.loads(logf.read_text().strip().splitlines()[-1])
    assert (
        rec["upstream"] == "real"
        and rec["chars"] == 8
        and "SECRET" not in logf.read_text()
    )
    assert "bok" not in logf.read_text()  # no message content in the log
    srv.shutdown()
    up_srv.shutdown()
