"""Supervisor with real child processes and short intervals (plan v0.2, part 4)."""

import json
import sys
import threading
import time
import urllib.request
from http.server import ThreadingHTTPServer

from mila_stream.supervisor import (
    OK,
    WARN,
    ProcSpec,
    Supervisor,
    SupervisorConfig,
    check_health,
    crash_test,
    make_handler,
)

HEALTH_SERVER = """
import json, sys
from http.server import BaseHTTPRequestHandler, HTTPServer
port, mode = int(sys.argv[1]), sys.argv[2]
class H(BaseHTTPRequestHandler):
    def log_message(self, *a): pass
    def do_GET(self):
        code = 500 if mode == "sick" else 200
        body = json.dumps({"ok": True, "requests": 1, "upstreams": []}).encode()
        self.send_response(code); self.send_header("Content-Length", str(len(body)))
        self.end_headers(); self.wfile.write(body)
HTTPServer(("127.0.0.1", port), H).serve_forever()
"""


def free_port():
    import socket

    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    p = s.getsockname()[1]
    s.close()
    return p


def cfg(tmp_path, procs, **kw):
    base = dict(
        check_every_s=0.2,
        start_grace_s=0.6,
        fail_after=2,
        max_restarts=3,
        restart_window_s=60,
        stop_timeout_s=3,
        log_dir=str(tmp_path / "logs"),
        difficulty_after_s=0,
    )
    base.update(kw)
    return SupervisorConfig(processes=procs, **base)


def run_until(sup, cond, timeout=20.0):
    t0 = time.time()
    while time.time() - t0 < timeout:
        sup.tick()
        if cond():
            return True
        time.sleep(0.05)
    return False


def events(tmp_path):
    p = tmp_path / "logs" / "events.jsonl"
    return [json.loads(x) for x in p.read_text(encoding="utf-8").splitlines()]


def fast_restarts(sup):
    # tests do not wait for the real backoff (2, 4, 8 s ...)
    for m in sup.procs:
        orig = m._schedule_restart

        def quick(now, why, m=m, orig=orig):
            orig(now, why)
            if m.state == "ponovno":
                m.next_start = now + 0.1

        m._schedule_restart = quick


def test_crashing_process_is_restarted_then_given_up(tmp_path):
    spec = ProcSpec(name="pada", cmd=[sys.executable, "-c", "import sys; sys.exit(3)"])
    sup = Supervisor(cfg(tmp_path, [spec]))
    fast_restarts(sup)
    assert run_until(sup, lambda: sup.procs[0].state == "odustao")
    m = sup.procs[0]
    assert m.restarts == 3
    st = sup.status()
    assert st["difficulty"] is True and "pada: odustao" in st["reasons"]
    kinds = [e["kind"] for e in events(tmp_path)]
    assert kinds.count("exit") == 4 and "gave_up" in kinds


def test_clean_exit_is_finished_not_restarted(tmp_path):
    spec = ProcSpec(name="chat", cmd=[sys.executable, "-c", "pass"])
    sup = Supervisor(cfg(tmp_path, [spec]))
    assert run_until(sup, lambda: sup.procs[0].state == "zavrsio")
    assert sup.procs[0].restarts == 0
    assert sup.status()["difficulty"] is False


def test_unhealthy_process_is_restarted(tmp_path):
    port = free_port()
    script = tmp_path / "srv.py"
    script.write_text(HEALTH_SERVER, encoding="utf-8")
    spec = ProcSpec(
        name="mozak",
        cmd=[sys.executable, str(script), str(port), "sick"],
        health={"kind": "http", "url": f"http://127.0.0.1:{port}/health"},
    )
    sup = Supervisor(cfg(tmp_path, [spec]))
    fast_restarts(sup)
    try:
        assert run_until(sup, lambda: sup.procs[0].restarts >= 1)
        assert any(e["kind"] == "unhealthy_restart" for e in events(tmp_path))
    finally:
        sup.stop_all()
    assert sup.procs[0].proc.poll() is not None  # no orphan left


def test_crash_test_measures_recovery(tmp_path):
    port = free_port()
    script = tmp_path / "srv.py"
    script.write_text(HEALTH_SERVER, encoding="utf-8")
    spec = ProcSpec(
        name="mozak",
        cmd=[sys.executable, str(script), str(port), "ok"],
        health={"kind": "brain", "url": f"http://127.0.0.1:{port}/health"},
    )
    sup = Supervisor(cfg(tmp_path, [spec]))
    fast_restarts(sup)
    stop = threading.Event()
    loop = threading.Thread(target=sup.run, args=(stop, 0.05), daemon=True)
    loop.start()
    try:
        res = crash_test(sup, stop, timeout_s=20)
    finally:
        stop.set()
        loop.join(10)
    assert res[0]["name"] == "mozak" and res[0]["ok"] is True
    assert res[0]["recovery_s"] < 10
    assert sup.procs[0].restarts == 1
    assert sup.procs[0].proc.poll() is not None


def test_status_file_check(tmp_path):
    p = tmp_path / "status.json"
    h = {"kind": "status_file", "path": str(p), "max_age_s": 30}
    assert check_health(h).restart is True  # missing
    p.write_text(json.dumps({"t": time.time(), "source": "running", "events_in": 4}))
    assert check_health(h).state == OK
    p.write_text(json.dumps({"t": time.time() - 100, "source": "running"}))
    c = check_health(h)
    assert c.restart is True and ("100" in c.detail or "99" in c.detail)
    p.write_text(json.dumps({"t": time.time(), "source": "stopped:QuotaExhausted"}))
    assert check_health(h).state == WARN


def test_olv_check_page_and_stuck(tmp_path):
    from http.server import BaseHTTPRequestHandler

    answers = []

    class Olv(BaseHTTPRequestHandler):
        def log_message(self, *a):
            pass

        def do_GET(self):
            body = json.dumps(answers.pop(0)).encode()
            self.send_response(200)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    srv = ThreadingHTTPServer(("127.0.0.1", 0), Olv)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    h = {
        "kind": "olv",
        "url": f"http://127.0.0.1:{srv.server_port}/inject-status",
        "stuck_s": 60,
    }
    answers[:] = [
        {"ok": True, "clients": 0, "busy": False, "busy_for_s": 0},
        {"ok": True, "clients": 1, "busy": True, "busy_for_s": 12},
        {"ok": True, "clients": 1, "busy": True, "busy_for_s": 200},
    ]
    a, b, c = check_health(h), check_health(h), check_health(h)
    assert a.state == WARN and not a.restart
    assert b.state == OK
    assert c.restart and "zapelo" in c.detail
    srv.shutdown()


def test_status_page_and_overlay(tmp_path):
    sup = Supervisor(cfg(tmp_path, [], difficulty_sound=""))
    srv = ThreadingHTTPServer(("127.0.0.1", 0), make_handler(sup))
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    base = f"http://127.0.0.1:{srv.server_port}"
    st = json.loads(urllib.request.urlopen(base + "/status.json").read())
    assert (
        st["difficulty"] is False and "umjetna inteligencija" in st["overlay"]["label"]
    )
    ov = urllib.request.urlopen(base + "/overlay").read().decode("utf-8")
    assert "<b>AI</b>" in ov
    page = urllib.request.urlopen(base + "/").read().decode("utf-8")
    assert "Mila: nadzor" in page
    srv.shutdown()
