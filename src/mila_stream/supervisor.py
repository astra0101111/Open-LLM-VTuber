"""Supervisor (plan v0.2, part 4): starts Mila's processes, checks that each one really
works every check_every_s seconds, restarts what failed, and shows the operator a status
page and OBS an overlay with the AI label (part 5) and a "technical difficulties"
banner.

What it checks (a process existing is not enough):
- olv: /inject-status answers, a page with Mila is connected, and one answer has not
  been running longer than stuck_s (voice or page never reported the end of playback);
- brain: /health of brain_proxy answers; upstream failures are shown, not restarted
  (restarting the connector does not fix a provider that is down);
- status_file: the chat runner rewrites its status file every few seconds;
- http: any URL answers 200.

It only ever stops or kills processes it started itself.

Run: python -m mila_stream.supervisor --config mila_stream.yaml
Status page: http://127.0.0.1:8090/   OBS overlay: http://127.0.0.1:8090/overlay
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import signal
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import yaml

log = logging.getLogger("mila_stream.supervisor")

ASSETS = Path(__file__).with_name("assets")
WINDOWS = os.name == "nt"

OK, WARN, FAIL = "ok", "upozorenje", "greska"


@dataclass
class ProcSpec:
    name: str
    cmd: list
    cwd: str = "."
    env: dict = field(default_factory=dict)  # extra, non-secret variables only
    health: dict = field(default_factory=dict)
    critical: bool = True  # shown as technical difficulties when down
    restart_on_clean_exit: bool = False  # exit code 0 = finished (e.g. chat ended)
    start_delay_s: float = 0.0


@dataclass
class SupervisorConfig:
    host: str = "127.0.0.1"
    port: int = 8090
    check_every_s: float = 30.0
    fail_after: int = 2
    start_grace_s: float = 60.0
    max_restarts: int = 5
    restart_window_s: float = 600.0
    stop_timeout_s: float = 10.0
    log_dir: str = "logs/supervisor"
    max_log_mb: float = 20.0
    overlay_label: str = (
        "AI · Mila je virtualni lik kojim upravlja umjetna inteligencija"
    )
    difficulty_text: str = "Tehničke poteškoće, vraćamo se odmah"
    difficulty_after_s: float = 5.0  # banner only for problems that last this long
    difficulty_sound: str = ""  # optional audio file played once per incident
    processes: list = field(default_factory=list)


def load_supervisor_config(path: str | Path) -> SupervisorConfig:
    raw = yaml.safe_load(Path(path).read_text(encoding="utf-8")) or {}
    s = dict(raw.get("supervisor") or {})
    procs = [ProcSpec(**p) for p in s.pop("processes", [])]
    cfg = SupervisorConfig(**s)
    cfg.processes = procs
    return cfg


# -- health checks --------------------------------------------------------------


@dataclass
class Check:
    state: str
    detail: str
    restart: bool = False  # a restart of this process can fix it


def _get_json(url: str, headers: dict | None = None, timeout: float = 5.0):
    req = urllib.request.Request(url, headers=headers or {})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        body = r.read()
        return r.status, json.loads(body) if body else None


def check_health(h: dict, now: float | None = None) -> Check:
    kind = h.get("kind", "")
    now = time.time() if now is None else now
    try:
        if kind == "olv":
            token = os.environ.get(h.get("token_env", "MILA_INJECT_TOKEN"), "")
            status, d = _get_json(h["url"], {"x-mila-token": token})
            if status != 200 or not isinstance(d, dict):
                return Check(FAIL, f"HTTP {status}", True)
            if int(d.get("clients", 0)) == 0:
                return Check(WARN, "stranica s Milom nije spojena")
            busy_for = float(d.get("busy_for_s") or 0)
            if busy_for > float(h.get("stuck_s", 180)):
                return Check(FAIL, f"odgovor traje {busy_for:.0f} s (zapelo)", True)
            return Check(OK, "govori" if d.get("busy") else "spremna")
        if kind == "brain":
            status, d = _get_json(h["url"])
            if not isinstance(d, dict):
                return Check(FAIL, f"HTTP {status}", True)
            if not d.get("ok", True):
                return Check(
                    WARN,
                    f"mozak ne odgovara ({d.get('failures_in_row')} zaredom): "
                    f"{d.get('last_error', '')}",
                )
            cooling = [u["name"] for u in d.get("upstreams", []) if u.get("cooling_s")]
            if cooling:
                return Check(OK, "radi; na pričuvi, odmara: " + ", ".join(cooling))
            return Check(OK, f"radi ({d.get('requests', 0)} upita)")
        if kind == "status_file":
            p = Path(h["path"])
            if not p.exists():
                return Check(FAIL, "nema statusne datoteke", True)
            d = json.loads(p.read_text(encoding="utf-8"))
            age = now - float(d.get("t", 0))
            if age > float(h.get("max_age_s", 30)):
                return Check(FAIL, f"status star {age:.0f} s", True)
            src = str(d.get("source", ""))
            if src.startswith("stopped"):
                return Check(WARN, f"čitanje chata stalo: {src}")
            return Check(OK, f"chat radi, primljeno {d.get('events_in', 0)}")
        if kind == "http":
            status, _ = _get_json(h["url"])
            return (
                Check(OK, "radi")
                if status == 200
                else Check(FAIL, f"HTTP {status}", True)
            )
    except urllib.error.HTTPError as e:
        return Check(FAIL, f"HTTP {e.code}", True)
    except Exception as e:  # refused, timeout, bad JSON
        return Check(FAIL, f"ne odgovara: {type(e).__name__}", True)
    return Check(OK, "bez provjere")


# -- one supervised process -----------------------------------------------------


class Managed:
    def __init__(self, spec: ProcSpec, cfg: SupervisorConfig, events):
        self.spec = spec
        self.cfg = cfg
        self.events = events
        self.proc: subprocess.Popen | None = None
        self.state = "ceka"  # ceka, radi, ponovno, zavrsio, odustao, zaustavljen
        self.started_at = 0.0
        self.restart_times: list[float] = []
        self.restarts = 0
        self.next_start = 0.0
        self.fails = 0
        self.check: Check | None = None
        self.check_at = 0.0
        self.next_check = 0.0
        self.problem_since: float | None = None
        self._log_fh = None

    # start / stop

    def _cmd(self) -> list:
        cmd = [str(c) for c in self.spec.cmd]
        if cmd and cmd[0] in ("python", "python3"):
            cmd[0] = sys.executable
        return cmd

    def _open_log(self):
        d = Path(self.cfg.log_dir)
        d.mkdir(parents=True, exist_ok=True)
        p = d / f"{self.spec.name}.log"
        if p.exists() and p.stat().st_size > self.cfg.max_log_mb * 1e6:
            os.replace(p, p.with_suffix(".log.1"))
        return open(p, "ab")

    def start(self, now: float) -> None:
        env = {**os.environ, **{k: str(v) for k, v in self.spec.env.items()}}
        env.setdefault("PYTHONUNBUFFERED", "1")
        # child output goes to a log file; on Windows the default code page cannot
        # write č, ć or đ and the child would crash on its first Croatian line
        env.setdefault("PYTHONIOENCODING", "utf-8")
        self._log_fh = self._open_log()
        kw = {}
        if WINDOWS:
            kw["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP
        self.proc = subprocess.Popen(
            self._cmd(),
            cwd=self.spec.cwd,
            env=env,
            stdout=self._log_fh,
            stderr=subprocess.STDOUT,
            stdin=subprocess.DEVNULL,
            **kw,
        )
        self.state = "radi"
        self.started_at = now
        self.fails = 0
        self.check = None
        self.next_check = now + min(2.0, self.cfg.check_every_s)
        self.events("start", name=self.spec.name, pid=self.proc.pid)

    def stop(self, reason: str) -> None:
        p = self.proc
        if p is None or p.poll() is not None:
            self._close_log()
            return
        try:
            if WINDOWS:
                p.terminate()
            else:
                p.send_signal(signal.SIGINT)  # lets Python code run its cleanup
            p.wait(timeout=self.cfg.stop_timeout_s)
        except subprocess.TimeoutExpired:
            self.kill()
        except OSError:
            pass
        self.events("stop", name=self.spec.name, reason=reason, code=p.poll())
        self._close_log()

    def kill(self) -> None:
        """Hard kill of this process and its children (only processes we started)."""
        p = self.proc
        if p is None or p.poll() is not None:
            return
        if WINDOWS:
            subprocess.run(
                ["taskkill", "/T", "/F", "/PID", str(p.pid)],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
        else:
            p.kill()
        try:
            p.wait(timeout=5)
        except subprocess.TimeoutExpired:
            pass

    def _close_log(self):
        if self._log_fh:
            self._log_fh.close()
            self._log_fh = None

    # one step of the loop

    def _schedule_restart(self, now: float, why: str) -> None:
        self.restart_times = [
            t for t in self.restart_times if now - t < self.cfg.restart_window_s
        ]
        if len(self.restart_times) >= self.cfg.max_restarts:
            self.state = "odustao"
            self.events(
                "gave_up",
                name=self.spec.name,
                reason=why,
                restarts_in_window=len(self.restart_times),
            )
            return
        n = len(self.restart_times)
        self.restart_times.append(now)
        self.restarts += 1
        self.state = "ponovno"
        self.next_start = now + min(60.0, 2.0 * (2**n))
        self.events(
            "restart_scheduled",
            name=self.spec.name,
            reason=why,
            in_s=self.next_start - now,
        )

    def tick(self, now: float) -> None:
        if self.state in ("odustao", "zavrsio", "zaustavljen"):
            return
        if self.state == "ceka":
            if now >= self.next_start:
                self.start(now)
            return
        if self.state == "ponovno":
            if now >= self.next_start:
                self.start(now)
            return
        code = self.proc.poll() if self.proc else None
        if code is not None:
            self._close_log()
            self.events("exit", name=self.spec.name, code=code)
            if code == 0 and not self.spec.restart_on_clean_exit:
                self.state = "zavrsio"
                return
            self._schedule_restart(now, f"izašao s kodom {code}")
            return
        h = self.spec.health
        if h and now >= self.next_check:
            c = check_health(h, now)
            prev = self.check.state if self.check else None
            self.check, self.check_at = c, now
            # while starting up: check often, but do not count failures yet
            in_grace = now - self.started_at < self.cfg.start_grace_s
            fast = in_grace and c.state != OK
            self.next_check = now + (
                min(2.0, self.cfg.check_every_s) if fast else self.cfg.check_every_s
            )
            if prev != c.state:
                self.events(
                    "health", name=self.spec.name, state=c.state, detail=c.detail
                )
            if in_grace:
                self.fails = 0
            elif c.state == FAIL and c.restart:
                self.fails += 1
                if self.fails >= self.cfg.fail_after:
                    self.events(
                        "unhealthy_restart", name=self.spec.name, detail=c.detail
                    )
                    self.stop("ne radi: " + c.detail)
                    self._schedule_restart(now, c.detail)
            else:
                self.fails = 0

    def problem(self) -> str:
        """Non-empty when this process is a reason for the difficulties banner."""
        if self.state in ("ponovno", "odustao"):
            return f"{self.spec.name}: {self.state}"
        if self.state == "radi" and self.check and self.check.state != OK:
            return f"{self.spec.name}: {self.check.detail}"
        return ""

    def snapshot(self, now: float) -> dict:
        return {
            "name": self.spec.name,
            "state": self.state,
            "pid": self.proc.pid if self.proc and self.state == "radi" else None,
            "uptime_s": round(now - self.started_at) if self.state == "radi" else None,
            "restarts": self.restarts,
            "check": {
                "state": self.check.state,
                "detail": self.check.detail,
                "age_s": round(now - self.check_at, 1),
            }
            if self.check
            else None,
            "critical": self.spec.critical,
        }


# -- the supervisor -------------------------------------------------------------


class Supervisor:
    def __init__(self, cfg: SupervisorConfig, clock=time.time):
        self.cfg = cfg
        self.clock = clock
        self.lock = threading.Lock()
        Path(cfg.log_dir).mkdir(parents=True, exist_ok=True)
        self.events_path = Path(cfg.log_dir) / "events.jsonl"
        self.procs = [Managed(s, cfg, self.event) for s in cfg.processes]
        t0 = clock()
        for m in self.procs:
            m.next_start = t0 + m.spec.start_delay_s
        self.difficulty_since: float | None = None
        self.incident = 0
        self.stopping = False

    def event(self, kind: str, **data) -> None:
        rec = {"t": round(self.clock(), 3), "kind": kind, **data}
        log.info("%s %s", kind, data)
        with open(self.events_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")

    def tick(self) -> None:
        now = self.clock()
        with self.lock:
            for m in self.procs:
                m.tick(now)
            problems = [m.problem() for m in self.procs if m.spec.critical]
            if any(problems):
                if self.difficulty_since is None:
                    self.difficulty_since = now
                    self.incident += 1
            else:
                self.difficulty_since = None

    def status(self) -> dict:
        now = self.clock()
        with self.lock:
            reasons = [
                p for p in (m.problem() for m in self.procs if m.spec.critical) if p
            ]
            shown = (
                self.difficulty_since is not None
                and now - self.difficulty_since >= self.cfg.difficulty_after_s
            )
            stream = None
            for m in self.procs:
                if m.spec.health.get("kind") == "status_file":
                    try:
                        stream = json.loads(
                            Path(m.spec.health["path"]).read_text(encoding="utf-8")
                        )
                    except (OSError, ValueError):
                        stream = None
            return {
                "t": round(now, 3),
                "difficulty": shown,
                "incident": self.incident,
                "reasons": reasons,
                "processes": [m.snapshot(now) for m in self.procs],
                "stream": stream,
                "overlay": {
                    "label": self.cfg.overlay_label,
                    "difficulty_text": self.cfg.difficulty_text,
                    "sound": bool(
                        self.cfg.difficulty_sound
                        and Path(self.cfg.difficulty_sound).exists()
                    ),
                },
            }

    def all_ok(self) -> bool:
        with self.lock:
            return all(
                m.state == "radi"
                and (not m.spec.health or (m.check and m.check.state == OK))
                for m in self.procs
            )

    def stop_all(self) -> None:
        self.stopping = True
        with self.lock:
            for m in reversed(self.procs):
                m.stop("supervisor se gasi")
                m.state = "zaustavljen"

    def run(self, stop_event: threading.Event, step_s: float = 0.5) -> None:
        self.event("supervisor_start", processes=[m.spec.name for m in self.procs])
        try:
            while not stop_event.is_set():
                self.tick()
                stop_event.wait(step_s)
        finally:
            self.stop_all()
            self.event("supervisor_stop")


# -- crash test (plan: each part is killed on purpose, recovery time is measured) --


def crash_test(
    sup: Supervisor, stop_event: threading.Event, timeout_s: float = 180.0
) -> list:
    """Kill each supervised process in turn (only ours) and time until it is healthy."""
    results = []
    deadline = time.time() + timeout_s
    while not sup.all_ok() and time.time() < deadline and not stop_event.is_set():
        time.sleep(1)
    if not sup.all_ok():
        return [{"name": "*", "ok": False, "detail": "nije sve zdravo prije testa"}]
    for m in sup.procs:
        if stop_event.is_set():
            break
        t0 = time.time()
        sup.event("crash_test_kill", name=m.spec.name)
        with sup.lock:
            m.kill()
        # first wait for the supervisor to notice, then for health to come back
        while time.time() - t0 < timeout_s and not stop_event.is_set():
            time.sleep(0.5)
            with sup.lock:
                back = (
                    m.state == "radi"
                    and m.started_at > t0
                    and (not m.spec.health or (m.check and m.check.state == OK))
                )
            if back:
                break
        took = round(time.time() - t0, 1)
        ok = took < timeout_s
        results.append({"name": m.spec.name, "ok": ok, "recovery_s": took})
        sup.event("crash_test_result", name=m.spec.name, ok=ok, recovery_s=took)
    return results


# -- HTTP: status page, JSON, overlay -------------------------------------------


def make_handler(sup: Supervisor):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *a):
            pass

        def _send(self, code: int, body: bytes, ctype: str):
            self.send_response(code)
            self.send_header("Content-Type", ctype)
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self):
            path = self.path.split("?")[0].rstrip("/") or "/"
            if path == "/status.json":
                body = json.dumps(sup.status(), ensure_ascii=False).encode("utf-8")
                return self._send(200, body, "application/json; charset=utf-8")
            if path in ("/", "/overlay"):
                name = "status.html" if path == "/" else "overlay.html"
                return self._send(
                    200, (ASSETS / name).read_bytes(), "text/html; charset=utf-8"
                )
            if path == "/overlay/zvuk" and sup.cfg.difficulty_sound:
                p = Path(sup.cfg.difficulty_sound)
                if p.exists():
                    ctype = "audio/mpeg" if p.suffix.lower() == ".mp3" else "audio/wav"
                    return self._send(200, p.read_bytes(), ctype)
            self._send(404, b"not found", "text/plain")

    return Handler


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="mila_stream.supervisor")
    ap.add_argument("--config", default="mila_stream.yaml")
    ap.add_argument(
        "--crash-test",
        action="store_true",
        help="kill each supervised process once and measure recovery; then stop",
    )
    a = ap.parse_args(argv)
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    cfg = load_supervisor_config(a.config)
    if not cfg.processes:
        print("no supervisor.processes in config")
        return 2
    sup = Supervisor(cfg)
    srv = ThreadingHTTPServer((cfg.host, cfg.port), make_handler(sup))
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    print(
        f"status: http://{cfg.host}:{cfg.port}/  overlay: http://{cfg.host}:{cfg.port}/overlay",
        flush=True,
    )
    stop = threading.Event()
    signal.signal(signal.SIGINT, lambda *_: stop.set())
    if hasattr(signal, "SIGBREAK"):
        signal.signal(signal.SIGBREAK, lambda *_: stop.set())
    loop = threading.Thread(target=sup.run, args=(stop,), daemon=True)
    loop.start()
    code = 0
    if a.crash_test:
        res = crash_test(sup, stop)
        out = Path(cfg.log_dir) / f"crash-test-{datetime.now():%Y-%m-%d-%H%M}.json"
        out.write_text(json.dumps(res, ensure_ascii=False, indent=1), encoding="utf-8")
        print(json.dumps(res, ensure_ascii=False, indent=1))
        code = 0 if res and all(r["ok"] for r in res) else 1
        stop.set()
    else:
        while not stop.is_set():
            stop.wait(1)
    loop.join(timeout=cfg.stop_timeout_s * (len(cfg.processes) + 1))
    srv.shutdown()
    return code


if __name__ == "__main__":
    raise SystemExit(main())
