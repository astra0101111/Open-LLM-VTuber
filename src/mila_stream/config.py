"""Configuration for the stream layer: one YAML file, mila_stream.yaml by default.

Secrets never go in the file: the YouTube API key and the inject token are read from
environment variables whose names the file gives.
"""

from __future__ import annotations

from dataclasses import dataclass, field, fields
from pathlib import Path

import yaml

from .filters import FilterConfig
from .selector import QueueConfig
from .sim import SimConfig
from .sink import OlvConfig
from .youtube import YouTubeConfig


@dataclass
class StreamConfig:
    platform: str = "youtube"  # youtube | sim
    youtube: YouTubeConfig = field(default_factory=YouTubeConfig)
    olv: OlvConfig = field(default_factory=OlvConfig)
    queue: QueueConfig = field(default_factory=QueueConfig)
    filter: FilterConfig = field(default_factory=FilterConfig)
    sim: SimConfig = field(default_factory=SimConfig)
    log_dir: str = "logs/mila_stream"
    state_file: str = "cache/mila_stream_state.json"
    tick_s: float = 0.25
    status_file: str = "cache/mila_stream_status.json"  # read by the supervisor
    status_every_s: float = 2.0
    paid_ledger_file: str = "cache/mila_paid_open.json"
    dead_letter_file: str = "logs/mila_dead_letters.jsonl"
    olv_stuck_s: float = 120.0  # one answer running longer than this is logged as stuck
    supervisor: dict = field(default_factory=dict)  # read by supervisor.py
    brain: dict = field(default_factory=dict)  # read by brain_proxy.py


def _fill(cls, data: dict):
    names = {f.name for f in fields(cls)}
    unknown = set(data) - names
    if unknown:
        raise ValueError(f"{cls.__name__}: unknown keys {sorted(unknown)}")
    kwargs = {}
    for k, v in data.items():
        if k == "names" and isinstance(v, list):
            v = tuple(v)
        kwargs[k] = v
    return cls(**kwargs)


def load_config(path: str | Path | None) -> StreamConfig:
    if path is None or not Path(path).exists():
        return StreamConfig()
    raw = yaml.safe_load(Path(path).read_text(encoding="utf-8")) or {}
    sub = {
        "youtube": YouTubeConfig,
        "olv": OlvConfig,
        "queue": QueueConfig,
        "filter": FilterConfig,
        "sim": SimConfig,
    }
    kwargs = {}
    for k, v in raw.items():
        if k in sub:
            kwargs[k] = _fill(sub[k], v or {})
        else:
            kwargs[k] = v
    unknown = set(kwargs) - {f.name for f in fields(StreamConfig)}
    if unknown:
        raise ValueError(f"mila_stream: unknown keys {sorted(unknown)}")
    return StreamConfig(**kwargs)
