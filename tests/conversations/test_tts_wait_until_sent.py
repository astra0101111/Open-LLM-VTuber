"""wait_until_sent: backend-synth-complete must not be written while audio
payloads are still being sent (legacy websockets raises AssertionError in
_drain_helper on concurrent writes). Runs without the app's heavy dependencies:
the modules tts_manager imports are stubbed."""

import asyncio
import importlib.util
import sys
import types
from pathlib import Path

SRC = Path(__file__).resolve().parents[2] / "src" / "open_llm_vtuber"


def _load_tts_manager():
    def mod(name, **attrs):
        m = types.ModuleType(name)
        m.__path__ = []
        for k, v in attrs.items():
            setattr(m, k, v)
        sys.modules[name] = m
        return m

    class _Log:
        def __getattr__(self, _):
            return lambda *a, **k: None

    saved = {
        k: sys.modules.get(k)
        for k in [
            "loguru",
            "open_llm_vtuber",
            "open_llm_vtuber.conversations",
            "open_llm_vtuber.agent",
            "open_llm_vtuber.agent.output_types",
            "open_llm_vtuber.live2d_model",
            "open_llm_vtuber.tts",
            "open_llm_vtuber.tts.tts_interface",
            "open_llm_vtuber.utils",
            "open_llm_vtuber.utils.stream_audio",
            "open_llm_vtuber.conversations.types",
        ]
    }
    mod("loguru", logger=_Log())
    mod("open_llm_vtuber")
    mod("open_llm_vtuber.conversations")
    mod("open_llm_vtuber.agent")
    mod("open_llm_vtuber.agent.output_types", DisplayText=object, Actions=object)
    mod("open_llm_vtuber.live2d_model", Live2dModel=object)
    mod("open_llm_vtuber.tts")
    mod("open_llm_vtuber.tts.tts_interface", TTSInterface=object)
    mod("open_llm_vtuber.utils")
    mod(
        "open_llm_vtuber.utils.stream_audio",
        prepare_audio_payload=lambda audio_path=None, **k: {
            "type": "audio",
            "audio": audio_path,
        },
    )
    mod("open_llm_vtuber.conversations.types", WebSocketSend=object)
    spec = importlib.util.spec_from_file_location(
        "open_llm_vtuber.conversations.tts_manager",
        SRC / "conversations" / "tts_manager.py",
    )
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    for k, v in saved.items():
        if v is None:
            sys.modules.pop(k, None)
        else:
            sys.modules[k] = v
    return m


class _Engine:
    async def async_generate_audio(self, text, file_name_no_ext=None):
        await asyncio.sleep(0.001)  # fast cloud TTS: all sentences ready at once
        return f"/tmp/{text}.wav"

    def remove_file(self, *a, **k):
        pass


class _Socket:
    """Records overlapping writes, like the legacy websockets drain assertion."""

    def __init__(self):
        self.inflight = 0
        self.overlaps = 0
        self.sent = []

    async def send(self, data):
        self.inflight += 1
        if self.inflight > 1:
            self.overlaps += 1
        await asyncio.sleep(0.02)  # big audio payload, transport paused
        self.sent.append(data)
        self.inflight -= 1


class _Display:
    name = "Mila"


async def _turn(mgr, ws, use_new):
    for i in range(5):
        await mgr.speak(
            f"s{i}",
            display_text=_Display(),
            actions=None,
            live2d_model=None,
            tts_engine=_Engine(),
            websocket_send=ws.send,
        )
    if use_new:
        await mgr.wait_until_sent()
    else:
        await asyncio.gather(*mgr.task_list)
    await ws.send('{"type": "backend-synth-complete"}')


def test_old_behaviour_overlaps():
    tm = _load_tts_manager()
    ws = _Socket()
    asyncio.run(_turn(tm.TTSTaskManager(), ws, use_new=False))
    assert ws.overlaps > 0


def test_wait_until_sent_no_overlap_and_complete_is_last():
    tm = _load_tts_manager()
    ws = _Socket()
    asyncio.run(_turn(tm.TTSTaskManager(), ws, use_new=True))
    assert ws.overlaps == 0
    assert len(ws.sent) == 6
    assert ws.sent[-1] == '{"type": "backend-synth-complete"}'
    assert [s for s in ws.sent[:-1]] == [
        f'{{"type": "audio", "audio": "/tmp/s{i}.wav"}}' for i in range(5)
    ]
