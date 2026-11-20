import ast
import asyncio
import logging
import types
from pathlib import Path

from mila_stream import compaction as C

AGENT = (
    Path(__file__).resolve().parents[2]
    / "src/open_llm_vtuber/agent/agents/basic_memory_agent.py"
)


def conv(n, size=200):
    out = []
    for i in range(n):
        out.append(
            {
                "role": "user",
                "content": f"[YouTube chat] Gledatelj{i} piše: «" + "x" * size + "»",
            }
        )
        out.append({"role": "assistant", "content": f"Odgovor {i} " + "y" * size})
    return out


def test_pick_cut_keeps_user_start_and_tail():
    m = conv(20)
    k = C.pick_cut(m)
    assert 2 <= k < len(m) - 6 and m[k]["role"] == "user"
    assert (
        abs(
            sum(len(x["content"]) for x in m[:k]) / sum(len(x["content"]) for x in m)
            - 0.5
        )
        < 0.1
    )
    assert C.pick_cut(conv(3)) == 0


def test_transcript_and_summary_message():
    t = C.transcript(
        [
            {"role": "user", "content": "bok"},
            {"role": "assistant", "content": "hej"},
            C.summary_message("- stara bilješka"),
        ]
    )
    assert t.splitlines() == [
        "Chat: bok",
        "Mila: hej",
        C.SUMMARY_TAG,
        "- stara bilješka",
    ]
    assert C.summary_request([{"role": "user", "content": "bok"}])[0][
        "content"
    ].startswith(C.SUMMARY_INSTRUCTION)


def _agent_methods():
    """Load the two compaction methods from basic_memory_agent.py without importing OLLV."""
    tree = ast.parse(AGENT.read_text(encoding="utf-8"))
    cls = next(
        n
        for n in tree.body
        if isinstance(n, ast.ClassDef) and n.name == "BasicMemoryAgent"
    )
    fns = [
        n
        for n in cls.body
        if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))
        and n.name in ("_maybe_compact_history", "_compact_history")
    ]
    ns = {"asyncio": asyncio, "_compaction": C, "logger": logging.getLogger("t")}
    exec(
        compile(
            ast.fix_missing_locations(ast.Module(body=fns, type_ignores=[])),
            str(AGENT),
            "exec",
        ),
        ns,
    )
    return ns["_maybe_compact_history"], ns["_compact_history"]


def test_agent_compacts_in_background_once():
    maybe, compact = _agent_methods()

    class FakeLLM:
        calls = 0

        async def chat_completion(self, messages, system=None):
            FakeLLM.calls += 1
            assert C.SUMMARY_INSTRUCTION in messages[0]["content"]
            for t in ["- Gledatelj0 ", "pita za igre"]:
                yield t

    agent = types.SimpleNamespace(
        _memory=conv(40),
        _compact_after_tokens=2000,
        _compaction_task=None,
        _llm=FakeLLM(),
    )
    agent._compact_history = lambda: compact(agent)
    before = len(agent._memory)

    async def run():
        maybe(agent)
        assert agent._compaction_task is not None
        maybe(agent)  # already running: no second task
        await agent._compaction_task

    asyncio.run(run())
    assert FakeLLM.calls == 1
    assert agent._memory[0]["content"].startswith(C.SUMMARY_TAG)
    assert "pita za igre" in agent._memory[0]["content"]
    assert len(agent._memory) < before and agent._memory[1]["role"] == "user"
    assert agent._compaction_task is None


def test_agent_keeps_history_if_summary_fails():
    maybe, compact = _agent_methods()

    class BadLLM:
        async def chat_completion(self, messages, system=None):
            raise RuntimeError("429")
            yield ""

    mem = conv(40)
    agent = types.SimpleNamespace(
        _memory=list(mem),
        _compact_after_tokens=2000,
        _compaction_task=None,
        _llm=BadLLM(),
    )
    agent._compact_history = lambda: compact(agent)

    async def run():
        maybe(agent)
        await agent._compaction_task

    asyncio.run(run())
    assert agent._memory == mem and agent._compaction_task is None
