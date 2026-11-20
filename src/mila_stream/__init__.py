"""Mila stream layer: reads live-stream chat, filters it, picks what Mila answers
and sends it into Open-LLM-VTuber through the /inject-text route.

It runs as its own process next to the OLLV server (see scripts/run_mila_stream.py),
so the stream logic survives changes to the core.
"""

__version__ = "0.1.0"
