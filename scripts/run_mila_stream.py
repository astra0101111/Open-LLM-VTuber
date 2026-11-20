"""Start Mila's stream layer next to a running Open-LLM-VTuber server.

    uv run python scripts/run_mila_stream.py --config mila_stream.yaml
    uv run python scripts/run_mila_stream.py --sim --dry-run

Needs MILA_INJECT_TOKEN (same value as the server's) and, for YouTube,
MILA_YOUTUBE_API_KEY in the environment.
"""

import os
import sys

sys.path.insert(
    0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "src")
)

from mila_stream.__main__ import main

if __name__ == "__main__":
    raise SystemExit(main())
