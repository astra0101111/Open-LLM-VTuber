"""Start Mila's supervisor: it starts the brain connector, Open-LLM-VTuber and the
stream layer from the `supervisor` section of mila_stream.yaml, checks them every
30 s, restarts what fails, and serves the status page and the OBS overlay.

    uv run python scripts/run_mila_supervisor.py --config mila_stream.yaml
    uv run python scripts/run_mila_supervisor.py --config mila_stream.yaml --crash-test

Secrets (MILA_INJECT_TOKEN, MILA_YOUTUBE_API_KEY, brain keys) come from the
environment and are passed on to the processes; they are never written to logs.
"""

import os
import sys

sys.path.insert(
    0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "src")
)

from mila_stream.supervisor import main

if __name__ == "__main__":
    raise SystemExit(main())
