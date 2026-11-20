"""python -m mila_stream [--config mila_stream.yaml] [--sim] [--dry-run]"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging

from .config import load_config
from .runner import run


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="mila_stream", description=__doc__)
    ap.add_argument("--config", default="mila_stream.yaml")
    ap.add_argument(
        "--sim", action="store_true", help="simulated Croatian chat instead of YouTube"
    )
    ap.add_argument(
        "--dry-run", action="store_true", help="print what Mila would get; send nothing"
    )
    args = ap.parse_args(argv)
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    cfg = load_config(args.config)
    if args.sim:
        cfg.platform = "sim"
    counts = asyncio.run(run(cfg, dry_run=args.dry_run))
    print(json.dumps(counts, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
