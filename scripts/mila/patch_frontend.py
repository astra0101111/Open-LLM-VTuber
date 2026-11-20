"""Mouth threshold for PSD2Live models in the prebuilt OLLV web frontend.

PSD2Live shows closed lips only below 0.05 mouth openness and squeezes the open mouth
into a thin line just above it. The frontend computes openness from audio loudness, so
quiet parts of speech fell into that range. This inserts, right after
`_=Math.min(1,_*1.5)` in frontend/assets/main-*.js:

    ,_=_<0.04?0:Math.max(0.12,_)

so the mouth is either closed or at least 0.48 open (x4 in the model). Verified on
2026-11-20 with the k6f model: 0 thin-line frames out of 80 (15 before).

Idempotent; keeps the original as <file>.izvorno; edits bytes, so line endings stay.
Usage: python scripts/mila/patch_frontend.py [--check] [--frontend frontend]
"""

import argparse
import shutil
import sys
from pathlib import Path

ANCHOR = b"_=Math.min(1,_*1.5)"
INSERT = b",_=_<0.04?0:Math.max(0.12,_)"


def patch(frontend: Path, check: bool = False) -> int:
    files = sorted((frontend / "assets").glob("main-*.js"))
    if not files:
        print(f"no main-*.js in {frontend / 'assets'}")
        return 2
    rc = 0
    for f in files:
        data = f.read_bytes()
        if ANCHOR + INSERT in data:
            print(f"{f.name}: already patched")
            continue
        n = data.count(ANCHOR)
        if n != 1:
            print(f"{f.name}: anchor found {n} times, expected 1; not touching it")
            rc = 1
            continue
        if check:
            print(f"{f.name}: would patch")
            continue
        backup = f.with_name(f.name + ".izvorno")
        if not backup.exists():
            shutil.copy2(f, backup)
        f.write_bytes(data.replace(ANCHOR, ANCHOR + INSERT, 1))
        print(f"{f.name}: patched (+{len(INSERT)} bytes), original in {backup.name}")
    return rc


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--frontend", default=str(Path(__file__).resolve().parents[2] / "frontend")
    )
    ap.add_argument("--check", action="store_true")
    a = ap.parse_args()
    sys.exit(patch(Path(a.frontend), a.check))
