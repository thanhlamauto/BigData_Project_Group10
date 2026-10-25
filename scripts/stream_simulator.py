"""Publish new live events, a known update, or explicit edge-case traffic."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from services.producer.cli import run  # noqa: E402

if __name__ == "__main__":
    raise SystemExit(run(simulator=True))
