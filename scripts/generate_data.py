"""Generate byte-stable seed JSONL; --all also rebuilds the small companion fixtures."""

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from services.producer.events import (  # noqa: E402
    SAMPLE,
    edge_cases,
    live_events,
    question_cases,
    seed_events,
    write_jsonl,
)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--n", type=int, default=400)
    parser.add_argument("--out", type=Path, default=SAMPLE / "seed_events.jsonl")
    parser.add_argument("--all", action="store_true", help="Write companion files beside --out")
    args = parser.parse_args()
    if args.n < 1 or (args.all and args.n < 10):
        parser.error("n must be >= 1 (>= 10 with --all)")
    write_jsonl(args.out, seed_events(args.seed, args.n))
    if args.all:
        folder = args.out.parent
        write_jsonl(folder / "fixtures.jsonl", seed_events(args.seed, 10))
        write_jsonl(folder / "live_events.jsonl", live_events(args.seed))
        write_jsonl(folder / "test_cases.jsonl", question_cases(args.seed))
        lines, expectations = edge_cases(args.seed)
        (folder / "edge_cases.jsonl").write_text(
            "\n".join(lines) + "\n", encoding="utf-8", newline="\n"
        )
        (folder / "edge_case_expectations.json").write_text(
            json.dumps(expectations, indent=2) + "\n", encoding="utf-8", newline="\n"
        )
    print(f"generated {args.n} seed events: {args.out}")


if __name__ == "__main__":
    main()
