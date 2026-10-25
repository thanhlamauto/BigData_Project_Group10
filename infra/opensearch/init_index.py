"""Idempotent shared index bootstrap; run from the repository root."""

import os
import sys
from pathlib import Path

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from services.indexer.upsert import initialize  # noqa: E402

if __name__ == "__main__":
    with httpx.Client(base_url=os.getenv("OPENSEARCH_URL", "http://localhost:9200"), timeout=30) as client:
        index = os.getenv("OPENSEARCH_INDEX", "streamrag-docs-v1")
        initialize(client, index, int(os.getenv("EMBEDDING_DIM", "384")))
        print(f"ready index={index}")
