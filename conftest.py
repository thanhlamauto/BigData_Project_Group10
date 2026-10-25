"""Make the existing TV5 API and shared producer/indexer importable in one test run."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent / "members/TV5-Nguyen-Ngoc-Minh-rag-api-llm-tests"))
