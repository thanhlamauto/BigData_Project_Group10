import json
import sys
from pathlib import Path

import pytest

TV5_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(TV5_DIR))


@pytest.fixture
def records():
    return [
        json.loads(line)
        for line in (TV5_DIR / "fixtures/documents.jsonl").read_text(encoding="utf-8").splitlines()
    ]


@pytest.fixture
def documents(records):
    from services.api.models import Document

    return [Document.model_validate(record) for record in records]
