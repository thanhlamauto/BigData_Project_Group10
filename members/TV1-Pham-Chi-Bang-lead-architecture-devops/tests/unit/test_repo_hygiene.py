"""Guards for repository-level configuration owned by TV1."""

from pathlib import Path

TV1_DIR = Path(__file__).resolve().parents[2]
REPO_ROOT = TV1_DIR.parents[1]

REQUIRED_ENV_KEYS = {
    "KAFKA_BOOTSTRAP_SERVERS",
    "RAW_TOPIC",
    "CLEANED_TOPIC",
    "MINIO_ENDPOINT",
    "MINIO_ACCESS_KEY",
    "MINIO_SECRET_KEY",
    "LAKE_BUCKET",
    "OPENSEARCH_URL",
    "OPENSEARCH_INDEX",
    "EMBEDDING_MODEL",
    "EMBEDDING_DIM",
    "LLM_PROVIDER",
    "LLM_BASE_URL",
    "LLM_MODEL",
    "LLM_API_KEY",
}


def read_env_example() -> dict[str, str]:
    values = {}
    for line in (TV1_DIR / ".env.example").read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#"):
            key, _, value = line.partition("=")
            values[key.strip()] = value.strip()
    return values


def test_env_example_has_required_keys():
    missing = REQUIRED_ENV_KEYS - read_env_example().keys()
    assert not missing, f".env.example is missing: {sorted(missing)}"


def test_env_example_has_no_llm_api_key():
    assert read_env_example()["LLM_API_KEY"] == ""


def test_env_example_uses_offline_llm_by_default():
    assert read_env_example()["LLM_PROVIDER"] == "mock"


def test_topics_match_contract():
    env = read_env_example()
    assert env["RAW_TOPIC"] == "streamrag.raw.v1"
    assert env["CLEANED_TOPIC"] == "streamrag.cleaned.v1"


def test_gitignore_excludes_local_env_and_runtime_data():
    ignored = set((REPO_ROOT / ".gitignore").read_text().splitlines())
    for pattern in (".env", "data/lake/", "checkpoints/", "*.parquet"):
        assert pattern in ignored, f"{pattern} must be in .gitignore"
