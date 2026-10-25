"""Atomic external versioning: equal or older revisions are acknowledged no-ops."""

from datetime import UTC, datetime
from urllib.parse import quote


def mapping(dimension=384):
    return {
        "settings": {"index.knn": True, "number_of_shards": 1, "number_of_replicas": 0},
        "mappings": {
            "properties": {
                "embedding": {
                    "type": "knn_vector",
                    "dimension": dimension,
                    "method": {"name": "hnsw", "engine": "lucene", "space_type": "cosinesimil"},
                },
                **{
                    name: {"type": "keyword"}
                    for name in ("document_id", "event_id", "source", "category", "source_url")
                },
                "version": {"type": "long"},
                "title": {"type": "text"},
                "body": {"type": "text"},
                **{
                    name: {"type": "date"}
                    for name in ("event_time", "ingested_at", "processed_at", "indexed_at")
                },
            }
        },
    }


def initialize(client, index, dimension=384):
    path = f"/{quote(index, safe='')}"
    response = client.put(path, json=mapping(dimension))
    if (
        response.status_code == 400
        and response.json().get("error", {}).get("type") == "resource_already_exists_exception"
    ):
        current = client.get(path + "/_mapping")
        current.raise_for_status()
        actual = current.json()[index]["mappings"]["properties"]["embedding"]["dimension"]
        if actual != dimension:
            raise ValueError("Existing index embedding dimension differs")
        return
    response.raise_for_status()


def upsert(client, index, event, vector):
    document = {**event, "embedding": vector, "indexed_at": datetime.now(UTC).isoformat()}
    path = f"/{quote(index, safe='')}/_doc/{quote(event['document_id'], safe='')}"
    response = client.put(
        path, params={"version": event["version"], "version_type": "external"}, json=document
    )
    if response.status_code == 409:
        return "skipped_stale_or_duplicate"
    response.raise_for_status()
    return "indexed"
