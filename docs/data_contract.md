# StreamRAG event contract v1

Owner: TV2 (Nguyễn Thanh Lâm). Wire schema:
[`event.schema.json`](../data/contracts/event.schema.json), JSON Schema draft 2020-12.
Raw topic: `streamrag.raw.v1`; cleaned topic: `streamrag.cleaned.v1`.
Each Kafka message is one JSON object encoded in UTF-8, keyed by `document_id`.

| Field | Type / rule | Meaning |
|---|---|---|
| `schema_version` | integer, exactly 1 | Wire contract version |
| `event_id` | nonblank string | One logical event; keep the ID when retrying or replaying |
| `document_id` | nonblank string | Logical document; unchanged across revisions and used as Kafka key |
| `version` | integer ≥ 1 | Increasing document revision; late v1 must not replace v2 |
| `operation` | `upsert` | Only supported operation in M1 |
| `source` | nonblank string | Default `synthetic-status` |
| `source_url` | optional null or nonblank string | Optional provenance reference; synthetic samples use null |
| `title` / `body` | nonblank strings | Short English title and content; whitespace-only is invalid |
| `category` | `incident`, `maintenance`, `resolved`, `status` | Status classification |
| `event_time` | UTC ISO 8601 ending in `Z` | Time of the underlying event |
| `ingested_at` | UTC ISO 8601 ending in `Z` | Producer submission time, updated on normal publish |

All fields except `source_url` are required. Unknown fields are rejected. UTC strings
include seconds and optionally fractional seconds. Offset notation (`+00:00`) and
timezone-free timestamps are rejected for this wire contract. Validators must
enable `date-time` format checking to reject impossible dates as well as checking
the `Z` pattern. `validate_event` provides the producer's dependency-free checks;
the optional schema conformance test uses `Draft202012Validator` + `FormatChecker`.

```json
{
  "schema_version": 1,
  "event_id": "evt-live-42-000001",
  "document_id": "inc-atlas-001",
  "version": 2,
  "operation": "upsert",
  "source": "synthetic-status",
  "source_url": null,
  "title": "Atlas Search maintenance notice 001 live update",
  "body": "Atlas Search maintenance starts at 21:30 UTC on 2026-10-27. Expected duration is 30 minutes. Status: scheduled.",
  "category": "maintenance",
  "event_time": "2026-10-27T09:00:00Z",
  "ingested_at": "2026-10-27T09:00:03Z"
}
```

One document is one searchable chunk in M1. Cite `document_id`, `title`, `event_time`,
and `version`; do not invent a source URL when it is null/absent. Producers preserve
event identity across retries. Equal-version replay is idempotent; a higher version
updates the document; a lower version cannot overwrite it. The indexer owns that
version check; `ingested_at` is not a revision ordering key.

Malformed JSON, missing/invalid fields, or blank bodies must be rejected and counted
by Spark without crashing the stream. Spark owns whitespace normalization, UTC
parsing, deduplication state, watermark behavior, and the cleaned record projection.
In M1, consumers should retain these fields; downstream storage metadata such as
`ingest_date` belongs outside the raw event schema. Kafka producer idempotence alone
does not implement downstream deduplication or an exactly-once pipeline.

Changing required fields, categories, or meaning after freeze is a breaking change:
coordinate the schema, producer, Spark, indexer, API, docs, and fixture tests together.
The ten valid fixtures provide a file-based handoff before dependent services run.
