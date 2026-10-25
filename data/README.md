# StreamRAG synthetic status dataset

Source: data authored by Group 10's generator, with eight fictional services.
All English notices, dates, states, and incidents are synthetic; no external feed,
personal data, or scraped text is used. License: the repository has not declared a
distribution license; no separate dataset license is granted here. The group can
select one before distributing beyond this project.

| File in `sample/` | Count | Purpose |
|---|---:|---|
| `fixtures.jsonl` | 10 | Valid contract examples; first ten seed records |
| `seed_events.jsonl` | 400 | Distinct v1 documents, eight services |
| `live_events.jsonl` | 10 | Five v2 revisions plus five new v1 documents |
| `test_cases.jsonl` | 20 | Ten seed and ten after-stream questions with facts/version |
| `edge_cases.jsonl` | 26 lines | Ordered test traffic, deliberately includes invalid JSON |
| `edge_case_expectations.json` | 26 entries | One-based line numbers and expected behavior |

From the repository root, using Python 3.11+:

```bash
python scripts/generate_data.py --seed 42 --n 400 --out data/sample/seed_events.jsonl
python scripts/generate_data.py --seed 42 --n 400 --all
python -m pytest tests/unit -q
```

The first command reproduces seed bytes exactly; the second regenerates all six
fixtures. Alternate `--seed`/`--n`/`--out` support experiments. `--all` needs at
least ten seed records. Store large datasets in ignored `data/generated/`.

Schema and field semantics: [data contract](../docs/data_contract.md).
Replay and Kafka commands: [producer README](../services/producer/README.md).

## Q&A evaluation

Publish seed first; evaluate `phase=seed`. Next publish **all** of `live_events.jsonl`
and evaluate `phase=after_stream`. Each case has `question`, `expected_document_id`,
`expected_version`, `expected_facts` (list of literal facts), and `phase`.
For example, Atlas maintenance is 20:30 UTC before the update and 21:30 UTC after.
Running only `--update inc-atlas-001` exercises the Atlas case, not all ten live cases.
Stable IDs mean repeated live-file playback is a replay, not another version bump.

## Ordered edge-case scenario

Start after seed. Consume all three partitions; there is no total ordering across
keys, while all versions of one document share a key and preserve partition order.

| Lines | Count | Expected downstream behavior |
|---|---:|---|
| 1–10 | 10 duplicate | Same event IDs as the seed. Deduplicate/idempotently upsert; no extra searchable copies. |
| 11–15 | 5 late | Valid events dated 20/10. Spark applies its documented watermark policy and records the outcome. |
| 16–20 | 5 update | Same document IDs as seed, v2 replaces v1. Atlas now says 21:30 UTC. |
| 21 | 1 stale | New event ID for Atlas v1 after v2. Indexer must preserve v2. |
| 22–26 | 5 invalid | Malformed JSON, missing document ID, blank body, bad timestamp, version zero. Reject and count without stopping the stream. |

The five invalid records are intentionally not schema-valid. Do not run a blanket
"all lines pass schema" assertion against this file. It must be sent using the
explicit simulator `--edge-cases` mode. Normal `--event-file` replay rejects it.
Late-event acceptance is deliberately not promised outside Spark's watermark;
current code provides fixtures and expected behaviors, not proof of downstream
deduplication or full pipeline integration.
