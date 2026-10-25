"""Unit tests for stream_processor.transforms on a local SparkSession (no Kafka needed).

Requires pyspark and a Java runtime; skipped otherwise. Run them with `make test` in the
TV3 folder, which uses the pinned Spark image.
"""

import json
import sys
from pathlib import Path

import pytest

pytest.importorskip("pyspark", reason="pyspark not installed (see TV3 README)")

TV3_DIR = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(TV3_DIR / "services"))

from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402
from stream_processor import transforms  # noqa: E402
from stream_processor.schema import BRONZE_COLUMNS, SILVER_SCHEMA  # noqa: E402

DEV_FIXTURES = TV3_DIR / "tests" / "fixtures" / "raw_events_dev.jsonl"

# The sample message from plan section 3.2 (contract v0).
SAMPLE = {
    "schema_version": 1,
    "event_id": "evt-000501",
    "document_id": "inc-atlas-001",
    "version": 2,
    "operation": "upsert",
    "source": "synthetic-status",
    "source_url": None,
    "title": "Atlas Search maintenance update",
    "body": "The maintenance start time changed from 20:30 to 21:30 UTC.",
    "category": "maintenance",
    "event_time": "2026-10-27T09:00:00Z",
    "ingested_at": "2026-10-27T09:00:03Z",
}

ISO_UTC = "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.master("local[1]")
        .appName("test-stream-processor")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


def event(**overrides):
    return {**SAMPLE, **overrides}


def check(spark, *values):
    """Run parse_and_validate on raw values (dicts are JSON-encoded, strings kept as-is)."""
    rows = [(json.dumps(v) if isinstance(v, dict) else v,) for v in values]
    return transforms.parse_and_validate(spark.createDataFrame(rows, "raw_value string"))


def as_dicts(df):
    """Collect rows; timestamps rendered as UTC ISO strings so results do not depend on TZ."""
    cols = [
        F.date_format(c, ISO_UTC).alias(c) if t == "timestamp" else F.col(c) for c, t in df.dtypes
    ]
    return [row.asDict() for row in df.select(*cols).collect()]


def reason_of(spark, value):
    return as_dicts(check(spark, value))[0]["reject_reason"]


def test_sample_message_is_valid_and_timestamps_are_utc(spark):
    [row] = as_dicts(check(spark, SAMPLE))
    assert row["reject_reason"] is None
    assert row["event_id"] == "evt-000501"
    assert row["version"] == 2
    assert row["source_url"] is None
    assert row["event_time"] == "2026-10-27T09:00:00.000Z"
    assert row["ingested_at"] == "2026-10-27T09:00:03.000Z"
    assert row["processed_at"] is not None


def test_title_and_body_whitespace_is_collapsed(spark):
    [row] = as_dicts(check(spark, event(title="  Atlas \t Search\n update ", body=" a  b\n\nc ")))
    assert row["reject_reason"] is None
    assert row["title"] == "Atlas Search update"
    assert row["body"] == "a b c"


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("2026-10-27T16:00:00+07:00", "2026-10-27T09:00:00.000Z"),
        ("2026-10-27T09:00:00.25Z", "2026-10-27T09:00:00.250Z"),
    ],
)
def test_timestamps_with_offset_or_fraction_are_normalized_to_utc(spark, raw, expected):
    [row] = as_dicts(check(spark, event(event_time=raw)))
    assert row["reject_reason"] is None
    assert row["event_time"] == expected


def test_empty_source_url_becomes_null(spark):
    [row] = as_dicts(check(spark, event(source_url="  ")))
    assert row["reject_reason"] is None
    assert row["source_url"] is None


def without(field):
    return {k: v for k, v in SAMPLE.items() if k != field}


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("", "empty_value"),
        ('{"event_id": "evt-1",', "malformed_json"),
        ("not json", "malformed_json"),
        ("[]", "malformed_json"),
        ("{}", "malformed_json"),
        (without("event_id"), "missing_event_id"),
        (event(event_id="  "), "empty_event_id"),
        (without("body"), "missing_body"),
        (event(body=None), "missing_body"),
        (event(body=" \n\t "), "empty_body"),
        (event(title=""), "empty_title"),
        (event(version="two"), "invalid_version"),
        (event(version=0), "invalid_version"),
        (event(schema_version=2), "unsupported_schema_version"),
        (event(operation="delete"), "unsupported_operation"),
        (without("ingested_at"), "missing_ingested_at"),
        (event(event_time="27/10/2026 09:00"), "invalid_event_time"),
        (event(event_time="2026-10-27T09:00:00"), "invalid_event_time"),  # no offset
        (event(event_time="2026-02-30T09:00:00Z"), "invalid_event_time"),
        (event(ingested_at="2026-10-27 09:00:03Z"), "invalid_ingested_at"),
    ],
)
def test_invalid_records_are_flagged_with_a_reason(spark, value, expected):
    assert reason_of(spark, value) == expected


def test_select_valid_matches_silver_schema_and_partitions_by_ingested_date(spark):
    late_night = event(event_id="evt-2", ingested_at="2026-10-27T23:59:59+00:00")
    next_day_utc = event(event_id="evt-3", ingested_at="2026-10-28T06:00:00+07:00")
    silver = transforms.select_valid(check(spark, late_night, next_day_utc, "bad"))
    # Names and types are the contract; nullability flags vary with how a column was derived.
    assert [(f.name, f.dataType) for f in silver.schema] == [
        (f.name, f.dataType) for f in SILVER_SCHEMA
    ]
    dates = {r["event_id"]: str(r["ingest_date"]) for r in silver.collect()}
    assert dates == {"evt-2": "2026-10-27", "evt-3": "2026-10-27"}


def test_mixed_batch_splits_into_valid_and_rejected_without_failing(spark):
    checked = check(spark, SAMPLE, "{oops", event(body=""))
    assert transforms.select_valid(checked).count() == 1
    rejected = as_dicts(transforms.select_rejected(checked))
    assert [r["reject_reason"] for r in rejected] == ["malformed_json", "empty_body"]
    assert rejected[0]["raw_value"] == "{oops"
    assert set(rejected[0]) == {"raw_value", "processed_at", "reject_reason"}


def test_to_bronze_keeps_raw_value_and_kafka_coordinates(spark):
    kafka_rows = spark.createDataFrame(
        [(b"inc-atlas-001", b'{"x": 1}', "streamrag.raw.v1", 2, 41)],
        "key binary, value binary, topic string, partition int, offset long",
    ).withColumn(
        "timestamp", F.to_timestamp(F.lit("2026-10-27T23:30:00Z"), "yyyy-MM-dd'T'HH:mm:ssX")
    )
    bronze = transforms.to_bronze(kafka_rows)
    assert tuple(bronze.columns) == BRONZE_COLUMNS
    [row] = bronze.collect()
    assert (row["kafka_key"], row["raw_value"]) == ("inc-atlas-001", '{"x": 1}')
    assert (row["partition"], row["offset"]) == (2, 41)
    assert str(row["ingest_date"]) == "2026-10-27"


def test_bronze_metadata_is_carried_onto_rejected_rows(spark):
    kafka_rows = spark.createDataFrame(
        [(b"k", b"{broken", "streamrag.raw.v1", 0, 7)],
        "key binary, value binary, topic string, partition int, offset long",
    ).withColumn("timestamp", F.current_timestamp())
    checked = transforms.parse_and_validate(transforms.to_bronze(kafka_rows))
    [row] = transforms.select_rejected(checked).collect()
    assert (row["offset"], row["reject_reason"]) == (7, "malformed_json")


def test_dev_fixture_file_has_expected_valid_and_invalid_counts(spark):
    lines = DEV_FIXTURES.read_text().splitlines()
    checked = check(spark, *lines)
    reasons = sorted(r["reject_reason"] for r in as_dicts(transforms.select_rejected(checked)))
    assert transforms.select_valid(checked).count() == 4  # includes 1 duplicate: dedup is M1-10
    assert reasons == [
        "empty_body",
        "invalid_event_time",
        "malformed_json",
        "missing_body",
        "unsupported_operation",
    ]
