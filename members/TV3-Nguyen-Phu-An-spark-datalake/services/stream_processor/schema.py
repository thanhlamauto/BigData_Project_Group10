"""Spark schemas for the StreamRAG event contract v1 (plan section 3.2).

Contract v0 comes from the sample message in the plan. Update this file when TV2 merges
`data/contracts/event.schema.json`, and keep the field list in sync with it.
"""

from pyspark.sql.types import (
    DateType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

SUPPORTED_SCHEMA_VERSION = 1
SUPPORTED_OPERATIONS = ("upsert",)

# ISO 8601 with an explicit offset ("Z" or "+07:00"), optional fraction up to microseconds.
# Strings without an offset are rejected: their time zone is ambiguous.
TIMESTAMP_PATTERN = "yyyy-MM-dd'T'HH:mm:ss[.SSSSSS]XXX"
TIMESTAMP_FIELDS = ("event_time", "ingested_at")

# Shape of the JSON value on `streamrag.raw.v1`. Timestamps are read as strings so the
# transform can parse them strictly and tell "missing" apart from "wrong format".
RAW_EVENT_SCHEMA = StructType(
    [
        StructField("schema_version", IntegerType()),
        StructField("event_id", StringType()),
        StructField("document_id", StringType()),
        StructField("version", LongType()),
        StructField("operation", StringType()),
        StructField("source", StringType()),
        StructField("source_url", StringType()),
        StructField("title", StringType()),
        StructField("body", StringType()),
        StructField("category", StringType()),
        StructField("event_time", StringType()),
        StructField("ingested_at", StringType()),
    ]
)

CONTRACT_FIELDS = tuple(field.name for field in RAW_EVENT_SCHEMA.fields)
NULLABLE_FIELDS = ("source_url",)
REQUIRED_FIELDS = tuple(name for name in CONTRACT_FIELDS if name not in NULLABLE_FIELDS)

# Silver layer: validated + normalized contract fields, plus processing metadata.
SILVER_SCHEMA = StructType(
    [
        StructField("schema_version", IntegerType()),
        StructField("event_id", StringType()),
        StructField("document_id", StringType()),
        StructField("version", LongType()),
        StructField("operation", StringType()),
        StructField("source", StringType()),
        StructField("source_url", StringType()),
        StructField("title", StringType()),
        StructField("body", StringType()),
        StructField("category", StringType()),
        StructField("event_time", TimestampType()),
        StructField("ingested_at", TimestampType()),
        StructField("processed_at", TimestampType()),
        StructField("ingest_date", DateType()),
    ]
)

# Bronze layer: the raw Kafka record, untouched, plus its Kafka coordinates.
BRONZE_COLUMNS = (
    "kafka_key",
    "raw_value",
    "topic",
    "partition",
    "offset",
    "kafka_timestamp",
    "ingest_date",
)
