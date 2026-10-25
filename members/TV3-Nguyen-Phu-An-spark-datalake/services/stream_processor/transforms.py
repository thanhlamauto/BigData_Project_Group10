"""Pure DataFrame transforms for the stream processor.

Each function takes and returns a DataFrame and behaves the same on batch and streaming
input, so it is unit tested with a local SparkSession and no Kafka. All date/time logic
assumes `spark.sql.session.timeZone=UTC` (set by job.py and the tests).

Flow: Kafka rows -> to_bronze -> parse_and_validate -> select_valid (Silver)
                                                     -> select_rejected (bad records)
"""

import operator
from functools import reduce

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

from stream_processor.schema import (
    CONTRACT_FIELDS,
    RAW_EVENT_SCHEMA,
    REQUIRED_FIELDS,
    SILVER_SCHEMA,
    SUPPORTED_OPERATIONS,
    SUPPORTED_SCHEMA_VERSION,
    TIMESTAMP_FIELDS,
    TIMESTAMP_PATTERN,
)

REJECT_REASON = "reject_reason"
TEXT_FIELDS = ("title", "body")
STRING_FIELDS = tuple(
    field.name for field in RAW_EVENT_SCHEMA.fields if isinstance(field.dataType, StringType)
)
_PARSED = "_parsed"


def to_bronze(kafka_df: DataFrame) -> DataFrame:
    """Kafka source rows -> Bronze rows: the raw value as text plus its Kafka coordinates."""
    return kafka_df.select(
        F.col("key").cast("string").alias("kafka_key"),
        F.col("value").cast("string").alias("raw_value"),
        "topic",
        "partition",
        "offset",
        F.col("timestamp").alias("kafka_timestamp"),
        F.to_date("timestamp").alias("ingest_date"),
    )


def normalize_text(col: Column) -> Column:
    """Collapse every run of whitespace (spaces, tabs, newlines) to one space, then trim."""
    return F.trim(F.regexp_replace(col, r"\s+", " "))


def parse_timestamp(col: Column) -> Column:
    """ISO 8601 string with an offset -> TimestampType (an instant); null if it does not parse."""
    return F.to_timestamp(col, TIMESTAMP_PATTERN)


def parse_and_validate(df: DataFrame, processed_at: Column | None = None) -> DataFrame:
    """Parse the `raw_value` JSON against the contract, normalize it and flag bad rows.

    Returns all input columns, then every contract field (normalized, timestamps parsed),
    `processed_at`, and `reject_reason`: null for a valid row, otherwise the first rule the
    row broke (see `_reject_reason`). Invalid rows are flagged, never dropped or raised, so
    one bad message cannot crash the stream.
    """
    parsed = df.withColumn(_PARSED, F.from_json("raw_value", RAW_EVENT_SCHEMA))
    normalized = parsed.select(
        *df.columns,
        *[_normalize_field(name).alias(name) for name in CONTRACT_FIELDS],
    )
    checked = normalized.withColumn(REJECT_REASON, _reject_reason())
    for name in TIMESTAMP_FIELDS:
        checked = checked.withColumn(name, parse_timestamp(F.col(name)))
    checked = checked.withColumn(
        "processed_at", F.current_timestamp() if processed_at is None else processed_at
    )
    # Keep reject_reason last so the column order reads: input, contract, metadata.
    return checked.select(*[c for c in checked.columns if c != REJECT_REASON], REJECT_REASON)


def select_valid(checked: DataFrame) -> DataFrame:
    """Valid rows in the Silver layout, partitioned by the UTC date of `ingested_at`."""
    return (
        checked.filter(F.col(REJECT_REASON).isNull())
        .withColumn("ingest_date", F.to_date("ingested_at"))
        .select(*SILVER_SCHEMA.fieldNames())
    )


def select_rejected(checked: DataFrame) -> DataFrame:
    """Invalid rows: the raw value, why it was rejected, and any input metadata columns."""
    keep = [c for c in checked.columns if c not in CONTRACT_FIELDS]
    return checked.filter(F.col(REJECT_REASON).isNotNull()).select(*keep)


def _normalize_field(name: str) -> Column:
    col = F.col(f"{_PARSED}.{name}")
    if name in TEXT_FIELDS:
        return normalize_text(col)
    if name == "source_url":
        trimmed = F.trim(col)
        return F.when(trimmed != "", trimmed)  # "" -> null: never cite an empty URL
    if name in STRING_FIELDS:
        return F.trim(col)
    return col


def _reject_reason() -> Column:
    """First broken rule, in order, or null. Reason names are stable: they appear in logs."""
    raw = F.col("raw_value")
    checks = [
        (raw.isNull() | (F.trim(raw) == ""), "empty_value"),
        # Not JSON, not an object, or an object with none of the contract fields.
        (reduce(operator.and_, [F.col(n).isNull() for n in CONTRACT_FIELDS]), "malformed_json"),
    ]
    for name in REQUIRED_FIELDS:
        value = F.col(name)
        # from_json nulls a field whose JSON type is wrong; the raw JSON tells us it was there.
        in_json = F.get_json_object(raw, f"$.{name}").isNotNull()
        checks.append((value.isNull() & in_json, f"invalid_{name}"))
        checks.append((value.isNull(), f"missing_{name}"))
        if name in STRING_FIELDS:
            checks.append((value == "", f"empty_{name}"))
    checks += [
        (F.col("schema_version") != SUPPORTED_SCHEMA_VERSION, "unsupported_schema_version"),
        (~F.col("operation").isin(*SUPPORTED_OPERATIONS), "unsupported_operation"),
        (F.col("version") < 1, "invalid_version"),
    ]
    checks += [(parse_timestamp(F.col(n)).isNull(), f"invalid_{n}") for n in TIMESTAMP_FIELDS]
    return F.coalesce(*[F.when(cond, F.lit(reason)) for cond, reason in checks])
