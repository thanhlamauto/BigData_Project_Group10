"""Spark Structured Streaming job: Kafka raw topic -> Bronze + Silver Parquet (M1.1 spike).

Run with spark-submit (see README.md). Configuration comes from environment variables that
match the TV1 `.env.example`. The cleaned topic sink is added in M1.2 (task M1-09).

Each micro-batch is handled in `foreachBatch`: one Kafka read and one checkpoint feed every
sink, and the batch logs input/valid/invalid counts. A retried batch can write its files
again, so the pipeline is at-least-once; downstream must stay idempotent on event_id and
document_id + version.
"""

import logging
import os
import time
from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from stream_processor import transforms

log = logging.getLogger("stream_processor")

QUERY_NAME = "raw_to_lake"


@dataclass(frozen=True)
class Config:
    bootstrap_servers: str
    raw_topic: str
    lake_root: str
    checkpoint_root: str
    starting_offsets: str
    trigger: str
    s3_endpoint: str
    s3_access_key: str
    s3_secret_key: str

    @classmethod
    def from_env(cls, env=os.environ) -> "Config":
        lake_root = env.get("LAKE_ROOT") or f"s3a://{env.get('LAKE_BUCKET', 'streamrag-lake')}"
        lake_root = lake_root.rstrip("/")
        return cls(
            bootstrap_servers=env.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
            raw_topic=env.get("RAW_TOPIC", "streamrag.raw.v1"),
            lake_root=lake_root,
            checkpoint_root=(env.get("CHECKPOINT_ROOT") or f"{lake_root}/checkpoints").rstrip("/"),
            starting_offsets=env.get("STARTING_OFFSETS", "earliest"),
            # "5 seconds" (any Spark interval) or "availableNow" (process the backlog, exit).
            trigger=env.get("TRIGGER", "5 seconds"),
            s3_endpoint=env.get("MINIO_ENDPOINT", "http://localhost:9000"),
            s3_access_key=env.get("MINIO_ACCESS_KEY", ""),
            s3_secret_key=env.get("MINIO_SECRET_KEY", ""),
        )

    @property
    def uses_s3(self) -> bool:
        return self.lake_root.startswith("s3a://") or self.checkpoint_root.startswith("s3a://")


def build_spark(cfg: Config) -> SparkSession:
    builder = (
        SparkSession.builder.appName("streamrag-stream-processor")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", os.environ.get("SHUFFLE_PARTITIONS", "4"))
    )
    if cfg.uses_s3:
        s3a = {
            "fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "fs.s3a.endpoint": cfg.s3_endpoint,
            "fs.s3a.access.key": cfg.s3_access_key,
            "fs.s3a.secret.key": cfg.s3_secret_key,
            "fs.s3a.aws.credentials.provider": (
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
            ),
            # MinIO: bucket in the path, plain HTTP on the local network.
            "fs.s3a.path.style.access": "true",
            "fs.s3a.connection.ssl.enabled": str(cfg.s3_endpoint.startswith("https")).lower(),
        }
        for key, value in s3a.items():
            builder = builder.config(f"spark.hadoop.{key}", value)
    return builder.getOrCreate()


def write_parquet(df: DataFrame, path: str) -> None:
    df.write.mode("append").partitionBy("ingest_date").parquet(path)


def process_batch(cfg: Config, batch: DataFrame, batch_id: int) -> None:
    started = time.monotonic()
    # One processed_at per micro-batch, identical on every row and every sink.
    processed_at = F.timestamp_seconds(F.lit(time.time()))
    bronze = transforms.to_bronze(batch).persist()
    checked = transforms.parse_and_validate(bronze, processed_at=processed_at).persist()
    try:
        total, invalid = checked.agg(F.count("*"), F.count(transforms.REJECT_REASON)).first()
        if total == 0:
            log.info("batch=%d input=0 (nothing to do)", batch_id)
            return
        write_parquet(bronze, f"{cfg.lake_root}/bronze/events")
        write_parquet(transforms.select_valid(checked), f"{cfg.lake_root}/silver/events")
        reasons = {}
        if invalid:
            rejected = transforms.select_rejected(checked)
            write_parquet(rejected, f"{cfg.lake_root}/bronze/rejected")
            counts = rejected.groupBy(transforms.REJECT_REASON).count().collect()
            reasons = {r[0]: r[1] for r in sorted(counts, key=lambda r: -r[1])}
        log.info(
            "batch=%d input=%d valid=%d invalid=%d reasons=%s took_ms=%d",
            batch_id,
            total,
            total - invalid,
            invalid,
            reasons,
            (time.monotonic() - started) * 1000,
        )
    finally:
        checked.unpersist()
        bronze.unpersist()


def start(cfg: Config, spark: SparkSession):
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", cfg.bootstrap_servers)
        .option("subscribe", cfg.raw_topic)
        .option("startingOffsets", cfg.starting_offsets)  # only used on the first run
        .load()
    )
    writer = (
        raw.writeStream.queryName(QUERY_NAME)
        .foreachBatch(lambda batch, batch_id: process_batch(cfg, batch, batch_id))
        .option("checkpointLocation", f"{cfg.checkpoint_root}/{QUERY_NAME}")
    )
    if cfg.trigger == "availableNow":
        writer = writer.trigger(availableNow=True)
    else:
        writer = writer.trigger(processingTime=cfg.trigger)
    return writer.start()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    cfg = Config.from_env()
    spark = build_spark(cfg)
    spark.sparkContext.setLogLevel(os.environ.get("SPARK_LOG_LEVEL", "WARN"))
    log.info(
        "start topic=%s kafka=%s lake=%s checkpoints=%s trigger=%s",
        cfg.raw_topic,
        cfg.bootstrap_servers,
        cfg.lake_root,
        cfg.checkpoint_root,
        cfg.trigger,
    )
    start(cfg, spark).awaitTermination()


if __name__ == "__main__":
    main()
