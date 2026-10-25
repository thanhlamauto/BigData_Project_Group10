"""Read the Bronze/Silver Parquet back and print counts + a Silver sample (evidence helper).

spark-submit inspect_lake.py        # same env vars as job.py (LAKE_ROOT / LAKE_BUCKET, MINIO_*)
"""

from pyspark.sql.utils import AnalysisException

from stream_processor.job import Config, build_spark


def main() -> None:
    cfg = Config.from_env()
    spark = build_spark(cfg)
    spark.sparkContext.setLogLevel("ERROR")
    for layer in ("bronze/events", "bronze/rejected", "silver/events"):
        path = f"{cfg.lake_root}/{layer}"
        try:
            print(f"{path}: {spark.read.parquet(path).count()} rows")
        except AnalysisException:
            print(f"{path}: (not written yet)")
    try:
        silver = spark.read.parquet(f"{cfg.lake_root}/silver/events")
    except AnalysisException:
        return
    cols = ["event_id", "document_id", "version", "title", "event_time", "ingest_date"]
    silver.select(*cols).orderBy("ingested_at").show(20, truncate=False)


if __name__ == "__main__":
    main()
