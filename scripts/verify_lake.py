"""Run via spark-submit in TV3's image with its existing S3A configuration."""

import argparse
import json

from stream_processor.job import Config, build_spark


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--expected-input", type=int, required=True)
    parser.add_argument("--expected-invalid", type=int, required=True)
    args = parser.parse_args()
    config = Config.from_env()
    spark = build_spark(config)
    spark.sparkContext.setLogLevel("ERROR")
    try:
        bronze = spark.read.parquet(config.lake_root + "/bronze/events")
        silver = spark.read.parquet(config.lake_root + "/silver/events")
        rejected = spark.read.parquet(config.lake_root + "/bronze/rejected")
        report = {
            "bronze": bronze.count(),
            "silver": silver.count(),
            "rejected": rejected.count(),
            "reasons": {
                row.reject_reason: row["count"]
                for row in rejected.groupBy("reject_reason").count().collect()
            },
            "storage": config.lake_root,
        }
        assert report["bronze"] == args.expected_input, report
        assert report["rejected"] == args.expected_invalid, report
        assert report["silver"] == args.expected_input - args.expected_invalid, report
        print("LAKE_RESULT=" + json.dumps(report, sort_keys=True))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
