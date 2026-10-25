import json

from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[2]").appName("tv2-verification").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
bronze = spark.read.parquet("/lake/bronze/events")
silver = spark.read.parquet("/lake/silver/events")
rejected = spark.read.parquet("/lake/bronze/rejected")
result = {
    "bronze": bronze.count(),
    "silver": silver.count(),
    "rejected": rejected.count(),
    "reasons": {
        r.reject_reason: r["count"] for r in rejected.groupBy("reject_reason").count().collect()
    },
    "unique_silver_event_ids": silver.select("event_id").distinct().count(),
    "atlas_versions": sorted(
        r.version
        for r in silver.filter("document_id = 'inc-atlas-001'").select("version").collect()
    ),
}
assert result["bronze"] == 436, result
assert result["silver"] == 431, result
assert result["rejected"] == 5, result
assert len(result["reasons"]) == 5, result
assert result["unique_silver_event_ids"] == 416, result
assert result["atlas_versions"] == [1, 1, 1, 2, 2], result
print("TV2_RESULT=" + json.dumps(result, sort_keys=True))
spark.stop()
