from __future__ import annotations

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import col


def ingest_exposures(
    spark: SparkSession,
    raw_base: str,
    catalog: str,
    namespace: str,
    experiment_id: str,
) -> None:
    raw_base = raw_base.replace("s3://", "s3a://", 1).rstrip("/")
    path = f"{raw_base}/exposures.parquet"
    tbl = f"{catalog}.{namespace}.exposures"

    df = spark.read.parquet(path)

    if "experiment_id" not in df.columns:
        raise ValueError(f"Missing experiment_id in {path}")

    df = (
        df
        .withColumn("experiment_id", F.col("experiment_id").cast("string"))
        .withColumn("user_id", F.col("user_id").cast("string"))
        .withColumn("variant", F.col("variant").cast("string"))
    )

    # Create partitioned Iceberg table (critical for idempotent per-experiment overwrite)
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {tbl} (
          experiment_id STRING,
          user_id STRING,
          variant STRING,
          exposure_ts TIMESTAMP,
          unit STRING
        )
        USING iceberg
        PARTITIONED BY (experiment_id)
        """
    )

    # Partition overwrite (safe because partition spec includes experiment_id)
    df.writeTo(tbl).overwrite(col("experiment_id") == experiment_id)

    print(f"[ingest_exposures] wrote {df.count():,} rows to {tbl} experiment_id={experiment_id} from={path}")
