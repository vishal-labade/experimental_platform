from __future__ import annotations

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import col


def ingest_outcomes(
    spark: SparkSession,
    raw_base: str,
    catalog: str,
    namespace: str,
    experiment_id: str,
) -> None:
    """
    Ingest outcomes.parquet for one experiment_id into Iceberg.

    Contract:
      raw_base = s3a://<bucket>/exp_platform/<experiment_id>
      file     = {raw_base}/outcomes.parquet

    Robustness:
      - If parquet has outcome_ts but not ts, we map outcome_ts -> ts.
      - If parquet has both outcome_ts and ts, we keep ONLY ts in final write.
      - Any extra columns in parquet are ignored so schema drift doesn't break ingestion.
    """
    raw_base = raw_base.replace("s3://", "s3a://", 1).rstrip("/")
    path = f"{raw_base}/outcomes.parquet"
    tbl = f"{catalog}.{namespace}.outcomes"

    df = spark.read.parquet(path)

    if "experiment_id" not in df.columns:
        raise ValueError(f"Missing experiment_id in {path}")

    # Decide canonical timestamp column name: ts
    # - prefer existing 'ts'
    # - else use 'outcome_ts'
    if "ts" in df.columns:
        ts_col = F.col("ts")
    elif "outcome_ts" in df.columns:
        ts_col = F.col("outcome_ts")
    else:
        ts_col = F.lit(None)

    df = (
        df
        .withColumn("experiment_id", F.col("experiment_id").cast("string"))
        .withColumn("user_id", F.col("user_id").cast("string"))
        .withColumn("metric_name", F.col("metric_name").cast("string"))
        .withColumn("value", F.col("value").cast("double"))
        .withColumn("ts", ts_col.cast("timestamp"))
    )

    # Create partitioned table (required for per-experiment overwrite)
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {tbl} (
          experiment_id STRING,
          user_id STRING,
          metric_name STRING,
          value DOUBLE,
          ts TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (experiment_id)
        """
    )

    # ✅ Align to table schema explicitly (prevents "too many data columns")
    df2 = df.select("experiment_id", "user_id", "metric_name", "value", "ts")

    df2.writeTo(tbl).overwrite(col("experiment_id") == experiment_id)

    print(f"[ingest_outcomes] wrote {df2.count():,} rows to {tbl} experiment_id={experiment_id} from={path}")
