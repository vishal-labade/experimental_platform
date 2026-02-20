from __future__ import annotations

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import col


def ingest_registry(
    spark: SparkSession,
    raw_base: str,
    catalog: str,
    namespace: str,
    experiment_id: str,
) -> None:
    """
    Ingest experiment_registry.parquet for one experiment_id into Iceberg.

    Raw schema (current):
      experiment_id, name, start_ts, end_ts, owner,
      variants, primary_metrics, guardrail_metrics, notes

    Robustness:
      - Registry parquet can have extra columns; we select a canonical set.
      - Missing optional columns are filled with nulls.
    """
    raw_base = raw_base.replace("s3://", "s3a://", 1).rstrip("/")
    path = f"{raw_base}/experiment_registry.parquet"
    tbl = f"{catalog}.{namespace}.experiment_registry"

    df = spark.read.parquet(path)

    if "experiment_id" not in df.columns:
        raise ValueError(f"Missing experiment_id in {path}")

    df = df.withColumn("experiment_id", F.col("experiment_id").cast("string"))
    df = df.filter(F.col("experiment_id") == F.lit(experiment_id))

    def c(name: str, dtype: str):
        return (F.col(name).cast(dtype) if name in df.columns else F.lit(None).cast(dtype)).alias(name)

    # Canonical columns (metadata registry)
    df2 = df.select(
        c("experiment_id", "string"),
        c("name", "string"),
        c("start_ts", "timestamp"),
        c("end_ts", "timestamp"),
        c("owner", "string"),
        # arrays
        (F.col("variants").cast("array<string>") if "variants" in df.columns else F.array().cast("array<string>")).alias("variants"),
        (F.col("primary_metrics").cast("array<string>") if "primary_metrics" in df.columns else F.array().cast("array<string>")).alias("primary_metrics"),
        (F.col("guardrail_metrics").cast("array<string>") if "guardrail_metrics" in df.columns else F.array().cast("array<string>")).alias("guardrail_metrics"),
        c("notes", "string"),
    )

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {tbl} (
          experiment_id STRING,
          name STRING,
          start_ts TIMESTAMP,
          end_ts TIMESTAMP,
          owner STRING,
          variants ARRAY<STRING>,
          primary_metrics ARRAY<STRING>,
          guardrail_metrics ARRAY<STRING>,
          notes STRING
        )
        USING iceberg
        PARTITIONED BY (experiment_id)
        """
    )

    df2.writeTo(tbl).overwrite(col("experiment_id") == experiment_id)

    print(
        f"[ingest_registry] wrote {df2.count():,} rows to {tbl} "
        f"experiment_id={experiment_id} from={path}"
    )
