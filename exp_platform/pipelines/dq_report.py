from __future__ import annotations

from pyspark.sql import SparkSession, functions as F


def dq_report(
    spark: SparkSession,
    experiment_id: str,
    catalog: str = "iceberg",
    namespace: str = "exp",
) -> None:
    """
    Simple DQ checks over Iceberg tables for a single experiment_id.

    Expects tables:
      - {catalog}.{namespace}.exposures
      - {catalog}.{namespace}.outcomes
      - {catalog}.{namespace}.experiment_registry
    """
    print("\n=== DQ REPORT ===")
    print(f"experiment_id: {experiment_id}")
    print(f"tables: {catalog}.{namespace}.*")

    exposures = spark.table(f"{catalog}.{namespace}.exposures").where(F.col("experiment_id") == experiment_id)
    outcomes = spark.table(f"{catalog}.{namespace}.outcomes").where(F.col("experiment_id") == experiment_id)
    registry = spark.table(f"{catalog}.{namespace}.experiment_registry").where(F.col("experiment_id") == experiment_id)

    exp_rows = exposures.count()
    out_rows = outcomes.count()
    reg_rows = registry.count()

    print(f"- exposures rows: {exp_rows:,}")
    print(f"- outcomes rows:  {out_rows:,}")
    print(f"- registry rows:  {reg_rows:,}")

    # Basic null checks
    exp_null_users = exposures.where(F.col("user_id").isNull()).count()
    out_null_users = outcomes.where(F.col("user_id").isNull()).count()

    print(f"- exposures null user_id: {exp_null_users:,}")
    print(f"- outcomes null user_id:  {out_null_users:,}")

    # Variant distribution (exposures)
    print("\n- exposures by variant:")
    exposures.groupBy("variant").count().orderBy("variant").show(50, truncate=False)

    # Metric distribution (outcomes)
    print("- outcomes by metric_name:")
    outcomes.groupBy("metric_name").count().orderBy("metric_name").show(50, truncate=False)

    # Optional: time range sanity if exposure_ts exists
    if "exposure_ts" in exposures.columns:
        rng = exposures.agg(
            F.min("exposure_ts").alias("min_exposure_ts"),
            F.max("exposure_ts").alias("max_exposure_ts"),
        ).collect()[0]
        print(f"- exposure_ts range: {rng['min_exposure_ts']} -> {rng['max_exposure_ts']}")

    print("✅ DQ report complete\n")
