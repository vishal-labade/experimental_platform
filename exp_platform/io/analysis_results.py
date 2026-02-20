from __future__ import annotations

from typing import List, Optional, Sequence

from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, LongType, DoubleType, TimestampType,
)

ANALYSIS_RESULTS_SCHEMA: StructType = StructType([
    StructField("experiment_id", StringType(), True),
    StructField("metric_name", StringType(), True),

    StructField("n_control", LongType(), True),
    StructField("mean_control", DoubleType(), True),
    StructField("std_control", DoubleType(), True),

    StructField("n_treatment", LongType(), True),
    StructField("mean_treatment", DoubleType(), True),
    StructField("std_treatment", DoubleType(), True),

    StructField("delta", DoubleType(), True),
    StructField("se", DoubleType(), True),
    StructField("z", DoubleType(), True),
    StructField("p_value", DoubleType(), True),

    StructField("ci_low", DoubleType(), True),
    StructField("ci_high", DoubleType(), True),
    StructField("rel_lift", DoubleType(), True),

    StructField("computed_ts", TimestampType(), True),
    StructField("analysis_ts", TimestampType(), True),
])

ANALYSIS_RESULTS_COLS: List[str] = [f.name for f in ANALYSIS_RESULTS_SCHEMA.fields]


def _table_exists(spark: SparkSession, full_name: str) -> bool:
    try:
        spark.table(full_name).limit(0).collect()
        return True
    except Exception:
        return False


def _create_namespace_if_needed(spark: SparkSession, full_table: str) -> None:
    parts = full_table.split(".")
    if len(parts) >= 3:
        catalog, namespace = parts[0], parts[1]
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.{namespace}")


def _ensure_table(spark: SparkSession, full_table: str, sample_df: DataFrame) -> None:
    if _table_exists(spark, full_table):
        return
    _create_namespace_if_needed(spark, full_table)
    (
        sample_df.select(*ANALYSIS_RESULTS_COLS)
        .writeTo(full_table)
        .using("iceberg")
        .create()
    )


def _normalize_df(df: DataFrame) -> DataFrame:
    for c in ANALYSIS_RESULTS_COLS:
        if c not in df.columns:
            df = df.withColumn(c, F.lit(None))

    df = df.select(*ANALYSIS_RESULTS_COLS)

    return (
        df
        .withColumn("experiment_id", F.col("experiment_id").cast("string"))
        .withColumn("metric_name", F.col("metric_name").cast("string"))

        .withColumn("n_control", F.col("n_control").cast("long"))
        .withColumn("mean_control", F.col("mean_control").cast("double"))
        .withColumn("std_control", F.col("std_control").cast("double"))

        .withColumn("n_treatment", F.col("n_treatment").cast("long"))
        .withColumn("mean_treatment", F.col("mean_treatment").cast("double"))
        .withColumn("std_treatment", F.col("std_treatment").cast("double"))

        .withColumn("delta", F.col("delta").cast("double"))
        .withColumn("se", F.col("se").cast("double"))
        .withColumn("z", F.col("z").cast("double"))
        .withColumn("p_value", F.col("p_value").cast("double"))

        .withColumn("ci_low", F.col("ci_low").cast("double"))
        .withColumn("ci_high", F.col("ci_high").cast("double"))
        .withColumn("rel_lift", F.col("rel_lift").cast("double"))

        # timestamps: accept timestamp or parseable string
        .withColumn("computed_ts", F.to_timestamp(F.col("computed_ts")))
        .withColumn("analysis_ts", F.to_timestamp(F.col("analysis_ts")))
    )


def upsert_analysis_results(
    spark: SparkSession,
    full_table: str,
    df_in: DataFrame,
    *,
    experiment_id: str,
    metric_names: Optional[Sequence[str]] = None,
) -> None:
    df = _normalize_df(df_in)

    now_ts = F.current_timestamp()
    df = df.withColumn("computed_ts", F.coalesce(F.col("computed_ts"), now_ts))
    df = df.withColumn("analysis_ts", F.coalesce(F.col("analysis_ts"), now_ts))

    _ensure_table(spark, full_table, df.limit(0))

    exp_esc = experiment_id.replace("'", "''")
    if metric_names:
        escaped = [m.replace("'", "''") for m in metric_names]
        in_list = ", ".join([f"'{m}'" for m in escaped])
        spark.sql(
            f"DELETE FROM {full_table} "
            f"WHERE experiment_id = '{exp_esc}' AND metric_name IN ({in_list})"
        )
    else:
        spark.sql(f"DELETE FROM {full_table} WHERE experiment_id = '{exp_esc}'")

    df.writeTo(full_table).append()
