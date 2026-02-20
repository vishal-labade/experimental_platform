from __future__ import annotations

import argparse
from typing import Optional, Tuple

from pyspark.sql import SparkSession, DataFrame, functions as F

from exp_platform.spark.session import build_spark_session
from exp_platform.metrics.definitions import default_metric_specs, specs_by_name



# -------------------------
# Helpers
# -------------------------
def _require_cols(df: DataFrame, cols: list[str], name: str):
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise ValueError(f"{name} missing columns: {missing}. Has: {df.columns}")


def _normalize_outcomes_ts(outcomes):
    """
    Ensure downstream code always has `outcome_ts`.
    - If table has `ts`, rename it to `outcome_ts`.
    - If it already has `outcome_ts`, leave it.
    """
    if "outcome_ts" in outcomes.columns:
        return outcomes
    if "ts" in outcomes.columns:
        return outcomes.withColumnRenamed("ts", "outcome_ts")
    return outcomes.withColumn("outcome_ts", F.lit(None).cast("timestamp"))

def _load_outcomes(spark, catalog, namespace):
    outcomes = spark.table(f"{catalog}.{namespace}.outcomes")
    return _normalize_outcomes_ts(outcomes)


def _table_exists(spark: SparkSession, full_name: str) -> bool:
    # Spark supports 1-arg and (db,table) variants; handle both.
    try:
        return bool(spark.catalog.tableExists(full_name))
    except Exception:
        parts = full_name.split(".")
        if len(parts) < 2:
            return False
        db = ".".join(parts[:-1])
        tbl = parts[-1]
        try:
            return bool(spark.catalog.tableExists(db, tbl))
        except Exception:
            return False


def _delete_then_append(
    spark: SparkSession,
    df: DataFrame,
    full_name: str,
    experiment_id: str,
):
    """
    Persist per-experiment results into a shared Iceberg table without
    wiping other experiments.
    - If table missing: create
    - Else: delete experiment_id partition and append
    """
    if not _table_exists(spark, full_name):
        df.writeTo(full_name).using("iceberg").create()
        return

    spark.sql(f"DELETE FROM {full_name} WHERE experiment_id = '{experiment_id}'")
    df.writeTo(full_name).append()


def _get_registry_window(registry_df: DataFrame) -> Tuple[Optional[str], Optional[str]]:
    """
    Tries to infer start/end timestamps from registry, but does NOT require them.
    """
    cols = set(registry_df.columns)
    start_col = None
    end_col = None

    for c in ["start_ts", "start_time", "start_datetime", "start_date", "experiment_start_ts"]:
        if c in cols:
            start_col = c
            break
    for c in ["end_ts", "end_time", "end_datetime", "end_date", "experiment_end_ts"]:
        if c in cols:
            end_col = c
            break

    return start_col, end_col


def _is_pre_metric(metric_name_col):
    # Convention: any metric with "pre_" prefix is pre-period / covariate style.
    return metric_name_col.startswith(F.lit("pre_"))


def _apply_registry_window_if_present(
    joined: DataFrame,
    registry_df: DataFrame,
) -> DataFrame:
    """
    Apply registry window ONLY to non-pre metrics.
    Keep all pre_* metrics unfiltered (covariates).
    """
    start_col, end_col = _get_registry_window(registry_df)
    if not (start_col and end_col):
        return joined

    row = (
        registry_df.select(
            F.col(start_col).cast("timestamp").alias("start_ts"),
            F.col(end_col).cast("timestamp").alias("end_ts"),
        )
        .limit(1)
        .collect()
    )
    if not row:
        return joined

    start_ts, end_ts = row[0]["start_ts"], row[0]["end_ts"]
    if start_ts is None or end_ts is None:
        return joined

    # Keep:
    # - all pre_* metrics
    # - post metrics inside [start_ts, end_ts]
    return joined.where(
        _is_pre_metric(F.col("metric_name"))
        | ((F.col("outcome_ts") >= F.lit(start_ts)) & (F.col("outcome_ts") <= F.lit(end_ts)))
    )


# -------------------------
# Compute
# -------------------------
def compute_overall(spark: SparkSession, catalog: str, namespace: str, experiment_id: str) -> DataFrame:
    exposures = spark.table(f"{catalog}.{namespace}.exposures").where(F.col("experiment_id") == experiment_id)
    outcomes = _load_outcomes(spark, catalog, namespace)


    registry = spark.table(f"{catalog}.{namespace}.experiment_registry").where(F.col("experiment_id") == experiment_id)

    _require_cols(exposures, ["experiment_id", "user_id", "variant"], "exposures")
    _require_cols(outcomes, ["experiment_id", "user_id", "metric_name", "value", "outcome_ts"], "outcomes")

    specs = specs_by_name(default_metric_specs())
    allowed_metrics = list(specs.keys())

    joined = (
        outcomes.join(
            exposures.select("experiment_id", "user_id", "variant"),
            on=["experiment_id", "user_id"],
            how="left",
        )
        .withColumn("day", F.to_date("outcome_ts"))
    )

    # Analysis population: exposed users only (variant not null)
    joined = joined.where(F.col("variant").isNotNull())

    # Apply registry window to post metrics only
    joined = _apply_registry_window_if_present(joined, registry)

    # Keep only metrics in our metric spec list
    joined = joined.where(F.col("metric_name").isin(allowed_metrics))

    overall = (
        joined.groupBy("experiment_id", "variant", "metric_name")
        .agg(
            F.count(F.lit(1)).alias("n"),
            F.mean("value").alias("mean"),
            F.sum("value").alias("sum"),
            F.stddev("value").alias("stddev"),
            F.min("value").alias("min"),
            F.max("value").alias("max"),
        )
        .withColumn("computed_ts", F.current_timestamp())
        .select(
            "experiment_id", "variant", "metric_name",
            "n", "mean", "sum", "stddev", "min", "max",
            "computed_ts",
        )
    )

    return overall


def compute_daily(spark: SparkSession, catalog: str, namespace: str, experiment_id: str) -> DataFrame:
    exposures = spark.table(f"{catalog}.{namespace}.exposures").where(F.col("experiment_id") == experiment_id)
    outcomes = _load_outcomes(spark, catalog, namespace)

    registry = spark.table(f"{catalog}.{namespace}.experiment_registry").where(F.col("experiment_id") == experiment_id)

    _require_cols(exposures, ["experiment_id", "user_id", "variant"], "exposures")
    _require_cols(outcomes, ["experiment_id", "user_id", "metric_name", "value", "outcome_ts"], "outcomes")

    specs = specs_by_name(default_metric_specs())
    allowed_metrics = list(specs.keys())

    joined = (
        outcomes.join(
            exposures.select("experiment_id", "user_id", "variant"),
            on=["experiment_id", "user_id"],
            how="left",
        )
        .withColumn("day", F.to_date("outcome_ts"))
    )

    joined = joined.where(F.col("variant").isNotNull())
    joined = _apply_registry_window_if_present(joined, registry)
    joined = joined.where(F.col("metric_name").isin(allowed_metrics))

    daily = (
        joined.groupBy("experiment_id", "variant", "metric_name", "day")
        .agg(
            F.count(F.lit(1)).alias("n"),
            F.mean("value").alias("mean"),
            F.sum("value").alias("sum"),
        )
        .withColumn("computed_ts", F.current_timestamp())
        .select(
            "experiment_id", "variant", "metric_name", "day",
            "n", "mean", "sum",
            "computed_ts",
        )
    )
    return daily


# -------------------------
# Write
# -------------------------
def write_tables(
    spark: SparkSession,
    experiment_id: str,
    overall_df: Optional[DataFrame],
    daily_df: Optional[DataFrame],
    catalog: str,
    namespace: str,
):
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.{namespace}")

    if overall_df is not None:
        tgt = f"{catalog}.{namespace}.metric_aggregates_overall"
        _delete_then_append(spark, overall_df, tgt, experiment_id)
        print(f"[metrics] upserted {tgt} for experiment_id={experiment_id}")

    if daily_df is not None:
        tgt = f"{catalog}.{namespace}.metric_aggregates_daily"
        _delete_then_append(spark, daily_df, tgt, experiment_id)
        print(f"[metrics] upserted {tgt} for experiment_id={experiment_id}")


# -------------------------
# CLI
# -------------------------
def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--experiment-id", required=True)
    p.add_argument("--catalog", default="iceberg")
    p.add_argument("--namespace", default="exp")
    p.add_argument("--mode", choices=["overall", "daily", "both"], default="both")
    return p.parse_args()


def main():
    args = parse_args()
    spark = build_spark_session(app_name=f"metrics_{args.experiment_id}")
    print("Spark Session Started:")

    overall_df = None
    daily_df = None

    if args.mode in ("overall", "both"):
        overall_df = compute_overall(spark, args.catalog, args.namespace, args.experiment_id)

    if args.mode in ("daily", "both"):
        daily_df = compute_daily(spark, args.catalog, args.namespace, args.experiment_id)

    write_tables(spark, args.experiment_id, overall_df, daily_df, args.catalog, args.namespace)

    # Smoke queries
    print("\nSmoke: overall aggregates")
    spark.sql(f"""
      SELECT metric_name, variant, n, mean
      FROM {args.catalog}.{args.namespace}.metric_aggregates_overall
      WHERE experiment_id = '{args.experiment_id}'
      ORDER BY metric_name, variant
    """).show(truncate=False)

    print("\nSmoke: daily aggregates (first 10 rows)")
    spark.sql(f"""
      SELECT day, metric_name, variant, n, mean
      FROM {args.catalog}.{args.namespace}.metric_aggregates_daily
      WHERE experiment_id = '{args.experiment_id}'
      ORDER BY day, metric_name, variant
      LIMIT 10
    """).show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
