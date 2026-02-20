from __future__ import annotations

import argparse
import math
from datetime import datetime, timezone

from pyspark.sql import functions as F

from exp_platform.memo import init_memo, upsert_stage
from exp_platform.spark.session import build_spark_session


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _norm_cdf(x: float) -> float:
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def _p_two_sided_from_z(z: float | None) -> float | None:
    if z is None:
        return None
    return 2.0 * (1.0 - _norm_cdf(abs(z)))


def _ci95(est: float, se: float | None) -> tuple[float | None, float | None]:
    if se is None or se <= 0:
        return (None, None)
    return (est - 1.96 * se, est + 1.96 * se)


def _ensure_cuped_table(spark, full_table: str) -> None:
    """
    Create exp.exp.analysis_results_cuped if it does not exist.
    Schema matches what you printed via DESCRIBE.
    """
    # Spark supports tableExists with qualified names for catalog.db.table in most builds.
    # We'll attempt both: direct tableExists and fallback to DESCRIBE try/except.
    exists = False
    try:
        exists = spark.catalog.tableExists(full_table)
    except Exception:
        try:
            spark.sql(f"DESCRIBE {full_table}")
            exists = True
        except Exception:
            exists = False

    if exists:
        return

    spark.sql(f"""
CREATE TABLE IF NOT EXISTS {full_table} (
  experiment_id         STRING,
  metric_name           STRING,
  covariate_name        STRING,
  theta                 DOUBLE,
  x_mean                DOUBLE,
  n_control             BIGINT,
  mean_control_raw      DOUBLE,
  mean_control_cuped    DOUBLE,
  n_treatment           BIGINT,
  mean_treatment_raw    DOUBLE,
  mean_treatment_cuped  DOUBLE,
  delta_raw             DOUBLE,
  se_raw                DOUBLE,
  ci_low_raw            DOUBLE,
  ci_high_raw           DOUBLE,
  p_value_raw           DOUBLE,
  delta_cuped           DOUBLE,
  se_cuped              DOUBLE,
  ci_low_cuped          DOUBLE,
  ci_high_cuped         DOUBLE
)
USING iceberg
PARTITIONED BY (experiment_id)
""".strip())


def _merge_upsert(spark, full_table: str, df, key_cols: list[str]) -> None:
    """Upsert df into Iceberg table using MERGE, matching on key_cols."""
    tmp = f"_cuped_updates_{int(_utcnow().timestamp() * 1e6)}"
    df.createOrReplaceTempView(tmp)

    cols = df.columns
    on_expr = " AND ".join([f"t.{k} = s.{k}" for k in key_cols])
    set_expr = ", ".join([f"t.{c} = s.{c}" for c in cols])
    ins_cols = ", ".join(cols)
    ins_vals = ", ".join([f"s.{c}" for c in cols])

    spark.sql(f"""
MERGE INTO {full_table} t
USING {tmp} s
ON {on_expr}
WHEN MATCHED THEN UPDATE SET {set_expr}
WHEN NOT MATCHED THEN INSERT ({ins_cols}) VALUES ({ins_vals})
""")

    spark.catalog.dropTempView(tmp)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--experiment-id", required=True)
    ap.add_argument("--catalog", default="exp")
    ap.add_argument("--namespace", default="exp")
    ap.add_argument("--out-table", default="analysis_results_cuped")
    ap.add_argument("--target-metric", default="revenue")
    ap.add_argument("--covariate", default="pre_revenue")
    args = ap.parse_args()

    spark = build_spark_session("analyze_experiment_cuped")
    print("Spark Session Started:")

    full_table = f"{args.catalog}.{args.namespace}.{args.out_table}"
    outcomes_tbl = f"{args.catalog}.{args.namespace}.outcomes"
    exposures_tbl = f"{args.catalog}.{args.namespace}.exposures"

    # ✅ Create table if needed
    _ensure_cuped_table(spark, full_table)

    exposures = (
        spark.table(exposures_tbl)
        .where(F.col("experiment_id") == args.experiment_id)
        .select("experiment_id", "user_id", "variant")
        .dropDuplicates(["experiment_id", "user_id"])
    )

    outcomes = (
        spark.table(outcomes_tbl)
        .where(F.col("experiment_id") == args.experiment_id)
        .where(F.col("metric_name").isin([args.target_metric, args.covariate]))
        .select("experiment_id", "user_id", "metric_name", "value")
    )

    joined = outcomes.join(exposures, ["experiment_id", "user_id"], "inner")

    # user-level x (covariate) and y (target)
    user = (
        joined.groupBy("experiment_id", "user_id", "variant")
        .pivot("metric_name", [args.target_metric, args.covariate])
        .agg(F.first("value"))
        .withColumnRenamed(args.target_metric, "y")
        .withColumnRenamed(args.covariate, "x")
        .dropna(subset=["x", "y"])
    )

    # pooled theta
    pooled = user.agg(
        F.avg("x").alias("x_mean"),
        F.var_samp("x").alias("x_var"),
        F.covar_samp("y", "x").alias("yx_covar"),
    ).collect()[0]

    x_mean = float(pooled["x_mean"]) if pooled["x_mean"] is not None else 0.0
    x_var = float(pooled["x_var"]) if pooled["x_var"] is not None else 0.0
    yx_covar = float(pooled["yx_covar"]) if pooled["yx_covar"] is not None else 0.0
    theta = (yx_covar / x_var) if x_var > 0 else 0.0

    user = user.withColumn("y_cuped", F.col("y") - F.lit(theta) * (F.col("x") - F.lit(x_mean)))

    def _group_stats(col: str):
        return (
            user.groupBy("variant")
            .agg(
                F.count(F.lit(1)).alias("n"),
                F.avg(col).alias("mean"),
                F.var_samp(col).alias("var"),
            )
            .collect()
        )

    raw_stats = {r["variant"]: r for r in _group_stats("y")}
    cuped_stats = {r["variant"]: r for r in _group_stats("y_cuped")}

    if "control" not in raw_stats or "treatment" not in raw_stats:
        raise ValueError("Need both control and treatment variants for CUPED.")

    n_c = int(raw_stats["control"]["n"])
    n_t = int(raw_stats["treatment"]["n"])

    mean_c_raw = float(raw_stats["control"]["mean"])
    mean_t_raw = float(raw_stats["treatment"]["mean"])
    var_c_raw = float(raw_stats["control"]["var"]) if raw_stats["control"]["var"] is not None else 0.0
    var_t_raw = float(raw_stats["treatment"]["var"]) if raw_stats["treatment"]["var"] is not None else 0.0

    mean_c_cuped = float(cuped_stats["control"]["mean"])
    mean_t_cuped = float(cuped_stats["treatment"]["mean"])
    var_c_cuped = float(cuped_stats["control"]["var"]) if cuped_stats["control"]["var"] is not None else 0.0
    var_t_cuped = float(cuped_stats["treatment"]["var"]) if cuped_stats["treatment"]["var"] is not None else 0.0

    # Raw inference
    delta_raw = mean_t_raw - mean_c_raw
    se_raw = math.sqrt((var_c_raw / n_c) + (var_t_raw / n_t)) if n_c > 0 and n_t > 0 else None
    z_raw = (delta_raw / se_raw) if se_raw and se_raw > 0 else None
    p_raw = _p_two_sided_from_z(z_raw)
    ci_low_raw, ci_high_raw = _ci95(delta_raw, se_raw)

    # CUPED inference
    delta_cuped = mean_t_cuped - mean_c_cuped
    se_cuped = math.sqrt((var_c_cuped / n_c) + (var_t_cuped / n_t)) if n_c > 0 and n_t > 0 else None
    z_cuped = (delta_cuped / se_cuped) if se_cuped and se_cuped > 0 else None
    p_cuped = _p_two_sided_from_z(z_cuped)  # schema has no p_value_cuped column
    ci_low_cuped, ci_high_cuped = _ci95(delta_cuped, se_cuped)

    # Memo stage
    memo_path = init_memo(args.experiment_id, confidence=0.95, overwrite=False)
    md = f"""## CUPED (Variance Reduction)

Target metric: `{args.target_metric}`
Covariate: `{args.covariate}`

- theta: `{theta:.10g}`
- x_mean: `{x_mean:.6g}`

### Raw (no CUPED)
- control mean: `{mean_c_raw:.6g}`
- treatment mean: `{mean_t_raw:.6g}`
- delta: `{delta_raw:.6g}`
- p-value: `{p_raw if p_raw is not None else "NA"}`
- ci95: `[{ci_low_raw if ci_low_raw is not None else "NA"}, {ci_high_raw if ci_high_raw is not None else "NA"}]`

### CUPED-adjusted
- control mean: `{mean_c_cuped:.6g}`
- treatment mean: `{mean_t_cuped:.6g}`
- delta: `{delta_cuped:.6g}`
- p-value: `{p_cuped if p_cuped is not None else "NA"}`  (not stored in Iceberg schema)
- ci95: `[{ci_low_cuped if ci_low_cuped is not None else "NA"}, {ci_high_cuped if ci_high_cuped is not None else "NA"}]`
"""
    upsert_stage(memo_path, "CUPED", md)
    print(f"[memo] updated {memo_path} stage=CUPED")

    # Write exactly the CUPED schema you showed
    out_row = [{
        "experiment_id": args.experiment_id,
        "metric_name": args.target_metric,
        "covariate_name": args.covariate,
        "theta": float(theta),
        "x_mean": float(x_mean),

        "n_control": n_c,
        "mean_control_raw": float(mean_c_raw),
        "mean_control_cuped": float(mean_c_cuped),

        "n_treatment": n_t,
        "mean_treatment_raw": float(mean_t_raw),
        "mean_treatment_cuped": float(mean_t_cuped),

        "delta_raw": float(delta_raw),
        "se_raw": float(se_raw) if se_raw is not None else None,
        "ci_low_raw": float(ci_low_raw) if ci_low_raw is not None else None,
        "ci_high_raw": float(ci_high_raw) if ci_high_raw is not None else None,
        "p_value_raw": float(p_raw) if p_raw is not None else None,

        "delta_cuped": float(delta_cuped),
        "se_cuped": float(se_cuped) if se_cuped is not None else None,
        "ci_low_cuped": float(ci_low_cuped) if ci_low_cuped is not None else None,
        "ci_high_cuped": float(ci_high_cuped) if ci_high_cuped is not None else None,
    }]

    df_out = spark.createDataFrame(out_row)

    _merge_upsert(
        spark,
        full_table,
        df_out,
        key_cols=["experiment_id", "metric_name", "covariate_name"],
    )

    print(f"[analyze_experiment_cuped] wrote results to {full_table} for experiment_id={args.experiment_id}")
    spark.stop()


if __name__ == "__main__":
    main()
