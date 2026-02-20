from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone

from pyspark.sql import functions as F

from exp_platform.spark.session import build_spark_session
from exp_platform.spark.analysis_results import upsert_analysis_results
from exp_platform.memo import init_memo, upsert_stage


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _fmt(x, digits: int = 6) -> str:
    if x is None:
        return "NA"
    try:
        return f"{float(x):.{digits}g}"
    except Exception:
        return str(x)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--experiment-id", required=True)
    ap.add_argument("--catalog", default="exp")
    ap.add_argument("--namespace", default="exp")
    ap.add_argument("--out-table", default="analysis_results")
    ap.add_argument("--target-metric", default="revenue")
    ap.add_argument("--covariate", default="pre_revenue")
    args = ap.parse_args()

    spark = build_spark_session("analyze_experiment_cuped")
    print("Spark Session Started:")

    full_table = f"{args.catalog}.{args.namespace}.{args.out_table}"
    outcomes_tbl = f"{args.catalog}.{args.namespace}.outcomes"
    exposures_tbl = f"{args.catalog}.{args.namespace}.exposures"

    # Join outcomes to exposures so we have variant
    outcomes = (
        spark.table(outcomes_tbl)
        .where(F.col("experiment_id") == args.experiment_id)
        .select(
            "experiment_id", "user_id", "metric_name", "value",
            F.col("ts").alias("outcome_ts"),
        )
    )

    exposures = (
        spark.table(exposures_tbl)
        .where(F.col("experiment_id") == args.experiment_id)
        .select("experiment_id", "user_id", "variant")
    )

    joined = outcomes.join(exposures, ["experiment_id", "user_id"], "inner")

    # pivot user-level target & covariate
    user = (
        joined.where(F.col("metric_name").isin([args.target_metric, args.covariate]))
        .groupBy("experiment_id", "user_id", "variant")
        .pivot("metric_name", [args.target_metric, args.covariate])
        .agg(F.first("value"))
        .dropna(subset=[args.target_metric, args.covariate])
    )

    # CUPED theta = Cov(Y, X)/Var(X) on pooled sample
    stats = (
        user.select(
            F.col(args.target_metric).alias("y"),
            F.col(args.covariate).alias("x"),
        )
        .agg(
            F.avg("x").alias("x_mean"),
            F.var_samp("x").alias("x_var"),
            F.covar_samp("y", "x").alias("yx_covar"),
        )
        .collect()[0]
    )

    x_mean = float(stats["x_mean"])
    x_var = float(stats["x_var"]) if stats["x_var"] is not None else 0.0
    yx_covar = float(stats["yx_covar"]) if stats["yx_covar"] is not None else 0.0
    theta = (yx_covar / x_var) if x_var and x_var > 0 else 0.0

    user_adj = user.withColumn(
        "y_cuped",
        F.col(args.target_metric) - F.lit(theta) * (F.col(args.covariate) - F.lit(x_mean)),
    )

    # group stats per variant on adjusted metric
    g = (
        user_adj.groupBy("variant")
        .agg(
            F.count(F.lit(1)).alias("n"),
            F.avg("y_cuped").alias("mean"),
            F.stddev_samp("y_cuped").alias("std"),
            F.var_samp("y_cuped").alias("var"),
        )
        .collect()
    )
    by = {r["variant"]: r for r in g}

    control = by.get("control")
    treat = by.get("treatment")
    if control is None or treat is None:
        raise ValueError("Need both control and treatment variants for CUPED.")

    n_c = int(control["n"])
    n_t = int(treat["n"])
    mean_c = float(control["mean"])
    mean_t = float(treat["mean"])
    std_c = float(control["std"]) if control["std"] is not None else None
    std_t = float(treat["std"]) if treat["std"] is not None else None
    var_c = float(control["var"]) if control["var"] is not None else 0.0
    var_t = float(treat["var"]) if treat["var"] is not None else 0.0

    delta = mean_t - mean_c
    se = ((var_c / n_c) + (var_t / n_t)) ** 0.5 if n_c > 0 and n_t > 0 else None
    z = (delta / se) if se and se > 0 else None

    # normal approx p-value / CI (keep consistent with AB pipeline style)
    import math

    def norm_cdf(x: float) -> float:
        return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))

    p = None
    ci_low = None
    ci_high = None
    if z is not None:
        p = 2.0 * (1.0 - norm_cdf(abs(z)))
        ci_low = delta - 1.96 * se
        ci_high = delta + 1.96 * se

    rel_lift = (delta / mean_c) if mean_c and mean_c != 0 else None

    generated_at = _utcnow()

    payload = {
        "experiment_id": args.experiment_id,
        "analysis": "cuped",
        "generated_at": generated_at.isoformat(),
        "target_metric": args.target_metric,
        "covariate": args.covariate,
        "theta": theta,
        "x_mean": x_mean,
        "results": [{
            "metric_name": f"{args.target_metric}__cuped",
            "control_n": n_c,
            "treatment_n": n_t,
            "control_mean": mean_c,
            "treatment_mean": mean_t,
            "absolute_lift": delta,
            "relative_lift": rel_lift,
            "stderr": se,
            "z": z,
            "p_value": p,
            "ci95_low": ci_low,
            "ci95_high": ci_high,
            "method": "welch_normal_approx",
        }],
    }
    print(json.dumps(payload, indent=2))

    # ---- Memo (CUPED stage) ----
    memo_path = init_memo(args.experiment_id, confidence=0.95, overwrite=False)

    md = f"""## CUPED (Variance Reduction)

- Covariate: `{args.covariate}`
- Target metric: `{args.target_metric}`

- Theta: `{_fmt(theta, 10)}`
- x_mean: `{_fmt(x_mean)}`

- control_mean (cuped): `{_fmt(mean_c)}`
- treatment_mean (cuped): `{_fmt(mean_t)}`

- delta: `{_fmt(delta)}`
- rel_lift: `{_fmt(rel_lift)}`
- p-value: `{_fmt(p)}`
- ci95: `[{_fmt(ci_low)}, {_fmt(ci_high)}]`

Notes:
- This section is generated by the **CUPED pipeline** (pooled theta; per-user adjustment).
- Interpretation: CUPED tightens variance only when the covariate is correlated with the target metric.
"""

    upsert_stage(memo_path, "CUPED", md)
    print(f"[memo] updated {memo_path} stage=CUPED")

    # Write to analysis_results (NOTE: timestamps are Python datetime, not strings)
    rows = [{
        "experiment_id": args.experiment_id,
        "metric_name": f"{args.target_metric}__cuped",
        "n_control": n_c,
        "mean_control": mean_c,
        "std_control": std_c,
        "n_treatment": n_t,
        "mean_treatment": mean_t,
        "std_treatment": std_t,
        "delta": delta,
        "se": se,
        "z": z,
        "p_value": p,
        "ci_low": ci_low,
        "ci_high": ci_high,
        "rel_lift": rel_lift,
        "computed_ts": generated_at,
        "analysis_ts": generated_at,
    }]
    df_out = spark.createDataFrame(rows)  # let upsert normalize/cast
    upsert_analysis_results(
        spark, full_table, df_out,
        experiment_id=args.experiment_id,
        metric_names=[f"{args.target_metric}__cuped"],
    )

    print(f"[analyze_experiment_cuped] wrote results to {full_table} for experiment_id={args.experiment_id}")
    spark.stop()


if __name__ == "__main__":
    main()
