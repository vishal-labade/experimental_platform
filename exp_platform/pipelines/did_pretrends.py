from __future__ import annotations

import argparse
from datetime import datetime, timezone

from pyspark.sql import functions as F

from exp_platform.spark.session import build_spark_session
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


def _window_from_registry_or_exposures(spark, catalog: str, ns: str, experiment_id: str):
    reg_tbl = f"{catalog}.{ns}.experiment_registry"
    exp_tbl = f"{catalog}.{ns}.exposures"

    start_ts = None
    end_ts = None
    try:
        reg = spark.table(reg_tbl).where(F.col("experiment_id") == experiment_id)
        cols = set(reg.columns)
        if "start_ts" in cols and "end_ts" in cols:
            r = reg.select("start_ts", "end_ts").limit(1).collect()
            if r:
                start_ts, end_ts = r[0]["start_ts"], r[0]["end_ts"]
    except Exception:
        pass

    if start_ts is None or end_ts is None:
        r = (
            spark.table(exp_tbl)
            .where(F.col("experiment_id") == experiment_id)
            .agg(F.min("exposure_ts").alias("min_ts"), F.max("exposure_ts").alias("max_ts"))
            .collect()[0]
        )
        start_ts, end_ts = r["min_ts"], r["max_ts"]

    if start_ts is None or end_ts is None:
        raise ValueError("Could not determine window (registry missing and exposures empty).")

    return start_ts, end_ts


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--experiment-id", required=True)
    ap.add_argument("--catalog", default="iceberg")
    ap.add_argument("--namespace", default="exp")
    ap.add_argument("--metric-pre", default="pre_revenue")
    args = ap.parse_args()

    spark = build_spark_session("did_pretrends")
    print("Spark Session Started:")

    outcomes_tbl = f"{args.catalog}.{args.namespace}.outcomes"
    exposures_tbl = f"{args.catalog}.{args.namespace}.exposures"

    window_start, window_end = _window_from_registry_or_exposures(
        spark, args.catalog, args.namespace, args.experiment_id
    )

    exposures = (
        spark.table(exposures_tbl)
        .where(F.col("experiment_id") == args.experiment_id)
        .select("experiment_id", "user_id", "variant", "exposure_ts")
    )

    outcomes = (
        spark.table(outcomes_tbl)
        .where(F.col("experiment_id") == args.experiment_id)
        .select(
            "experiment_id", "user_id", "metric_name", "value",
            F.col("ts").alias("outcome_ts"),
        )
    )

    joined = (
        outcomes.join(exposures, ["experiment_id", "user_id"], "inner")
        .where(F.col("metric_name") == args.metric_pre)
        .withColumn("k_days", F.datediff(F.col("outcome_ts"), F.col("exposure_ts")))
        .where(F.col("k_days") < 0)
    )

    memo_path = init_memo(args.experiment_id, confidence=0.95, overwrite=False)

    distinct_k = joined.select("k_days").distinct().count()
    if distinct_k < 3:
        print("\n[pretrends] pre-slope test (k<0 only):")
        print("  slope=nan  se=nan  p=nan  verdict=INSUFFICIENT_DATA")

        md = f"""## DiD Pre-trend Diagnostics

- Metric: `{args.metric_pre}`
- Grain: `k_days` (relative to exposure, pre period only)
- Window start: `{window_start}`
- Window end: `{window_end}`

- distinct pre points: `{distinct_k}`
- Verdict: **INSUFFICIENT_DATA**

Notes:
- Need ≥ 3 distinct pre-period time points to fit a slope on (treatment-control) deltas.
"""
        upsert_stage(memo_path, "PRETRENDS", md)
        print(f"[memo] updated {memo_path} stage=PRETRENDS")

        spark.stop()
        return

    daily = (
        joined.groupBy("k_days", "variant")
        .agg(F.avg("value").alias("mean"))
    )

    delta = (
        daily.groupBy("k_days")
        .agg(
            F.max(F.when(F.col("variant") == "treatment", F.col("mean"))).alias("treat_mean"),
            F.max(F.when(F.col("variant") == "control", F.col("mean"))).alias("control_mean"),
        )
        .withColumn("delta", F.col("treat_mean") - F.col("control_mean"))
        .dropna(subset=["delta"])
    )

    agg = (
        delta.agg(
            F.count(F.lit(1)).alias("n"),
            F.avg("k_days").alias("xbar"),
            F.avg("delta").alias("ybar"),
            F.avg(F.col("k_days") * F.col("delta")).alias("xybar"),
            F.avg(F.col("k_days") * F.col("k_days")).alias("x2bar"),
            F.avg(F.col("delta") * F.col("delta")).alias("y2bar"),
        ).collect()[0]
    )

    n = int(agg["n"])
    xbar = float(agg["xbar"])
    ybar = float(agg["ybar"])
    xybar = float(agg["xybar"])
    x2bar = float(agg["x2bar"])
    y2bar = float(agg["y2bar"])

    varx = x2bar - xbar * xbar
    covxy = xybar - xbar * ybar
    if varx <= 0 or n < 3:
        print("\n[pretrends] pre-slope test (k<0 only):")
        print("  slope=nan  se=nan  p=nan  verdict=INSUFFICIENT_DATA")

        md = f"""## DiD Pre-trend Diagnostics

- Metric: `{args.metric_pre}`
- Grain: `k_days` (relative to exposure, pre period only)
- Window start: `{window_start}`
- Window end: `{window_end}`

- n pre points: `{n}`
- var(x): `{_fmt(varx)}`
- Verdict: **INSUFFICIENT_DATA**
"""
        upsert_stage(memo_path, "PRETRENDS", md)
        print(f"[memo] updated {memo_path} stage=PRETRENDS")

        spark.stop()
        return

    slope = covxy / varx

    a = ybar - slope * xbar
    resid2 = y2bar - 2*slope*xybar - 2*a*ybar + (slope*slope)*x2bar + 2*a*slope*xbar + a*a
    sigma2 = max(resid2, 0.0) * (n / max(n - 2, 1))
    se = (sigma2 / (n * varx)) ** 0.5 if sigma2 > 0 else 0.0

    import math
    z = slope / se if se > 0 else float("inf")
    p = 2.0 * (1.0 - 0.5 * (1.0 + math.erf(abs(z) / math.sqrt(2.0))))

    verdict = "PASS" if (p is not None and p > 0.05) else "FAIL"

    print("\n[pretrends] pre-slope test (k<0 only):")
    print(f"  slope={slope:.8g}  se={se:.8g}  p={p:.6g}  verdict={verdict}")

    md = f"""## DiD Pre-trend Diagnostics

- Metric: `{args.metric_pre}`
- Grain: `k_days` (relative to exposure, pre period only)
- Window start: `{window_start}`
- Window end: `{window_end}`

- num_pre_points: `{n}`
- slope(treatment-control): `{_fmt(slope, 10)}`
- se: `{_fmt(se, 10)}`
- p-value: `{_fmt(p)}`
- Verdict: **{verdict}**

Interpretation:
- PASS means we do **not** detect evidence of a linear pre-trend in treatment-control deltas (at α=0.05).
- This is a diagnostic, not a proof; inspect plots / segments for higher assurance in real experiments.
"""
    upsert_stage(memo_path, "PRETRENDS", md)
    print(f"[memo] updated {memo_path} stage=PRETRENDS")

    spark.stop()


if __name__ == "__main__":
    main()
