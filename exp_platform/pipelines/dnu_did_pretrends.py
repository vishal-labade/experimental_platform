from __future__ import annotations

import argparse
import math
from pathlib import Path

import numpy as np
import pandas as pd

from pyspark.sql import functions as F, types as T

from exp_platform.spark.session import build_spark_session


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--experiment-id", required=True)
    p.add_argument("--metric", default="revenue")  # post metric for pre-trend diagnostics
    p.add_argument("--catalog", default="iceberg")
    p.add_argument("--namespace", default="exp")
    p.add_argument("--out-namespace", default="exp")
    p.add_argument("--memo-dir", default="data/memos")

    # pre/post event-study window in time buckets
    p.add_argument("--grain", choices=["day", "week"], default="day")
    p.add_argument("--pre-buckets", type=int, default=28)
    p.add_argument("--post-buckets", type=int, default=28)

    # diagnostics thresholds
    p.add_argument("--alpha-fail", type=float, default=0.05)
    p.add_argument("--alpha-warn", type=float, default=0.10)

    # optional plotting (runs on driver)
    p.add_argument("--no-plots", action="store_true")

    return p.parse_args()


def _get_window(registry_df):
    cols = set(registry_df.columns)
    cand = [
        ("start_ts", "end_ts"),
        ("start_time", "end_time"),
        ("experiment_start_ts", "experiment_end_ts"),
    ]
    for s, e in cand:
        if s in cols and e in cols:
            row = (
                registry_df.select(
                    F.col(s).cast("timestamp").alias("start_ts"),
                    F.col(e).cast("timestamp").alias("end_ts"),
                )
                .limit(1)
                .collect()
            )
            if row:
                return row[0]["start_ts"], row[0]["end_ts"]
    return None, None


def _bucket_ts(col_ts, grain: str):
    if grain == "day":
        return F.date_trunc("day", col_ts)
    if grain == "week":
        return F.date_trunc("week", col_ts)
    raise ValueError(f"Unsupported grain: {grain}")


def _event_time_k(bucket_ts_col, start_ts_lit, grain: str):
    if grain == "day":
        return F.datediff(bucket_ts_col, start_ts_lit)
    if grain == "week":
        return F.floor(F.datediff(bucket_ts_col, start_ts_lit) / F.lit(7))
    raise ValueError(f"Unsupported grain: {grain}")


def _norm_cdf(x: float) -> float:
    # standard normal CDF via erf
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def _ols_hc1(X: np.ndarray, y: np.ndarray):
    """
    OLS with HC1 robust covariance (statsmodels-like).
    Returns:
      beta (p,), se (p,), z (p,), pval (p,), cov (p,p)
    """
    X = np.asarray(X, dtype=float)
    y = np.asarray(y, dtype=float)

    n, p = X.shape
    if n <= p:
        raise ValueError(f"Not enough rows for OLS: n={n}, p={p}")

    # beta = (X'X)^-1 X'y  (use lstsq for stability)
    beta, *_ = np.linalg.lstsq(X, y, rcond=None)

    # residuals
    e = y - X @ beta

    # (X'X)^-1
    XtX_inv = np.linalg.inv(X.T @ X)

    # Meat: X' diag(e^2) X
    # Efficiently: sum_i e_i^2 * x_i x_i'
    # => X.T @ (X * e^2[:,None])
    Xe2 = X * (e * e)[:, None]
    meat = X.T @ Xe2

    # HC1 scaling: n/(n-p)
    scale = n / (n - p)
    cov = scale * (XtX_inv @ meat @ XtX_inv)

    se = np.sqrt(np.maximum(np.diag(cov), 0.0))
    z = np.full_like(se, np.nan, dtype=float)
    pval = np.full_like(se, np.nan, dtype=float)

    for i in range(p):
        if se[i] > 0:
            z[i] = beta[i] / se[i]
            pval[i] = 2.0 * (1.0 - _norm_cdf(abs(z[i])))

    return beta, se, z, pval, cov


def main():
    args = parse_args()
    spark = build_spark_session(app_name=f"did_pretrends_{args.experiment_id}")
    print("Spark Session Started:")

    exposures = (
        spark.table(f"{args.catalog}.{args.namespace}.exposures")
        .where(F.col("experiment_id") == args.experiment_id)
        .select("experiment_id", "user_id", "variant")
    )

    registry = (
        spark.table(f"{args.catalog}.{args.namespace}.experiment_registry")
        .where(F.col("experiment_id") == args.experiment_id)
    )

    start_ts, end_ts = _get_window(registry)
    if start_ts is None or end_ts is None:
        raise ValueError("Registry is missing start/end timestamps required for event window.")

    outcomes = (
        spark.table(f"{args.catalog}.{args.namespace}.outcomes")
        .where(F.col("experiment_id") == args.experiment_id)
        .where(F.col("metric_name") == args.metric)
        .select("experiment_id", "user_id", "metric_name", "value", "outcome_ts")
    )

    # window around start_ts for event time
    start_lit = F.lit(start_ts).cast("timestamp")

    if args.grain == "day":
        pre_start = F.expr(f"timestamp('{start_ts}') - INTERVAL {args.pre_buckets} DAYS")
        post_end = F.expr(f"timestamp('{start_ts}') + INTERVAL {args.post_buckets} DAYS")
    else:
        pre_start = F.expr(f"timestamp('{start_ts}') - INTERVAL {args.pre_buckets} WEEKS")
        post_end = F.expr(f"timestamp('{start_ts}') + INTERVAL {args.post_buckets} WEEKS")

    scoped = outcomes.where((F.col("outcome_ts") >= pre_start) & (F.col("outcome_ts") <= post_end))

    buck = (
        scoped.join(exposures, on=["experiment_id", "user_id"], how="inner")
        .withColumn("bucket_ts", _bucket_ts(F.col("outcome_ts"), args.grain))
        .withColumn("k", _event_time_k(F.col("bucket_ts"), start_lit, args.grain))
        .select("experiment_id", "metric_name", "variant", "user_id", "bucket_ts", "k", "value")
    )

    # per-user-per-bucket aggregation
    per_user_bucket = (
        buck.groupBy("experiment_id", "metric_name", "variant", "bucket_ts", "k", "user_id")
        .agg(F.mean("value").alias("y"))
    )

    # cell means
    cell = (
        per_user_bucket.groupBy("experiment_id", "metric_name", "variant", "bucket_ts", "k")
        .agg(
            F.count("*").alias("n_users"),
            F.mean("y").alias("mean_y"),
            F.stddev("y").alias("std_y"),
        )
        .withColumn("se_mean", F.col("std_y") / F.sqrt(F.col("n_users")))
    )

    # pivot mean_y per variant per bucket
    pivot = (
        cell.groupBy("experiment_id", "metric_name", "bucket_ts", "k")
        .pivot("variant", ["control", "treatment"])
        .agg(F.first("mean_y"))
    )

    # Spark delta (keep timestamps in Spark for Iceberg write)
    delta_for_write = (
        pivot.where(F.col("control").isNotNull() & F.col("treatment").isNotNull())
        .withColumn("delta", F.col("treatment") - F.col("control"))
        .select("experiment_id", "metric_name", "bucket_ts", "k", "delta")
        .orderBy("k")
    )

    # Driver delta (NO timestamps -> avoid pandas datetime casting issues)
    delta_for_driver = (
        delta_for_write
        .select("k", "delta")
        .orderBy("k")
    )

    delta_pdf = delta_for_driver.toPandas()
    if delta_pdf.empty:
        raise ValueError("No bucket-level data found for pretrend diagnostics. Check metric/time coverage.")

    # -----------------------------
    # Driver-side event-study (no statsmodels)
    # -----------------------------
    ref_k = -1

    ks = delta_pdf["k"].astype(int).to_numpy()
    unique_ks = sorted(set(int(x) for x in ks.tolist()))
    used_ks = [k for k in unique_ks if k != ref_k]

    # Design matrix: [const | 1[k==used_ks[0]] | ... ]
    X = np.zeros((len(delta_pdf), 1 + len(used_ks)), dtype=float)
    X[:, 0] = 1.0
    for j, k in enumerate(used_ks, start=1):
        X[:, j] = (ks == k).astype(float)

    y = delta_pdf["delta"].astype(float).to_numpy()

    beta, se, z, pval, _cov = _ols_hc1(X, y)

    # Build coefficient table for non-constant terms only
    rows = []
    for j, k in enumerate(used_ks, start=1):
        b = float(beta[j])
        s = float(se[j])
        p = float(pval[j]) if not np.isnan(pval[j]) else float("nan")
        rows.append({
            "k": int(k),
            "beta": b,
            "se": s,
            "p_value": p,
            "ci_low": b - 1.96 * s,
            "ci_high": b + 1.96 * s,
        })
    est = pd.DataFrame(rows).sort_values("k")

    # Pretrend verdict logic (simple + robust, no joint chi-square dependency):
    # FAIL if any lead p < alpha_fail
    # WARN if any lead p < alpha_warn
    lead_est = est[est["k"] < 0].copy()
    sig_leads_fail = lead_est[lead_est["p_value"] < args.alpha_fail]
    sig_leads_warn = lead_est[lead_est["p_value"] < args.alpha_warn]

    verdict = "PASS"
    if len(sig_leads_fail) > 0:
        verdict = "FAIL"
    elif len(sig_leads_warn) > 0:
        verdict = "WARN"

    print("\n[pretrends] delta (treatment - control) by k:")
    print(delta_pdf.head(12).to_string(index=False))

    print("\n[pretrends] event-study betas on delta (ref k=-1):")
    print(est.head(40).to_string(index=False))

    print(f"\n[pretrends] verdict={verdict} "
          f"(FAIL if any lead p<{args.alpha_fail}; WARN if any lead p<{args.alpha_warn})")

    # Optional plots
    if not args.no_plots:
        import matplotlib.pyplot as plt

        plt.figure()
        plt.plot(delta_pdf["k"], delta_pdf["delta"], marker="o")
        plt.axvline(0, linestyle="--")
        plt.axhline(0, linestyle="--")
        plt.title(f"Pretrend diagnostic: delta = treatment - control ({args.metric})")
        plt.xlabel("event time k (buckets from start)")
        plt.ylabel("delta")
        plt.tight_layout()
        plt.show()

        plt.figure()
        plt.errorbar(est["k"], est["beta"], yerr=1.96 * est["se"], fmt="o")
        plt.axhline(0, linestyle="--")
        plt.axvline(ref_k, linestyle="--")
        plt.axvline(0, linestyle=":")
        plt.title("Event study on delta (leads/lags); leads should be ~0")
        plt.xlabel("k (relative to start)")
        plt.ylabel("beta_k (vs ref k=-1)")
        plt.tight_layout()
        plt.show()

    # -----------------------------
    # Write artifacts to Iceberg
    # -----------------------------
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {args.catalog}.{args.out_namespace}")

    # 1) delta series (write from Spark, no pandas timestamps)
    delta_tbl = f"{args.catalog}.{args.out_namespace}.did_pretrend_delta"
    delta_out = (
        delta_for_write
        .withColumn("grain", F.lit(args.grain))
        .withColumn("analysis_ts", F.current_timestamp())
        .select("experiment_id", "metric_name", "grain", "bucket_ts", "k", "delta", "analysis_ts")
    )
    delta_out.writeTo(delta_tbl).using("iceberg").createOrReplace()
    print(f"[pretrends] wrote {delta_tbl}")

    # 2) event-study coefficients (driver->Spark)
    coef_schema = T.StructType([
        T.StructField("experiment_id", T.StringType(), False),
        T.StructField("metric_name", T.StringType(), False),
        T.StructField("grain", T.StringType(), False),
        T.StructField("k", T.IntegerType(), False),
        T.StructField("beta", T.DoubleType(), False),
        T.StructField("se", T.DoubleType(), False),
        T.StructField("p_value", T.DoubleType(), False),
        T.StructField("ci_low", T.DoubleType(), False),
        T.StructField("ci_high", T.DoubleType(), False),
        T.StructField("ref_k", T.IntegerType(), False),
    ])
    coef_out = spark.createDataFrame(
        [{
            "experiment_id": args.experiment_id,
            "metric_name": args.metric,
            "grain": args.grain,
            "k": int(r["k"]),
            "beta": float(r["beta"]),
            "se": float(r["se"]),
            "p_value": float(r["p_value"]),
            "ci_low": float(r["ci_low"]),
            "ci_high": float(r["ci_high"]),
            "ref_k": int(ref_k),
        } for _, r in est.iterrows()],
        schema=coef_schema
    ).withColumn("analysis_ts", F.current_timestamp())

    coef_tbl = f"{args.catalog}.{args.out_namespace}.did_pretrend_event_study"
    coef_out.writeTo(coef_tbl).using("iceberg").createOrReplace()
    print(f"[pretrends] wrote {coef_tbl}")

    # 3) verdict table (no joint stat/p-value to avoid extra deps)
    lead_ks = [int(k) for k in est["k"].tolist() if int(k) < 0]
    test_schema = T.StructType([
        T.StructField("experiment_id", T.StringType(), False),
        T.StructField("metric_name", T.StringType(), False),
        T.StructField("grain", T.StringType(), False),
        T.StructField("pre_buckets", T.IntegerType(), False),
        T.StructField("post_buckets", T.IntegerType(), False),
        T.StructField("alpha_fail", T.DoubleType(), False),
        T.StructField("alpha_warn", T.DoubleType(), False),
        T.StructField("joint_stat", T.DoubleType(), True),
        T.StructField("joint_p_value", T.DoubleType(), True),
        T.StructField("num_leads", T.IntegerType(), False),
        T.StructField("num_sig_leads_0p05", T.IntegerType(), False),
        T.StructField("verdict", T.StringType(), False),
    ])
    test_out = spark.createDataFrame([{
        "experiment_id": args.experiment_id,
        "metric_name": args.metric,
        "grain": args.grain,
        "pre_buckets": int(args.pre_buckets),
        "post_buckets": int(args.post_buckets),
        "alpha_fail": float(args.alpha_fail),
        "alpha_warn": float(args.alpha_warn),
        "joint_stat": None,
        "joint_p_value": None,
        "num_leads": int(len(lead_ks)),
        "num_sig_leads_0p05": int(len(sig_leads_fail)),
        "verdict": verdict,
    }], schema=test_schema).withColumn("analysis_ts", F.current_timestamp())

    test_tbl = f"{args.catalog}.{args.out_namespace}.did_pretrend_tests"
    test_out.writeTo(test_tbl).using("iceberg").createOrReplace()
    print(f"[pretrends] wrote {test_tbl}")

    # -----------------------------
    # Append memo section
    # -----------------------------
    memo_path = Path(args.memo_dir) / f"{args.experiment_id}.md"
    if memo_path.exists():
        add = []
        add.append("\n---\n## DiD Pre-trend Diagnostics (Chunk 6.2)\n")
        add.append(f"- Metric: `{args.metric}`\n")
        add.append(f"- Grain: `{args.grain}`\n")
        add.append(f"- Window: pre `{args.pre_buckets}` buckets, post `{args.post_buckets}` buckets (relative to start)\n")
        add.append(f"- Reference bucket: `k={ref_k}`\n")
        add.append(f"- Verdict: `{verdict}` (FAIL if any lead p<{args.alpha_fail}; WARN if any lead p<{args.alpha_warn})\n")
        memo_path.write_text(memo_path.read_text() + "\n".join(add))
        print(f"[memo] appended pretrend section to {memo_path}")

    spark.stop()


if __name__ == "__main__":
    main()
