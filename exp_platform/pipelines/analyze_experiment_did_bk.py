from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone

from pyspark.sql import functions as F

from exp_platform.spark.session import build_spark_session
from exp_platform.spark.analysis_results import ANALYSIS_RESULTS_SCHEMA, upsert_analysis_results
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

    # Try registry (support either timestamps or dates if present)
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

    # Fallback to exposures range
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
    ap.add_argument("--out-table", default="analysis_results")
    ap.add_argument("--metric-pre", default="pre_revenue")
    ap.add_argument("--metric-post", default="revenue")
    args = ap.parse_args()

    spark = build_spark_session("analyze_experiment_did")
    print("Spark Session Started:")

    full_table = f"{args.catalog}.{args.namespace}.{args.out_table}"
    outcomes_tbl = f"{args.catalog}.{args.namespace}.outcomes"
    exposures_tbl = f"{args.catalog}.{args.namespace}.exposures"

    window_start, window_end = _window_from_registry_or_exposures(
        spark, args.catalog, args.namespace, args.experiment_id
    )

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
        .select("experiment_id", "user_id", "variant", "exposure_ts")
    )

    joined = outcomes.join(exposures, ["experiment_id", "user_id"], "inner")

    # Build user-level pre/post
    user = (
        joined.where(F.col("metric_name").isin([args.metric_pre, args.metric_post]))
        .groupBy("experiment_id", "user_id", "variant")
        .pivot("metric_name", [args.metric_pre, args.metric_post])
        .agg(F.first("value"))
        .dropna(subset=[args.metric_pre, args.metric_post])
        .withColumnRenamed(args.metric_pre, "pre")
        .withColumnRenamed(args.metric_post, "post")
    )

    # DiD on means: (T_post - T_pre) - (C_post - C_pre)
    cells = (
        user.groupBy("variant")
        .agg(
            F.count(F.lit(1)).alias("n"),
            F.avg("pre").alias("pre_mean"),
            F.avg("post").alias("post_mean"),
            F.var_samp("pre").alias("pre_var"),
            F.var_samp("post").alias("post_var"),
        )
        .collect()
    )
    by = {r["variant"]: r for r in cells}
    c = by.get("control")
    t = by.get("treatment")
    if c is None or t is None:
        raise ValueError("Need both control and treatment variants for DiD.")

    n_c = int(c["n"])
    n_t = int(t["n"])
    c_pre = float(c["pre_mean"])
    c_post = float(c["post_mean"])
    t_pre = float(t["pre_mean"])
    t_post = float(t["post_mean"])

    did = (t_post - t_pre) - (c_post - c_pre)

    # Conservative SE (independent normal approx on 4 means)
    c_pre_var = float(c["pre_var"]) if c["pre_var"] is not None else 0.0
    c_post_var = float(c["post_var"]) if c["post_var"] is not None else 0.0
    t_pre_var = float(t["pre_var"]) if t["pre_var"] is not None else 0.0
    t_post_var = float(t["post_var"]) if t["post_var"] is not None else 0.0

    import math
    se = math.sqrt(
        (t_post_var / n_t) + (t_pre_var / n_t) + (c_post_var / n_c) + (c_pre_var / n_c)
    ) if n_c > 0 and n_t > 0 else None
    z = (did / se) if se and se > 0 else None

    def norm_cdf(x: float) -> float:
        return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))

    p = None
    ci_low = None
    ci_high = None
    if z is not None:
        p = 2.0 * (1.0 - norm_cdf(abs(z)))
        ci_low = did - 1.96 * se
        ci_high = did + 1.96 * se

    rel_lift = (did / c_post) if c_post and c_post != 0 else None

    generated_at = _utcnow()

    payload = {
        "experiment_id": args.experiment_id,
        "analysis": "did",
        "generated_at": generated_at.isoformat(),
        "metric_pre": args.metric_pre,
        "metric_post": args.metric_post,
        "window_start": str(window_start),
        "window_end": str(window_end),
        "cells": {
            "control_pre": c_pre,
            "control_post": c_post,
            "treatment_pre": t_pre,
            "treatment_post": t_post,
        },
        "did": {
            "did": did,
            "se": se,
            "z": z,
            "p_value": p,
            "ci_low": ci_low,
            "ci_high": ci_high,
            "confidence": 0.95,
        },
    }
    print(json.dumps(payload, indent=2))

    # ---- Memo (DID stage) ----
    memo_path = init_memo(args.experiment_id, confidence=0.95, overwrite=False)

    md = f"""## Difference-in-Differences (DiD)

- Metric pre: `{args.metric_pre}`
- Metric post: `{args.metric_post}`

- Window start: `{window_start}`
- Window end: `{window_end}`

### Cell means
- control_pre: `{_fmt(c_pre)}`
- control_post: `{_fmt(c_post)}`
- treatment_pre: `{_fmt(t_pre)}`
- treatment_post: `{_fmt(t_post)}`

### Estimate
- did: `{_fmt(did)}`
- se: `{_fmt(se)}`
- z: `{_fmt(z)}`
- p-value: `{_fmt(p)}`
- ci95: `[{_fmt(ci_low)}, {_fmt(ci_high)}]`
- rel_lift (vs control post): `{_fmt(rel_lift)}`

Notes:
- This section is generated by the **DiD pipeline** using a conservative SE (independent 4-mean approximation).
- Use DiD when randomization is imperfect or rollout is staggered; validate pre-trends separately.
"""

    upsert_stage(memo_path, "DID", md)
    print(f"[memo] updated {memo_path} stage=DID")

    metric_name = f"{args.metric_post}__did"
    rows = [{
        "experiment_id": args.experiment_id,
        "metric_name": metric_name,
        "n_control": n_c,
        "mean_control": c_post,
        "std_control": None,
        "n_treatment": n_t,
        "mean_treatment": t_post,
        "std_treatment": None,
        "delta": did,
        "se": se,
        "z": z,
        "p_value": p,
        "ci_low": ci_low,
        "ci_high": ci_high,
        "rel_lift": rel_lift,
        "computed_ts": generated_at,
        "analysis_ts": generated_at,
    }]
    df_out = spark.createDataFrame(rows, schema=ANALYSIS_RESULTS_SCHEMA)

    upsert_analysis_results(
        spark, full_table, df_out,
        experiment_id=args.experiment_id,
        metric_names=[metric_name],
    )

    print(f"[analyze_experiment_did] wrote results to {full_table} for experiment_id={args.experiment_id}")
    spark.stop()


if __name__ == "__main__":
    main()
