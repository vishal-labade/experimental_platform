from __future__ import annotations

import argparse
import math
from datetime import datetime, timezonex

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


def _ensure_did_table(spark, full_table: str) -> None:
    """
    Create exp.exp.did_results if it does not exist.
    Schema matches what you printed via DESCRIBE.
    """
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
  experiment_id STRING,
  metric_name   STRING,
  metric_pre    STRING,
  metric_post   STRING,
  start_ts      TIMESTAMP,
  end_ts        TIMESTAMP,
  y_treat_pre   DOUBLE,
  y_treat_post  DOUBLE,
  y_ctrl_pre    DOUBLE,
  y_ctrl_post   DOUBLE,
  did           DOUBLE,
  se            DOUBLE,
  z             DOUBLE,
  p_value       DOUBLE,
  ci_low        DOUBLE,
  ci_high       DOUBLE,
  confidence    DOUBLE,
  analysis_ts   TIMESTAMP
)
USING iceberg
PARTITIONED BY (experiment_id)
""".strip())


def _merge_upsert(spark, full_table: str, df, key_cols: list[str]) -> None:
    """Upsert df into Iceberg table using MERGE, matching on key_cols."""
    tmp = f"_did_updates_{int(_utcnow().timestamp() * 1e6)}"
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


def _window_from_registry_or_exposures(spark, catalog: str, ns: str, experiment_id: str):
    """Best-effort: use registry start/end if present, else fall back to min/max exposure_ts."""
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
    ap.add_argument("--catalog", default="exp")
    ap.add_argument("--namespace", default="exp")
    ap.add_argument("--out-table", default="did_results")
    ap.add_argument("--metric-pre", default="pre_revenue")
    ap.add_argument("--metric-post", default="revenue")
    ap.add_argument("--metric-name", default=None)  # stored in did_results.metric_name
    ap.add_argument("--confidence", type=float, default=0.95)
    args = ap.parse_args()

    metric_name = args.metric_name or args.metric_post

    spark = build_spark_session("analyze_experiment_did")
    print("Spark Session Started:")

    full_table = f"{args.catalog}.{args.namespace}.{args.out_table}"
    outcomes_tbl = f"{args.catalog}.{args.namespace}.outcomes"
    exposures_tbl = f"{args.catalog}.{args.namespace}.exposures"

    # ✅ Create table if needed
    _ensure_did_table(spark, full_table)

    start_ts, end_ts = _window_from_registry_or_exposures(
        spark, args.catalog, args.namespace, args.experiment_id
    )

    exposures = (
        spark.table(exposures_tbl)
        .where(F.col("experiment_id") == args.experiment_id)
        .select("experiment_id", "user_id", "variant")
        .dropDuplicates(["experiment_id", "user_id"])
    )

    outcomes = (
        spark.table(outcomes_tbl)
        .where(F.col("experiment_id") == args.experiment_id)
        .where(F.col("metric_name").isin([args.metric_pre, args.metric_post]))
        .select("experiment_id", "user_id", "metric_name", "value")
    )

    joined = outcomes.join(exposures, ["experiment_id", "user_id"], "inner")

    user = (
        joined.groupBy("experiment_id", "user_id", "variant")
        .pivot("metric_name", [args.metric_pre, args.metric_post])
        .agg(F.first("value"))
        .withColumnRenamed(args.metric_pre, "pre")
        .withColumnRenamed(args.metric_post, "post")
        .dropna(subset=["pre", "post"])
    )

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
    if "control" not in by or "treatment" not in by:
        raise ValueError("Need both control and treatment variants for DiD.")

    c = by["control"]
    t = by["treatment"]

    n_c = int(c["n"])
    n_t = int(t["n"])

    y_ctrl_pre = float(c["pre_mean"])
    y_ctrl_post = float(c["post_mean"])
    y_treat_pre = float(t["pre_mean"])
    y_treat_post = float(t["post_mean"])

    did = (y_treat_post - y_treat_pre) - (y_ctrl_post - y_ctrl_pre)

    c_pre_var = float(c["pre_var"]) if c["pre_var"] is not None else 0.0
    c_post_var = float(c["post_var"]) if c["post_var"] is not None else 0.0
    t_pre_var = float(t["pre_var"]) if t["pre_var"] is not None else 0.0
    t_post_var = float(t["post_var"]) if t["post_var"] is not None else 0.0

    # Conservative normal approx: treat the 4 means as independent
    se = math.sqrt(
        (t_post_var / n_t) + (t_pre_var / n_t) + (c_post_var / n_c) + (c_pre_var / n_c)
    ) if n_c > 0 and n_t > 0 else None

    z = (did / se) if se and se > 0 else None
    p = _p_two_sided_from_z(z)
    ci_low, ci_high = _ci95(did, se)

    analysis_ts = _utcnow()

    memo_path = init_memo(args.experiment_id, confidence=args.confidence, overwrite=False)
    md = f"""## Difference-in-Differences (DiD)

metric_pre: `{args.metric_pre}`
metric_post: `{args.metric_post}`
metric_name (stored): `{metric_name}`

window: `{start_ts}` → `{end_ts}`

### Cell means
- control_pre: `{y_ctrl_pre:.6g}`
- control_post: `{y_ctrl_post:.6g}`
- treatment_pre: `{y_treat_pre:.6g}`
- treatment_post: `{y_treat_post:.6g}`

### Estimate
- did: `{did:.6g}`
- se: `{se if se is not None else "NA"}`
- p-value: `{p if p is not None else "NA"}`
- ci95: `[{ci_low if ci_low is not None else "NA"}, {ci_high if ci_high is not None else "NA"}]`
"""
    upsert_stage(memo_path, "DID", md)
    print(f"[memo] updated {memo_path} stage=DID")

    out_row = [{
        "experiment_id": args.experiment_id,
        "metric_name": metric_name,
        "metric_pre": args.metric_pre,
        "metric_post": args.metric_post,
        "start_ts": start_ts,
        "end_ts": end_ts,
        "y_treat_pre": y_treat_pre,
        "y_treat_post": y_treat_post,
        "y_ctrl_pre": y_ctrl_pre,
        "y_ctrl_post": y_ctrl_post,
        "did": float(did),
        "se": float(se) if se is not None else None,
        "z": float(z) if z is not None else None,
        "p_value": float(p) if p is not None else None,
        "ci_low": float(ci_low) if ci_low is not None else None,
        "ci_high": float(ci_high) if ci_high is not None else None,
        "confidence": float(args.confidence),
        "analysis_ts": analysis_ts,
    }]

    df_out = spark.createDataFrame(out_row)

    _merge_upsert(
        spark,
        full_table,
        df_out,
        key_cols=["experiment_id", "metric_name", "metric_pre", "metric_post"],
    )

    print(f"[analyze_experiment_did] wrote results to {full_table} for experiment_id={args.experiment_id}")
    spark.stop()


if __name__ == "__main__":
    main()
