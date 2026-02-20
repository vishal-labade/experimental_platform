from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from typing import Any, Dict, List

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, LongType, DoubleType, TimestampType
)

from exp_platform.spark.session import build_spark_session
from exp_platform.stats.ab_inference import ab_from_aggregates
from exp_platform.io.analysis_writer import upsert_analysis_results
from exp_platform.memo import init_memo, upsert_stage


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="A/B analysis: outcomes joined to exposures, writes to analysis_results (AB only)."
    )
    p.add_argument("--experiment-id", required=True)
    p.add_argument("--catalog", default="exp")
    p.add_argument("--namespace", default="exp")
    p.add_argument("--output-table", default="analysis_results")
    return p.parse_args()


def _compute_joined_outcome_stats(
    spark,
    catalog: str,
    namespace: str,
    experiment_id: str,
) -> List[Dict[str, Any]]:
    """
    Compute per-metric per-variant aggregates by joining outcomes -> exposures to get variant.
    Returns list of dicts compatible with ab_from_aggregates().
    """
    exposures = (
        spark.table(f"{catalog}.{namespace}.exposures")
        .where(F.col("experiment_id") == experiment_id)
        .select("experiment_id", "user_id", "variant")
        .dropDuplicates(["experiment_id", "user_id"])
    )

    outcomes = (
        spark.table(f"{catalog}.{namespace}.outcomes")
        .where(F.col("experiment_id") == experiment_id)
        .withColumn("outcome_ts", F.col("ts"))
        .select("experiment_id", "user_id", "metric_name", "value", "outcome_ts")
    )

    joined = outcomes.join(exposures, on=["experiment_id", "user_id"], how="inner")

    agg = (
        joined.groupBy("metric_name", "variant")
        .agg(
            F.count(F.lit(1)).alias("n"),
            F.avg("value").alias("mean"),
            F.var_samp("value").alias("var"),
            # FIX: populate stddev (including for binary conversion)
            F.stddev_samp("value").alias("std"),
        )
    )

    rows = [r.asDict(recursive=True) for r in agg.collect()]
    return rows


def _explicit_schema() -> StructType:
    return StructType([
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


def _to_rows_for_table(
    experiment_id: str,
    results: List[Dict[str, Any]],
    ts_dt: datetime,
    std_map: Dict[str, Dict[str, Any]] | None = None,
) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for r in results:
        metric = r["metric_name"]

        ctrl_n = int(r.get("control_n", 0))
        trt_n = int(r.get("treatment_n", 0))
        ctrl_mean = float(r.get("control_mean", 0.0))
        trt_mean = float(r.get("treatment_mean", 0.0))

        delta = float(r.get("absolute_lift", 0.0))
        se = float(r.get("stderr", 0.0))
        ci_low = float(r.get("ci95_low", 0.0))
        ci_high = float(r.get("ci95_high", 0.0))
        rel_lift = float(r.get("relative_lift", 0.0))

        out.append({
            "experiment_id": experiment_id,
            "metric_name": metric,

            "n_control": ctrl_n,
            "mean_control": ctrl_mean,
            "std_control": (
                float(std_map.get(metric, {}).get("control"))
                if std_map and std_map.get(metric, {}).get("control") is not None
                else None
            ),

            "n_treatment": trt_n,
            "mean_treatment": trt_mean,
            "std_treatment": (
                float(std_map.get(metric, {}).get("treatment"))
                if std_map and std_map.get(metric, {}).get("treatment") is not None
                else None
            ),

            "delta": delta,
            "se": se,
            "z": float(r.get("z", 0.0)),
            "p_value": float(r.get("p_value", 1.0)),
            "ci_low": ci_low,
            "ci_high": ci_high,
            "rel_lift": rel_lift,

            "computed_ts": ts_dt,
            "analysis_ts": ts_dt,
        })
    return out


def _fmt(x: Any, digits: int = 6) -> str:
    if x is None:
        return "NA"
    try:
        return f"{float(x):.{digits}g}"
    except Exception:
        return str(x)


def main() -> None:
    args = parse_args()
    spark = build_spark_session(app_name=f"analyze_ab_{args.experiment_id}")

    full_table = f"{args.catalog}.{args.namespace}.{args.output_table}"

    stats_rows = _compute_joined_outcome_stats(spark, args.catalog, args.namespace, args.experiment_id)

    # Map metric -> (std_control, std_treatment) for writing into analysis_results
    std_map: Dict[str, Dict[str, Any]] = {}
    for row in stats_rows:
        m = row.get("metric_name")
        v = row.get("variant")
        if m is None or v is None:
            continue
        std_map.setdefault(m, {})[v] = row.get("std")

    results = ab_from_aggregates(stats_rows)

    ts_dt = datetime.now(timezone.utc)

    print(json.dumps({
        "experiment_id": args.experiment_id,
        "analysis": "ab",
        "generated_at": ts_dt.isoformat(),
        "results": results,
    }, indent=2))

    # ---- Memo stage (AB) ----
    memo_path = init_memo(args.experiment_id, confidence=0.95, overwrite=False)

    md_lines: List[str] = []
    md_lines.append("## Summary Table\n")
    md_lines.append("| metric | control mean | treatment mean | delta | rel lift | CI low | CI high | p-value | method |")
    md_lines.append("|---|---:|---:|---:|---:|---:|---:|---:|---|")

    for r in sorted(results, key=lambda x: x.get("metric_name", "")):
        md_lines.append(
            f"| {r.get('metric_name','')} | "
            f"{_fmt(r.get('control_mean'))} | "
            f"{_fmt(r.get('treatment_mean'))} | "
            f"{_fmt(r.get('absolute_lift'))} | "
            f"{_fmt(r.get('relative_lift'))} | "
            f"{_fmt(r.get('ci95_low'))} | "
            f"{_fmt(r.get('ci95_high'))} | "
            f"{_fmt(r.get('p_value'))} | "
            f"{r.get('method','')} |"
        )

    md_lines.append("\n---\n")
    md_lines.append("## Notes\n")
    md_lines.append("- This section is generated by the **A/B pipeline** (joined outcomes→exposures; per-metric tests).\n")
    md_lines.append("- `pre_revenue` is a randomization balance check; significant differences suggest imbalance.\n")

    upsert_stage(memo_path, "AB", "\n".join(md_lines))
    print(f"[memo] updated {memo_path} stage=AB")

    # ---- Write AB results only (NO CUPED / DID rows) ----
    rows = _to_rows_for_table(args.experiment_id, results, ts_dt, std_map=std_map)
    df_out = spark.createDataFrame(rows, schema=_explicit_schema())

    metric_names = [r["metric_name"] for r in rows]
    upsert_analysis_results(
        spark,
        full_table,
        df_out,
        experiment_id=args.experiment_id,
        metric_names=metric_names,
    )

    spark.stop()
    print(f"[analyze_experiment] wrote AB results to {full_table} for experiment_id={args.experiment_id}")


if __name__ == "__main__":
    main()
