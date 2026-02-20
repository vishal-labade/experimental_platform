from __future__ import annotations

import argparse
from pathlib import Path
from datetime import datetime, timezone

from exp_platform.spark.session import build_spark_session


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Write experiment memo from Iceberg outputs.")
    p.add_argument("--experiment-id", required=True)
    p.add_argument("--out-dir", default="data/memos")
    return p.parse_args()


def _safe_show(df, label: str, limit: int = 50) -> str:
    # Avoid df.toPandas() entirely (timestamp dtype issues + heavy dependency)
    try:
        if df.rdd.isEmpty():
            return f"{label}: NO DATA\n"
    except Exception:
        # If rdd.isEmpty fails due to analysis/optimizer edge cases, fall back to take(1)
        if len(df.take(1)) == 0:
            return f"{label}: NO DATA\n"

    cols = df.columns
    rows = df.limit(limit).collect()

    # Render a simple table-like text block
    lines = [f"{label} (showing up to {limit} rows):"]
    lines.append(" | ".join(cols))
    lines.append("-+-".join(["-" * max(3, len(c)) for c in cols]))

    for r in rows:
        vals = []
        for c in cols:
            v = r[c]
            # Normalize timestamps and None safely
            if v is None:
                vals.append("NULL")
            else:
                vals.append(str(v))
        lines.append(" | ".join(vals))

    return "\n".join(lines) + "\n"



def main() -> None:
    args = parse_args()
    exp_id = args.experiment_id

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{exp_id}.md"

    spark = build_spark_session(f"memo_{exp_id}")

    # Use the tables you already write today.
    # NOTE: your pipelines write CUPED + DiD into iceberg.exp.analysis_results as well.
    analysis = spark.sql(f"""
    SELECT *
    FROM iceberg.exp.analysis_results
    WHERE experiment_id = '{exp_id}'
    ORDER BY analysis_ts DESC, computed_ts DESC
""")


    overall = spark.sql(f"""
        SELECT *
        FROM iceberg.exp.metric_aggregates_overall
        WHERE experiment_id = '{exp_id}'
        ORDER BY metric_name, variant
    """)

    daily = spark.sql(f"""
        SELECT *
        FROM iceberg.exp.metric_aggregates_daily
        WHERE experiment_id = '{exp_id}'
        ORDER BY day, metric_name, variant
    """)

    now = datetime.now(timezone.utc).isoformat()

    # Always write *something* (even if tables are empty)
    lines = []
    lines.append(f"# Decision Memo — {exp_id}")
    lines.append("")
    lines.append(f"> Generated at: `{now}`")
    lines.append("")
    lines.append("## Executive Summary")
    lines.append("- Status: GENERATED")
    lines.append("- Source: Iceberg outputs (`iceberg.exp.*`) scoped by `experiment_id`")
    lines.append("")
    lines.append("## Metric Aggregates (Overall)")
    lines.append("```")
    lines.append(_safe_show(overall, "metric_aggregates_overall").strip())
    lines.append("```")
    lines.append("")
    lines.append("## Analysis Results (AB / CUPED / DiD)")
    lines.append("```")
    lines.append(_safe_show(analysis, "analysis_results").strip())
    lines.append("```")
    lines.append("")
    lines.append("## Metric Aggregates (Daily) — sample")
    lines.append("```")
    lines.append(_safe_show(daily, "metric_aggregates_daily").strip())
    lines.append("```")
    lines.append("")
    lines.append("## Artifacts")
    lines.append(f"- Memo: `{out_path}`")
    lines.append(f"- Iceberg: `iceberg.exp.*` (filter: `experiment_id = '{exp_id}'`)")

    out_path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    spark.stop()
    print(f"[write_memo] wrote memo: {out_path}")


if __name__ == "__main__":
    main()
