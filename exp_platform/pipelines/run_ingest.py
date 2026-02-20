# exp_platform/pipelines/run_ingest.py
from __future__ import annotations

import argparse

from exp_platform.config import Config
from exp_platform.spark.session import build_spark_session
from exp_platform.pipelines.ingest_exposures import ingest_exposures
from exp_platform.pipelines.ingest_outcomes import ingest_outcomes
from exp_platform.pipelines.ingest_registry import ingest_registry
from exp_platform.pipelines.ingest_params import ingest_experiment_params

from exp_platform.pipelines.dq_report import dq_report


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Ingest raw experiment parquet into Iceberg (idempotent per experiment_id).")
    p.add_argument("--experiment-id", required=True)

    # Config
    p.add_argument("--config", default="configs/dev.yaml", help="Path to config YAML")

    # Overrides (optional)
    p.add_argument("--raw-bucket", default=None, help="Override cfg.minio.raw_bucket")
    p.add_argument("--catalog", default="iceberg")
    p.add_argument("--namespace", default="exp")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    cfg = Config.load(args.config)

    raw_bucket = args.raw_bucket or cfg.get("minio.raw_bucket", "raw")

    # raw_base is EXPERIMENT-SCOPED and matches generate_synth output layout
    raw_base = f"s3a://{raw_bucket}/exp_platform/{args.experiment_id}"

    spark = build_spark_session(app_name=f"ingest_{args.experiment_id}")

    # Ensure namespace exists
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {args.catalog}.{args.namespace}")

    print("Spark Session Started:")
    print(f"- raw_base: {raw_base}")
    print(f"- target namespace: {args.catalog}.{args.namespace}")

    ingest_exposures(spark, raw_base, args.catalog, args.namespace, args.experiment_id)
    ingest_outcomes(spark, raw_base, args.catalog, args.namespace, args.experiment_id)
    ingest_registry(spark, raw_base, args.catalog, args.namespace, args.experiment_id)
    ingest_experiment_params(
    spark=spark,
    config_path=args.config,
    catalog=args.catalog,
    namespace=args.namespace,
    experiment_id=args.experiment_id,
)


    # Optional DQ report (kept here to preserve your current behavior)
    dq_report(spark, args.experiment_id, args.catalog, args.namespace)

    spark.stop()
    print("✅ ingest complete")


if __name__ == "__main__":
    main()
