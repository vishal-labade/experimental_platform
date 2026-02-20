from __future__ import annotations

from exp_platform.config import Config


def build_spark(cfg):
    from pyspark.sql import SparkSession

    master = cfg.get("spark.master", "local[*]")
    app = cfg.get("spark.app_name", "exp-platform-smoke")

    catalog = cfg.get("spark.iceberg_catalog", "demo")
    warehouse = cfg.get("spark.iceberg_warehouse")
    endpoint = cfg.get("spark.minio_endpoint")
    ak = cfg.get("spark.minio_access_key")
    sk = cfg.get("spark.minio_secret_key")
    path_style = str(cfg.get("spark.s3a_path_style_access", True)).lower()

    spark = (
        SparkSession.builder
        .appName(app)
        .master(master)
        # Iceberg catalog
        .config(f"spark.sql.catalog.{catalog}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{catalog}.type", "hadoop")
        .config(f"spark.sql.catalog.{catalog}.warehouse", warehouse)
        # S3A (MinIO)
        .config("spark.hadoop.fs.s3a.endpoint", endpoint)
        .config("spark.hadoop.fs.s3a.access.key", ak)
        .config("spark.hadoop.fs.s3a.secret.key", sk)
        .config("spark.hadoop.fs.s3a.path.style.access", path_style)
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )
    return spark, catalog


def main():
    cfg = Config.load("configs/dev.yaml")
    exp_id = cfg.get("synthetic.experiment_id")
    raw_dir = cfg.get("storage.raw_dir", "data/raw")
    base = f"{raw_dir}/{exp_id}"

    spark, catalog = build_spark(cfg)

    # Create namespace
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.exp")

    exposures = spark.read.parquet(f"{base}/exposures.parquet")
    outcomes = spark.read.parquet(f"{base}/outcomes.parquet")
    registry = spark.read.parquet(f"{base}/experiment_registry.parquet")

    exposures.createOrReplaceTempView("exposures_raw")
    outcomes.createOrReplaceTempView("outcomes_raw")
    registry.createOrReplaceTempView("registry_raw")

    spark.sql(f"""
      CREATE TABLE IF NOT EXISTS {catalog}.exp.exposures
      USING iceberg
      AS SELECT * FROM exposures_raw
    """)

    spark.sql(f"""
      CREATE TABLE IF NOT EXISTS {catalog}.exp.outcomes
      USING iceberg
      AS SELECT * FROM outcomes_raw
    """)

    spark.sql(f"""
      CREATE TABLE IF NOT EXISTS {catalog}.exp.experiment_registry
      USING iceberg
      AS SELECT * FROM registry_raw
    """)

    # sanity
    spark.sql(f"SELECT variant, COUNT(*) c FROM {catalog}.exp.exposures GROUP BY 1").show()
    spark.sql(f"SELECT metric_name, COUNT(*) c FROM {catalog}.exp.outcomes GROUP BY 1").show()

    spark.stop()


if __name__ == "__main__":
    main()