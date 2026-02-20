from __future__ import annotations

import glob
import socket

from pyspark.sql import SparkSession


def build_spark():
    # Since exp-runner is on host networking, this is safe and matches your existing setup:
    driver_host = "10.0.0.80"

    jar_paths = sorted(glob.glob("/opt/spark-jars/*.jar"))
    jars_csv = ",".join(jar_paths)

    spark = (
        SparkSession.builder
        .appName("exp_platform_smoke")
        .master("spark://10.0.0.80:7077")

        # Driver network settings (keep consistent with your working config)
        .config("spark.driver.host", driver_host)
        .config("spark.driver.bindAddress", "0.0.0.0")

        # Resources (optional—same as you used)
        .config("spark.executor.instances", "2")
        .config("spark.executor.cores", "10")
        .config("spark.executor.memory", "18g")
        .config("spark.executor.memoryOverhead", "4g")
        .config("spark.driver.memory", "10g")
        .config("spark.driver.maxResultSize", "2g")

        # S3A / MinIO (same endpoint as you verified)
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.endpoint", "http://10.0.0.80:9100")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")

        # Iceberg catalog (same as your working config)
        .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.iceberg.type", "hadoop")
        .config("spark.sql.catalog.iceberg.warehouse", "s3a://iceberg-warehouse/warehouse/")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.defaultCatalog", "iceberg")
        .config("spark.sql.catalog.iceberg.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")

        # Iceberg S3 settings (again pointing at MinIO)
        .config("spark.sql.catalog.iceberg.s3.endpoint", "http://10.0.0.80:9100")
        .config("spark.sql.catalog.iceberg.s3.path-style-access", "true")
        .config("spark.sql.catalog.iceberg.s3.access-key-id", "minioadmin")
        .config("spark.sql.catalog.iceberg.s3.secret-access-key", "minioadmin")

        # ---- Region for Iceberg AWS SDK (required) ----
        .config("spark.sql.catalog.iceberg.s3.region", "us-east-1")
        .config("spark.sql.catalog.iceberg.client.region", "us-east-1")
        .config("spark.sql.catalog.iceberg.aws.region", "us-east-1")
    )

    # Ship jars from client to executors (no cluster change)
    if jars_csv:
        spark = (
            spark
            .config("spark.jars", jars_csv)
            .config("spark.driver.extraClassPath", ":".join(jar_paths))
            .config("spark.executor.extraClassPath", ":".join(jar_paths))
        )

    return spark.getOrCreate()


def main():
    exp_id = "exp_checkout_button_001"
    raw_base = f"s3a://raw/exp_platform/{exp_id}"

    spark = build_spark()

    exposures = spark.read.parquet(f"{raw_base}/exposures.parquet")
    outcomes = spark.read.parquet(f"{raw_base}/outcomes.parquet")
    registry = spark.read.parquet(f"{raw_base}/experiment_registry.parquet")

    spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.exp")

    exposures.writeTo("iceberg.exp.exposures").using("iceberg").createOrReplace()
    outcomes.writeTo("iceberg.exp.outcomes").using("iceberg").createOrReplace()
    registry.writeTo("iceberg.exp.experiment_registry").using("iceberg").createOrReplace()

    spark.sql("SELECT variant, COUNT(*) c FROM iceberg.exp.exposures GROUP BY 1").show()
    spark.sql("SELECT metric_name, COUNT(*) c FROM iceberg.exp.outcomes GROUP BY 1").show()

    spark.stop()


if __name__ == "__main__":
    main()
