# exp_platform/spark/session.py

from __future__ import annotations

from pathlib import Path
import subprocess

import socket
import glob
from pyspark.sql import SparkSession


def _container_driver_ip() -> str:
    """
    Resolve the container's IP so Spark executors can call back to the driver.
    This avoids hardcoding host IPs.
    """
    return socket.gethostbyname(socket.gethostname())

def _ensure_pyfiles_zip() -> str:
    zip_path = Path("/tmp/exp_platform.zip")
    subprocess.check_call(
        ["python3", "-m", "zipfile", "-c", str(zip_path), "exp_platform"],
        cwd="/workspace",
    )
    return str(zip_path)

def build_spark_session(app_name: str = "exp_platform_client") -> SparkSession:
    pyzip = _ensure_pyfiles_zip()
    driver_ip = _container_driver_ip()

    # ---- JAR SHIPPING (THIS IS THE CHANGE YOU ASKED ABOUT) ----
    jar_paths = sorted(glob.glob("/opt/spark-jars/*.jar"))
    jars_csv = ",".join(jar_paths)

    builder = (
        SparkSession.builder
        .appName(app_name)
        .master("spark://10.0.0.80:7077")

        # ---- driver networking (container-safe) ----
        .config("spark.driver.host", driver_ip)
        .config("spark.driver.bindAddress", "0.0.0.0")

         # ---- ship your project code to executors ----
        .config("spark.submit.pyFiles", pyzip)

        # ---- executor / driver resources (same as your existing setup) ----
        .config("spark.executor.instances", "2")
        .config("spark.executor.cores", "10")
        .config("spark.executor.memory", "18g")
        .config("spark.executor.memoryOverhead", "4g")
        .config("spark.driver.memory", "10g")
        .config("spark.driver.maxResultSize", "2g")

        # ---- AQE + shuffle ----
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.shuffle.partitions", "288")
        .config("spark.sql.files.maxPartitionBytes", "256m")

        # ---- MinIO / S3A (unchanged from your cluster) ----
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.endpoint", "http://10.0.0.80:9100")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )

        # ---- Iceberg catalog (unchanged) ----
        .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.iceberg.type", "hadoop")
        .config("spark.sql.catalog.iceberg.warehouse", "s3a://iceberg-warehouse/warehouse/")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.defaultCatalog", "iceberg")
        .config("spark.sql.catalog.iceberg.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")

        # ---- Iceberg S3 settings ----
        .config("spark.sql.catalog.iceberg.s3.endpoint", "http://10.0.0.80:9100")
        .config("spark.sql.catalog.iceberg.s3.path-style-access", "true")
        .config("spark.sql.catalog.iceberg.s3.access-key-id", "minioadmin")
        .config("spark.sql.catalog.iceberg.s3.secret-access-key", "minioadmin")
        .config("spark.sql.catalog.iceberg.s3.region", "us-east-1")
        .config("spark.sql.catalog.iceberg.client.region", "us-east-1")
        .config("spark.sql.catalog.iceberg.aws.region", "us-east-1")

    )

    # 🚨 THIS is the critical "no cluster change" trick
    if jars_csv:
        builder = (
            builder
            .config("spark.jars", jars_csv)
            .config("spark.driver.extraClassPath", ":".join(jar_paths))
            .config("spark.executor.extraClassPath", ":".join(jar_paths))
        )

    return builder.getOrCreate()
