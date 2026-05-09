"""Create a one-row Delta table in LocalStack S3 — a minimal fixture for
local Glue-backed tests."""

from __future__ import annotations

import argparse

from pyspark.sql import SparkSession


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--path", required=True, help="Delta table path, e.g. s3a://bucket/prefix/table")
    parser.add_argument("--s3-endpoint", default="http://localstack:4566")
    args = parser.parse_args()

    spark = (
        SparkSession.builder.appName("idac-localstack-seed-delta-table")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop.fs.s3a.endpoint", args.s3_endpoint)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.access.key", "test")
        .config("spark.hadoop.fs.s3a.secret.key", "test")
        .getOrCreate()
    )

    rows = [("seed-1", "local")]
    df = spark.createDataFrame(rows, ["id", "env"])
    df.write.format("delta").mode("overwrite").save(args.path)

    print(f"[seed-delta-table] wrote Delta table at {args.path}")
    spark.stop()


if __name__ == "__main__":
    main()
