"""Smoke test: trivial spark.range(1000).count() to verify master + worker + executor wiring."""

from pyspark.sql import SparkSession


def main() -> None:
    spark = SparkSession.builder.appName("idac-localstack-smoke-hello").getOrCreate()
    n = spark.range(1000).count()
    print(f"[smoke-hello] spark.range(1000).count() = {n}")
    assert n == 1000, f"expected 1000, got {n}"
    spark.stop()


if __name__ == "__main__":
    main()
