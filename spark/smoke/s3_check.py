"""Smoke test: list s3a://idac-airflow-bucket-local/Airflow_configs/ to verify
hadoop-aws + s3a:// + LocalStack auth all work end-to-end."""

from pyspark.sql import SparkSession


def main() -> None:
    spark = SparkSession.builder.appName("idac-localstack-smoke-s3").getOrCreate()

    bucket = "idac-airflow-bucket-local"
    prefix = "Airflow_configs/"
    path = f"s3a://{bucket}/{prefix}"

    sc = spark.sparkContext
    hadoop_conf = sc._jsc.hadoopConfiguration()

    fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(
        sc._jvm.java.net.URI.create(path),
        hadoop_conf,
    )
    statuses = fs.listStatus(sc._jvm.org.apache.hadoop.fs.Path(path))

    total = 0
    for status in statuses:
        size = status.getLen()
        total += size
        print(f"[smoke-s3] {status.getPath().toString()} -> {size} bytes")

    assert len(statuses) > 0, (
        f"no objects under {path}; did publish-airflow run? "
        f"(docker compose run --rm publish-airflow)"
    )
    print(f"[smoke-s3] OK: {len(statuses)} objects, {total} bytes total under {path}")
    spark.stop()


if __name__ == "__main__":
    main()
