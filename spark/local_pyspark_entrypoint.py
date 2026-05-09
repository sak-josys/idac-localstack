"""Wraps a target PySpark script and rewrites a few SparkSession.builder confs
that upstream scripts hardcode for prod (s3.amazonaws.com endpoint, SSL on,
DefaultAWSCredentialsProviderChain) so the script targets LocalStack instead.
"""

from __future__ import annotations

import os
import runpy
import sys

from pyspark.sql import SparkSession


LOCALSTACK_S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://localstack:4566")


_original_config = SparkSession.Builder.config


def _patched_config(self, key=None, value=None, conf=None, **kwargs):  # type: ignore[no-untyped-def]
    if key == "spark.hadoop.fs.s3a.endpoint" and value == "s3.amazonaws.com":
        value = LOCALSTACK_S3_ENDPOINT
    elif key == "spark.hadoop.fs.s3a.connection.ssl.enabled":
        value = "false"
    elif key == "spark.hadoop.fs.s3a.aws.credentials.provider":
        value = "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
    return _original_config(self, key=key, value=value, conf=conf, **kwargs)


SparkSession.Builder.config = _patched_config  # type: ignore[method-assign]


def main() -> None:
    if len(sys.argv) < 2:
        raise SystemExit("usage: local_pyspark_entrypoint.py <target_script.py> [args...]")

    target = sys.argv[1]
    sys.argv = [target, *sys.argv[2:]]
    runpy.run_path(target, run_name="__main__")


if __name__ == "__main__":
    main()
