"""
Seed synthetic 'COMPLETED' rows into local_common_db.auditlog.

Why this exists:
The sidsum DAG (and several upstream silver/gold pipelines) gate themselves on
the auditlog table — they fail-fast unless the *previous* run for a given
jobId/jobName/tenantId triple is COMPLETED. On a fresh local stack the table
is the placeholder created by setup-glue (jobId=createAuditLog/...), so any
real DAG aborts before doing useful work. This script inserts one synthetic
COMPLETED row per requested triple so the gate query sees green.

Schema must mirror idac-dp-config/ddl/configs/common/auditlog.json exactly,
otherwise Athena will reject reads against the partition (Hive parquet schema
is column-positional — extras or missing columns surface as cryptic errors
during the silver job, not here).
"""

from __future__ import annotations

import logging
import os
import sys
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    ArrayType,
    IntegerType,
    MapType,
    StringType,
    StructField,
    StructType,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
log = logging.getLogger("seed-auditlog")


# ---------------------------------------------------------------------------
# Schema — keep in lockstep with idac-dp-config/ddl/configs/common/auditlog.json
# (additional_fields + partition_keys, in that order). Don't reorder; Spark's
# parquet writer materializes columns positionally inside each partition.
# ---------------------------------------------------------------------------

_PARTITION_STRUCT = StructType(
    [
        StructField("tableName", StringType(), True),
        StructField("tablePath", StringType(), True),
        StructField("partition", ArrayType(MapType(StringType(), StringType())), True),
    ]
)

AUDITLOG_SCHEMA = StructType(
    [
        StructField("jobRunId", StringType(), True),
        StructField("sessionId", StringType(), True),
        StructField("layer", StringType(), True),
        StructField("dataStartTime", StringType(), True),
        StructField("dataEndTime", StringType(), True),
        StructField("jobStartTime", StringType(), True),
        StructField("jobEndTime", StringType(), True),
        StructField("status", StringType(), True),
        StructField("error", StringType(), True),
        StructField("inputPartitionsProcessed", ArrayType(_PARTITION_STRUCT), True),
        StructField("outputPartitionsProcessed", ArrayType(_PARTITION_STRUCT), True),
        StructField("noOfValidRecords", IntegerType(), True),
        StructField("noOfInvalidRecords", IntegerType(), True),
        StructField("noOfCorruptRecords", IntegerType(), True),
        # Partition columns — listed last so partitionBy() picks them up.
        StructField("jobId", StringType(), True),
        StructField("jobName", StringType(), True),
        StructField("tenantId", StringType(), True),
    ]
)


def _now_iso() -> str:
    # The gate query uses string ORDER BY jobEndTime, so YYYY-MM-DD HH:MM:SS
    # sorts correctly. Matching the exact format upstream writes.
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def _make_row(job_id: str, job_name: str, tenant_id: str) -> tuple:
    ts = _now_iso()
    return (
        f"seed-{job_id}-{tenant_id}-001",  # jobRunId
        "seed-bootstrap",  # sessionId
        "silver",  # layer
        "1970-01-01 00:00:00",  # dataStartTime
        ts,  # dataEndTime
        ts,  # jobStartTime
        ts,  # jobEndTime
        "COMPLETED",  # status
        "",  # error
        [],  # inputPartitionsProcessed
        [],  # outputPartitionsProcessed
        0,  # noOfValidRecords
        0,  # noOfInvalidRecords
        0,  # noOfCorruptRecords
        job_id,
        job_name,
        tenant_id,
    )


def _parse_seeds(raw: str) -> list[tuple[str, str, str]]:
    seeds: list[tuple[str, str, str]] = []
    for entry in raw.split(";"):
        entry = entry.strip()
        if not entry:
            continue
        parts = entry.split(":")
        if len(parts) != 3:
            raise ValueError(
                f"Invalid AUDITLOG_SEEDS entry '{entry}'. "
                "Expected jobId:jobName:tenantId (3 colon-separated fields)."
            )
        seeds.append((parts[0], parts[1], parts[2]))
    return seeds


def main() -> int:
    table_path = os.environ["AUDITLOG_TABLE_PATH"]  # e.g. s3a://idac-data-local/local/common/auditlog/
    seeds_raw = os.environ.get("AUDITLOG_SEEDS_PENDING", "").strip()

    if not seeds_raw:
        log.info("AUDITLOG_SEEDS_PENDING is empty — nothing to write.")
        return 0

    seeds = _parse_seeds(seeds_raw)
    log.info("seeding %d row(s) into %s", len(seeds), table_path)
    for jid, jname, tid in seeds:
        log.info("  - jobId=%s jobName=%s tenantId=%s", jid, jname, tid)

    spark = (
        SparkSession.builder.appName("seed-auditlog")
        # Empty struct/map handling is fragile in older Spark; 3.4 is fine but
        # be explicit about the types via createDataFrame(schema=...) below.
        .getOrCreate()
    )
    try:
        rows = [_make_row(jid, jname, tid) for (jid, jname, tid) in seeds]
        df = spark.createDataFrame(rows, schema=AUDITLOG_SCHEMA)
        log.info("seed dataframe schema:")
        df.printSchema()
        # `mode=append` is intentional — the launcher pre-filters seeds against
        # Glue, so by the time we reach this code we know the partition is new.
        df.write.mode("append").partitionBy(
            "jobId", "jobName", "tenantId"
        ).parquet(table_path)
        log.info("wrote %d seed row(s)", len(seeds))
    finally:
        spark.stop()

    return 0


if __name__ == "__main__":
    sys.exit(main())
