"""Batch driver for registering Glue databases + tables in LocalStack.

Wraps idac-dp-config/ddl/glueDDL.py so all configs run inside a single
SparkSession instead of paying the per-file JVM startup tax (~30s each).
Collects all failures and exits non-zero at the end so docker compose surfaces
a hard failure if any single config breaks.
"""

import argparse
import logging
import os
import sys

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
logger = logging.getLogger("setup-glue.batch")

# dp-config is bind-mounted at /dp-config (see compose).
DP_CONFIG_DDL = os.environ.get("DP_CONFIG_DDL", "/dp-config/ddl")
if DP_CONFIG_DDL not in sys.path:
    sys.path.insert(0, DP_CONFIG_DDL)

import glueDDL  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--config-paths",
        nargs="+",
        required=True,
        help="S3 URIs of resolved Glue config JSONs to register.",
    )
    parser.add_argument(
        "--region",
        default=os.environ.get("AWS_REGION", "ap-northeast-1"),
        help="AWS region for the Glue client (default ap-northeast-1).",
    )
    args = parser.parse_args()

    # boto3 1.29+ honors AWS_ENDPOINT_URL natively, so this routes to LocalStack.
    glueDDL.initialize_glue_client(args.region)

    failures = []
    for idx, path in enumerate(args.config_paths, start=1):
        logger.info("[%d/%d] registering %s", idx, len(args.config_paths), path)
        try:
            glueDDL.main(path)
        except Exception as exc:  # noqa: BLE001
            logger.exception("Failed to register %s", path)
            failures.append((path, str(exc)))

    if failures:
        logger.error("%d/%d configs failed:", len(failures), len(args.config_paths))
        for path, err in failures:
            logger.error("  - %s: %s", path, err)
        return 1

    logger.info(
        "All %d configs registered successfully in LocalStack Glue.",
        len(args.config_paths),
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
