# DP config → LocalStack S3

Upload logic lives in **`scripts/publish_airflow_configs_to_s3.sh`** (local env only). It is invoked by **`scripts/run-publish-to-localstack.sh`**.

**Prefer the unified entrypoint** (CI upload mirror for both repos):

```bash
# From idac-localstack/ — with LocalStack reachable at localhost:4566
export AWS_ENDPOINT_URL=http://localhost:4566
bash scripts/publish-airflow-to-localstack.sh
```

Or with Docker Compose (sync services + LocalStack + single **`publish-airflow`** job):

```bash
docker compose up -d --build
```

## Layout after sync

**`publishers/dp-config/airflow/configs/`** mirrors **`idac-dp-config/airflow/configs/`** (see root **`scripts/sync/sync-dp-config.sh`**).

## S3

**`s3://idac-airflow-bucket-local/Airflow_configs/local_*.conf`**
