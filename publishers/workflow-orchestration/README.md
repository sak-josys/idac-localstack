# Workflow orchestration → LocalStack S3

This publisher mirrors **two** upstream artefact sets that ship from
`idac-workflow-orchestration` and uploads each to its own LocalStack bucket
(matching the QA layout 1:1).

| Artefact      | Upload script                       | Compose service          | Bucket / prefix                                                                |
| ------------- | ----------------------------------- | ------------------------ | ------------------------------------------------------------------------------ |
| Airflow DAGs  | `scripts/publish_airflow_orch_to_s3.sh`  | `publish-airflow`        | `s3://idac-airflow-bucket-local/Airflow Orchestration/`                         |
| PySpark jobs  | `scripts/publish_pyscripts_to_s3.sh`     | `publish-spark-scripts`  | `s3://idac-emr-bucket-local/local/migration_transformation/artifacts/scripts/` |

Unified entrypoint: **`docker compose up -d --build`** (or
`../../scripts/publish-airflow-to-localstack.sh` for the airflow path only).

## Layout after sync (flat, same content as upstream repo)

```text
publishers/workflow-orchestration/
├── scripts/
│   ├── publish_airflow_orch_to_s3.sh           # DAGs → idac-airflow-bucket-local
│   ├── run-publish-to-localstack.sh
│   ├── publish_pyscripts_to_s3.sh              # PySpark → idac-emr-bucket-local
│   └── run-publish-pyscripts-to-localstack.sh
├── dags/                 # from idac-workflow-orchestration/dags/        (raw mirror)
├── pyScripts/            # from idac-workflow-orchestration/scripts/{churn_deletion,delta_optimization,vacuum_delta}/
├── requirements.txt
└── startup_script.sh     # from internal_scripts/startup_script.sh
```

The processed DAG runtime folder lives outside this publisher:

```text
airflow/dags/             # processed slice of dags/idac_airflow/ — what MWAA mounts
```

The DAG publish step builds a short-lived **`tmp/josys-workflow-orchestration-0.1/`** only to match the CI script’s paths; nothing long-lived under **`staging/`**.

## Why two DAG folders?

- **`dags/`** is the read-only mirror of upstream — placeholders like `{bucket_suffix}` / `{aws_region}` / `{openlineage_api_key}` are still unsubstituted, and it includes `test/` which we don't want Airflow to load.
- **`airflow/dags/`** is the processed slice MWAA actually consumes (only `idac_airflow/`, placeholders replaced with `local` / `ap-northeast-1`). It is bind-mounted into the LocalStack-spawned Airflow container at `/opt/airflow/dags`.

Pointing MWAA at `dags/` directly would load `test/` files and run DAGs against literal `{bucket_suffix}`-style URLs.

## S3 troubleshooting

If you only see **`Airflow_configs/`** in `idac-airflow-bucket-local`, the workflow-orchestration step failed — check **`docker compose logs publish-airflow`**. Common causes: missing **`publishers/workflow-orchestration/dags/`** after sync, or (fixed in script) **`aws s3 rm`** on an empty **`Airflow Orchestration/`** prefix returning non-zero on LocalStack.

If `idac-emr-bucket-local` is missing, check **`docker compose logs publish-spark-scripts`** — the publisher creates the bucket on demand on first run.
