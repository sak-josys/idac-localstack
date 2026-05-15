# Transformation engine → LocalStack S3

Publishes Scala transformation JARs from a **locally checked-out** `idac-transformation-engine` working copy through the same shape upstream uses, so a developer can test unpushed changes end-to-end against the LocalStack stack.

## Pipeline

```
idac-transformation-engine/<module>/                      (local repo, possibly dirty)
        │
        │  build-or-detect-jar.sh
        │  - reuse existing target/scala-2.12/<module>.jar if present
        │  - else: sbt assembly inside the builder container
        ▼
target/scala-2.12/<module>.jar
        │
        │  jar-to-codeartifact.sh
        │  - zip into <module>-0.1.zip
        │  - aws codeartifact publish-package-version (LocalStack endpoint)
        ▼
LocalStack CodeArtifact (idac-build / idac-build-ingestion / josys-transformation-engine-<module>)
        │
        │  codeartifact-to-intermediate.sh   (forked from upstream)
        ▼
tmp/<module>/target/scala-2.12/<module>.jar
        │
        │  intermediate-to-s3.sh             (forked from upstream)
        ▼
s3://idac-emr-bucket-local/local/<module>_transformation/artifacts/jars/{<module>.jar, dependencies/*}
        │
        ▼
Airflow DAG → emr-emulator → spark-submit picks up this JAR
```

## Modules

| Module           | Source path in upstream repo               | CodeArtifact package                         | Final S3 prefix                                                    |
|------------------|--------------------------------------------|----------------------------------------------|--------------------------------------------------------------------|
| silver           | `idac-transformation-engine/silver/`           | `josys-transformation-engine-silver`         | `s3://idac-emr-bucket-local/local/silver_transformation/artifacts/jars/`     |
| gold             | `idac-transformation-engine/gold/`             | `josys-transformation-engine-gold`           | `s3://idac-emr-bucket-local/local/gold_transformation/artifacts/jars/`       |
| migration        | `idac-transformation-engine/migration/`        | `josys-transformation-engine-migration`      | `s3://idac-emr-bucket-local/local/migration_transformation/artifacts/jars/`  |
| data_compaction  | `idac-transformation-engine/data_compaction/`  | `josys-transformation-engine-data-compaction`| `s3://idac-emr-bucket-local/local/data_compaction/artifacts/jars/`           |

## Layout (this folder)

```
publishers/transformation-engine/
  README.md                                  ← you are here
  Dockerfile.builder                         ← JDK 11 + sbt image (fallback build path)
  scripts/
    publish-transformation-module.sh         ← top-level entry: build → CA → intermediate → S3
    bootstrap-codeartifact.sh                ← idempotent: create-domain + create-repository on LocalStack
    build-or-detect-jar.sh                   ← hybrid build (use existing JAR or invoke builder)
    jar-to-codeartifact.sh                   ← package + publish-package-version into LocalStack CodeArtifact
    codeartifact-to-intermediate.sh          ← forked upstream: pull from LocalStack CodeArtifact into tmp/
    intermediate-to-s3.sh                    ← forked upstream: upload final layout to LocalStack S3
```

## Commands (developer)

> Filled in once the scripts are implemented. See the parent `idac-localstack/README.md` for the full developer command list.

```bash
# publish a single module
docker compose run --rm publish-transformation silver

# publish everything
docker compose run --rm publish-transformation all
```

## Boundary with QA bootstrap

This publisher **only ever uses the local working copy of `idac-transformation-engine`**. It never touches QA. The QA mirror flow (`scripts/bootstrap/clone-emr-artifacts-from-qa.sh`, gated on `BOOTSTRAP=true`) is a separate path used only for first-time environment seeding.
