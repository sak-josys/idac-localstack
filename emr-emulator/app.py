"""Local emr-containers (EMR-on-EKS) emulator.

Implements just enough of the API for `aws emr-containers ...` and Airflow's
EmrContainerOperator to drive a job through PENDING -> SUBMITTED -> RUNNING ->
COMPLETED. State is in-memory only.
"""

from __future__ import annotations

import logging
import os
import shlex
import threading
import time
import uuid
from typing import Any

import docker
from docker.errors import DockerException, NotFound
from fastapi import FastAPI, HTTPException, Request


REGION = "ap-northeast-1"
ACCOUNT = "000000000000"

VIRTUAL_CLUSTERS: dict[str, dict[str, Any]] = {}
JOB_RUNS: dict[str, dict[str, Any]] = {}

# In-flight execs by jobRun id, used by cancel-job-run to interrupt the docker exec.
_ACTIVE_EXECS: dict[str, str] = {}

SPARK_MASTER_URL = os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077")
SPARK_MASTER_CONTAINER = os.getenv("SPARK_MASTER_CONTAINER", "idac-localstack-spark-master-1")
SPARK_SUBMIT_BIN = os.getenv("SPARK_SUBMIT_BIN", "/opt/spark/bin/spark-submit")
S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://localstack:4566")

# Tail length retained on the JobRun's stateDetails (rendered by Airflow).
LOG_TAIL_LINES = 80

logger = logging.getLogger("emr-emulator")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")


# Lazy so uvicorn boots even if /var/run/docker.sock isn't mounted.
_docker_client: docker.DockerClient | None = None


def _docker() -> docker.DockerClient:
    global _docker_client
    if _docker_client is None:
        _docker_client = docker.from_env()
    return _docker_client


app = FastAPI(title="emr-emulator", version="0.1.0")


def _now() -> float:
    return time.time()


def _vc_arn(vc_id: str) -> str:
    return f"arn:aws:emr-containers:{REGION}:{ACCOUNT}:/virtualclusters/{vc_id}"


def _jr_arn(vc_id: str, jr_id: str) -> str:
    return f"arn:aws:emr-containers:{REGION}:{ACCOUNT}:/virtualclusters/{vc_id}/jobruns/{jr_id}"


def _new_id(prefix: str) -> str:
    return f"{prefix}{uuid.uuid4().hex[:13]}"


def _strip_deploy_mode(tokens: list[str]) -> list[str]:
    """Drop ``--deploy-mode <X>`` from caller-supplied sparkSubmitParameters.

    Production sets ``--deploy-mode cluster``; on standalone we want ``client``
    so spark-submit's exit code reflects job result and we can stream driver logs.
    """
    out: list[str] = []
    skip = 0
    for tok in tokens:
        if skip:
            skip -= 1
            continue
        if tok == "--deploy-mode":
            skip = 1
            continue
        out.append(tok)
    return out


def _build_spark_submit_cmd(spark_driver: dict[str, Any]) -> list[str]:
    """Translate sparkSubmitJobDriver into a spark-submit argv.

    Caller --confs come first; LocalStack overrides come last so Spark's
    "last value wins" rule beats the prod-shaped settings (SSL, SSE-KMS, etc.).
    """
    entry_point = spark_driver.get("entryPoint", "")
    user_params = spark_driver.get("sparkSubmitParameters", "") or ""
    user_args = spark_driver.get("entryPointArguments", []) or []

    # EMR-on-EKS contracts use s3://; Hadoop on Spark only knows s3a://.
    if entry_point.startswith("s3://"):
        entry_point = "s3a://" + entry_point[len("s3://") :]

    cmd: list[str] = [
        SPARK_SUBMIT_BIN,
        "--master", SPARK_MASTER_URL,
        "--deploy-mode", "client",
    ]

    cmd.extend(_strip_deploy_mode(shlex.split(user_params)))

    # LocalStack-friendly S3A + right-sized executors. SSE-KMS unset because
    # LocalStack has no KMS key; SSL off because LocalStack speaks plain HTTP.
    cmd.extend([
        "--conf", f"spark.hadoop.fs.s3a.endpoint={S3_ENDPOINT}",
        "--conf", "spark.hadoop.fs.s3a.path.style.access=true",
        "--conf", "spark.hadoop.fs.s3a.connection.ssl.enabled=false",
        "--conf", "spark.hadoop.fs.s3a.access.key=test",
        "--conf", "spark.hadoop.fs.s3a.secret.key=test",
        "--conf", "spark.hadoop.fs.s3a.server-side-encryption-algorithm=",
        "--conf", "spark.hadoop.fs.s3a.bucket-key-enabled=false",
        "--conf", "spark.dynamicAllocation.enabled=false",
        "--conf", "spark.executor.instances=1",
        "--conf", "spark.executor.cores=1",
        "--conf", "spark.executor.memory=1G",
        "--conf", "spark.executor.memoryOverhead=256m",
        "--conf", "spark.driver.memory=1G",
        "--conf", "spark.driver.memoryOverhead=256m",
    ])

    cmd.append(entry_point)
    cmd.extend(str(a) for a in user_args)
    return cmd


def _terminate_exec(jr_id: str) -> None:
    """Best-effort SIGTERM on the in-flight spark-submit (cancel-job-run)."""
    exec_id = _ACTIVE_EXECS.get(jr_id)
    if not exec_id:
        return
    try:
        api = _docker().api
        info = api.exec_inspect(exec_id)
        pid = info.get("Pid")
        if not pid:
            return
        kill_exec = api.exec_create(SPARK_MASTER_CONTAINER, cmd=["kill", str(pid)])
        api.exec_start(kill_exec["Id"], detach=True)
        logger.info("sent SIGTERM to pid %s for job %s", pid, jr_id)
    except Exception:
        logger.exception("failed to terminate exec for job %s", jr_id)


def _drive_job(jr_id: str) -> None:
    job = JOB_RUNS.get(jr_id)
    if job is None:
        return

    spark_driver = job.get("jobDriver", {}).get("sparkSubmitJobDriver") or {}
    if not spark_driver.get("entryPoint"):
        job["state"] = "FAILED"
        job["finishedAt"] = _now()
        job["failureReason"] = "VALIDATION_ERROR"
        job["stateDetails"] = "missing jobDriver.sparkSubmitJobDriver.entryPoint"
        logger.warning("job %s -> FAILED (no entryPoint)", jr_id)
        return

    cmd = _build_spark_submit_cmd(spark_driver)
    job["state"] = "SUBMITTED"
    logger.info("job %s SUBMITTED: %s", jr_id, " ".join(shlex.quote(c) for c in cmd))

    try:
        api = _docker().api
        try:
            api.inspect_container(SPARK_MASTER_CONTAINER)
        except NotFound:
            job["state"] = "FAILED"
            job["finishedAt"] = _now()
            job["failureReason"] = "INTERNAL_ERROR"
            job["stateDetails"] = (
                f"spark-master container '{SPARK_MASTER_CONTAINER}' not running. "
                f"Bring it up with `docker compose up -d spark-master spark-worker`."
            )
            logger.error("job %s -> FAILED: master container missing", jr_id)
            return

        # Demuxed exec so stdout/stderr interleave like `docker logs`.
        exec_inst = api.exec_create(
            SPARK_MASTER_CONTAINER,
            cmd=cmd,
            environment={
                "AWS_ACCESS_KEY_ID": "test",
                "AWS_SECRET_ACCESS_KEY": "test",
                "AWS_DEFAULT_REGION": REGION,
                "AWS_ENDPOINT_URL": S3_ENDPOINT,
            },
            stdout=True,
            stderr=True,
            tty=False,
        )
        exec_id = exec_inst["Id"]
        _ACTIVE_EXECS[jr_id] = exec_id

        job["state"] = "RUNNING"
        chunks: list[str] = []
        try:
            for chunk in api.exec_start(exec_id, stream=True):
                if not chunk:
                    continue
                chunks.append(chunk.decode("utf-8", errors="replace"))
        finally:
            _ACTIVE_EXECS.pop(jr_id, None)

        rc = api.exec_inspect(exec_id).get("ExitCode")
        output = "".join(chunks)
        tail = "\n".join(output.splitlines()[-LOG_TAIL_LINES:])
    except DockerException as exc:
        job["state"] = "FAILED"
        job["finishedAt"] = _now()
        job["failureReason"] = "INTERNAL_ERROR"
        job["stateDetails"] = f"docker SDK error: {exc}"
        logger.exception("job %s -> FAILED via docker SDK", jr_id)
        return
    except Exception as exc:  # pragma: no cover
        job["state"] = "FAILED"
        job["finishedAt"] = _now()
        job["failureReason"] = "INTERNAL_ERROR"
        job["stateDetails"] = str(exc)
        logger.exception("job %s -> FAILED unexpectedly", jr_id)
        return

    job["finishedAt"] = _now()
    if job.get("state") == "CANCEL_PENDING":
        job["state"] = "CANCELLED"
        job["stateDetails"] = tail or "cancelled"
        logger.info("job %s -> CANCELLED", jr_id)
    elif rc == 0:
        job["state"] = "COMPLETED"
        job["stateDetails"] = tail
        logger.info("job %s -> COMPLETED (rc=0)", jr_id)
    else:
        job["state"] = "FAILED"
        job["failureReason"] = f"USER_ERROR: spark-submit exit {rc}"
        job["stateDetails"] = tail
        logger.warning("job %s -> FAILED (rc=%s)", jr_id, rc)


@app.get("/_health")
def health() -> dict[str, str]:
    spark_ok = "unknown"
    try:
        _docker().api.inspect_container(SPARK_MASTER_CONTAINER)
        spark_ok = "true"
    except NotFound:
        spark_ok = "false"
    except DockerException:
        spark_ok = "unreachable"
    return {
        "status": "ok",
        "phase": "3",
        "spark_master_container": SPARK_MASTER_CONTAINER,
        "spark_master_reachable": spark_ok,
    }


@app.post("/virtualclusters")
async def create_virtual_cluster(request: Request) -> dict[str, Any]:
    body = await request.json() if (await request.body()) else {}
    name = body.get("name") or f"vc-{uuid.uuid4().hex[:6]}"
    vc_id = _new_id("vc-")
    vc = {
        "id": vc_id,
        "name": name,
        "arn": _vc_arn(vc_id),
        "state": "RUNNING",
        "containerProvider": body.get("containerProvider", {}),
        "createdAt": _now(),
        "tags": body.get("tags", {}),
    }
    VIRTUAL_CLUSTERS[vc_id] = vc
    logger.info("created virtual cluster %s (%s)", vc_id, name)
    return {"id": vc_id, "name": name, "arn": vc["arn"]}


@app.get("/virtualclusters")
def list_virtual_clusters() -> dict[str, Any]:
    return {"virtualClusters": list(VIRTUAL_CLUSTERS.values())}


@app.get("/virtualclusters/{vc_id}")
def describe_virtual_cluster(vc_id: str) -> dict[str, Any]:
    vc = VIRTUAL_CLUSTERS.get(vc_id)
    if vc is None:
        raise HTTPException(status_code=404, detail=f"VirtualCluster {vc_id} not found")
    return {"virtualCluster": vc}


@app.delete("/virtualclusters/{vc_id}")
def delete_virtual_cluster(vc_id: str) -> dict[str, Any]:
    vc = VIRTUAL_CLUSTERS.pop(vc_id, None)
    if vc is None:
        raise HTTPException(status_code=404, detail=f"VirtualCluster {vc_id} not found")
    return {"id": vc_id}


@app.post("/virtualclusters/{vc_id}/jobruns")
async def start_job_run(vc_id: str, request: Request) -> dict[str, Any]:
    if vc_id not in VIRTUAL_CLUSTERS:
        # Auto-register so a stale virtual_cluster_id from prod conf still works.
        VIRTUAL_CLUSTERS[vc_id] = {
            "id": vc_id,
            "name": f"auto-{vc_id}",
            "arn": _vc_arn(vc_id),
            "state": "RUNNING",
            "containerProvider": {},
            "createdAt": _now(),
            "tags": {},
        }
        logger.info("auto-registered virtual cluster %s on first start-job-run", vc_id)

    body = await request.json()
    jr_id = _new_id("jr-")
    job: dict[str, Any] = {
        "id": jr_id,
        "name": body.get("name", "unnamed"),
        "virtualClusterId": vc_id,
        "arn": _jr_arn(vc_id, jr_id),
        "state": "PENDING",
        "clientToken": body.get("clientToken", ""),
        "executionRoleArn": body.get("executionRoleArn", ""),
        "releaseLabel": body.get("releaseLabel", ""),
        "configurationOverrides": body.get("configurationOverrides", {}),
        "jobDriver": body.get("jobDriver", {}),
        "tags": body.get("tags", {}),
        "createdAt": _now(),
        "createdBy": "local-emr-emulator",
        "finishedAt": None,
        "stateDetails": "",
        "failureReason": None,
    }
    JOB_RUNS[jr_id] = job
    threading.Thread(target=_drive_job, args=(jr_id,), daemon=True).start()

    logger.info("started job %s (vc=%s, name=%s)", jr_id, vc_id, job["name"])
    return {
        "id": jr_id,
        "name": job["name"],
        "arn": job["arn"],
        "virtualClusterId": vc_id,
    }


@app.get("/virtualclusters/{vc_id}/jobruns")
def list_job_runs(vc_id: str) -> dict[str, Any]:
    runs = [j for j in JOB_RUNS.values() if j["virtualClusterId"] == vc_id]
    return {"jobRuns": runs}


@app.get("/virtualclusters/{vc_id}/jobruns/{jr_id}")
def describe_job_run(vc_id: str, jr_id: str) -> dict[str, Any]:
    job = JOB_RUNS.get(jr_id)
    if job is None or job["virtualClusterId"] != vc_id:
        raise HTTPException(status_code=404, detail=f"JobRun {jr_id} not found")
    return {"jobRun": job}


@app.delete("/virtualclusters/{vc_id}/jobruns/{jr_id}")
def cancel_job_run(vc_id: str, jr_id: str) -> dict[str, Any]:
    job = JOB_RUNS.get(jr_id)
    if job is None or job["virtualClusterId"] != vc_id:
        raise HTTPException(status_code=404, detail=f"JobRun {jr_id} not found")
    if job["state"] in ("COMPLETED", "FAILED", "CANCELLED"):
        return {"id": jr_id, "virtualClusterId": vc_id}
    job["state"] = "CANCEL_PENDING"
    # _drive_job sees CANCEL_PENDING when exec_start returns and finalizes state.
    _terminate_exec(jr_id)
    return {"id": jr_id, "virtualClusterId": vc_id}
