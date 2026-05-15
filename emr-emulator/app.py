"""Local emr-containers (EMR-on-EKS) emulator.

Implements just enough of the API for `aws emr-containers ...` and Airflow's
EmrContainerOperator to drive a job through PENDING -> SUBMITTED -> RUNNING ->
COMPLETED. State is in-memory; driver logs are persisted both to a host-mounted
volume and to S3 at the EMR-on-EKS-shaped path the operator already specifies in
`monitoringConfiguration.s3MonitoringConfiguration.logUri`.
"""

from __future__ import annotations

import gzip
import json
import logging
import os
import re
import shlex
import threading
import time
import uuid
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import boto3
import docker
from botocore.config import Config as BotoConfig
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
JOB_LOGS_DIR = Path(os.getenv("JOB_LOGS_DIR", "/var/job-logs"))

# Last line of stderr that looks like a Python/Java exception summary; surfaced
# in stateDetails as a one-line "why" so engineers don't open a log to find out.
_ERROR_SUMMARY_RE = re.compile(r"^[\w.$]+(?:Error|Exception|Failure|Failed)(?::.*)?$")

logger = logging.getLogger("emr-emulator")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")


# Lazy so uvicorn boots even if /var/run/docker.sock isn't mounted.
_docker_client: docker.DockerClient | None = None
_s3_client: Any | None = None


def _docker() -> docker.DockerClient:
    global _docker_client
    if _docker_client is None:
        _docker_client = docker.from_env()
    return _docker_client


def _s3() -> Any:
    """Lazy boto3 S3 client targeting LocalStack via path-style addressing."""
    global _s3_client
    if _s3_client is None:
        _s3_client = boto3.client(
            "s3",
            endpoint_url=os.getenv("AWS_ENDPOINT_URL", S3_ENDPOINT),
            region_name=os.getenv("AWS_DEFAULT_REGION", REGION),
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "test"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "test"),
            config=BotoConfig(s3={"addressing_style": "path"}, retries={"max_attempts": 3}),
        )
    return _s3_client


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


# Prod-shaped Hadoop confs that the engine's airflow layer ships in every
# sparkSubmitParameters payload. These describe what the EMR-on-EKS cluster
# does in QA/prod and are simply wrong on LocalStack — they enable SSL,
# SSE-KMS, S3 bucket-key encryption, none of which LocalStack supports and
# all of which break S3A in subtle ways (e.g. SSL=true makes S3A wrap
# requests through HTTPS even when the endpoint is plain http://, which has
# manifested as "request reaches real AWS instead of LocalStack" depending
# on where in the connection setup the failure surfaces). We strip them
# unconditionally and re-set the LocalStack-correct values further down so
# there is exactly one value per key in the final argv — defensive against
# any "first wins" parsing surprises in spark-submit.
_STRIP_USER_CONF_KEYS = (
    "spark.hadoop.fs.s3a.server-side-encryption-algorithm",
    "spark.hadoop.fs.s3a.connection.ssl.enabled",
    "spark.hadoop.fs.s3a.bucket-key-enabled",
)


def _strip_conflicting_confs(tokens: list[str]) -> list[str]:
    """Remove ``--conf <KEY>=<V>`` pairs whose KEY conflicts with our overrides."""
    out: list[str] = []
    i = 0
    while i < len(tokens):
        tok = tokens[i]
        if tok == "--conf" and i + 1 < len(tokens):
            kv = tokens[i + 1]
            key = kv.split("=", 1)[0]
            if key in _STRIP_USER_CONF_KEYS:
                i += 2
                continue
        out.append(tok)
        i += 1
    return out


_S3_SCHEME_PREFIX = "s3://"
_S3A_SCHEME_PREFIX = "s3a://"


def _rewrite_s3_to_s3a(token: str) -> str:
    """Rewrite every ``s3://`` occurrence in a single token to ``s3a://``.

    Real EMR exposes the ``s3://`` scheme via EMRFS; standalone Spark only ships
    the Hadoop ``s3a`` connector, so the bare ``s3`` scheme blows up at filesystem
    resolution time (``UnsupportedFileSystemException: No FileSystem for scheme
    "s3"``) — most visibly when Spark resolves ``--jars s3://...`` glob paths
    before the driver even starts.

    Substring (not prefix) replace is intentional: ``s3://`` shows up not just
    at the start of a token but also embedded — e.g. ``clientConf=s3://...``
    in entryPointArguments, or ``--conf spark.something.path=s3://...``. We
    don't try to be clever about whether a given occurrence is a real URI;
    the literal ``s3://`` substring is unique enough that false positives are
    not a concern in practice.
    """
    if _S3_SCHEME_PREFIX not in token:
        return token
    return token.replace(_S3_SCHEME_PREFIX, _S3A_SCHEME_PREFIX)


def _build_spark_submit_cmd(spark_driver: dict[str, Any]) -> list[str]:
    """Translate sparkSubmitJobDriver into a spark-submit argv.

    Caller --confs come first; LocalStack overrides come last so Spark's
    "last value wins" rule beats the prod-shaped settings (SSL, SSE-KMS, etc.).
    """
    entry_point = spark_driver.get("entryPoint", "")
    user_params = spark_driver.get("sparkSubmitParameters", "") or ""
    user_args = spark_driver.get("entryPointArguments", []) or []

    entry_point = _rewrite_s3_to_s3a(entry_point)

    cmd: list[str] = [
        SPARK_SUBMIT_BIN,
        "--master", SPARK_MASTER_URL,
        "--deploy-mode", "client",
    ]

    user_tokens = _strip_conflicting_confs(_strip_deploy_mode(shlex.split(user_params)))
    cmd.extend(_rewrite_s3_to_s3a(t) for t in user_tokens)

    # LocalStack-friendly S3A + right-sized executors. SSE-KMS unset because
    # LocalStack has no KMS key; SSL off because LocalStack speaks plain HTTP.
    cmd.extend([
        "--conf", f"spark.hadoop.fs.s3a.endpoint={S3_ENDPOINT}",
        "--conf", "spark.hadoop.fs.s3a.endpoint.region=us-east-1",
        "--conf", "spark.hadoop.fs.s3a.path.style.access=true",
        "--conf", "spark.hadoop.fs.s3a.connection.ssl.enabled=false",
        "--conf", "spark.hadoop.fs.s3a.access.key=test",
        "--conf", "spark.hadoop.fs.s3a.secret.key=test",
        "--conf", "spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        "--conf", "spark.hadoop.fs.s3a.server-side-encryption-algorithm=",
        "--conf", "spark.hadoop.fs.s3a.bucket-key-enabled=false",
        "--conf", "spark.hadoop.fs.s3.impl=org.apache.hadoop.fs.s3a.S3AFileSystem",
        "--conf", "spark.hadoop.fs.AbstractFileSystem.s3.impl=org.apache.hadoop.fs.s3a.S3A",
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


def _extract_log_uri(job: dict[str, Any]) -> str | None:
    """Pull s3MonitoringConfiguration.logUri out of the JobRun body Airflow sent us."""
    return (
        job.get("configurationOverrides", {})
        .get("monitoringConfiguration", {})
        .get("s3MonitoringConfiguration", {})
        .get("logUri")
    )


def _build_log_object_uri(log_uri: str, vc_id: str, jr_id: str, filename: str) -> str:
    """Append the EMR-on-EKS-shaped suffix AWS would generate for driver logs."""
    base = log_uri if log_uri.endswith("/") else log_uri + "/"
    return (
        f"{base}{vc_id}/jobs/{jr_id}/containers/spark-{jr_id}/spark-{jr_id}-driver/{filename}"
    )


def _put_gzipped(s3_uri: str, body: bytes) -> None:
    """Gzip and upload to s3://bucket/key. Caller already validated the URI."""
    parsed = urlparse(s3_uri)
    _s3().put_object(
        Bucket=parsed.netloc,
        Key=parsed.path.lstrip("/"),
        Body=gzip.compress(body),
        ContentType="text/plain; charset=utf-8",
        ContentEncoding="gzip",
    )


def _failure_summary(stderr_text: str) -> str:
    """One-line 'why' from stderr (Python/Java exception summary if present)."""
    for raw in reversed(stderr_text.splitlines()):
        line = raw.strip()
        if not line:
            continue
        # Skip log4j-shaped chatter lines like "25/05/08 12:34:56 INFO ...".
        if re.match(r"^\d{2}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}", line):
            continue
        if line.startswith("Caused by:"):
            return line
        if _ERROR_SUMMARY_RE.match(line):
            return line
    return ""


def _persist_logs(
    jr_id: str,
    vc_id: str,
    job_name: str,
    cmd: list[str],
    stdout_bytes: bytes,
    stderr_bytes: bytes,
    rc: int | None,
    started_at: float,
    finished_at: float,
    state: str,
    log_uri: str | None,
) -> dict[str, str | None]:
    """Tee logs to host volume + (if logUri set) gzip+upload to S3.

    Returns a dict with the resolved stdout/stderr URIs (S3 if uploaded, host
    volume path otherwise) plus the local volume paths, for stateDetails.
    """
    job_dir = JOB_LOGS_DIR / jr_id
    job_dir.mkdir(parents=True, exist_ok=True)
    (job_dir / "stdout.log").write_bytes(stdout_bytes)
    (job_dir / "stderr.log").write_bytes(stderr_bytes)
    (job_dir / "command.txt").write_text(
        " ".join(shlex.quote(c) for c in cmd) + "\n", encoding="utf-8"
    )

    stdout_s3: str | None = None
    stderr_s3: str | None = None
    if log_uri:
        try:
            stdout_s3 = _build_log_object_uri(log_uri, vc_id, jr_id, "stdout.gz")
            stderr_s3 = _build_log_object_uri(log_uri, vc_id, jr_id, "stderr.gz")
            _put_gzipped(stdout_s3, stdout_bytes)
            _put_gzipped(stderr_s3, stderr_bytes)
        except Exception:
            logger.exception("job %s: failed to upload logs to %s", jr_id, log_uri)
            stdout_s3 = stderr_s3 = None
    else:
        logger.warning(
            "job %s: monitoringConfiguration.s3MonitoringConfiguration.logUri "
            "not set; skipping S3 upload (host volume copy at %s)",
            jr_id, job_dir,
        )

    status = {
        "id": jr_id,
        "virtualClusterId": vc_id,
        "name": job_name,
        "state": state,
        "exitCode": rc,
        "startedAt": started_at,
        "finishedAt": finished_at,
        "logUri": log_uri,
        "stdoutS3": stdout_s3,
        "stderrS3": stderr_s3,
        "stdoutLocal": str(job_dir / "stdout.log"),
        "stderrLocal": str(job_dir / "stderr.log"),
    }
    (job_dir / "status.json").write_text(json.dumps(status, indent=2), encoding="utf-8")
    return {
        "stdout_s3": stdout_s3,
        "stderr_s3": stderr_s3,
        "stdout_local": str(job_dir / "stdout.log"),
        "stderr_local": str(job_dir / "stderr.log"),
    }


def _state_details(state: str, rc: int | None, paths: dict[str, str | None], summary: str) -> str:
    """Compose the lean stateDetails: cause on failure, log paths always."""
    stdout_loc = paths.get("stdout_s3") or paths.get("stdout_local")
    stderr_loc = paths.get("stderr_s3") or paths.get("stderr_local")
    if state == "COMPLETED":
        return f"stdout: {stdout_loc}"
    if state == "CANCELLED":
        return f"cancelled. stdout: {stdout_loc}"
    cause = summary or "(no exception summary in stderr; see stderr.gz for context)"
    return (
        f"spark-submit exit {rc}: {cause}\n"
        f"stdout: {stdout_loc}\n"
        f"stderr: {stderr_loc}"
    )


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
    started_at = _now()
    job["state"] = "SUBMITTED"
    logger.info("job %s SUBMITTED: %s", jr_id, " ".join(shlex.quote(c) for c in cmd))

    log_uri = _extract_log_uri(job)
    vc_id = job["virtualClusterId"]
    stdout_bytes = b""
    stderr_bytes = b""

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
        stdout_chunks: list[bytes] = []
        stderr_chunks: list[bytes] = []
        try:
            # demux=True yields (stdout_bytes_or_None, stderr_bytes_or_None) tuples
            # so we can persist each stream to its own file (matches AWS layout).
            for stdout_chunk, stderr_chunk in api.exec_start(
                exec_id, stream=True, demux=True
            ):
                if stdout_chunk:
                    stdout_chunks.append(stdout_chunk)
                if stderr_chunk:
                    stderr_chunks.append(stderr_chunk)
        finally:
            _ACTIVE_EXECS.pop(jr_id, None)

        rc = api.exec_inspect(exec_id).get("ExitCode")
        stdout_bytes = b"".join(stdout_chunks)
        stderr_bytes = b"".join(stderr_chunks)
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

    finished_at = _now()
    if job.get("state") == "CANCEL_PENDING":
        final_state = "CANCELLED"
    elif rc == 0:
        final_state = "COMPLETED"
    else:
        final_state = "FAILED"

    paths = _persist_logs(
        jr_id=jr_id,
        vc_id=vc_id,
        job_name=job.get("name", "unnamed"),
        cmd=cmd,
        stdout_bytes=stdout_bytes,
        stderr_bytes=stderr_bytes,
        rc=rc,
        started_at=started_at,
        finished_at=finished_at,
        state=final_state,
        log_uri=log_uri,
    )

    summary = _failure_summary(stderr_bytes.decode("utf-8", errors="replace"))
    job["finishedAt"] = finished_at
    job["state"] = final_state
    job["stateDetails"] = _state_details(final_state, rc, paths, summary)
    if final_state == "FAILED":
        job["failureReason"] = f"USER_ERROR: spark-submit exit {rc}"
        logger.warning("job %s -> FAILED (rc=%s): %s", jr_id, rc, summary or "<no summary>")
    elif final_state == "CANCELLED":
        logger.info("job %s -> CANCELLED", jr_id)
    else:
        logger.info("job %s -> COMPLETED (rc=0)", jr_id)


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
