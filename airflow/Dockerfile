FROM amazon/aws-cli:latest

RUN yum install -y bash >/dev/null && yum clean all

WORKDIR /workspace

# Override the base image's "aws" entrypoint so we run the bootstrap script.
ENTRYPOINT ["/bin/sh", "/workspace/airflow/scripts/bootstrap-mwaa.sh"]
