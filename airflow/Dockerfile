FROM amazon/aws-cli:latest

RUN yum install -y bash >/dev/null && yum clean all

WORKDIR /workspace

# amazon/aws-cli image sets ENTRYPOINT to "aws" — override so we run the bootstrap script.
ENTRYPOINT ["/bin/sh", "/workspace/airflow/scripts/bootstrap-mwaa.sh"]
