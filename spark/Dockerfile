# Spark image for the local cluster (master + worker) and emr-emulator's
# spark-submit. Versions match EMR 6.15 so local behavior matches QA/prod.

FROM apache/spark:3.4.1

USER root

ARG HADOOP_AWS_VERSION=3.3.4
ARG AWS_SDK_BUNDLE_VERSION=1.12.262
ARG DELTA_VERSION=2.4.0
ARG SCALA_BINARY=2.12

# Bake S3A + Delta JARs into $SPARK_HOME/jars to skip Maven on first job.
RUN apt-get update \
 && apt-get install -y --no-install-recommends curl ca-certificates \
 && rm -rf /var/lib/apt/lists/* \
 && curl -fsSL -o /opt/spark/jars/hadoop-aws-${HADOOP_AWS_VERSION}.jar \
        https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar \
 && curl -fsSL -o /opt/spark/jars/aws-java-sdk-bundle-${AWS_SDK_BUNDLE_VERSION}.jar \
        https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_BUNDLE_VERSION}/aws-java-sdk-bundle-${AWS_SDK_BUNDLE_VERSION}.jar \
 && curl -fsSL -o /opt/spark/jars/delta-core_${SCALA_BINARY}-${DELTA_VERSION}.jar \
        https://repo1.maven.org/maven2/io/delta/delta-core_${SCALA_BINARY}/${DELTA_VERSION}/delta-core_${SCALA_BINARY}-${DELTA_VERSION}.jar \
 && curl -fsSL -o /opt/spark/jars/delta-storage-${DELTA_VERSION}.jar \
        https://repo1.maven.org/maven2/io/delta/delta-storage/${DELTA_VERSION}/delta-storage-${DELTA_VERSION}.jar

# Pre-create the Ivy cache — upstream always passes --packages even though the
# JAR is on the classpath, and the missing dir crashes the resolver pre-start.
RUN mkdir -p /home/spark/.ivy2/cache /home/spark/.ivy2/jars \
 && chown -R spark:spark /home/spark/.ivy2

COPY requirements.txt /tmp/spark-requirements.txt
RUN python3 -m pip install --no-cache-dir -r /tmp/spark-requirements.txt

RUN apt-get update \
 && apt-get install -y --no-install-recommends unzip \
 && curl -fsSL "https://awscli.amazonaws.com/awscli-exe-linux-$(uname -m).zip" -o /tmp/awscliv2.zip \
 && unzip -q /tmp/awscliv2.zip -d /tmp \
 && /tmp/aws/install \
 && rm -rf /tmp/awscliv2.zip /tmp/aws \
 && apt-get purge -y unzip \
 && apt-get autoremove -y \
 && rm -rf /var/lib/apt/lists/*

USER spark
