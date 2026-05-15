# Fallback Scala builder for idac-transformation-engine.
# Used by build-or-detect-jar.sh when target/scala-2.12/<module>.jar is missing.
# Pinned to match upstream CI exactly (silver.yaml -> setup-java@v3 + sbt 1.10.0):
#   - JDK 11 Temurin
#   - sbt 1.10.0
#   - Scala 2.12 (resolved transitively by build.sbt)

FROM eclipse-temurin:11-jdk-jammy

ARG SBT_VERSION=1.10.0

ENV DEBIAN_FRONTEND=noninteractive \
    SBT_VERSION=${SBT_VERSION} \
    PATH="/usr/local/sbt/bin:${PATH}" \
    LANG=C.UTF-8 \
    LC_ALL=C.UTF-8

# bash for the entrypoint, ca-certificates for HTTPS to repo.scala-sbt.org/Maven Central,
# curl for the sbt download, gnupg for tarball verification, zip for the publisher
# (jar-to-codeartifact.sh wraps the assembled jar in a zip before publish-package-version).
RUN apt-get update && apt-get install -y --no-install-recommends \
        bash \
        ca-certificates \
        curl \
        gnupg \
        zip \
    && rm -rf /var/lib/apt/lists/*

# sbt is fetched as a versioned tarball rather than installed via apt because the
# GitHub-Actions apt route on silver.yaml is brittle (key + repo) and we want the exact
# 1.10.0 launcher. The launcher then downloads the matching scala/zinc/etc on first run.
RUN curl -fsSL "https://github.com/sbt/sbt/releases/download/v${SBT_VERSION}/sbt-${SBT_VERSION}.tgz" \
        -o /tmp/sbt.tgz \
    && tar -xzf /tmp/sbt.tgz -C /usr/local \
    && rm /tmp/sbt.tgz \
    && /usr/local/sbt/bin/sbt --script-version

# /work is the bind-mount target for the transformation-engine repo.
# /root/.ivy2 and /root/.sbt are the cache mounts the compose service maps to a named volume,
# so a second `assembly` reuses resolved deps instead of re-downloading ~hundreds of MB.
WORKDIR /work
VOLUME ["/root/.ivy2", "/root/.sbt", "/root/.cache/coursier"]

# Sane default: run sbt assembly against whatever is mounted at /work.
# The orchestrator overrides this with `sbt "project <module>" assembly` to scope builds.
ENTRYPOINT ["sbt"]
CMD ["assembly"]
