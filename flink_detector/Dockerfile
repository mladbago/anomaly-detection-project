# Custom Flink image used by the JobManager, TaskManager AND the job submitter.
#
# The base flink:1.18.1 image is built on eclipse-temurin (Ubuntu Jammy), which
# ships Python 3.10 -- exactly what PyFlink 1.18 supports. We add PyFlink, pull
# the Kafka connector fat-jar straight into /opt/flink/lib (NO local .jar files
# committed to the repo), and bake the project code in so every Flink role can
# import `common` and `flink_detector`.
FROM flink:1.18.1-scala_2.12-java11

# --- System Python + build tooling + curl (for the submitter's readiness probe) ---
RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends \
        python3 python3-pip python3-dev build-essential curl; \
    ln -sf /usr/bin/python3 /usr/bin/python; \
    rm -rf /var/lib/apt/lists/*

# --- PyFlink matching the cluster version ---
RUN pip3 install --no-cache-dir apache-flink==1.18.1

# --- Kafka SQL connector fat-jar, downloaded at build time into Flink's lib ---
ARG KAFKA_CONNECTOR_VERSION=3.2.0-1.18
RUN curl -fSL -o /opt/flink/lib/flink-sql-connector-kafka-${KAFKA_CONNECTOR_VERSION}.jar \
    https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/${KAFKA_CONNECTOR_VERSION}/flink-sql-connector-kafka-${KAFKA_CONNECTOR_VERSION}.jar

# --- Project code, importable everywhere via PYTHONPATH ---
COPY common /opt/project/common
COPY flink_detector /opt/project/flink_detector
ENV PYTHONPATH=/opt/project
