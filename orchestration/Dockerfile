# Airflow image for the SolarIQ batch layer.
#
# The daily reconciliation reads Parquet from MinIO with Spark, so the scheduler
# and workers need a JVM and PySpark alongside Airflow. Building one image is
# simpler here than running a separate Spark cluster and submitting to it: the
# job processes one compressed simulated day of a five-plant portfolio, which is
# comfortably a single-machine workload.
#
# Java 17 specifically. Spark 3.5 supports Java 8/11/17 and the Debian default
# is newer, which starts and then fails on module-access errors.

FROM apache/airflow:2.10.3-python3.11

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless \
        procps \
        # psql: airflow-init creates Airflow's metadata database before
        # `airflow db migrate` can connect to it.
        postgresql-client \
        # curl: the webserver healthcheck.
        curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Spark resolves the JVM through JAVA_HOME; the path differs by architecture, so
# it is derived rather than hard-coded.
RUN JAVA_HOME_PATH="$(dirname "$(dirname "$(readlink -f "$(command -v java)")")")" \
    && echo "JAVA_HOME=${JAVA_HOME_PATH}" >> /etc/environment \
    && ln -sfn "${JAVA_HOME_PATH}" /opt/java-home
ENV JAVA_HOME=/opt/java-home

USER airflow

# Processing dependencies, pinned identically to the host requirements so the
# container and a developer's virtualenv run the same code.
COPY processing/requirements.txt /tmp/processing-requirements.txt

# Certificate verification stays ON by default. Some developer machines run
# antivirus or corporate software that intercepts TLS, which breaks PyPI's
# certificate chain inside the build; those machines pass their interceptor's CA
# bundle as a BuildKit secret rather than the repository disabling verification
# for everyone:
#
#   docker compose build \
#     --build-arg BUILDKIT_INLINE_CACHE=1 \
#     --secret id=ca_bundle,src="/path/to/ca.pem" airflow-init
#
# The secret is optional and is never written into a layer.
RUN --mount=type=secret,id=ca_bundle,uid=50000 \
    if [ -s /run/secrets/ca_bundle ]; then \
        echo "Using a supplied CA bundle for PyPI (TLS interception detected)"; \
        pip install --no-cache-dir --cert /run/secrets/ca_bundle -r /tmp/processing-requirements.txt; \
    else \
        pip install --no-cache-dir -r /tmp/processing-requirements.txt; \
    fi

# Spark's Kafka and S3A connectors are downloaded at first use by default, which
# fails on a machine without internet during a demo. Resolving them at build time
# puts them in the image's Ivy cache instead.
ENV SPARK_JARS_PACKAGES="org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262"

# `processing` and `storage` are mounted at /opt/airflow/solariq by Compose;
# adding it to PYTHONPATH lets the DAG import them without packaging.
ENV PYTHONPATH=/opt/airflow/solariq
