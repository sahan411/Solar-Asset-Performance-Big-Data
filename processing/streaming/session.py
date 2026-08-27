"""SparkSession construction for the SolarIQ streaming job.

Connector versions are pinned to the Spark distribution rather than guessed.
PySpark 3.5.3 bundles Hadoop 3.3.4 and Scala 2.12 (verified against the jars in
the installed distribution), which fixes all three coordinates:

    org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3
        Scala suffix and version must both match the Spark runtime.
    org.apache.hadoop:hadoop-aws:3.3.4
        Must match hadoop-client-api/runtime exactly. A mismatched hadoop-aws is
        the usual cause of NoSuchMethodError on S3A writes.
    com.amazonaws:aws-java-sdk-bundle:1.12.262
        The SDK version hadoop-aws 3.3.4 was compiled against.

If the Spark version in requirements.txt changes, all three must be revisited.
"""

from __future__ import annotations

import os

from pyspark.sql import SparkSession

from processing.common.config import ObjectStoreSettings
from processing.common.logging import get_logger

log = get_logger("spark-session")

SPARK_VERSION = "3.5.3"
SCALA_BINARY_VERSION = "2.12"
HADOOP_VERSION = "3.3.4"
AWS_SDK_VERSION = "1.12.262"

DEFAULT_PACKAGES = (
    f"org.apache.spark:spark-sql-kafka-0-10_{SCALA_BINARY_VERSION}:{SPARK_VERSION}",
    f"org.apache.hadoop:hadoop-aws:{HADOOP_VERSION}",
    f"com.amazonaws:aws-java-sdk-bundle:{AWS_SDK_VERSION}",
)


def _configure_s3a(spark: SparkSession, object_store: ObjectStoreSettings) -> None:
    """Point Spark's S3A filesystem at MinIO.

    MinIO needs path-style access (bucket in the path, not the hostname) because
    it does not serve virtual-hosted-style buckets, and SSL disabled for the
    local HTTP endpoint.
    """
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()  # noqa: SLF001 - the only supported API
    hadoop_conf.set("fs.s3a.endpoint", object_store.endpoint)
    hadoop_conf.set("fs.s3a.access.key", object_store.access_key)
    hadoop_conf.set("fs.s3a.secret.key", object_store.secret_key)
    hadoop_conf.set("fs.s3a.path.style.access", "true")
    hadoop_conf.set("fs.s3a.connection.ssl.enabled", str(object_store.endpoint.startswith("https")).lower())
    hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    # Credentials come from the config above, not from an EC2 metadata service
    # that does not exist here; without this the provider chain stalls on timeouts.
    hadoop_conf.set(
        "fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
    )


def create_spark_session(
    app_name: str = "solariq-telemetry-stream",
    object_store: ObjectStoreSettings | None = None,
    master: str | None = None,
    extra_config: dict[str, str] | None = None,
) -> SparkSession:
    """Build the SparkSession used by the streaming job.

    `object_store` is optional so unit tests can run without MinIO credentials.
    When omitted, S3A is simply not configured. `extra_config` allows callers
    (notably the test harness) to pin environment-specific Spark settings.
    """
    builder = SparkSession.builder.appName(app_name)

    if master:
        builder = builder.master(master)

    # Container images may pre-bake the jars; an explicit env var lets deployment
    # override the download list (including setting it empty).
    packages = os.getenv("SPARK_JARS_PACKAGES")
    if packages is None:
        packages = ",".join(DEFAULT_PACKAGES)
    if packages:
        builder = builder.config("spark.jars.packages", packages)

    builder = (
        builder
        # Everything in this system is UTC; pinning the session timezone stops
        # Spark from silently reinterpreting event timestamps in the host's zone.
        .config("spark.sql.session.timeZone", "UTC")
        # The demo portfolio is 5 plants; the default 200 shuffle partitions would
        # create hundreds of near-empty tasks per microbatch.
        .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SHUFFLE_PARTITIONS", "4"))
        # Leaves the committed Parquet output free of _SUCCESS marker clutter.
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "false")
    )

    for key, value in (extra_config or {}).items():
        builder = builder.config(key, value)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel(os.getenv("SPARK_LOG_LEVEL", "WARN"))

    if object_store is not None:
        _configure_s3a(spark, object_store)
        log.info(
            "s3a_configured",
            "Configured S3A for the raw telemetry archive",
            endpoint=object_store.endpoint,
            bucket=object_store.raw_bucket,
        )

    log.info(
        "spark_session_started",
        f"SparkSession ready ({spark.version})",
        app_name=app_name,
        spark_version=spark.version,
    )
    return spark
