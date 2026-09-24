FROM apache/spark:3.5.3

USER root
RUN python3 -m pip install --no-cache-dir psycopg2-binary==2.9.10

# Kafka connector for Structured Streaming (and its dependencies), downloaded
# at build time so the job starts without fetching anything.
ADD https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.3/spark-sql-kafka-0-10_2.12-3.5.3.jar /opt/spark/jars/
ADD https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.3/spark-token-provider-kafka-0-10_2.12-3.5.3.jar /opt/spark/jars/
ADD https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar /opt/spark/jars/
ADD https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar /opt/spark/jars/
RUN chmod 644 /opt/spark/jars/*.jar

COPY common/ /app/common/
COPY spark/ /app/spark/
RUN mkdir -p /checkpoints && chown spark /checkpoints

USER spark
ENV PYTHONPATH=/app PYTHONUNBUFFERED=1 TZ=UTC
CMD ["/opt/spark/bin/spark-submit", "--master", "local[2]", "/app/spark/stream_job.py"]
