# The official Airflow image plus the Kafka client, because the batch layer's
# first task (ingest_from_kafka) subscribes to the Kafka topic.
FROM apache/airflow:2.10.3-python3.11

RUN pip install --no-cache-dir confluent-kafka==2.6.1
