# Speed query and batch scheduler (Phase 8 plan 7.1, decision D8).
# Spark 4.0.1 matches pyspark==4.0.4 and the image proven by the 2026-10-01/02
# end-to-end runs. The legacy docker/spark-warehouse image (3.5.1) is separate.
FROM apache/spark:4.0.1

USER root

# Jar versions and where each comes from (plan section 5.3):
# - spark-sql-kafka / spark-token-provider-kafka 4.0.1: Spark's own modules.
# - kafka-clients 3.9.1, commons-pool2 2.12.0: <kafka.version> and
#   <commons-pool2.version> in spark-parent_2.13-4.0.1.pom.
# - hadoop-aws 3.4.1: <hadoop.version> in the same pom; the image ships
#   hadoop-client-api-3.4.1.
# - AWS SDK v2 bundle 2.24.6: <aws-java-sdk-v2.version> in
#   hadoop-project-3.4.1.pom, the version hadoop-aws 3.4.1 was built against.
#   (Spark's pom names 2.25.53 for its own modules; S3A was verified on
#   2.24.6 in Phase 8 WP3.)
# - postgresql 42.7.5: <postgresql.version> in the Spark pom, and the JDBC
#   driver the earlier end-to-end runs used.
ARG MAVEN=https://repo1.maven.org/maven2
RUN set -eux; cd /opt/spark/jars; \
    for jar in \
      org/apache/spark/spark-sql-kafka-0-10_2.13/4.0.1/spark-sql-kafka-0-10_2.13-4.0.1.jar \
      org/apache/spark/spark-token-provider-kafka-0-10_2.13/4.0.1/spark-token-provider-kafka-0-10_2.13-4.0.1.jar \
      org/apache/kafka/kafka-clients/3.9.1/kafka-clients-3.9.1.jar \
      org/apache/commons/commons-pool2/2.12.0/commons-pool2-2.12.0.jar \
      org/apache/hadoop/hadoop-aws/3.4.1/hadoop-aws-3.4.1.jar \
      software/amazon/awssdk/bundle/2.24.6/bundle-2.24.6.jar \
      org/postgresql/postgresql/42.7.5/postgresql-42.7.5.jar; \
    do curl -fsSLO "$MAVEN/$jar"; done

COPY requirements-marketplace.txt /tmp/requirements-marketplace.txt
RUN pip install --no-cache-dir -r /tmp/requirements-marketplace.txt \
      pandas==2.3.3 numpy==2.2.6 \
 && rm /tmp/requirements-marketplace.txt

# The jars are baked in: an empty SPARK_KAFKA_PACKAGE stops the speed service
# from resolving the Kafka connector over the network at start.
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYSPARK_PYTHON=python3 \
    PYSPARK_DRIVER_PYTHON=python3 \
    PYTHONPATH=/app:/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.9-src.zip \
    SPARK_KAFKA_PACKAGE=

# A named volume mounted here starts with this ownership, so the speed
# query's checkpoints are writable by the unprivileged user.
RUN mkdir -p /data/checkpoints && chown -R spark:spark /data

WORKDIR /app
USER spark
