# Batch stages and the speed query. Tag: ecommerce/spark-marketplace:4.0.1-r2.
# Spark 4.0.1 matches the jars below; pyspark itself comes from /opt/spark/python.
# The legacy Kaggle image (legacy/kaggle/docker/spark-warehouse, 3.5.1) is separate.
FROM apache/spark:4.0.1

USER root

# Jar versions and where each comes from:
# - spark-sql-kafka / spark-token-provider-kafka 4.0.1: Spark's own modules.
# - kafka-clients 3.9.1, commons-pool2 2.12.0: <kafka.version> and
#   <commons-pool2.version> in spark-parent_2.13-4.0.1.pom.
# - hadoop-aws 3.4.1: <hadoop.version> in the same pom.
# - AWS SDK v2 bundle 2.24.6: the version hadoop-aws 3.4.1 was built against.
# - postgresql 42.7.5: the JDBC driver the end-to-end runs used.
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

# The package without its [spark] extra: pyspark is the image's own. pandas and
# numpy are installed on their own, at the pins of constraints.txt.
COPY pyproject.toml constraints.txt README.md /build/
COPY src /build/src
RUN pip install --no-cache-dir -c /build/constraints.txt /build pandas numpy \
 && rm -rf /build

# The jars are baked in: an empty SPARK_KAFKA_PACKAGE stops the speed service
# from resolving the Kafka connector over the network at start.
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYSPARK_PYTHON=python3 \
    PYSPARK_DRIVER_PYTHON=python3 \
    PYTHONPATH=/app/src:/app:/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.9-src.zip \
    SPARK_KAFKA_PACKAGE=

# spark.driver.memory is set by SparkSessionFactory (SPARK_DRIVER_MEMORY, 3g):
# no JVM flag is patched in by a compose override any more.
# A named volume mounted here starts with this ownership, so the speed
# query's checkpoints are writable by the unprivileged user.
RUN mkdir -p /data/checkpoints && chown -R spark:spark /data

WORKDIR /app
USER spark
