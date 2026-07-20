FROM apache/spark:3.5.1

USER root

ARG POSTGRES_JDBC_VERSION=42.7.5
ARG HADOOP_AWS_VERSION=3.3.4
ARG AWS_SDK_BUNDLE_VERSION=1.12.262

RUN pip install --no-cache-dir psycopg2-binary==2.9.10 python-dotenv==1.0.1 && \
    curl -fsSL "https://repo1.maven.org/maven2/org/postgresql/postgresql/${POSTGRES_JDBC_VERSION}/postgresql-${POSTGRES_JDBC_VERSION}.jar" \
      -o "/opt/spark/jars/postgresql-${POSTGRES_JDBC_VERSION}.jar" && \
    curl -fsSL "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar" \
      -o "/opt/spark/jars/hadoop-aws-${HADOOP_AWS_VERSION}.jar" && \
    curl -fsSL "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_BUNDLE_VERSION}/aws-java-sdk-bundle-${AWS_SDK_BUNDLE_VERSION}.jar" \
      -o "/opt/spark/jars/aws-java-sdk-bundle-${AWS_SDK_BUNDLE_VERSION}.jar"

WORKDIR /app
ENTRYPOINT ["/opt/spark/bin/spark-submit", "/app/batch_layer/warehouse_job.py"]
