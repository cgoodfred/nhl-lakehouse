FROM apache/spark:3.5.7-python3

USER root

ARG ICEBERG_VERSION=1.10.0
ARG HADOOP_AWS_VERSION=3.3.4
ARG AWS_SDK_VERSION=1.12.770

# Curl JARs to Spark's classpath. /opt/spark/jars/ is auto-loaded by spark-submit.
RUN set -eux; \
    curl -fsSL -o /opt/spark/jars/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar \
      https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar; \
    curl -fsSL -o /opt/spark/jars/iceberg-aws-bundle-${ICEBERG_VERSION}.jar \
      https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar; \
    curl -fsSL -o /opt/spark/jars/hadoop-aws-${HADOOP_AWS_VERSION}.jar \
      https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar; \
    curl -fsSL -o /opt/spark/jars/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar \
      https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar

COPY jobs/ /opt/jobs/

# Wrapper entrypoint that materializes credential-bearing Spark configs from
# env vars before exec'ing Spark's real entrypoint. Required because Spark's
# ${env:VAR} substitution doesn't apply to operator-passed --conf args
# (verified directly: SparkConf retained the literal template string).
COPY entrypoint.sh /opt/silver-entrypoint.sh
RUN chmod +x /opt/silver-entrypoint.sh

USER spark

ENTRYPOINT ["/opt/silver-entrypoint.sh"]
