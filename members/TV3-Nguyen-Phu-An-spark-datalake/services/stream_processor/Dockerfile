# StreamRAG stream processor (owner: TV3). Build context: the TV3 `services/` folder.
#   docker build -f services/stream_processor/Dockerfile -t streamrag-stream-processor:dev services
FROM apache/spark:3.5.9-scala2.12-java17-python3-ubuntu

# Connector JARs, resolved once at build time so the job starts offline. Only the JARs
# Spark does not ship already are copied (it has hadoop-client, lz4, snappy, slf4j).
# Versions must match the image: Spark 3.5.9 / Scala 2.12 / Hadoop 3.3.4 (see README).
ARG SPARK_PACKAGES=org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.9,org.apache.hadoop:hadoop-aws:3.3.4

USER root
RUN echo "" > /tmp/noop.py \
 && /opt/spark/bin/spark-submit --conf spark.jars.ivy=/tmp/ivy --packages "$SPARK_PACKAGES" /tmp/noop.py \
 && cd /tmp/ivy/jars \
 && cp org.apache.spark_spark-sql-kafka-0-10_2.12-*.jar \
       org.apache.spark_spark-token-provider-kafka-0-10_2.12-*.jar \
       org.apache.kafka_kafka-clients-*.jar \
       org.apache.commons_commons-pool2-*.jar \
       org.apache.hadoop_hadoop-aws-*.jar \
       com.amazonaws_aws-java-sdk-bundle-*.jar \
       org.wildfly.openssl_wildfly-openssl-*.jar \
       /opt/spark/jars/ \
 && rm -rf /tmp/ivy /tmp/noop.py

COPY stream_processor /opt/streamrag/stream_processor
ENV PYTHONPATH=/opt/streamrag \
    PYTHONDONTWRITEBYTECODE=1
USER spark
WORKDIR /opt/streamrag

# local[*] is accepted for M1 (single container). Keep the driver small for laptops.
CMD ["/opt/spark/bin/spark-submit", "--master", "local[*]", "--driver-memory", "1g", \
     "/opt/streamrag/stream_processor/job.py"]
