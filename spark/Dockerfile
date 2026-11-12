FROM apache/spark:4.1.3-python3

USER root

# Kafka source/sink for Structured Streaming and the PostgreSQL JDBC driver.
# Versions match what Spark 4.1.3 was built against.
ARG MAVEN=https://repo1.maven.org/maven2
ADD ${MAVEN}/org/apache/spark/spark-sql-kafka-0-10_2.13/4.1.3/spark-sql-kafka-0-10_2.13-4.1.3.jar \
    ${MAVEN}/org/apache/spark/spark-token-provider-kafka-0-10_2.13/4.1.3/spark-token-provider-kafka-0-10_2.13-4.1.3.jar \
    ${MAVEN}/org/apache/kafka/kafka-clients/3.9.1/kafka-clients-3.9.1.jar \
    ${MAVEN}/org/apache/commons/commons-pool2/2.12.1/commons-pool2-2.12.1.jar \
    ${MAVEN}/org/postgresql/postgresql/42.7.7/postgresql-42.7.7.jar \
    /opt/spark/jars/
RUN chmod 644 /opt/spark/jars/*.jar

RUN pip install --no-cache-dir "psycopg[binary]==3.3.6" "prometheus-client==0.26.0"

WORKDIR /opt/app
COPY streaming ./streaming
COPY lake ./lake
COPY ml ./ml
COPY --chmod=755 run.sh ./run.sh
ENV PYTHONPATH=/opt/app

# Checkpoints live on a volume so the job resumes after a restart
RUN mkdir -p /data/checkpoints && chown -R spark:spark /data/checkpoints
ENV CHECKPOINT_DIR=/data/checkpoints

ENV SPARK_DRIVER_MEMORY=1g

USER spark

# One image, three services: the command picks the job (see docker-compose.yml).
# run.sh execs Python as the main process and Python starts Spark's JVM, so
# `docker stop` reaches the job's own signal handler and it stops cleanly.
ENTRYPOINT ["/opt/app/run.sh"]
CMD ["streaming.candles_job"]
