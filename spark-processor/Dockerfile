FROM apache/spark:3.5.0

USER root

RUN pip install --no-cache-dir pyspark==3.5.0

COPY processor.py /app/processor.py

WORKDIR /app

CMD ["spark-submit", \
     "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0", \
     "--master", "local[*]", \
     "--conf", "spark.sql.streaming.checkpointLocation=/tmp/checkpoint", \
     "/app/processor.py"]
