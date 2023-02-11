FROM bde2020/spark-python-template:3.1.2-hadoop3.2

ENV KAFKA_HOST=kafka:9092
ENV KAFKA_TOPIC=locations
ENV KAFKA_CONSUMER_GROUP=Spark-Group
ENV SPARK_APPLICATION_PYTHON_LOCATION /app/consumer_spark.py
ENV SPARK_APPLICATION_ARGS ""
ENV SPARK_SUBMIT_ARGS --packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.1.2,org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 --executor-memory 1G --executor-cores 1