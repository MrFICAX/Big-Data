FROM bde2020/spark-python-template:3.1.2-hadoop3.2

RUN apk add --no-cache py3-numpy
RUN cd /app pip install -r requirements.txt

ENV INFLUXDB_HOST "influxdb"
ENV INFLUXDB_PORT "8086"
ENV INFLUXDB_USERNAME "admin"
ENV INFLUXDB_PASSWORD "admin"
ENV INFLUXDB_TOKEN "n6zAU61C3paPpmfY3dvbQLoVORcqcd0STCCw811VB7MUxEKbppao5CSUg8sMaRzSlWiNI09OH6iHMzAWfU_1yw=="
ENV INFLUXDB_ORG "bigdata_projekat"
ENV INFLUXDB_BUCKET "locations"

ENV CLASSIFICATION_MODEL='hdfs://namenode:9000/model/RfModel'
ENV SCALER_LOC="hdfs://namenode:9000/model/Scaler"
ENV INDEXER_PIPELINE_LOC="hdfs://namenode:9000/model/IndexerPipeline"

ENV KAFKA_HOST=kafka-3:9092
ENV KAFKA_TOPIC=locations
ENV KAFKA_CONSUMER_GROUP=Spark-Group

ENV SPARK_APPLICATION_PYTHON_LOCATION /app/streaming_prediction.py
ENV SPARK_APPLICATION_ARGS "hdfs://namenode:9000/user/root/geolife_gps_sorted_without_nodata.csv"
ENV SPARK_SUBMIT_ARGS --packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.1.2,org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 --executor-memory 4G --executor-cores 4