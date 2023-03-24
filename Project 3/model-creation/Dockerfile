FROM bde2020/spark-python-template:3.1.2-hadoop3.2

RUN apk add --no-cache py3-numpy

ENV CLASSIFICATION_MODEL='hdfs://namenode:9000/model/RfModel'
ENV SCALER_LOC="hdfs://namenode:9000/model/Scaler"
ENV INDEXER_PIPELINE_LOC="hdfs://namenode:9000/model/IndexerPipeline"


ENV SPARK_APPLICATION_PYTHON_LOCATION /app/model_creation.py
ENV SPARK_APPLICATION_ARGS "hdfs://namenode:9000/user/root/geolife_gps_sorted_without_nodata.csv"
ENV SPARK_SUBMIT_ARGS --executor-memory 4G --executor-cores 4