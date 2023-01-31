FROM bde2020/spark-python-template:3.1.2-hadoop3.2

ENV SPARK_APPLICATION_PYTHON_LOCATION /app/GeoLifeAnalysisApp.py
ENV SPARK_APPLICATION_ARGS "hdfs://namenode:9000/geolife_gps_trajectories.csv hdfs://namenode:9000/geolife_gps_filtered year lat lon | 2010 2011 39.6 39.8 116.4 116.6"
ENV SPARK_SUBMIT_ARGS --executor-memory 6G --executor-cores 8