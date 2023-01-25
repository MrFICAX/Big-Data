FROM bde2020/spark-python-template:3.1.2-hadoop3.2

ENV SPARK_APPLICATION_PYTHON_LOCATION /app/GeoLifeAnalysisApp.py
ENV SPARK_APPLICATION_ARGS "hdfs://namenode:9000/geolife_gps_trajectories.csv 39.6 39.8"
ENV SPARK_SUBMIT_ARGS --executor-memory 3G --executor-cores 3