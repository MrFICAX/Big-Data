#! /bin/bash


docker cp ../data/geolife_gps_sorted.csv namenode-3:/data
docker cp ../data/geolife_gps_sorted_example.csv namenode-3:/data

docker exec -it namenode-3 hdfs dfs -put ./data/geolife_gps_sorted.csv . #geolife_gps_sorted_without_nodata.csv
docker exec -it namenode-3 hdfs dfs -put ./data/geolife_gps_sorted_example.csv . 


# docker exec -it namenode hdfs dfs -test -e /data/train.csv
# if [ $? -eq 1 ]
# then
#   echo "[INFO]: Adding csv file in the /data folder on the HDFS"
#   docker exec -it namenode hdfs dfs -put /data/train.csv /data/train.csv
# fi