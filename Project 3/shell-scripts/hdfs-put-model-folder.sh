#! /bin/bash


docker cp ../data/model namenode-3:/data

docker exec -it namenode-3 hdfs dfs -put ./data/model . #geolife_gps_sorted_without_nodata.csv

